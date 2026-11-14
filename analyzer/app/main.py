"""只读 SQL 分析页 HTTP 服务。

设计要点：
- 每条查询独占一条只读 SQLite 连接，在有界线程池中运行；分析库与采集库
  物理上是同一个文件（只读挂载），耗时查询跑在独立线程/连接上，不会阻塞
  采集端的实时写入、状态查看或其它查询。
- 查询状态机：
    running ──成功──▶ succeeded
       │            ──超时──▶ timeout
       │            ──出错──▶ failed
       └──取消──▶ canceling ──运算停止──▶ canceled
  终态只有 succeeded/failed/canceled/timeout；终态迁移在锁内做 CAS，
  完成与取消竞争只产生一个终态，迟到的取消/完成一律无操作，且取消只
  interrupt 本查询自己的连接，绝不影响其它查询或采集。
"""
from __future__ import annotations

import asyncio
import os
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel

from . import engine
from .engine import (
    DEADLINE_SECONDS, MAX_ROWS, DatabaseUnavailable, QueryCanceled,
    QueryRejected, QueryTimeout, precheck, run_query,
)

MAX_CONCURRENCY = int(os.environ.get("MAX_CONCURRENCY", "8"))
HISTORY_LIMIT = 200
STATIC_DIR = Path(__file__).parent / "static"

TERMINAL_STATES = frozenset({"succeeded", "failed", "canceled", "timeout"})

app = FastAPI(title="码头过磅数据 · 只读 SQL 分析")


class QueryIn(BaseModel):
    sql: str


class QueryRecord:
    def __init__(self, qid: str, sql: str):
        self.id = qid
        self.sql = sql
        self.state = "running"          # running / canceling / 四个终态
        self.error: str | None = None
        self.columns: list[str] = []
        self.rows: list[tuple] = []
        self.truncated = False
        self.elapsed_ms: float | None = None
        self.created_at = time.time()
        self.cancel_event = threading.Event()
        # 由引擎在打开连接后回填，取消时直接 interrupt 本查询的连接
        self.conn_holder: dict = {}
        self.lock = threading.Lock()
        # 并发槽只释放一次（与查询状态机解耦，避免终态判断误伤计数）
        self.released = False


class Registry:
    def __init__(self):
        self.lock = threading.Lock()
        self.items: dict[str, QueryRecord] = {}
        self.active = 0

    def create(self, sql: str) -> QueryRecord:
        with self.lock:
            if self.active >= MAX_CONCURRENCY:
                raise HTTPException(
                    429, f"并发查询已达上限 {MAX_CONCURRENCY}，请稍后重试或取消其它查询")
            rec = QueryRecord(uuid.uuid4().hex[:12], sql)
            self.items[rec.id] = rec
            self.active += 1
            # 仅清理终态历史，运行中的查询绝不动
            terminal = sorted(
                (r for r in self.items.values()
                 if r.state in TERMINAL_STATES),
                key=lambda r: r.created_at)
            for old in terminal[:-HISTORY_LIMIT]:
                self.items.pop(old.id, None)
            return rec

    def get(self, qid: str) -> QueryRecord | None:
        with self.lock:
            return self.items.get(qid)

    def list_meta(self) -> list[dict]:
        with self.lock:
            recs = sorted(self.items.values(),
                          key=lambda r: r.created_at, reverse=True)
            return [{
                "id": r.id, "sql": r.sql, "state": r.state,
                "error": r.error, "elapsed_ms": r.elapsed_ms,
                "truncated": r.truncated,
                "row_count": len(r.rows),
                "created_at": r.created_at,
            } for r in recs]

    def finalize(self, rec: QueryRecord) -> None:
        # 并发槽恰好释放一次；迟到的完成回调不影响计数，也不触碰终态
        with self.lock:
            if rec.released:
                return
            rec.released = True
            self.active -= 1


registry = Registry()
pool = ThreadPoolExecutor(max_workers=MAX_CONCURRENCY,
                          thread_name_prefix="sql-query")


def _worker(rec: QueryRecord) -> None:
    """线程池任务：执行查询并做唯一终态迁移。"""
    def settle(normal: str, error: str | None, result: dict | None = None) -> None:
        # 终态唯一入口（持 rec.lock 做 CAS）：取消一旦置位，终态恒为 canceled，
        # 与超时/完成的竞争只产生一个终态；迟到的 settle 不再覆盖既有终态。
        with rec.lock:
            if rec.state in TERMINAL_STATES:
                return
            if rec.cancel_event.is_set():
                rec.state, rec.error = "canceled", "查询已取消"
                return
            rec.state = normal
            rec.error = error
            if result is not None:
                rec.columns = result["columns"]
                rec.rows = result["rows"]
                rec.truncated = result["truncated"]
                rec.elapsed_ms = result["elapsed_ms"]

    try:
        result = run_query(rec.sql, rec.cancel_event, rec.conn_holder)
        settle("succeeded", None, result)
    except QueryCanceled as exc:
        settle("canceled", str(exc))
    except QueryTimeout as exc:
        settle("timeout", str(exc))
    except QueryRejected as exc:
        settle("failed", str(exc))
    except DatabaseUnavailable as exc:
        settle("failed", str(exc))
    except Exception as exc:  # 引擎层未预期错误：留痕但不泄露部分结果
        settle("failed", f"{type(exc).__name__}: {exc}")
    finally:
        registry.finalize(rec)


@app.post("/api/queries", status_code=202)
async def submit_query(body: QueryIn):
    sql = body.sql.strip().rstrip(";").strip()
    # 1) 提交前预校验（EXPLAIN + authorizer，不执行、不读数据）
    loop = asyncio.get_running_loop()
    try:
        await loop.run_in_executor(None, precheck, sql)
    except DatabaseUnavailable as exc:
        raise HTTPException(503, str(exc)) from exc
    except QueryRejected as exc:
        raise HTTPException(400, str(exc)) from exc
    # 2) 并发闸门（满了快速 429，不排队堆积）
    rec = registry.create(sql)
    # 3) 投递到有界线程池，立即返回查询号
    pool.submit(_worker, rec)
    return {"id": rec.id, "state": rec.state,
            "deadline_seconds": DEADLINE_SECONDS, "max_rows": MAX_ROWS}


@app.post("/api/queries/{qid}/cancel")
async def cancel_query(qid: str):
    rec = registry.get(qid)
    if rec is None:
        raise HTTPException(404, "查询不存在或历史已清理")
    with rec.lock:
        if rec.state in TERMINAL_STATES:
            # 幂等：迟到取消不覆盖终态
            return {"id": qid, "state": rec.state, "note": "查询已结束，取消无效"}
        rec.state = "canceling"
        rec.cancel_event.set()
    # 先让底层运算停下来（事件 + 直接中断本查询自己的连接）
    conn = rec.conn_holder.get("conn")
    if conn is not None:
        try:
            conn.interrupt()
        except Exception:
            pass
    return {"id": qid, "state": "canceling"}


@app.get("/api/queries")
async def list_queries():
    return registry.list_meta()


@app.get("/api/queries/{qid}")
async def get_query(qid: str):
    rec = registry.get(qid)
    if rec is None:
        raise HTTPException(404, "查询不存在或历史已清理")
    with rec.lock:
        return {
            "id": rec.id, "sql": rec.sql, "state": rec.state,
            "error": rec.error, "columns": rec.columns,
            "rows": rec.rows, "truncated": rec.truncated,
            "elapsed_ms": rec.elapsed_ms,
            "deadline_seconds": DEADLINE_SECONDS, "max_rows": MAX_ROWS,
        }


@app.get("/api/schema")
async def schema():
    """允许访问的两张表及其列名（只读自查，SQL 由本服务固定）。"""
    try:
        return await asyncio.get_running_loop().run_in_executor(
            None, engine.describe_schema)
    except Exception as exc:
        raise HTTPException(503, f"采集库暂不可用：{exc}") from exc


@app.get("/", include_in_schema=False)
async def index():
    return FileResponse(STATIC_DIR / "index.html")
