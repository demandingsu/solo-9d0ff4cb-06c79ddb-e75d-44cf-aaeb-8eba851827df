"""只读 SQL 分析引擎（全部同步调用，运行在线程池里）。

安全与资源约束：
1. 只允许读取 jobs、tickets 两张表；任何写入、改表、ATTACH、PRAGMA、
   访问其它库/表、危险函数（load_extension 等）一律拒绝（双层防护：
   提交前 EXPLAIN 预校验 + 工作连接上的 authorizer + 只读 URI 连接）。
2. 一次查询的整个生命周期（执行 + 取数）处于同一个 BEGIN 事务中，
   SQLite 事务读一个固定快照，查询期间采集端的写入不影响结果。
3. 执行与取数合计不得超过 deadline（默认 2s）；progress_handler 与
   分批 fetch 共同计时，超时或取消时先 interrupt 停止底层运算，
   连接立即关闭释放资源，不返回任何部分结果。
"""
from __future__ import annotations

import os
import sqlite3
import threading
import time

DB_PATH = os.environ.get("DB_PATH", "/data/collector.db")
DEADLINE_SECONDS = float(os.environ.get("QUERY_TIMEOUT", "2"))
MAX_ROWS = int(os.environ.get("MAX_ROWS", "500"))
FETCH_BATCH = 100

# 仅这两张表允许被读取
ALLOWED_TABLES = frozenset({"jobs", "tickets"})
# 即使 authorizer 放行 FUNCTION，也显式封禁可能触达文件系统/库外的函数
DENIED_FUNCTIONS = frozenset({
    "load_extension", "readfile", "writefile",
    "sqlite_dbpage", "pragma_database_list",
})


class DatabaseUnavailable(Exception):
    """采集库暂不可用（采集端未运行 / 卷未就绪 / 只读挂载缺 WAL 共享文件）。"""


class QueryRejected(Exception):
    """SQL 未通过安全/语法预校验（用户可修正后重试）。"""


class QueryCanceled(Exception):
    """用户取消：运算已停止，无部分结果。"""


class QueryTimeout(Exception):
    """超过 2 秒预算：运算已停止，无部分结果。"""


def _make_authorizer():
    """返回 SQLite authorizer 回调。白名单之外全部 DENY。"""

    def authorizer(kind: int, arg1, arg2, db_name, trigger_name):
        # 读取列：只允许 jobs / tickets。
        # 递归 CTE 的自引用会产生 SQLITE_READ(表=CTE名, 列='')：列名为空串
        # 表示虚拟机内部/递归临时表，不触达任何真实表，可放行；任何真实表
        # （含 sqlite_master、附加库表）的读取都带具体列名，照样按表白名单拦截。
        if kind == sqlite3.SQLITE_READ:
            if arg1 in ALLOWED_TABLES or arg2 == "":
                return sqlite3.SQLITE_OK
            return sqlite3.SQLITE_DENY
        # 普通 SELECT 与递归 CTE（递归 CTE 单独有一个事件类型）
        if kind in (sqlite3.SQLITE_SELECT, sqlite3.SQLITE_RECURSIVE):
            return sqlite3.SQLITE_OK
        # 内部固定快照事务的 BEGIN/ROLLBACK/COMMIT（只读连接上无法写）
        if kind == sqlite3.SQLITE_TRANSACTION:
            return sqlite3.SQLITE_OK
        # 函数：封禁危险函数，其余放行
        if kind == sqlite3.SQLITE_FUNCTION:
            if arg2 in DENIED_FUNCTIONS:
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK
        # 其余一切（INSERT/UPDATE/DELETE/CREATE/DROP/ALTER/ATTACH/
        # PRAGMA/SAVEPOINT/INDEX/VIEW/TRIGGER ...）拒绝
        return sqlite3.SQLITE_DENY

    return authorizer


def _open_ro() -> sqlite3.Connection:
    """以只读 URI 打开数据库；查询全程不可写。

    只读卷挂载下，WAL 库的 -wal/-shm 由采集端维护；若采集端尚未运行
    （文件不存在或无法建立只读视图），抛出 DatabaseUnavailable 让上层
    返回友好提示，而不是崩溃。
    """
    uri = f"file:{DB_PATH}?mode=ro"
    try:
        conn = sqlite3.connect(uri, uri=True, isolation_level=None,
                               timeout=DEADLINE_SECONDS)
    except sqlite3.OperationalError as exc:
        raise DatabaseUnavailable(
            f"采集库暂不可用（{DB_PATH}）：{exc}。请确认采集端正在运行后重试。") from exc
    try:
        # 必须在安装 authorizer 之前设置（PRAGMA 本身会被 authorizer 拒绝）
        conn.execute(f"PRAGMA busy_timeout={int(DEADLINE_SECONDS * 1000)}")
        conn.set_authorizer(_make_authorizer())
        # 触发一次真实读取，尽早暴露只读挂载缺 -shm 等打开后错误
        conn.execute("SELECT 1 FROM jobs LIMIT 1").fetchall()
    except sqlite3.OperationalError as exc:
        conn.close()
        raise DatabaseUnavailable(
            f"采集库暂不可用（只读视图无法建立）：{exc}。"
            "WAL 共享文件需采集端运行时维护，请稍后重试。") from exc
    return conn


def precheck(sql: str) -> None:
    """提交前同步预校验：单语句、语法正确、且通过 authorizer。

    用 EXPLAIN 触发编译期授权回调，但不真正执行查询，因此快速且不读数据。
    """
    if not sql or not sql.strip():
        raise QueryRejected("SQL 为空")
    if len(sql) > 10_000:
        raise QueryRejected("SQL 过长（上限 10000 字符）")
    conn: sqlite3.Connection | None = None
    try:
        conn = _open_ro()
        try:
            # pysqlite 不允许一次 execute 多条语句，多语句会抛 ProgrammingError
            conn.execute("EXPLAIN " + sql).fetchall()
        except sqlite3.DatabaseError as exc:
            # "not authorized" / "prohibited" 等授权错误
            raise QueryRejected(f"拒绝执行（仅允许只读查询 jobs、tickets）：{exc}") from exc
        except sqlite3.OperationalError as exc:
            raise QueryRejected(f"SQL 错误：{exc}") from exc
        except sqlite3.ProgrammingError as exc:
            raise QueryRejected(f"只允许单条 SELECT 语句：{exc}") from exc
    except QueryRejected:
        raise
    except DatabaseUnavailable:
        raise
    except sqlite3.OperationalError as exc:
        raise QueryRejected(f"无法打开采集库：{exc}") from exc
    finally:
        if conn is not None:
            conn.close()


def describe_schema() -> list[dict]:
    """可信内部调用：返回 jobs、tickets 的列名/类型（SQL 固定，无用户输入）。

    单独开一条只读连接、不安装面向用户的 authorizer，以便执行
    PRAGMA table_info；该函数不接受任何用户 SQL。
    """
    uri = f"file:{DB_PATH}?mode=ro"
    try:
        conn = sqlite3.connect(uri, uri=True, isolation_level=None)
        conn.execute("SELECT 1 FROM jobs LIMIT 1").fetchall()
    except sqlite3.OperationalError as exc:
        raise DatabaseUnavailable(
            f"采集库暂不可用：{exc}。请确认采集端正在运行。") from exc
    try:
        out = []
        for table in sorted(ALLOWED_TABLES):
            cols = conn.execute(f"PRAGMA table_info({table})").fetchall()
            out.append({"table": table,
                        "columns": [{"name": c[1], "type": c[2]} for c in cols]})
        return out
    finally:
        conn.close()


def run_query(sql: str, cancel_event: threading.Event,
              conn_holder: dict | None = None) -> dict:
    """在工作线程中执行查询，返回结构化结果。

    返回：{columns, rows, truncated, elapsed_ms}
    异常：QueryRejected / QueryCanceled / QueryTimeout / 其它 sqlite 错误
    conn_holder：可选字典，打开连接后回填 conn，供取消方直接 interrupt。
    """
    conn: sqlite3.Connection | None = None
    started = time.monotonic()
    deadline = started + DEADLINE_SECONDS

    def progress():
        # SQLite 每执行若干条虚拟机指令回调一次
        if cancel_event.is_set():
            return 1  # 中止运算
        if time.monotonic() >= deadline:
            return 1
        return 0

    try:
        conn = _open_ro()
        if conn_holder is not None:
            conn_holder["conn"] = conn
        conn.set_progress_handler(progress, 1000)
        # 整个执行+取数共用同一快照（只读连接上的 BEGIN 即固定读事务）
        conn.execute("BEGIN")
        cur = conn.execute(sql)
        columns = [d[0] for d in cur.description] if cur.description else []

        rows: list[tuple] = []
        truncated = False
        while True:
            # 取数同样受 2s 预算与取消控制（分批 fetch 之间检查）
            if cancel_event.is_set():
                raise QueryCanceled("查询已取消")
            if time.monotonic() >= deadline:
                raise QueryTimeout(f"执行与取数超过 {DEADLINE_SECONDS:g} 秒")
            batch = cur.fetchmany(FETCH_BATCH)
            if not batch:
                break
            remaining = MAX_ROWS - len(rows)
            if len(batch) >= remaining:
                rows.extend(batch[:remaining])
                truncated = True
                break
            rows.extend(batch)

        # 完成与取消竞争：取数结束后若取消已到达，放弃结果，终态只能是取消
        if cancel_event.is_set():
            raise QueryCanceled("查询已取消")
        elapsed_ms = round((time.monotonic() - started) * 1000, 1)
        return {"columns": columns, "rows": rows,
                "truncated": truncated, "elapsed_ms": elapsed_ms}
    except sqlite3.OperationalError as exc:
        msg = str(exc).lower()
        if cancel_event.is_set() or "interrupt" in msg:
            # interrupt 既可能来自取消也可能来自超时，按事件标志区分
            if cancel_event.is_set():
                raise QueryCanceled("查询已取消") from exc
            raise QueryTimeout(
                f"执行与取数超过 {DEADLINE_SECONDS:g} 秒，已停止运算") from exc
        raise
    finally:
        # 先确保底层运算停止（progress 已可能中断），再关闭连接释放资源
        if conn is not None:
            try:
                conn.set_progress_handler(None, 0)
                conn.interrupt()
                conn.rollback()
            except sqlite3.Error:
                pass
            conn.close()
