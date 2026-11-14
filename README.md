# 地磅单采集 → 结算 → 只读 SQL 分析

码头数据员把地磅单持续接入结算系统：采集端（FastAPI + SQLite）订阅门户的 SSE 推送，
并按固定快照分页补采历史；同单仅保留最高修订号，订阅正常且历史取齐后才开放有效单 CSV 下载。

**只读 SQL 分析页（analyzer，8002）** 面向码头数据分析员，按车牌核对重复过磅与净重分布、
定位待复核数据。它与采集端共用同一个 SQLite 文件，但以**只读卷挂载 + 只读连接 + SQL
白名单授权**三重方式保证只能读 `jobs`、`tickets`，不能写入、改表或访问任何其它数据库。

## 快速启动

```bash
docker compose up --build
```

| 服务 | 地址 | 说明 |
|---|---|---|
| 采集端 collector | http://localhost:8001 | 发起采集、查看同步状态、下载 CSV |
| 门户模拟器 portal | http://localhost:8000 | 控制面板：补称 / 作废 / 迟到旧版 / 断流 / 历史延迟 |
| **只读分析 analyzer** | **http://localhost:8002** | **提交/取消只读 SQL，查看列名与结果** |

浏览器打开 **http://localhost:8002**，在编辑器写 SQL（或点示例），「提交查询」后获得查询号，
运行中可按查询号「取消」，结果区显示列名、数据行、截断提示与耗时。

> 分析端以 `collector-data:/data:ro` 只读挂载采集库，文件系统层面即无法写入。
> SQLite 处于 WAL 模式，`-wal/-shm` 由采集端维护；请先让采集端跑起来（建库并保持连接），
> 分析端即可读到含实时写入的最新数据。采集端未运行时分析端返回「采集库暂不可用」友好提示。

## 分析页的硬约束（逐条对应需求）

1. **只允许读 `jobs`、`tickets`**。每条用户 SQL 在执行前先经 `EXPLAIN` 编译期授权预校验，
   工作连接再装一道 SQLite authorizer，且连接本身是 `mode=ro` 只读 URI。
   `INSERT/UPDATE/DELETE/REPLACE`、`CREATE/DROP/ALTER`、`ATTACH`、`PRAGMA`、
   `SAVEPOINT`、访问 `sqlite_master`/其它表/附加库、`load_extension/readfile/writefile`
   等危险函数，以及一次多条语句，全部在执行前被拒（HTTP 400），绝不落库。
2. **显示列名与结果**。成功响应带 `columns`（列名）与 `rows`，前端表格直接渲染。
3. **同一快照**。每条查询在自己的只读连接里 `BEGIN` 一个读事务，执行与取数全程读同一快照；
   查询期间采集端的持续写入不影响本次结果（已验证：外部连接计数增长而事务内计数恒定）。
4. **执行 + 取数合计 ≤ 2 秒**。`progress_handler` 在执行期按时间预算中断，取数分批
   `fetchmany` 并在批次间复核时间；超时先 `interrupt()` 停止底层运算、关闭连接释放资源，
   状态置 `timeout`，**不返回任何部分结果**。
5. **最多 500 行**。超出只保留前 500 行并置 `truncated=true`，前端显著注明「结果已截断」。
6. **耗时查询不阻塞实时采集 / 状态查看 / 另一条查询**。每条查询独占一条只读连接，跑在
   有界线程池（`MAX_CONCURRENCY`，默认 8）；分析端与采集端是不同进程/连接，WAL 下读写不互斥
   （已压测：4 条重型查询跑满时，采集端写入与状态查询 p50≈1ms、最大 < 50ms）。
7. **按查询号取消**。`POST /api/queries/{id}/cancel` 置取消事件并 `interrupt()` 本查询自己的
   连接（绝不触碰其它连接）；运算停止、资源释放后状态置 `canceled`，无部分结果。
8. **终态唯一**。状态机 `running/canceling → succeeded/failed/canceled/timeout`，终态迁移在
   锁内经单一入口 CAS：完成与取消竞争只产生一个终态，取消一旦置位恒为 `canceled`；
   迟到的取消/完成一律无操作，不覆盖终态，也不影响其它查询或采集（已用 60 组并发竞争验证）。

### 内置示例（页面一键填入）

- **重复过磅（待复核）**：按车牌 + 业务日期分组，`HAVING COUNT(*)>1` 找出同车牌多单，
  并列出关联单号、最小/最大/平均净重。
- **净重分布**：按净重区间（<15t、15–25t、25–35t、35–45t、≥45t）统计单数与平均净重。
- **跨表核对**：`jobs LEFT JOIN tickets` 汇总每个采集任务的有效/作废/总单数。
- **耗时查询**：递归 CTE 生成 300 万行并聚合，稳定触发 2 秒超时，演示「先停运算、无部分结果」。
- **写入应被拒绝** / **访问其它库应被拒绝**：演示安全拦截（预校验即 400）。

## 分析页 API 摘要

- `POST /api/queries` `{sql}` 预校验通过后创建查询，返回查询号（202）；非法 SQL 400，
  库不可用 503，并发已满 429。
- `POST /api/queries/{id}/cancel` 按查询号取消（幂等，终态后调用返回提示且不改终态）。
- `GET /api/queries` 查询列表与状态；`GET /api/queries/{id}` 详情（列名、行、截断、耗时、错误）。
- `GET /api/schema` 返回 `jobs`、`tickets` 的列名与类型（服务端固定 SQL，不接受用户输入）。

### analyzer 环境变量

| 变量 | 默认 | 说明 |
|---|---|---|
| `DB_PATH` | `/data/collector.db` | 只读分析的采集库路径（compose 只读挂载） |
| `QUERY_TIMEOUT` | `2` | 执行与取数合计上限（秒） |
| `MAX_ROWS` | `500` | 单次最多返回行数，超出截断并注明 |
| `MAX_CONCURRENCY` | `8` | 并发查询上限，超出快速 429 |

## 采集流程与状态机

```
connecting ──订阅接通──▶ backfilling ──历史取齐──▶ synced（开放下载）
     ▲                       │                        │
     │                       │ 历史失败                │ 断流
     │                       ▼                        ▼
     └──── 自动重连 ── reconnecting                failed（显示原因，保持未同步，自动重试）
```

1. **先接通订阅再补采历史**：SSE 连接建立后才发起历史分页请求，补采期间推送持续落库。
2. **固定快照分页**：历史第 1 页在门户侧冻结快照并返回 `snapshot_id`，后续页凭快照翻页；
   补采期间发生的补称/作废通过 SSE 送达，不会打乱分页。
3. **同单仅保留最高修订**：落库使用
   `INSERT ... ON CONFLICT DO UPDATE ... WHERE excluded.revision > 现有revision`，
   因此迟到旧版既不能覆盖新数据，也不能恢复已作废单。
4. **只有 `synced` 可下载**：断流立即转入 `reconnecting` 并关闭下载（CSV 接口返回 409）。
5. **代次（epoch）防护**：每次（重）连接 epoch +1；补采完成只有在 epoch 未变且仍处于
   本连接生命期内才推进到 `synced` —— 旧连接的补采完成不能恢复下载。
6. **历史失败可见**：超时、快照过期（410）、门户 5xx 等原因展示在任务列表中，
   状态保持 `failed`（未同步），后台按 `BACKFILL_RETRY_DELAY` 自动重试。

## 演示流程

在门户控制面板（http://localhost:8000）操作，观察采集端（http://localhost:8001）变化：

| 操作 | 预期 |
|---|---|
| 「补称」/「作废」 | 采集端秒级更新修订号/作废标记；作废单从 CSV 消失 |
| 「推送迟到旧版」 | 采集端忽略：修订号不变，已作废单不复活 |
| 「断开所有订阅」 | 任务立即变「订阅中断 · 重连中」，CSV 409；约 1s 后自动重连、重新补采并恢复可下载 |
| 「历史超时(8s)」后新建任务 | 任务变「历史补采失败」，原因显示超时；「恢复正常」后自动追齐 |
| 「快照过期(1.5s×TTL2s)」后新建任务 | 补采中途快照失效（410），显示原因并保持未同步 |

## API 摘要

采集端（8001）：

- `POST /api/jobs` `{portal_url, biz_date}` 创建采集任务
- `GET /api/jobs` / `GET /api/jobs/{id}` / `GET /api/jobs/{id}/tickets` 状态查询
- `GET /api/jobs/{id}/csv` 有效单 CSV（仅 `synced`，否则 409）

门户（8000）：

- `GET /api/sse?date=` SSE 推送（15s 心跳）
- `GET /api/history?date=&page=&snapshot_id=` 固定快照分页（每页 5 条）
- `POST /api/control/{reweigh,void,replay_old,new_ticket,disconnect,config}` 控制面

## 配置（collector 环境变量）

| 变量 | 默认 | 说明 |
|---|---|---|
| `DATA_DIR` | `/data` | SQLite 数据目录（compose 已挂卷，重启任务自动恢复） |
| `HISTORY_TIMEOUT` | `5` | 历史接口单次请求超时（秒） |
| `RECONNECT_DELAY` | `1` | 断流重连间隔（秒） |
| `BACKFILL_RETRY_DELAY` | `5` | 历史补采失败重试间隔（秒） |

## 本地开发（不用 Docker）

```bash
pip install -r portal/requirements.txt -r collector/requirements.txt -r analyzer/requirements.txt
(cd portal && uvicorn app.main:app --port 8000) &
(cd collector && DATA_DIR=./data uvicorn app.main:app --port 8001) &
# analyzer 只读指向同一个采集库文件（采集端需先建库并保持 WAL 连接）
(cd analyzer && DB_PATH=./data/collector.db uvicorn app.main:app --port 8002) &
```
