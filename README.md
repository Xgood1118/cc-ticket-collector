# 工单事件增量采集

把内部工单系统的事件数据同步到本地 SQLite 分析库，支持全量和增量，可重复运行。

## 组成

- `mock_api/`：提供工单事件的分页 HTTP 接口，模拟真实网关的限流和临时错误，采集侧不改动它。
- `collector/`：采集侧。`legacy_sync.py` 是只支持全量的旧脚本（保留不删）；
  `sync.py` 是新版采集程序，支持全量/增量、断点续跑、幂等去重和脏数据隔离；
  `schema.sql` 是分析库表结构。
- `tests/`：环境与边界测试。

## 启动接口

```bash
python -m mock_api.server 8090
```

接口：`GET /events?page=<n>&page_size=<s>&since=<iso>`，返回分页 JSON，含 `total`、`events`、`next_cursor`。

## 运行测试

```bash
python -m unittest discover -s tests -v
```

## 新版采集程序

```bash
# 全量采集（首次或重建库时使用）
python -m collector.sync --mode full --base-url http://127.0.0.1:8090 --db tickets.db

# 增量采集（按水位线只拉新增/变更；无水位线时自动回退为全量）
python -m collector.sync --mode incremental --db tickets.db

# 丢弃已有断点从头重跑
python -m collector.sync --mode full --restart --db tickets.db
```

可选参数：`--page-size`、`--max-retries`、`--overlap-seconds`（增量重叠窗口秒数，
默认 7）、`--timeout`。连接地址和库路径都走命令行参数，代码内不写任何凭据。

关键行为：

- **分页**：只按 `next_cursor` 判断结束，不信任接口里被注水的 `total`；游标循环会报错退出。
- **重试**：429 按 `Retry-After` 退避，5xx/网络错误按 1s、2s、4s… 指数退避；
  连续失败超过 `--max-retries` 抛 `SyncError` 并以退出码 2 失败，断点保留、不跳过数据。
- **清洗**：时间统一规整成 `YYYY-MM-DDTHH:MM:SS`（带时区的换算到 UTC），
  嵌套 `assignee` 拍平成 `assignee_id`/`assignee_name`；校验失败的记录原样进
  `quarantine` 表并记录原因，不丢弃、不阻断主流程，重复运行不重复隔离。
- **落库**：按 `ticket_id` UPSERT，同工单多次变更只更新不新增；`status=deleted`
  软删除同步到主表，有效数据统计可直接 `WHERE status != 'deleted'`。
- **续跑与幂等**：每页在同一事务提交数据和断点（`sync_state.active_run`），
  中断后重跑自动从断点页继续，累计统计不重不漏；完成后断点清除、水位线
  （`sync_state.watermark`）单调推进。增量窗口相对水位线固定向前重叠，
  覆盖同秒多事件、乱序和更新时间回拨。
- **统计**：每次结束打印 fetched / inserted / updated / deleted / quarantined /
  unchanged 数量和当前水位线。

## 旧脚本现状

`collector/legacy_sync.py` 只能全量、直接插入不去重、按 `updated_at` 取时间，
和接口实际字段对不上，重复运行会在主键上报错，也不处理脏数据和删除。
该文件保留作为对照，新逻辑全部在 `collector/sync.py`，未改动接口服务和库表结构。
