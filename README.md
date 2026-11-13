# 工单事件增量采集

把内部工单系统的事件数据同步到本地 SQLite 分析库，支持全量和增量，可重复运行。

## 组成

- `mock_api/`：提供工单事件的分页 HTTP 接口，模拟真实网关的限流和临时错误，采集侧不改动它。
- `collector/`：采集侧。`legacy_sync.py` 是只支持全量的旧脚本，`schema.sql` 是分析库表结构。
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

## 旧脚本现状

`collector/legacy_sync.py` 只能全量、直接插入不去重、按 `updated_at` 取时间，和接口实际字段对不上，重复运行会在主键上报错，也不处理脏数据和删除。新版本需要在不删除旧脚本、不改动接口服务和库表结构的前提下解决这些问题。
