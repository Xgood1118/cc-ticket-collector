-- 本地分析库表结构，采集侧保持这些表不被删改。

-- 工单主表，按业务主键唯一。
CREATE TABLE IF NOT EXISTS events (
    ticket_id      TEXT PRIMARY KEY,
    title          TEXT,
    priority       INTEGER,
    status         TEXT,
    last_modified  TEXT,
    assignee_id    TEXT,
    assignee_name  TEXT,
    created_at     TEXT
);

-- 隔离表，校验失败的记录落这里，等待人工处理。
CREATE TABLE IF NOT EXISTS quarantine (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ticket_id       TEXT,
    raw_payload     TEXT,
    reason          TEXT,
    quarantined_at  TEXT
);

-- 采集断点与水位线，键值形式存放。
CREATE TABLE IF NOT EXISTS sync_state (
    key    TEXT PRIMARY KEY,
    value  TEXT
);
