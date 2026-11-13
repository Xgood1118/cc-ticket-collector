"""旧版工单同步脚本（已上线，仅支持全量）。

已知问题先记录在这里，新版本在旁边实现，不要直接删除本文件：
- 只能全量拉取，没有增量，每次都从头跑。
- 直接 INSERT，重复运行会撞主键报错，也没有去重。
- 时间字段按 updated_at 取值，和接口实际返回对不上。
- 不做数据校验，脏数据会直接写进主表。
"""
import json
import os
import sqlite3
import urllib.request

PAGE_SIZE = 10


def _fetch_page(base_url, page):
    url = "%s/events?page=%d&page_size=%d" % (base_url, page, PAGE_SIZE)
    with urllib.request.urlopen(url) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _init_db(db_path):
    conn = sqlite3.connect(db_path)
    schema_path = os.path.join(os.path.dirname(__file__), "schema.sql")
    with open(schema_path, encoding="utf-8") as fh:
        conn.executescript(fh.read())
    return conn


def run_full(base_url, db_path):
    conn = _init_db(db_path)
    page = 1
    while True:
        data = _fetch_page(base_url, page)
        rows = data["events"]
        if not rows:
            break
        for e in rows:
            # 这里直接插入，没有去重，重复跑会在主键上报错。
            conn.execute(
                "INSERT INTO events (ticket_id, title, priority, status, "
                "last_modified, assignee_id, assignee_name, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    e["id"],
                    e.get("title"),
                    e.get("priority"),
                    e.get("status"),
                    e.get("updated_at"),
                    None,
                    None,
                    e.get("created_at"),
                ),
            )
        conn.commit()
        # 只靠 total 估算页数，没有看 next_cursor。
        total = data["total"]
        if page * PAGE_SIZE >= total:
            break
        page += 1
    conn.close()


# 后续如果要提速可以在这里加线程池并发拉页，先留个位置。
if __name__ == "__main__":
    run_full("http://127.0.0.1:8090", "tickets_legacy.db")
