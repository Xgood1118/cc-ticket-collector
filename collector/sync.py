"""工单事件采集程序（全量 / 增量、可续跑、幂等）。

设计要点：

1. 分页只依据接口返回的 ``next_cursor`` 判断结束，不信任被注水的 ``total``。
2. HTTP 429 按 ``Retry-After`` 退避；5xx 与网络抖动做有限次指数退避重试，
   连续失败超过上限抛 :class:`SyncError`，绝不静默跳过数据。
3. 字段统一命名、时间统一规整成 naive ISO 8601（``YYYY-MM-DDTHH:MM:SS``），
   嵌套的 ``assignee`` 拍平成 ``assignee_id`` / ``assignee_name``。
   校验失败的记录不丢弃、不阻断主流程，原样写入 ``quarantine`` 并记录原因。
4. 按业务主键 ``ticket_id`` 做 UPSERT，同一工单多次变更只更新不产生新行；
   ``status == 'deleted'`` 的工单在主表同步为已删除状态（软删除）。
5. 每页处理完即在同一事务内提交并把断点写入 ``sync_state``（``active_run``），
   任意阶段中断后重跑会从断点页继续；水位线 ``watermark`` 单调推进，
   增量窗口固定向前重叠一段时间，覆盖同秒多事件、乱序与时间回拨。
6. 仅依赖 Python 标准库。
"""

import argparse
import json
import os
import sqlite3
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from urllib.parse import urlencode

SCHEMA_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "schema.sql")

DEFAULT_PAGE_SIZE = 10
DEFAULT_MAX_RETRIES = 5
DEFAULT_OVERLAP_SECONDS = 7
DEFAULT_TIMEOUT = 10

WATERMARK_KEY = "watermark"
RUN_STATE_KEY = "active_run"

# datetime.fromisoformat 覆盖不了的历史格式，按顺序兜底尝试。
_FALLBACK_TIME_FORMATS = (
    "%Y/%m/%d %H:%M:%S",
    "%Y/%m/%d %H:%M",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
)

_EVENTS_COLUMNS = (
    "ticket_id",
    "title",
    "priority",
    "status",
    "last_modified",
    "assignee_id",
    "assignee_name",
    "created_at",
)
_COMPARE_COLUMNS = tuple(c for c in _EVENTS_COLUMNS if c != "ticket_id")


class SyncError(RuntimeError):
    """采集过程中无法恢复的错误（连续重试失败、响应结构非法等）。"""


# --------------------------------------------------------------------------- #
# 时间与数据清洗
# --------------------------------------------------------------------------- #
def normalize_time(value):
    """把各种时间表示规整成 naive ISO 8601 字符串，精确到秒。

    支持带时区的 ISO 8601（统一换算到 UTC 后去掉时区标记）以及若干
    ``YYYY/MM/DD HH:MM`` 形态的兜底格式。无法解析时抛 :class:`ValueError`。
    """
    if not isinstance(value, str):
        raise ValueError("时间字段必须是字符串，实际类型 %r" % type(value).__name__)
    text = value.strip()
    if not text:
        raise ValueError("时间字段为空")

    dt = None
    iso_text = text[:-1] + "+00:00" if text.endswith(("Z", "z")) else text
    try:
        dt = datetime.fromisoformat(iso_text)
    except ValueError:
        for fmt in _FALLBACK_TIME_FORMATS:
            try:
                dt = datetime.strptime(text, fmt)
                break
            except ValueError:
                continue
    if dt is None:
        raise ValueError("无法解析的时间: %r" % value)

    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt.replace(microsecond=0).isoformat()


def _require_nonempty_str(value, label):
    if not isinstance(value, str) or not value.strip():
        raise ValueError("%s 缺失或不是非空字符串" % label)
    return value.strip()


def clean_event(event):
    """清洗并校验一条事件。

    返回 ``(row, reason)``：

    - 校验通过：``row`` 为可写入 ``events`` 表的字典，``reason`` 为 ``None``；
    - 校验失败：``row`` 为 ``None``，``reason`` 为失败原因（字符串），
      原始记录由调用方原样写入隔离表。
    """
    if not isinstance(event, dict):
        return None, "记录不是 JSON 对象: %r" % type(event).__name__

    reasons = []

    ticket_id = None
    try:
        ticket_id = _require_nonempty_str(event.get("id"), "id")
    except ValueError as exc:
        reasons.append(str(exc))

    title = event.get("title")
    if title is None:
        reasons.append("title 缺失")
    elif not isinstance(title, str):
        reasons.append("title 类型不符: %r" % type(title).__name__)
    elif not title.strip():
        reasons.append("title 为空字符串")

    status = None
    try:
        status = _require_nonempty_str(event.get("status"), "status")
    except ValueError as exc:
        reasons.append(str(exc))

    priority = event.get("priority")
    if isinstance(priority, bool) or not isinstance(priority, int):
        reasons.append("priority 必须是整数，实际值 %r" % (priority,))

    last_modified = None
    try:
        last_modified = normalize_time(event.get("last_modified"))
    except ValueError as exc:
        reasons.append("last_modified 无效: %s" % exc)

    created_at = None
    try:
        created_at = normalize_time(event.get("created_at"))
    except ValueError as exc:
        reasons.append("created_at 无效: %s" % exc)

    assignee_id = None
    assignee_name = None
    assignee = event.get("assignee")
    if assignee is None:
        # 未分配是合法业务状态，不算脏数据。
        pass
    elif not isinstance(assignee, dict):
        reasons.append("assignee 必须是对象或 null，实际类型 %r" % type(assignee).__name__)
    else:
        try:
            assignee_id = _require_nonempty_str(assignee.get("id"), "assignee.id")
        except ValueError as exc:
            reasons.append(str(exc))
        name = assignee.get("name")
        if name is None:
            reasons.append("assignee.name 缺失")
        elif not isinstance(name, str) or not name.strip():
            reasons.append("assignee.name 不是非空字符串")
        else:
            assignee_name = name.strip()

    if reasons:
        return None, "; ".join(reasons)

    return {
        "ticket_id": ticket_id,
        "title": title.strip(),
        "priority": priority,
        "status": status,
        "last_modified": last_modified,
        "assignee_id": assignee_id,
        "assignee_name": assignee_name,
        "created_at": created_at,
    }, None


# --------------------------------------------------------------------------- #
# HTTP 拉取
# --------------------------------------------------------------------------- #
def _parse_retry_after(header_value):
    """解析 Retry-After 头。支持 delta-seconds 和 HTTP-date，无法解析返回 None。"""
    if header_value is None:
        return None
    try:
        return max(0.0, float(header_value))
    except (TypeError, ValueError):
        pass
    try:
        # RFC 7231 HTTP-date，例如 Wed, 21 Oct 2099 07:28:00 GMT。
        return max(0.0, datetime.strptime(
            header_value, "%a, %d %b %Y %H:%M:%S %Z"
        ).timestamp())
    except (TypeError, ValueError):
        return None


def fetch_page(base_url, cursor, page_size=DEFAULT_PAGE_SIZE, since=None,
               max_retries=DEFAULT_MAX_RETRIES, timeout=DEFAULT_TIMEOUT, sleeper=time.sleep):
    """拉取一页数据，返回接口 JSON（dict）。

    - 429：按 ``Retry-After`` 退避（缺省按指数退避）；
    - 5xx / 网络层错误：指数退避（1s、2s、4s ...）后重试；
    - 4xx（429 除外）：不重试，直接抛 :class:`SyncError`；
    - 连续失败超过 ``max_retries``：抛 :class:`SyncError`，不跳过该页。
    """
    params = {"page": cursor, "page_size": page_size}
    if since:
        params["since"] = since
    url = base_url.rstrip("/") + "/events?" + urlencode(params)

    failures = 0
    last_errors = []
    while True:
        req = urllib.request.Request(
            url, headers={"Accept": "application/json"}
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                body = resp.read().decode("utf-8")
            data = json.loads(body)
            if not isinstance(data, dict) or not isinstance(data.get("events"), list):
                raise SyncError("响应结构非法，缺少 events 列表: %s" % url)
            return data
        except urllib.error.HTTPError as exc:
            if exc.code == 429:
                wait = _parse_retry_after(exc.headers.get("Retry-After"))
                if wait is None:
                    wait = float(2 ** failures)
            elif 500 <= exc.code < 600:
                wait = float(2 ** failures)
            else:
                raise SyncError("HTTP %d 不可重试: %s" % (exc.code, url)) from exc
            last_errors.append("HTTP %d: %s" % (exc.code, exc.reason))
        # 注意 URLError 是 OSError 的子类，必须放在 OSError 前面，
        # 否则网络错误会被笼统归成 OSError。
        except (urllib.error.URLError, TimeoutError, OSError,
                json.JSONDecodeError, UnicodeDecodeError) as exc:
            wait = float(2 ** failures)
            last_errors.append(type(exc).__name__ + ": " + str(exc))

        failures += 1
        if failures > max_retries:
            raise SyncError(
                "拉取连续失败 %d 次，超过上限 %d，放弃: %s (%s)"
                % (failures, max_retries, url, " | ".join(last_errors[-3:]))
            )
        sleeper(wait)


def fetch_pager(base_url, page_size=DEFAULT_PAGE_SIZE, since=None,
                max_retries=DEFAULT_MAX_RETRIES, timeout=DEFAULT_TIMEOUT,
                sleeper=time.sleep, start_cursor="1"):
    """按 ``next_cursor`` 逐页产出 ``(cursor, data)``，直到没有下一页。"""
    cursor = str(start_cursor)
    seen = set()
    while True:
        if cursor in seen:
            raise SyncError("分页游标出现循环: %r" % cursor)
        seen.add(cursor)
        data = fetch_page(
            base_url, cursor, page_size=page_size, since=since,
            max_retries=max_retries, timeout=timeout, sleeper=sleeper,
        )
        yield cursor, data
        next_cursor = data.get("next_cursor")
        if next_cursor is None or str(next_cursor) == "":
            return
        cursor = str(next_cursor)


# --------------------------------------------------------------------------- #
# SQLite 落库
# --------------------------------------------------------------------------- #
def init_db(db_path):
    """打开分析库并确保表结构存在（表结构以 schema.sql 为准，不做删改）。"""
    conn = sqlite3.connect(db_path)
    with open(SCHEMA_PATH, encoding="utf-8") as fh:
        conn.executescript(fh.read())
    conn.commit()
    return conn


def _get_state(conn, key):
    row = conn.execute(
        "SELECT value FROM sync_state WHERE key=?", (key,)
    ).fetchone()
    return row[0] if row else None


def _set_state(conn, key, value):
    conn.execute(
        "INSERT INTO sync_state(key, value) VALUES(?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value),
    )


def _zero_stats():
    return {
        "fetched": 0,
        "inserted": 0,
        "updated": 0,
        "deleted": 0,
        "quarantined": 0,
        "unchanged": 0,
    }


def _quarantine(conn, stats, event, reason):
    """脏数据原样落隔离表；同一 (ticket_id, raw_payload) 只保留一份。"""
    payload = json.dumps(event, ensure_ascii=False, sort_keys=True)
    ticket_id = event.get("id") if isinstance(event, dict) else None
    exists = conn.execute(
        "SELECT 1 FROM quarantine "
        "WHERE COALESCE(ticket_id, '') = COALESCE(?, '') AND raw_payload = ? LIMIT 1",
        (ticket_id, payload),
    ).fetchone()
    if not exists:
        conn.execute(
            "INSERT INTO quarantine(ticket_id, raw_payload, reason, quarantined_at) "
            "VALUES(?, ?, ?, ?)",
            (ticket_id, payload, reason,
             datetime.now().replace(microsecond=0).isoformat(timespec="seconds")),
        )
        stats["quarantined"] += 1


_UPSERT_SQL = (
    "INSERT INTO events(ticket_id, title, priority, status, last_modified, "
    "assignee_id, assignee_name, created_at) VALUES(?, ?, ?, ?, ?, ?, ?, ?) "
    "ON CONFLICT(ticket_id) DO UPDATE SET title=excluded.title, "
    "priority=excluded.priority, status=excluded.status, "
    "last_modified=excluded.last_modified, assignee_id=excluded.assignee_id, "
    "assignee_name=excluded.assignee_name, created_at=excluded.created_at"
)


def _apply_event(conn, stats, row):
    """按业务主键 upsert 一条干净记录，并区分新增 / 更新 / 无变化 / 删除。"""
    existing = conn.execute(
        "SELECT %s FROM events WHERE ticket_id = ?"
        % ", ".join(_COMPARE_COLUMNS),
        (row["ticket_id"],),
    ).fetchone()

    if existing is not None and tuple(existing) == tuple(
        row[c] for c in _COMPARE_COLUMNS
    ):
        stats["unchanged"] += 1
        return

    conn.execute(_UPSERT_SQL, tuple(row[c] for c in _EVENTS_COLUMNS))
    if existing is None:
        stats["inserted"] += 1
    else:
        stats["updated"] += 1
    if row["status"] == "deleted":
        stats["deleted"] += 1


# --------------------------------------------------------------------------- #
# 采集主流程
# --------------------------------------------------------------------------- #
def _new_run(conn, mode, overlap_seconds):
    """开始一次全新运行：清掉旧断点，计算增量水位线与重叠窗口。"""
    since = None
    if mode == "incremental":
        watermark = _get_state(conn, WATERMARK_KEY)
        if watermark:
            since_dt = datetime.fromisoformat(watermark)
            since = (since_dt.timestamp() - overlap_seconds)
            since = datetime.fromtimestamp(since).isoformat(timespec="seconds")
    run = {
        "mode": mode,
        "cursor": "1",
        "since": since,
        "stats": _zero_stats(),
        "max_seen": None,
    }
    # 运行一开始就落断点：哪怕第一页就连续失败，断点也已存在，重跑可续。
    conn.execute("DELETE FROM sync_state WHERE key = ?", (RUN_STATE_KEY,))
    _set_state(conn, RUN_STATE_KEY, json.dumps(run, ensure_ascii=False))
    return run


def run_sync(base_url, db_path, mode="full", page_size=DEFAULT_PAGE_SIZE,
             max_retries=DEFAULT_MAX_RETRIES, overlap_seconds=DEFAULT_OVERLAP_SECONDS,
             timeout=DEFAULT_TIMEOUT, restart=False, sleeper=time.sleep,
             on_page_committed=None):
    """执行一次采集，返回统计字典。

    mode: ``"full"`` 全量；``"incremental"`` 增量（无水位线时自动回退全量）。
    restart: 为 True 时丢弃已有断点从头跑。
    on_page_committed: 每页提交后的回调 ``fn(stats, next_cursor)``，
    回调内抛异常可用来模拟中断（断点已落盘，重跑可续）。
    """
    if mode not in ("full", "incremental"):
        raise ValueError("mode 只能是 full / incremental: %r" % mode)

    conn = init_db(db_path)
    try:
        raw_run = None if restart else _get_state(conn, RUN_STATE_KEY)
        if raw_run is not None:
            try:
                raw_run = json.loads(raw_run)
            except (TypeError, ValueError):
                raw_run = None

        if isinstance(raw_run, dict) and raw_run.get("mode") == mode:
            # 续跑：断点、统计和已见最大时间都从上次中断处恢复。
            run = raw_run
            run.setdefault("stats", _zero_stats())
            run.setdefault("max_seen", None)
            run.setdefault("since", None)
        else:
            run = _new_run(conn, mode, overlap_seconds)
            conn.commit()

        pager = fetch_pager(
            base_url, page_size=page_size, since=run["since"],
            max_retries=max_retries, timeout=timeout, sleeper=sleeper,
            start_cursor=run["cursor"],
        )
        for cursor, data in pager:
            for event in data["events"]:
                run["stats"]["fetched"] += 1
                row, reason = clean_event(event)
                if reason is not None:
                    _quarantine(conn, run["stats"], event, reason)
                    continue
                _apply_event(conn, run["stats"], row)
                if run["max_seen"] is None or row["last_modified"] > run["max_seen"]:
                    run["max_seen"] = row["last_modified"]

            next_cursor = data.get("next_cursor")
            # 先在同一事务里落数据，再落断点，保证页级原子性。
            if next_cursor:
                run["cursor"] = str(next_cursor)
            _set_state(conn, RUN_STATE_KEY, json.dumps(run, ensure_ascii=False))
            conn.commit()
            if on_page_committed is not None:
                on_page_committed(run["stats"], next_cursor)
            if not next_cursor:
                break

        # 全部页拉完：水位线单调推进，并清掉运行断点。
        if run["max_seen"] is not None:
            old_watermark = _get_state(conn, WATERMARK_KEY)
            if old_watermark is None or run["max_seen"] > old_watermark:
                _set_state(conn, WATERMARK_KEY, run["max_seen"])
        conn.execute("DELETE FROM sync_state WHERE key = ?", (RUN_STATE_KEY,))
        conn.commit()
        return dict(run["stats"])
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# 命令行
# --------------------------------------------------------------------------- #
def build_parser():
    parser = argparse.ArgumentParser(
        prog="ticket-collector",
        description="工单事件采集：支持全量 / 增量、断点续跑、幂等去重。",
    )
    parser.add_argument("--mode", choices=["full", "incremental"], required=True,
                        help="full=全量采集；incremental=按水位线增量采集")
    parser.add_argument("--base-url", default="http://127.0.0.1:8090",
                        help="工单事件接口地址（默认 http://127.0.0.1:8090）")
    parser.add_argument("--db", default="tickets.db",
                        help="本地 SQLite 分析库路径（默认 tickets.db）")
    parser.add_argument("--page-size", type=int, default=DEFAULT_PAGE_SIZE,
                        help="每页条数（默认 %d）" % DEFAULT_PAGE_SIZE)
    parser.add_argument("--max-retries", type=int, default=DEFAULT_MAX_RETRIES,
                        help="单页连续失败最大重试次数（默认 %d）" % DEFAULT_MAX_RETRIES)
    parser.add_argument("--overlap-seconds", type=int,
                        default=DEFAULT_OVERLAP_SECONDS,
                        help="增量查询相对水位线向前重叠的秒数（默认 %d，"
                             "覆盖同秒多事件、乱序与时间回拨）" % DEFAULT_OVERLAP_SECONDS)
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT,
                        help="单次 HTTP 请求超时秒数（默认 %d）" % DEFAULT_TIMEOUT)
    parser.add_argument("--restart", action="store_true",
                        help="丢弃已有断点和本次重叠窗口，从头重新采集")
    return parser


def _print_summary(mode, stats, db_path):
    watermark = None
    if os.path.exists(db_path):
        conn = sqlite3.connect(db_path)
        try:
            watermark = _get_state(conn, WATERMARK_KEY)
        finally:
            conn.close()
    print("采集完成 (mode=%s)" % mode)
    print("  拉取 fetched    : %d" % stats["fetched"])
    print("  新增 inserted   : %d" % stats["inserted"])
    print("  更新 updated    : %d" % stats["updated"])
    print("  删除 deleted    : %d" % stats["deleted"])
    print("  隔离 quarantined: %d" % stats["quarantined"])
    print("  无变化 unchanged: %d" % stats["unchanged"])
    print("  水位线 watermark: %s" % (watermark if watermark else "(无)"))


def main(argv=None):
    args = build_parser().parse_args(argv)
    try:
        stats = run_sync(
            args.base_url,
            args.db,
            mode=args.mode,
            page_size=args.page_size,
            max_retries=args.max_retries,
            overlap_seconds=args.overlap_seconds,
            timeout=args.timeout,
            restart=args.restart,
        )
    except SyncError as exc:
        print("采集失败: %s" % exc, file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("采集被中断，断点已保存，重新运行可续跑。", file=sys.stderr)
        return 130
    _print_summary(args.mode, stats, args.db)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
