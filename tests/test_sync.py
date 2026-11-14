"""新版采集程序 collector.sync 的关键边界测试。

所有边界（分页注水 total、429/503、脏数据、删除、乱序、时间回拨、中断续跑）
都用仓库自带的 mock 接口和真实 SQLite 文件库复现，不 mock 被测代码本身。
"""
import http.server
import json
import os
import shutil
import sqlite3
import sys
import tempfile
import threading
import unittest
import urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import mock_api.dataset as dataset_mod  # noqa: E402
import mock_api.server as server_mod  # noqa: E402
from collector import sync  # noqa: E402
from collector.sync import (  # noqa: E402
    SyncError,
    clean_event,
    fetch_page,
    normalize_time,
    run_sync,
)


def _start_mock_server():
    server = server_mod.build_server(0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, "http://127.0.0.1:%d" % server.server_address[1]


def _patch_dataset(events):
    """临时替换 mock 服务返回的数据集，测试结束自动还原。"""
    original = dataset_mod.load_events
    server_mod.load_events = lambda: events
    return original


class _CollectorTestBase(unittest.TestCase):
    def setUp(self):
        self.server, self.base_url = _start_mock_server()
        self.tmpdir = tempfile.mkdtemp(prefix="ticket-sync-")
        self.addCleanup(self._cleanup)
        self.db_path = os.path.join(self.tmpdir, "tickets.db")

    def _cleanup(self):
        self.server.shutdown()
        self.server.server_close()
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _connect(self):
        return sqlite3.connect(self.db_path)

    def _table_dump(self):
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT ticket_id, title, priority, status, last_modified, "
                "assignee_id, assignee_name, created_at FROM events ORDER BY ticket_id"
            ).fetchall()
            quarantine = conn.execute(
                "SELECT ticket_id, reason FROM quarantine ORDER BY ticket_id"
            ).fetchall()
            watermark = conn.execute(
                "SELECT value FROM sync_state WHERE key='watermark'"
            ).fetchone()
            active = conn.execute(
                "SELECT value FROM sync_state WHERE key='active_run'"
            ).fetchone()
            return rows, quarantine, watermark, active
        finally:
            conn.close()


class FullSyncTest(_CollectorTestBase):
    def test_full_sync_paginates_by_next_cursor_and_cleans(self):
        sleeper_calls = []
        stats = run_sync(
            self.base_url, self.db_path, mode="full",
            sleeper=lambda s: sleeper_calls.append(s),
        )

        # 24 条全部拉到；脏数据 T-1012（缺 title）/T-1013（priority 非法）
        # /T-1014（时间无法解析）进隔离表，其余 21 条落主表。
        self.assertEqual(stats["fetched"], 24)
        self.assertEqual(stats["inserted"], 21)
        self.assertEqual(stats["quarantined"], 3)
        self.assertEqual(stats["updated"], 0)
        self.assertEqual(stats["deleted"], 2)  # T-1010、T-1024 软删除状态
        self.assertEqual(stats["unchanged"], 0)

        rows, quarantine, watermark, active = self._table_dump()
        self.assertEqual(len(rows), 21)

        # 嵌套 assignee 被拍平；None assignee 视为未分配。
        t1001 = next(r for r in rows if r[0] == "T-1001")
        self.assertEqual((t1001[5], t1001[6]), ("u1", "张维"))
        t1009 = next(r for r in rows if r[0] == "T-1009")
        self.assertIsNone(t1009[5])
        self.assertIsNone(t1009[6])

        # 斜杠格式时间被规整成 ISO 8601。
        t1015 = next(r for r in rows if r[0] == "T-1015")
        self.assertEqual(t1015[4], "2026-11-07T14:30:00")

        # 删除工单留在主表但带 deleted 状态，没有混进有效数据。
        deleted = [r[0] for r in rows if r[3] == "deleted"]
        self.assertEqual(deleted, ["T-1010", "T-1024"])
        self.assertEqual(
            conn_count(self.db_path, "events WHERE status != 'deleted'"), 19
        )

        # 隔离表：ticket_id、原因、原始 JSON 都在。
        q_ids = [q[0] for q in quarantine]
        self.assertEqual(q_ids, ["T-1012", "T-1013", "T-1014"])
        for ticket_id, reason in quarantine:
            self.assertTrue(reason)
        conn = self._connect()
        try:
            payloads = conn.execute(
                "SELECT raw_payload FROM quarantine ORDER BY ticket_id"
            ).fetchall()
        finally:
            conn.close()
        for (payload,) in payloads:
            self.assertIn("T-101", json.loads(payload)["id"])

        # 水位线取本批最大更新时间（乱序数据里的最大值）；运行断点已清。
        self.assertEqual(watermark[0], "2026-11-10T10:00:00")
        self.assertIsNone(active)

        # 真的走到了退避逻辑（page2 首次 429、page3 首次 503）。
        self.assertTrue(sleeper_calls)

    def test_rerun_is_idempotent(self):
        first = run_sync(self.base_url, self.db_path, mode="full")
        self.assertEqual(first["inserted"], 21)
        dump_after_first, q_after, _, active = self._table_dump()
        self.assertIsNone(active)

        second = run_sync(self.base_url, self.db_path, mode="full")
        # 再跑一遍：没有新增/更新/隔离，全部命中无变化，表内容逐字节一致。
        self.assertEqual(second["inserted"], 0)
        self.assertEqual(second["updated"], 0)
        self.assertEqual(second["quarantined"], 0)
        self.assertEqual(second["unchanged"], 21)
        self.assertEqual(second["fetched"], 24)
        dump_after_second, q_second, _, _ = self._table_dump()
        self.assertEqual(dump_after_first, dump_after_second)
        self.assertEqual(q_after, q_second)
        self.assertEqual(conn_count(self.db_path, "events"), 21)
        self.assertEqual(conn_count(self.db_path, "quarantine"), 3)

    def test_injected_rate_limit_and_503_are_retried(self):
        sleeps = []
        stats = run_sync(
            self.base_url, self.db_path, mode="full",
            max_retries=3, sleeper=lambda s: sleeps.append(s),
        )
        self.assertEqual(stats["fetched"], 24)
        # mock 对第 2、3 页首请求分别注入 429 / 503，重试后成功。
        self.assertGreaterEqual(self.server.hit_counter.get(2, 0), 2)
        self.assertGreaterEqual(self.server.hit_counter.get(3, 0), 2)
        self.assertTrue(all(isinstance(s, (int, float)) and s >= 0 for s in sleeps))


class RetryExhaustionTest(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="ticket-sync-")
        self.db_path = os.path.join(self.tmpdir, "tickets.db")

        class _Always500(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_GET(self):
                body = b"{}"
                self.send_response(500)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        self.fail_server = HTTPServer(("127.0.0.1", 0), _Always500)
        self.thread = threading.Thread(
            target=self.fail_server.serve_forever, daemon=True
        )
        self.thread.start()
        self.fail_url = "http://127.0.0.1:%d" % self.fail_server.server_address[1]

    def tearDown(self):
        self.fail_server.shutdown()
        self.fail_server.server_close()
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_consecutive_failures_raise_and_keep_checkpoint(self):
        sleeps = []
        with self.assertRaises(SyncError) as ctx:
            run_sync(
                self.fail_url, self.db_path, mode="full",
                max_retries=2, sleeper=lambda s: sleeps.append(s),
            )
        self.assertIn("失败", str(ctx.exception))
        # 失败后断点仍然保留，且指向第一页（没有任何数据被跳过）。
        conn = sqlite3.connect(self.db_path)
        try:
            raw = conn.execute(
                "SELECT value FROM sync_state WHERE key='active_run'"
            ).fetchone()
        finally:
            conn.close()
        self.assertIsNotNone(raw)
        self.assertEqual(json.loads(raw[0])["cursor"], "1")
        self.assertEqual(len(sleeps), 2)  # 首次 + 2 次重试 = 2 次退避

    def test_backoff_sequence_is_exponential(self):
        sleeps = []
        with self.assertRaises(SyncError):
            run_sync(
                self.fail_url, self.db_path, mode="full",
                max_retries=4, sleeper=lambda s: sleeps.append(s),
            )
        self.assertEqual(sleeps, [1.0, 2.0, 4.0, 8.0])


class ResumeTest(_CollectorTestBase):
    class _InterruptAfterPages:
        def __init__(self, limit):
            self.limit = limit
            self.calls = 0
            self.cursors = []

        def __call__(self, stats, next_cursor):
            self.cursors.append(next_cursor)
            self.last_stats = dict(stats)
            self.calls += 1
            if self.calls >= self.limit:
                raise KeyboardInterrupt("simulated interruption")

    def test_interrupted_run_resumes_to_identical_result(self):
        # 参考库：一次完整跑完。
        ref_db = os.path.join(self.tmpdir, "ref.db")
        ref_stats = run_sync(self.base_url, ref_db, mode="full")

        # 中断库：第 2 页提交后中断，再续跑。
        hook = self._InterruptAfterPages(2)
        with self.assertRaises(KeyboardInterrupt):
            run_sync(
                self.base_url, self.db_path, mode="full",
                on_page_committed=hook,
            )
        hook_stats_snapshot = dict(hook.last_stats)
        mid_rows, _, _, active = self._table_dump()
        self.assertIsNotNone(active)  # 断点已落盘
        self.assertLess(len(mid_rows) + conn_count(self.db_path, "quarantine"), 24)

        resumed_stats = run_sync(self.base_url, self.db_path, mode="full")

        # 续跑的增量统计只算续跑阶段；最终库内容与一次完整跑完完全一致。
        final_rows, final_q, watermark, final_active = self._table_dump()
        ref_conn = sqlite3.connect(ref_db)
        try:
            ref_dump = ref_conn.execute(
                "SELECT ticket_id, title, priority, status, last_modified, "
                "assignee_id, assignee_name, created_at FROM events ORDER BY ticket_id"
            ).fetchall()
            ref_q = ref_conn.execute(
                "SELECT ticket_id, reason, raw_payload FROM quarantine ORDER BY ticket_id"
            ).fetchall()
            ref_watermark = ref_conn.execute(
                "SELECT value FROM sync_state WHERE key='watermark'"
            ).fetchone()
        finally:
            ref_conn.close()
        self.assertEqual(final_rows, ref_dump)
        self.assertEqual(
            [(q[0], q[1]) for q in final_q],
            [(q[0], q[1]) for q in ref_q],
        )
        self.assertEqual(watermark, ref_watermark)
        self.assertIsNone(final_active)

        # 续跑返回的是跨中断累计的全流程统计：fetched 从断点的 20 累加到 24，
        # 说明只重新拉了第 3 页（4 条），前两页没有重复请求。
        self.assertEqual(resumed_stats["fetched"], 24)
        self.assertEqual(
            resumed_stats["fetched"] - hook_stats_snapshot["fetched"], 4
        )
        # 落库类指标累计后与一次完整跑完完全一致（不重不漏）。
        self.assertEqual(
            hook_stats_snapshot["inserted"] + (
                resumed_stats["inserted"] - hook_stats_snapshot["inserted"]
            ),
            ref_stats["inserted"],
        )
        self.assertEqual(resumed_stats["inserted"], ref_stats["inserted"])
        self.assertEqual(
            resumed_stats["quarantined"], ref_stats["quarantined"]
        )
        self.assertEqual(resumed_stats["deleted"], ref_stats["deleted"])

        conn = sqlite3.connect(self.db_path)
        try:
            total_events = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
            total_q = conn.execute("SELECT COUNT(*) FROM quarantine").fetchone()[0]
        finally:
            conn.close()
        self.assertEqual(total_events, ref_stats["inserted"])
        self.assertEqual(total_q, ref_stats["quarantined"])

    def test_restart_flag_discards_checkpoint(self):
        hook = self._InterruptAfterPages(1)
        with self.assertRaises(KeyboardInterrupt):
            run_sync(
                self.base_url, self.db_path, mode="full",
                on_page_committed=hook,
            )
        self.assertGreater(conn_count(self.db_path, "events"), 0)

        # restart=True 丢弃断点从头跑，最终仍是完整且幂等的结果。
        stats = run_sync(
            self.base_url, self.db_path, mode="full", restart=True
        )
        self.assertEqual(stats["fetched"], 24)
        self.assertEqual(conn_count(self.db_path, "events"), 21)
        self.assertEqual(conn_count(self.db_path, "quarantine"), 3)
        conn = sqlite3.connect(self.db_path)
        try:
            active = conn.execute(
                "SELECT 1 FROM sync_state WHERE key='active_run'"
            ).fetchone()
        finally:
            conn.close()
        self.assertIsNone(active)


def _patched_dataset():
    """在原始数据集上构造增量场景：变更、新增、删除、乱序、同秒多事件、回拨。"""
    by_id = {e["id"]: dict(e) for e in dataset_mod.load_events()}

    by_id["T-1001"].update(  # 普通变更
        {"title": "交换机端口告警-已升级", "status": "closed",
         "last_modified": "2026-11-11T09:00:00"}
    )
    by_id["T-1005"].update(  # 时间回拨：last_modified 早于当前水位线
        {"title": "磁盘使用率告警(复核)",
         "last_modified": "2026-11-11T07:00:00"}
    )
    by_id["T-1003"].update(  # 与 T-9001 同一更新时间（同秒多事件）
        {"status": "closed", "last_modified": "2026-11-11T08:00:00"}
    )
    by_id["T-1020"].update(  # 乱序：更新时间较早，排在数据集尾部
        {"title": "资产信息订正(补充位置)",
         "last_modified": "2026-11-11T07:30:00"}
    )
    by_id["T-1024"].update(  # 已删除工单再次变更，仍是 deleted
        {"title": "废弃工单清理(归档)",
         "last_modified": "2026-11-11T10:30:00"}
    )

    new1 = {  # 与 T-1003 同秒的新工单
        "id": "T-9001", "title": "新工单-A", "priority": 2, "status": "open",
        "last_modified": "2026-11-11T08:00:00",
        "assignee": {"id": "u5", "name": "赵磊"},
        "created_at": "2026-11-11T07:50:00",
    }
    new2 = {
        "id": "T-9002", "title": "新工单-B", "priority": 1, "status": "open",
        "last_modified": "2026-11-11T11:00:00",
        "assignee": None, "created_at": "2026-11-11T10:30:00",
    }

    head = [new1, new2]  # 新事件插在最前面，制造跨页乱序
    rests = list(by_id.values())
    return head + rests


class IncrementalSyncTest(_CollectorTestBase):
    def test_incremental_watermark_changes_deletes_rollback(self):
        original = _patch_dataset(dataset_mod.load_events())
        self.addCleanup(lambda: setattr(server_mod, "load_events", original))

        # 先全量建立基线（21 条主表 + 3 条隔离，水位线 2026-11-10T10:00:00）。
        run_sync(self.base_url, self.db_path, mode="full")

        # 切换到增量场景数据集。
        patched = _patched_dataset()
        server_mod.load_events = lambda: patched

        stats = run_sync(
            self.base_url, self.db_path, mode="incremental",
            overlap_seconds=7,
        )
        # 重叠窗口（10:00:00 - 7s = 09:59:53）之后服务端返回 9 条：
        # - 7 条干净事件（5 条旧工单变更 + 2 条新工单）；
        # - T-1014 的 "not-a-date" 字典序落在窗口内，再次进隔离（原始 payload
        #   已存在，不重复计数）；
        # - T-1015 的斜杠时间字符串字典序也落在窗口内，规整后与库中一致，无变化。
        # 这同时证明采集端不依赖服务端过滤的精确性：多给的数据也能正确幂等处理。
        self.assertEqual(stats["fetched"], 9)
        self.assertEqual(stats["inserted"], 2)       # T-9001、T-9002
        self.assertEqual(stats["updated"], 5)        # 5 条变更（含 deleted 的 T-1024）
        self.assertEqual(stats["quarantined"], 0)    # T-1014 已在隔离表，不重复计数
        self.assertEqual(stats["unchanged"], 1)      # T-1015
        self.assertEqual(stats["deleted"], 1)        # 仅 T-1024 本次再次落 deleted

        conn = self._connect()
        try:
            # 变更落库。
            self.assertEqual(
                conn.execute(
                    "SELECT status FROM events WHERE ticket_id='T-1003'"
                ).fetchone()[0],
                "closed",
            )
            self.assertEqual(
                conn.execute(
                    "SELECT title FROM events WHERE ticket_id='T-1020'"
                ).fetchone()[0],
                "资产信息订正(补充位置)",
            )
            self.assertEqual(
                conn.execute(
                    "SELECT title FROM events WHERE ticket_id='T-1001'"
                ).fetchone()[0],
                "交换机端口告警-已升级",
            )
            # 新工单入库，负责人拍平。
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM events WHERE ticket_id LIKE 'T-900%'"
                ).fetchone()[0],
                2,
            )
            assignee = conn.execute(
                "SELECT assignee_id, assignee_name FROM events "
                "WHERE ticket_id='T-9001'"
            ).fetchone()
            self.assertEqual(assignee, ("u5", "赵磊"))
            # 删除状态仍在主表中可查，但有效数据统计剔除它们。
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM events WHERE status='deleted'"
                ).fetchone()[0],
                2,
            )
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM events WHERE status!='deleted'"
                ).fetchone()[0],
                21,
            )
            watermark = conn.execute(
                "SELECT value FROM sync_state WHERE key='watermark'"
            ).fetchone()[0]
        finally:
            conn.close()
        # 水位线推进到本批最大时间。
        self.assertEqual(watermark, "2026-11-11T11:00:00")

        # 再跑一次增量：新水位线 11:00:00 的重叠窗口（10:59:53 起）服务端
        # 按字符串比较返回 3 条——T-9002 无变化、T-1015 无变化、T-1014 脏数据
        # 在隔离表已存在不重复计数；不产生任何写入，水位线保持不变。
        rerun = run_sync(
            self.base_url, self.db_path, mode="incremental",
            overlap_seconds=7,
        )
        self.assertEqual(rerun["fetched"], 3)
        self.assertEqual(rerun["inserted"], 0)
        self.assertEqual(rerun["updated"], 0)
        self.assertEqual(rerun["quarantined"], 0)
        self.assertEqual(rerun["deleted"], 0)
        self.assertEqual(rerun["unchanged"], 2)

        # 水位线单调不回退（时间回拨的 T-1005 也不能把它拉低）。
        conn = self._connect()
        try:
            wm2 = conn.execute(
                "SELECT value FROM sync_state WHERE key='watermark'"
            ).fetchone()[0]
        finally:
            conn.close()
        self.assertEqual(wm2, "2026-11-11T11:00:00")

    def test_incremental_without_watermark_falls_back_to_full(self):
        stats = run_sync(self.base_url, self.db_path, mode="incremental")
        self.assertEqual(stats["fetched"], 24)
        self.assertEqual(stats["inserted"], 21)
        self.assertEqual(stats["quarantined"], 3)
        conn = self._connect()
        try:
            watermark = conn.execute(
                "SELECT value FROM sync_state WHERE key='watermark'"
            ).fetchone()[0]
        finally:
            conn.close()
        self.assertEqual(watermark, "2026-11-10T10:00:00")


class CleaningUnitTest(unittest.TestCase):
    def _valid(self, **overrides):
        event = {
            "id": "T-1",
            "title": "正常工单",
            "priority": 2,
            "status": "open",
            "last_modified": "2026-11-01T08:00:00",
            "assignee": {"id": "u1", "name": "张三"},
            "created_at": "2026-11-01T07:50:00",
        }
        event.update(overrides)
        return event

    def test_valid_event_flattened(self):
        row, reason = clean_event(self._valid())
        self.assertIsNone(reason)
        self.assertEqual(row["ticket_id"], "T-1")
        self.assertEqual(row["assignee_id"], "u1")
        self.assertEqual(row["assignee_name"], "张三")
        self.assertEqual(row["last_modified"], "2026-11-01T08:00:00")

    def test_null_assignee_is_unassigned_not_dirty(self):
        row, reason = clean_event(self._valid(assignee=None))
        self.assertIsNone(reason)
        self.assertIsNone(row["assignee_id"])
        self.assertIsNone(row["assignee_name"])

    def test_dirty_cases(self):
        cases = [
            (self._valid(id=" "), "id"),
            (self._valid(title=None), "title"),
            (self._valid(priority="high"), "priority"),
            (self._valid(priority=True), "priority"),  # bool 不算整数
            (self._valid(status=""), "status"),
            (self._valid(last_modified="not-a-date"), "last_modified"),
            (self._valid(last_modified="2026/13/40"), "last_modified"),
            (self._valid(assignee={"id": "u1"}), "assignee"),
            (self._valid(assignee={"name": "李四"}), "assignee.id"),
            (self._valid(assignee="u1"), "assignee"),
            (self._valid(created_at=None), "created_at"),
        ]
        for event, fragment in cases:
            row, reason = clean_event(event)
            self.assertIsNone(row, event)
            self.assertIsNotNone(reason)
            self.assertIn(fragment, reason)

    def test_non_dict_record(self):
        row, reason = clean_event(["not", "a", "dict"])
        self.assertIsNone(row)
        self.assertIn("不是 JSON 对象", reason)

    def test_normalize_time_formats_and_timezone(self):
        self.assertEqual(
            normalize_time("2026/11/07 14:30"), "2026-11-07T14:30:00"
        )
        self.assertEqual(
            normalize_time("2026-11-07 14:30:00"), "2026-11-07T14:30:00"
        )
        self.assertEqual(
            normalize_time("2026-11-07T14:30:00Z"), "2026-11-07T14:30:00"
        )
        # 带时区的时间换算到 UTC：+08:00 的 14:30 等于 UTC 06:30。
        self.assertEqual(
            normalize_time("2026-11-07T14:30:00+08:00"),
            "2026-11-07T06:30:00",
        )
        for bad in ("nonsense", "", "   "):
            with self.assertRaises(ValueError):
                normalize_time(bad)
        with self.assertRaises(ValueError):
            normalize_time(123456)

    def test_parse_retry_after(self):
        self.assertEqual(sync._parse_retry_after("0"), 0.0)
        self.assertEqual(sync._parse_retry_after("3"), 3.0)
        self.assertIsNotNone(
            sync._parse_retry_after("Wed, 21 Oct 2099 07:28:00 GMT")
        )
        self.assertIsNone(sync._parse_retry_after("garbage"))
        self.assertIsNone(sync._parse_retry_after(None))

    def test_fetch_page_last_cursor_is_none(self):
        server, base_url = _start_mock_server()
        try:
            last_cursor = None
            for cursor, data in sync.fetch_pager(base_url, page_size=10):
                self.assertEqual(len(data["events"]), 10 if cursor != "3" else 4)
                last_cursor = data.get("next_cursor")
            # 最后一页 next_cursor 必须是 None，而不是看注水的 total。
            self.assertIsNone(last_cursor)
        finally:
            server.shutdown()
            server.server_close()

    def test_cli_main_returns_zero(self):
        server, base_url = _start_mock_server()
        tmpdir = tempfile.mkdtemp(prefix="ticket-cli-")
        try:
            db = os.path.join(tmpdir, "cli.db")
            rc = sync.main([
                "--mode", "full", "--base-url", base_url,
                "--db", db, "--max-retries", "3",
            ])
            self.assertEqual(rc, 0)
            conn = sqlite3.connect(db)
            try:
                self.assertEqual(
                    conn.execute("SELECT COUNT(*) FROM events").fetchone()[0], 21
                )
            finally:
                conn.close()
        finally:
            server.shutdown()
            server.server_close()
            shutil.rmtree(tmpdir, ignore_errors=True)


def conn_count(db_path, where):
    conn = sqlite3.connect(db_path)
    try:
        return conn.execute("SELECT COUNT(*) FROM %s" % where).fetchone()[0]
    finally:
        conn.close()


if __name__ == "__main__":
    unittest.main()
