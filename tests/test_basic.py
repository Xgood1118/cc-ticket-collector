"""基础环境测试：mock 接口可启动、数据集与库表结构可用。"""
import json
import os
import sqlite3
import sys
import threading
import unittest
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from mock_api.server import build_server  # noqa: E402


def _start_server():
    server = build_server(0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, server.server_address[1]


class BasicTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server, cls.port = _start_server()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()

    def _get(self, url):
        with urllib.request.urlopen("http://127.0.0.1:%d%s" % (self.port, url)) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def test_first_page_returns_events(self):
        data = self._get("/events?page=1&page_size=10")
        self.assertEqual(data["page"], 1)
        self.assertEqual(len(data["events"]), 10)
        self.assertIsNotNone(data["next_cursor"])

    def test_dataset_count(self):
        from mock_api.dataset import load_events
        self.assertEqual(len(load_events()), 24)

    def test_schema_initializes(self):
        schema_path = os.path.join(ROOT, "collector", "schema.sql")
        with open(schema_path, encoding="utf-8") as fh:
            conn = sqlite3.connect(":memory:")
            conn.executescript(fh.read())
        tables = {
            r[0]
            for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        self.assertTrue({"events", "quarantine", "sync_state"}.issubset(tables))


if __name__ == "__main__":
    unittest.main()
