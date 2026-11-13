"""工单事件查询接口（提供数据的服务，采集侧不要改动本文件）。

接口：GET /events?page=<n>&page_size=<s>&since=<iso>
返回分页 JSON。接口对请求做了限流和临时错误，调用方需要自行退避重试。
"""
import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlparse

from .dataset import load_events

# 特定页的首次请求会被注入临时错误，重试后恢复，模拟真实网关波动。
# key=页码，value=首次返回的 HTTP 状态码。
_FIRST_HIT_ERROR = {2: 429, 3: 503}
_RETRY_AFTER = 1


class _Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def _write(self, status, payload, extra_headers=None):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        if extra_headers:
            for k, v in extra_headers.items():
                self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path != "/events":
            self._write(404, {"error": "not found"})
            return

        q = parse_qs(parsed.query)
        page = int(q.get("page", ["1"])[0])
        page_size = int(q.get("page_size", ["10"])[0])
        since = q.get("since", [None])[0]

        hit_count = self.server.hit_counter.get(page, 0)
        self.server.hit_counter[page] = hit_count + 1
        injected = _FIRST_HIT_ERROR.get(page)
        if hit_count == 0 and injected is not None:
            headers = {"Retry-After": str(_RETRY_AFTER)} if injected == 429 else None
            self._write(injected, {"error": "temporary, please retry"}, headers)
            return

        events = load_events()
        if since:
            events = [e for e in events if str(e.get("last_modified", "")) >= since]

        total_actual = len(events)
        start = (page - 1) * page_size
        end = start + page_size
        page_rows = events[start:end]

        # total 故意返回比实际多的数量，不能作为分页结束的唯一依据。
        # next_cursor 才反映真实的下一页情况。
        next_cursor = str(page + 1) if end < total_actual else None
        self._write(200, {
            "page": page,
            "page_size": page_size,
            "total": total_actual + 5,
            "events": page_rows,
            "next_cursor": next_cursor,
        })


def build_server(port=8090):
    server = HTTPServer(("127.0.0.1", port), _Handler)
    server.hit_counter = {}
    return server


if __name__ == "__main__":
    import sys
    p = int(sys.argv[1]) if len(sys.argv) > 1 else 8090
    srv = build_server(p)
    print("mock ticket API on http://127.0.0.1:%d/events" % p)
    srv.serve_forever()
