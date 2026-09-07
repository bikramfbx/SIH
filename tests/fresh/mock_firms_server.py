"""Tiny FIRMS-area-API mock for offline end-to-end tests.

Serves CSV fixtures at the same route shape firms.py expects:
    GET /{map_key}/{source}/{bbox}/{days}
    GET /{map_key}/{source}/{bbox}/{days}/{start_date}

Behaviour switches (env MOCK_MODE, on the mock container):
    ok      normal: serve matching fixture or empty-header CSV
    fail500 always return 500 (ingestion must degrade gracefully and record
            a failed run, worker continues, health shows the error)
    slow    sleep 20s per request (timeout handling at the client)
"""

import os
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

FIXTURES_DIR = os.getenv("FIXTURES_DIR", "/fixtures")
MODE = os.getenv("MOCK_MODE", "ok")

EMPTY_HEADER = ("latitude,longitude,acq_date,acq_time,satellite,instrument,"
                "confidence,version,bright_ti4,bright_ti5,frp,daynight\r\n")


def _fixture_for(path):
    parts = [p for p in path.strip("/").split("/") if p]
    if len(parts) < 4:
        return None
    map_key, source, bbox, days = parts[:4]
    candidate = os.path.join(
        FIXTURES_DIR, f"{source}_{bbox}_{days}.csv".replace(",", "_"))
    if os.path.exists(candidate):
        with open(candidate, "rb") as fh:
            return fh.read()
    return EMPTY_HEADER.encode()


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if MODE == "slow":
            time.sleep(20)
        if MODE == "fail500":
            self.send_response(500)
            self.end_headers()
            self.wfile.write(b"mock: forced failure")
            return
        body = _fixture_for(self.path)
        if body is None:
            self.send_response(404)
            self.end_headers()
            return
        self.send_response(200)
        self.send_header("Content-Type", "text/csv")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        print("MOCKFIRMS", self.path, fmt % args, flush=True)


def main():
    port = int(os.getenv("PORT", "8100"))
    print(f"mock firms server on :{port} mode={MODE}", flush=True)
    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()


if __name__ == "__main__":
    main()