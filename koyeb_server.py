"""
koyeb_server.py
Minimal HTTP health-check server required by Koyeb's port binding check.
Runs alongside the bot in the same process via threading.
"""
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(b"OK")

    def log_message(self, *_):
        pass  # silence request logs


def start_health_server(port: int = 8000) -> None:
    """Start a non-blocking health-check HTTP server on *port*."""
    server = HTTPServer(("0.0.0.0", port), _Handler)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
