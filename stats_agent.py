#!/usr/bin/env python3
"""
Tiny HTTP server that serves `docker stats` as JSON.
Run on each instance machine:
  python3 stats_agent.py

Then tunnel the port back:
  ssh -L 9191:localhost:9091 zur2-s-d-114 -N
  ssh -L 9291:localhost:9091 zur2-s-d-100 -N

Set Stats URL in the dashboard to http://localhost:9191 / http://localhost:9291
"""
import json
import os
import subprocess
from http.server import BaseHTTPRequestHandler, HTTPServer


def _parse_mem_mb(s: str) -> float | None:
    s = s.strip().split('/')[0].strip()
    for suffix, mult in [('GiB', 1024), ('MiB', 1), ('GB', 1000), ('MB', 1), ('kB', 0.001)]:
        if s.endswith(suffix):
            try:
                return round(float(s[:-len(suffix)]) * mult, 1)
            except ValueError:
                pass
    return None


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            out = subprocess.check_output(
                ['docker', 'stats', '--no-stream', '--format', '{{json .}}'],
                stderr=subprocess.DEVNULL,
                timeout=15,
            )
            containers = []
            for line in out.decode().strip().split('\n'):
                if not line:
                    continue
                try:
                    c = json.loads(line)
                    containers.append({
                        'name': c.get('Name', ''),
                        'cpu_pct': float(c.get('CPUPerc', '0%').rstrip('%')),
                        'mem_mb': _parse_mem_mb(c.get('MemUsage', '')),
                    })
                except Exception:
                    pass
            body = json.dumps(containers).encode()
            self.send_response(200)
        except Exception as e:
            body = json.dumps({'error': str(e)}).encode()
            self.send_response(500)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_):
        pass


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 9091))
    print(f'Stats agent listening on :{port}')
    HTTPServer(('0.0.0.0', port), Handler).serve_forever()
