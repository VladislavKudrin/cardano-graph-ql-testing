#!/usr/bin/env python3
"""
GraphQL Performance Testing Dashboard — backend.

Each "instance" is one (locust process + GraphQL endpoint) pair.
Locust runs in web mode so we can drive it via its REST API.

Usage:
  pip install -r requirements.txt
  python server.py           # listens on http://0.0.0.0:5000
"""
from __future__ import annotations

import asyncio
import json
import os
import socket
import subprocess
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, Response
from pydantic import BaseModel

LOCUSTFILE = Path(__file__).parent / "locustfile.py"
UI_FILE = Path(__file__).parent / "ui.html"

instances: dict[str, dict[str, Any]] = {}
_poll_tasks: dict[str, asyncio.Task] = {}


# ── lifecycle ──────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(_app: FastAPI):
    yield
    for inst in list(instances.values()):
        _kill(inst)


app = FastAPI(title="graphql-perf", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── helpers ────────────────────────────────────────────────────────────────

def _find_port(start: int = 8089) -> int:
    p = start
    taken = {inst["port"] for inst in instances.values()}
    while p < 9200:
        if p not in taken:
            with socket.socket() as s:
                if s.connect_ex(("127.0.0.1", p)) != 0:
                    return p
        p += 1
    raise RuntimeError("No free port found")


def _kill(inst: dict) -> None:
    proc = inst.pop("process", None)
    if proc and proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            proc.kill()


def _safe(inst: dict) -> dict:
    return {k: v for k, v in inst.items() if k not in ("process",)}


def _get(id: str) -> dict:
    if id not in instances:
        raise HTTPException(404, "Instance not found")
    return instances[id]


async def _locust_get(port: int, path: str, timeout: float = 3) -> dict | None:
    try:
        async with httpx.AsyncClient() as c:
            r = await c.get(f"http://127.0.0.1:{port}{path}", timeout=timeout)
            return r.json()
    except Exception:
        return None


async def _locust_post(port: int, path: str, data: dict, timeout: float = 5) -> dict | None:
    try:
        async with httpx.AsyncClient() as c:
            r = await c.post(f"http://127.0.0.1:{port}{path}", data=data, timeout=timeout)
            return r.json()
    except Exception:
        return None


# ── background polling per instance ───────────────────────────────────────

async def _poll(id: str) -> None:
    while True:
        inst = instances.get(id)
        if not inst:
            break
        if inst["status"] not in ("running", "starting", "spawning"):
            break

        data = await _locust_get(inst["port"], "/stats/requests")
        if data:
            ts = time.time()
            state = data.get("state", "")
            if state == "stopped":
                inst["status"] = "stopped"
                break
            elif state in ("running", "spawning"):
                inst["status"] = state

            inst["users_current"] = data.get("user_count", 0)

            stats = data.get("stats", [])
            agg = next((s for s in stats if s.get("name") == "Aggregated"), None)
            per_query = [s for s in stats if s.get("name") != "Aggregated"]

            snap = {
                "ts": ts,
                "rps": data.get("total_rps", 0),
                "fail_ratio": round(data.get("fail_ratio", 0) * 100, 2),
                "users": data.get("user_count", 0),
                "p50": data.get("current_response_time_percentile_50") or 0,
                "p95": data.get("current_response_time_percentile_95") or 0,
                "agg": _fmt_stat(agg) if agg else None,
                "per_query": [_fmt_stat(s) for s in per_query],
            }
            inst["history"].append(snap)
            if len(inst["history"]) > 1800:   # 1h at 2s intervals
                inst["history"].pop(0)
            inst["last_snap"] = snap

        await asyncio.sleep(2)

    inst = instances.get(id)
    if inst and inst["status"] not in ("stopped", "idle", "error"):
        inst["status"] = "stopped"


def _fmt_stat(s: dict) -> dict:
    return {
        "name": s.get("name", ""),
        "method": s.get("method", ""),
        "reqs": s.get("num_requests", 0),
        "fails": s.get("num_failures", 0),
        "rps": round(s.get("current_rps", 0), 2),
        "avg": round(s.get("avg_response_time", 0)),
        "p50": s.get("50%") or 0,
        "p95": s.get("95%") or 0,
        "p99": s.get("99%") or 0,
        "min": round(s.get("min_response_time") or 0),
        "max": round(s.get("max_response_time") or 0),
    }


# ── routes ────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def ui() -> HTMLResponse:
    return HTMLResponse(UI_FILE.read_text())


class InstanceBody(BaseModel):
    name: str
    url: str


class StartBody(BaseModel):
    users: int = 5
    spawn_rate: float = 1.0
    run_time: str = ""          # e.g. "120s", "5m" — empty = run forever
    load_profile: str = "full"  # "light" or "full"
    payment_address: str = ""
    tx_hash: str = ""
    stake_address: str = ""


@app.get("/api/instances")
async def list_instances():
    return [_safe(inst) for inst in instances.values()]


@app.post("/api/instances", status_code=201)
async def create_instance(body: InstanceBody):
    id = str(uuid.uuid4())[:8]
    instances[id] = {
        "id": id,
        "name": body.name,
        "url": body.url.rstrip("/"),
        "port": _find_port(),
        "status": "idle",
        "process": None,
        "history": [],
        "last_snap": None,
        "users_current": 0,
        "started_at": None,
        "last_config": {},
    }
    return _safe(instances[id])


@app.patch("/api/instances/{id}")
async def update_instance(id: str, body: InstanceBody):
    inst = _get(id)
    if inst["status"] == "running":
        raise HTTPException(400, "Stop the test before editing")
    inst["name"] = body.name
    inst["url"] = body.url.rstrip("/")
    return _safe(inst)


@app.delete("/api/instances/{id}")
async def delete_instance(id: str):
    inst = _get(id)
    _kill(inst)
    task = _poll_tasks.pop(id, None)
    if task:
        task.cancel()
    del instances[id]
    return {"ok": True}


@app.post("/api/instances/{id}/start")
async def start_instance(id: str, body: StartBody):
    inst = _get(id)
    if inst["status"] in ("running", "spawning", "starting"):
        raise HTTPException(400, "Already running — stop first")

    _kill(inst)
    task = _poll_tasks.pop(id, None)
    if task:
        task.cancel()

    env = {**os.environ, "LOAD_PROFILE": body.load_profile}
    if body.payment_address:
        env["GQL_PAYMENT_ADDRESS"] = body.payment_address
    if body.tx_hash:
        env["GQL_TX_HASH"] = body.tx_hash
    if body.stake_address:
        env["GQL_STAKE_ADDRESS"] = body.stake_address

    cmd = [
        "locust",
        "-f", str(LOCUSTFILE),
        "--host", inst["url"],
        "--web-port", str(inst["port"]),
        "--web-host", "127.0.0.1",
        "--logfile", f"/tmp/locust-{id}.log",
    ]

    proc = subprocess.Popen(cmd, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    inst["process"] = proc
    inst["status"] = "starting"
    inst["history"] = []
    inst["last_snap"] = None
    inst["users_current"] = 0
    inst["started_at"] = time.time()
    inst["last_config"] = body.model_dump()

    asyncio.create_task(_do_start(id, body))
    return _safe(inst)


async def _do_start(id: str, body: StartBody) -> None:
    inst = instances.get(id)
    if not inst:
        return

    port = inst["port"]
    for _ in range(30):
        await asyncio.sleep(0.5)
        data = await _locust_get(port, "/", timeout=1)
        if data is not None or await _locust_get(port, "/stats/requests", timeout=1):
            break
    else:
        if id in instances:
            instances[id]["status"] = "error"
        return

    swarm = {"user_count": body.users, "spawn_rate": body.spawn_rate}
    if body.run_time:
        swarm["run_time"] = body.run_time
    await _locust_post(port, "/swarm", swarm)

    inst = instances.get(id)
    if inst:
        inst["status"] = "spawning"
        t = asyncio.create_task(_poll(id))
        _poll_tasks[id] = t


@app.post("/api/instances/{id}/stop")
async def stop_instance(id: str):
    inst = _get(id)
    port = inst["port"]

    await _locust_post(port, "/stop", {})
    await asyncio.sleep(0.5)
    _kill(inst)

    task = _poll_tasks.pop(id, None)
    if task:
        task.cancel()

    inst["status"] = "idle"
    inst["process"] = None
    return _safe(inst)


@app.post("/api/instances/{id}/reset")
async def reset_stats(id: str):
    inst = _get(id)
    await _locust_get(inst["port"], "/stats/reset")
    inst["history"] = []
    inst["last_snap"] = None
    return {"ok": True}


@app.get("/api/instances/{id}")
async def get_instance(id: str):
    return _safe(_get(id))


@app.get("/api/instances/{id}/metrics")
async def get_metrics(id: str, tail: int = 150):
    inst = _get(id)
    h = inst["history"]
    return {
        "history": h[-tail:],
        "last_snap": inst["last_snap"],
        "status": inst["status"],
        "users_current": inst["users_current"],
        "started_at": inst["started_at"],
    }


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 5000))
    uvicorn.run("server:app", host="0.0.0.0", port=port, reload=True)
