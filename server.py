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
import sqlite3
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
DB_PATH = Path(__file__).parent / "instances.db"

instances: dict[str, dict[str, Any]] = {}
_poll_tasks: dict[str, asyncio.Task] = {}
_docker_tasks: dict[str, asyncio.Task] = {}

pipeline: dict[str, Any] = {
    "running": False,
    "task": None,
    "entries": [],
    "known": set(),
    "initialized": False,  # first poll baselines known without creating entries
    "session_id": None,
}

_ASSET_CHECK_QUERY = """query($fp: AssetFingerprint!) {
  assets(where: { fingerprint: { _eq: $fp } }) {
    fingerprint policyId name description metadataHash
  }
}"""


# ── sqlite helpers ─────────────────────────────────────────────────────────

def _db_init() -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS instances (
                id       TEXT PRIMARY KEY,
                name     TEXT NOT NULL,
                url      TEXT NOT NULL,
                port     INTEGER NOT NULL,
                stats_url TEXT DEFAULT ''
            )
        """)
        try:
            conn.execute("ALTER TABLE instances ADD COLUMN stats_url TEXT DEFAULT ''")
        except Exception:
            pass
        conn.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_sessions (
                id TEXT PRIMARY KEY,
                started_at REAL NOT NULL,
                stopped_at REAL,
                instances_json TEXT NOT NULL DEFAULT '{}'
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_entries (
                session_id TEXT NOT NULL,
                asset_id TEXT NOT NULL,
                policy_id TEXT DEFAULT '',
                asset_name TEXT DEFAULT '',
                fingerprint TEXT,
                tx_hash TEXT DEFAULT '',
                included_at TEXT DEFAULT '',
                detected_at REAL NOT NULL,
                instances_json TEXT NOT NULL DEFAULT '{}',
                PRIMARY KEY (session_id, asset_id)
            )
        """)


def _db_save(inst: dict) -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO instances (id, name, url, port, stats_url) VALUES (?, ?, ?, ?, ?)",
            (inst["id"], inst["name"], inst["url"], inst["port"], inst.get("stats_url", "")),
        )


def _db_delete(id: str) -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("DELETE FROM instances WHERE id = ?", (id,))


def _db_load() -> list[dict]:
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        return [dict(r) for r in conn.execute("SELECT * FROM instances ORDER BY rowid")]


def _db_session_create(session_id: str, instance_names: dict) -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "INSERT INTO pipeline_sessions (id, started_at, instances_json) VALUES (?, ?, ?)",
            (session_id, time.time(), json.dumps(instance_names)),
        )


def _db_session_stop(session_id: str) -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("UPDATE pipeline_sessions SET stopped_at=? WHERE id=?",
                     (time.time(), session_id))


def _db_entry_upsert(session_id: str, entry: dict) -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """INSERT OR REPLACE INTO pipeline_entries
               (session_id, asset_id, policy_id, asset_name, fingerprint, tx_hash, included_at, detected_at, instances_json)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (session_id, entry["asset_id"], entry.get("policy_id", ""), entry.get("asset_name", ""),
             entry.get("fingerprint"), entry.get("tx_hash", ""), entry.get("included_at", ""),
             entry["detected_at"], json.dumps(entry.get("instances", {}))),
        )


# ── lifecycle ──────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(_app: FastAPI):
    _db_init()
    for row in _db_load():
        instances[row["id"]] = {
            "id": row["id"],
            "name": row["name"],
            "url": row["url"],
            "port": row["port"],
            "status": "idle",
            "process": None,
            "history": [],
            "last_snap": None,
            "users_current": 0,
            "started_at": None,
            "last_config": {},
            "failures": [],
            "ram_mb": None,
            "cpu_pct": None,
            "cpu_prev": None,
            "cpu_prev_ts": None,
            "stats_url": row.get("stats_url", ""),
            "docker_stats": [],
            "last_seen_id": 0,
            "inst_initialized": False,
        }
    yield
    for inst in list(instances.values()):
        _kill(inst)
    if pipeline["task"]:
        pipeline["task"].cancel()


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


async def _fetch_status(url: str) -> dict | None:
    try:
        async with httpx.AsyncClient() as c:
            r = await c.get(f"{url}/api/status", timeout=3)
            if r.status_code == 200:
                return r.json()
    except Exception:
        return None


def _parse_mem_mb(s: str) -> float | None:
    s = s.strip().split('/')[0].strip()
    for suffix, mult in [('GiB', 1024), ('MiB', 1), ('gib', 1024), ('mib', 1), ('GB', 1000), ('MB', 1), ('kB', 0.001)]:
        if s.endswith(suffix):
            try:
                return round(float(s[:-len(suffix)]) * mult, 1)
            except ValueError:
                pass
    return None


async def _fetch_docker_stats(stats_url: str) -> list[dict]:
    try:
        async with httpx.AsyncClient() as c:
            r = await c.get(stats_url.rstrip('/'), timeout=20)
            if r.status_code == 200:
                return r.json()
    except Exception as e:
        print(f"[docker-stats] {stats_url}: {type(e).__name__}")
    return []


async def _poll_docker_stats(id: str) -> None:
    while True:
        await asyncio.sleep(10)
        inst = instances.get(id)
        if not inst or inst["status"] not in ("running", "starting", "spawning"):
            break
        if inst.get("stats_url"):
            stats = await _fetch_docker_stats(inst["stats_url"])
            if stats:
                inst["docker_stats"] = stats


async def _fetch_multi_asset_max_id(url: str) -> int | None:
    try:
        async with httpx.AsyncClient() as c:
            r = await c.get(f"{url}/api/multi-asset/max-id", timeout=10)
            if r.status_code == 200:
                return int(r.json().get("max_id", 0))
    except Exception as e:
        print(f"[pipeline] max-id fetch error from {url}: {type(e).__name__}")
    return None


async def _fetch_multi_asset(url: str, after_id: int) -> dict | None:
    try:
        async with httpx.AsyncClient() as c:
            r = await c.get(f"{url}/api/multi-asset/recent", params={"afterId": after_id}, timeout=10)
            if r.status_code == 200:
                return r.json()
    except Exception as e:
        print(f"[pipeline] multi-asset fetch error from {url}: {type(e).__name__}")
    return None


# ── background polling per instance ───────────────────────────────────────

async def _poll(id: str) -> None:
    while True:
        inst = instances.get(id)
        if not inst:
            break
        if inst["status"] not in ("running", "starting", "spawning"):
            break

        data, failures_data, status_data = await asyncio.gather(
            _locust_get(inst["port"], "/stats/requests"),
            _locust_get(inst["port"], "/stats/failures"),
            _fetch_status(inst["url"]),
        )
        if status_data:
            rss = (status_data.get("memory") or {}).get("rss", 0)
            inst["ram_mb"] = round(rss / 1024 / 1024, 1) if rss else None
            cpu = status_data.get("cpu") or {}
            if cpu:
                total_us = cpu.get("user", 0) + cpu.get("system", 0)
                prev = inst.get("cpu_prev")
                prev_ts = inst.get("cpu_prev_ts")
                now_ts = time.time()
                if prev is not None and prev_ts is not None:
                    delta_us = total_us - prev
                    elapsed_us = (now_ts - prev_ts) * 1e6
                    if elapsed_us > 0:
                        inst["cpu_pct"] = round(delta_us / elapsed_us * 100, 1)
                inst["cpu_prev"] = total_us
                inst["cpu_prev_ts"] = now_ts

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
            per_query = [s for s in stats if s.get("name") != "Aggregated" and not s.get("name", "").startswith("~")]

            failures = []
            if failures_data:
                for f in failures_data.get("failures", []):
                    failures.append({
                        "name": f.get("name", ""),
                        "method": f.get("method", ""),
                        "error": f.get("error", ""),
                        "occurrences": f.get("occurrences", 0),
                    })

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
            if len(inst["history"]) > 1800:
                inst["history"].pop(0)
            inst["last_snap"] = snap
            inst["failures"] = failures

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


# ── pipeline monitor ──────────────────────────────────────────────────────

async def _gql(url: str, query: str, variables: dict | None = None) -> dict | None:
    try:
        async with httpx.AsyncClient() as c:
            r = await c.post(
                f"{url}/graphql",
                json={"query": query, "variables": variables or {}},
                timeout=60,
            )
            data = r.json()
            if data.get("errors"):
                print(f"[pipeline] GQL error from {url}: {data['errors'][0].get('message')}")
                return None
            return data.get("data")
    except Exception as e:
        print(f"[pipeline] GQL exception from {url}: {type(e).__name__}")
        return None


async def _pipeline_loop() -> None:
    while pipeline["running"]:
        current = list(instances.values())
        if current:
            await asyncio.gather(*[_instance_tick(inst) for inst in current])
        await asyncio.sleep(2)


async def _instance_tick(inst: dict) -> None:
    iid = inst["id"]

    # Step 1: on first tick, establish baseline cursor at current max multi_asset id
    if not inst.get("inst_initialized"):
        max_id = await _fetch_multi_asset_max_id(inst["url"])
        if max_id is not None:
            inst["last_seen_id"] = max_id
            inst["inst_initialized"] = True
            if not pipeline["initialized"]:
                pipeline["initialized"] = True
            print(f"[pipeline] {inst['name']}: baseline at multi_asset id={max_id}")
        return

    # Step 2: detect new assets at db-sync (multi_asset) level
    data = await _fetch_multi_asset(inst["url"], inst.get("last_seen_id", 0))
    if data:
        assets = data.get("assets", [])
        for asset in assets:
            new_id = asset.get("id", 0)
            if new_id > inst.get("last_seen_id", 0):
                inst["last_seen_id"] = new_id
            fingerprint = asset.get("fingerprint")
            if not fingerprint or fingerprint in pipeline["known"]:
                continue
            pipeline["known"].add(fingerprint)
            now = time.time()
            entry: dict[str, Any] = {
                "asset_id": fingerprint,
                "fingerprint": fingerprint,
                "policy_id": asset.get("policy_id", ""),
                "asset_name": asset.get("asset_name_hex", ""),
                "tx_hash": "",
                "included_at": "",
                "detected_at": now,
                "instances": {},
            }
            pipeline["entries"].insert(0, entry)
            if len(pipeline["entries"]) > 200:
                pipeline["entries"].pop()
            if pipeline.get("session_id"):
                _db_entry_upsert(pipeline["session_id"], entry)

    # Step 3: check pending entries for THIS instance
    now = time.time()
    for entry in pipeline["entries"][:50]:
        if iid not in entry["instances"]:
            entry["instances"][iid] = {
                "name": inst["name"],
                "asset_appeared_at": None,
                "metadata_appeared_at": None,
                "metadata_hash": None,
                "metadata_updates": 0,
            }
        idata = entry["instances"][iid]
        if idata["asset_appeared_at"] and idata["metadata_appeared_at"] is not None:
            continue
        check = await _gql(inst["url"], _ASSET_CHECK_QUERY, {"fp": entry["asset_id"]})
        if not check:
            continue
        assets_list = check.get("assets", [])
        changed = False
        if assets_list:
            a = assets_list[0]
            if idata["asset_appeared_at"] is None:
                idata["asset_appeared_at"] = now
                changed = True
            meta_hash = a.get("metadataHash")
            has_meta = bool(a.get("name") or a.get("description"))
            if has_meta:
                if idata["metadata_appeared_at"] is None:
                    idata["metadata_appeared_at"] = now
                    idata["metadata_hash"] = meta_hash
                    changed = True
                elif meta_hash and meta_hash != idata.get("metadata_hash"):
                    idata["metadata_hash"] = meta_hash
                    idata["metadata_updates"] = idata.get("metadata_updates", 0) + 1
                    changed = True
        if changed and pipeline.get("session_id"):
            _db_entry_upsert(pipeline["session_id"], entry)


# ── routes ────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def ui() -> HTMLResponse:
    return HTMLResponse(UI_FILE.read_text())


class InstanceBody(BaseModel):
    name: str
    url: str
    stats_url: str = ""


ALL_GROUPS = ["general", "assets", "transactions", "addresses", "staking"]


class StartBody(BaseModel):
    users: int = 5
    spawn_rate: float = 1.0
    run_time: str = ""
    query_groups: list[str] = ALL_GROUPS


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
        "failures": [],
        "ram_mb": None,
        "cpu_pct": None,
        "cpu_prev": None,
        "cpu_prev_ts": None,
        "stats_url": body.stats_url.strip(),
        "docker_stats": [],
        "last_seen_id": 0,
        "inst_initialized": False,
    }
    _db_save(instances[id])
    return _safe(instances[id])


@app.patch("/api/instances/{id}")
async def update_instance(id: str, body: InstanceBody):
    inst = _get(id)
    if inst["status"] == "running":
        raise HTTPException(400, "Stop the test before editing")
    inst["name"] = body.name
    inst["url"] = body.url.rstrip("/")
    inst["stats_url"] = body.stats_url.strip()
    _db_save(inst)
    return _safe(inst)


@app.delete("/api/instances/{id}")
async def delete_instance(id: str):
    inst = _get(id)
    _kill(inst)
    task = _poll_tasks.pop(id, None)
    if task:
        task.cancel()
    dt = _docker_tasks.pop(id, None)
    if dt:
        dt.cancel()
    del instances[id]
    _db_delete(id)
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

    groups = body.query_groups or ALL_GROUPS
    env = {**os.environ, "QUERY_GROUPS": ",".join(groups)}

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
    inst["failures"] = []
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
        if await _locust_get(port, "/stats/requests", timeout=1) is not None:
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
        if inst.get("stats_url"):
            dt = asyncio.create_task(_poll_docker_stats(id))
            _docker_tasks[id] = dt


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
    dt = _docker_tasks.pop(id, None)
    if dt:
        dt.cancel()

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
        "failures": inst.get("failures", []),
        "ram_mb": inst.get("ram_mb"),
        "cpu_pct": inst.get("cpu_pct"),
        "docker_stats": inst.get("docker_stats", []),
    }


@app.get("/api/pipeline")
async def pipeline_get():
    entries = pipeline["entries"][:100]
    now = time.time()

    # Compute per-instance summary stats
    summary: dict[str, dict] = {}
    for inst in instances.values():
        iid = inst["id"]
        asset_lags = []
        meta_lags = []
        instant = 0
        pending_asset = 0
        pending_meta = 0
        for e in entries:
            idata = (e.get("instances") or {}).get(iid)
            if not idata:
                continue
            det = e.get("detected_at", now)
            if idata["asset_appeared_at"]:
                lag = idata["asset_appeared_at"] - det
                if lag < 1:
                    instant += 1
                else:
                    asset_lags.append(lag)
                if idata["metadata_appeared_at"]:
                    meta_lags.append(idata["metadata_appeared_at"] - det)
                else:
                    pending_meta += 1
            else:
                pending_asset += 1
        summary[iid] = {
            "name": inst["name"],
            "total": len([e for e in entries if iid in (e.get("instances") or {})]),
            "asset_avg": round(sum(asset_lags) / len(asset_lags), 1) if asset_lags else None,
            "meta_avg": round(sum(meta_lags) / len(meta_lags), 1) if meta_lags else None,
            "asset_resolved": len(asset_lags),
            "meta_resolved": len(meta_lags),
            "instant": instant,
            "pending_asset": pending_asset,
            "pending_meta": pending_meta,
        }

    return {
        "running": pipeline["running"],
        "initialized": pipeline["initialized"],
        "entries": entries,
        "summary": summary,
    }


@app.post("/api/pipeline/start")
async def pipeline_start():
    if pipeline["running"]:
        return {"ok": True}
    session_id = str(uuid.uuid4())[:8]
    pipeline["session_id"] = session_id
    _db_session_create(session_id, {iid: inst["name"] for iid, inst in instances.items()})
    pipeline["running"] = True
    pipeline["task"] = asyncio.create_task(_pipeline_loop())
    return {"ok": True}


@app.post("/api/pipeline/stop")
async def pipeline_stop():
    pipeline["running"] = False
    if pipeline["task"]:
        pipeline["task"].cancel()
        pipeline["task"] = None
    if pipeline["session_id"]:
        _db_session_stop(pipeline["session_id"])
    pipeline["session_id"] = None
    return {"ok": True}


@app.post("/api/pipeline/clear")
async def pipeline_clear():
    pipeline["entries"] = []
    pipeline["known"] = set()
    pipeline["initialized"] = False
    pipeline["session_id"] = None
    for inst in instances.values():
        inst["inst_initialized"] = False
        inst["last_seen_id"] = 0
    return {"ok": True}


@app.get("/api/pipeline/sessions")
async def pipeline_sessions_list():
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT s.id, s.started_at, s.stopped_at, s.instances_json, COUNT(e.asset_id) as entry_count "
            "FROM pipeline_sessions s LEFT JOIN pipeline_entries e ON e.session_id = s.id "
            "GROUP BY s.id ORDER BY s.started_at DESC LIMIT 50"
        ).fetchall()
    return [
        {
            "id": r["id"],
            "started_at": r["started_at"],
            "stopped_at": r["stopped_at"],
            "instances": json.loads(r["instances_json"]),
            "entry_count": r["entry_count"],
        }
        for r in rows
    ]


@app.get("/api/pipeline/sessions/{sid}")
async def pipeline_session_detail(sid: str):
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        sess = conn.execute("SELECT * FROM pipeline_sessions WHERE id=?", (sid,)).fetchone()
        if not sess:
            raise HTTPException(404, "Session not found")
        entries = conn.execute(
            "SELECT * FROM pipeline_entries WHERE session_id=? ORDER BY detected_at DESC",
            (sid,)
        ).fetchall()
    return {
        "id": sess["id"],
        "started_at": sess["started_at"],
        "stopped_at": sess["stopped_at"],
        "instances": json.loads(sess["instances_json"]),
        "entries": [
            {
                "asset_id": e["asset_id"],
                "policy_id": e["policy_id"],
                "asset_name": e["asset_name"],
                "fingerprint": e["fingerprint"],
                "tx_hash": e["tx_hash"],
                "included_at": e["included_at"],
                "detected_at": e["detected_at"],
                "instances": json.loads(e["instances_json"]),
            }
            for e in entries
        ],
    }


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 5000))
    uvicorn.run("server:app", host="0.0.0.0", port=port, reload=True)
