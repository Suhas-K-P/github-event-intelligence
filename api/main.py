"""
api/main.py
────────────
FastAPI Application — REST API + WebSocket Gateway

Endpoints
─────────
  GET  /                         → serve dashboard HTML
  GET  /api/events               → paginated event list
  GET  /api/events/{id}          → single event detail
  GET  /api/stats                → global statistics
  GET  /api/stats/hourly         → events per hour (last 24 h)
  GET  /api/stats/event-types    → event type distribution
  GET  /api/trending             → top trending repos
  GET  /api/nlp/keywords         → top keywords
  GET  /api/nlp/sentiment        → sentiment breakdown
  GET  /api/contributors         → top contributors
  WS   /ws                       → real-time event stream

Real-time flow
──────────────
  Redis Pub/Sub  →  background task  →  WebSocket broadcast  →  Dashboard
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import redis.asyncio as aioredis
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import config
from api import database as db
from api.models import (
    EventSummary, EventDetail, GlobalStats, EventTypeCount,
    HourlyPoint, TrendingRepo, KeywordFreq, SentimentBreakdown,
    ContributorStat, PaginatedEvents, WSMessage,
)

log = logging.getLogger("api")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [API]  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# ─── App Setup ────────────────────────────────────────────────────────────────
app = FastAPI(
    title="GitHub Event Intelligence API",
    description="Real-time GitHub event analytics with NLP enrichment",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─── WebSocket Connection Manager ────────────────────────────────────────────
class ConnectionManager:
    """Manages all active WebSocket connections and broadcast."""

    def __init__(self) -> None:
        self._connections: list[WebSocket] = []
        self._lock = asyncio.Lock()

    async def connect(self, ws: WebSocket) -> None:
        await ws.accept()
        async with self._lock:
            self._connections.append(ws)
        log.info("WS connected — total clients: %d", self.count)

    async def disconnect(self, ws: WebSocket) -> None:
        async with self._lock:
            if ws in self._connections:
                self._connections.remove(ws)
        log.info("WS disconnected — total clients: %d", self.count)

    @property
    def count(self) -> int:
        return len(self._connections)

    async def broadcast(self, message: str) -> None:
        """Send to all connected clients; silently drop broken connections."""
        dead: list[WebSocket] = []
        for ws in list(self._connections):
            try:
                await ws.send_text(message)
            except Exception:
                dead.append(ws)
        for ws in dead:
            await self.disconnect(ws)

    async def send_personal(self, ws: WebSocket, message: str) -> None:
        try:
            await ws.send_text(message)
        except Exception:
            await self.disconnect(ws)


manager = ConnectionManager()


# ─── Redis Pub/Sub Listener ───────────────────────────────────────────────────
_redis_client: Optional[aioredis.Redis] = None


async def get_redis() -> aioredis.Redis:
    global _redis_client
    if _redis_client is None:
        _redis_client = aioredis.Redis(
            host=config.REDIS_HOST,
            port=config.REDIS_PORT,
            db=config.REDIS_DB,
            password=config.REDIS_PASSWORD,
            decode_responses=True,
        )
    return _redis_client


async def redis_pubsub_listener() -> None:
    """
    Background task: subscribes to Redis pub/sub channel and broadcasts
    every incoming message to all connected WebSocket clients.
    """
    backoff = 1
    while True:
        try:
            r = await get_redis()
            pubsub = r.pubsub()
            await pubsub.subscribe(config.PUBSUB_LIVE_EVENTS)
            log.info("Redis pub/sub listening on '%s'", config.PUBSUB_LIVE_EVENTS)
            backoff = 1

            async for message in pubsub.listen():
                if message["type"] != "message":
                    continue
                raw = message["data"]

                # Wrap in typed WS message envelope
                envelope = json.dumps({
                    "msg_type":  "event",
                    "data":      json.loads(raw),
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                })
                await manager.broadcast(envelope)

        except Exception as exc:
            log.warning("Pub/sub error (%s) — retrying in %ds", exc, backoff * 3)
            await asyncio.sleep(backoff * 3)
            backoff = min(backoff * 2, 30)


async def stats_broadcaster() -> None:
    """Push global stats to all WS clients every 10 seconds."""
    while True:
        await asyncio.sleep(10)
        if manager.count == 0:
            continue
        try:
            stats = await _build_stats()
            envelope = json.dumps({
                "msg_type":  "stats",
                "data":      stats,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            })
            await manager.broadcast(envelope)
        except Exception as exc:
            log.debug("Stats broadcast error: %s", exc)


# ─── App Lifecycle ────────────────────────────────────────────────────────────
@app.on_event("startup")
async def startup() -> None:
    log.info("═" * 60)
    log.info("  GitHub Event Intelligence — API Service  v1.0")
    log.info("  Dashboard : http://%s:%s", config.API_HOST, config.API_PORT)
    log.info("  API Docs  : http://%s:%s/docs", config.API_HOST, config.API_PORT)
    log.info("═" * 60)
    # Launch background tasks
    asyncio.create_task(redis_pubsub_listener())
    asyncio.create_task(stats_broadcaster())


# ─── Dashboard Route ──────────────────────────────────────────────────────────
@app.get("/", response_class=HTMLResponse, tags=["Dashboard"])
async def dashboard() -> HTMLResponse:
    """Serve the main dashboard HTML page."""
    dashboard_path = Path(__file__).parent.parent / "dashboard" / "index.html"
    if dashboard_path.exists():
        return HTMLResponse(content=dashboard_path.read_text(encoding="utf-8"))
    return HTMLResponse("<h1>Dashboard not found. Place dashboard/index.html.</h1>")


# ─── WebSocket Endpoint ───────────────────────────────────────────────────────
@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket) -> None:
    await manager.connect(ws)
    try:
        # Send immediate stats snapshot on connect
        stats = await _build_stats()
        await manager.send_personal(ws, json.dumps({
            "msg_type":  "stats",
            "data":      stats,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }))
        # Keep alive — echo any pings
        while True:
            data = await ws.receive_text()
            if data == "ping":
                await manager.send_personal(ws, json.dumps({
                    "msg_type": "ping", "data": "pong",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }))
    except WebSocketDisconnect:
        await manager.disconnect(ws)


# ─── Helper: Build Stats ──────────────────────────────────────────────────────
async def _build_stats() -> dict:
    try:
        r = await get_redis()
        redis_stats = await r.hgetall(config.KEY_STATS)
        queue_depth = await r.llen(config.QUEUE_RAW_EVENTS)
    except Exception:
        redis_stats = {}
        queue_depth = 0

    # DB aggregates
    row = db.fetchone("""
        SELECT
          COUNT(*)                                       AS total_events,
          COUNT(DISTINCT repo_name)                      AS unique_repos,
          COUNT(DISTINCT actor_login)                    AS unique_actors,
          SUM(sentiment_label = 'positive')              AS pos_count,
          SUM(sentiment_label = 'negative')              AS neg_count,
          SUM(sentiment_label = 'neutral')               AS neu_count,
          SUM(created_at >= DATE_SUB(NOW(), INTERVAL 1 HOUR))  AS last_hour,
          SUM(created_at >= DATE_SUB(NOW(), INTERVAL 24 HOUR)) AS last_24h
        FROM events
    """) or {}

    total = max(int(row.get("total_events") or 0), 1)
    pos   = int(row.get("pos_count") or 0)
    neg   = int(row.get("neg_count") or 0)
    neu   = int(row.get("neu_count") or 0)

    return {
        "total_events":     int(row.get("total_events")  or 0),
        "total_processed":  int(redis_stats.get("total_processed", 0)),
        "unique_repos":     int(row.get("unique_repos")  or 0),
        "unique_actors":    int(row.get("unique_actors") or 0),
        "positive_pct":     round(pos / total * 100, 1),
        "negative_pct":     round(neg / total * 100, 1),
        "neutral_pct":      round(neu / total * 100, 1),
        "events_last_hour": int(row.get("last_hour")     or 0),
        "events_last_24h":  int(row.get("last_24h")      or 0),
        "ws_clients":       manager.count,
        "queue_depth":      queue_depth,
        "last_processed":   redis_stats.get("last_processed"),
    }


# ─── REST Endpoints ───────────────────────────────────────────────────────────

@app.get("/api/stats", response_model=GlobalStats, tags=["Stats"])
async def get_stats() -> dict:
    """Global system statistics."""
    return await _build_stats()


@app.get("/api/stats/hourly", response_model=list[HourlyPoint], tags=["Stats"])
def get_hourly_stats(hours: int = Query(24, ge=1, le=168)) -> list[dict]:
    """Events per hour for the last N hours (default 24)."""
    rows = db.fetchall("""
        SELECT
          DATE_FORMAT(created_at, '%%Y-%%m-%%d %%H:00') AS hour,
          COUNT(*) AS count
        FROM events
        WHERE created_at >= DATE_SUB(NOW(), INTERVAL %s HOUR)
        GROUP BY hour
        ORDER BY hour ASC
    """, (hours,))
    return rows


@app.get("/api/stats/event-types", response_model=list[EventTypeCount], tags=["Stats"])
def get_event_type_dist(hours: int = Query(24, ge=1, le=168)) -> list[dict]:
    """Event type distribution for the last N hours."""
    rows = db.fetchall("""
        SELECT event_type,
               COUNT(*) AS count,
               ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct
        FROM events
        WHERE created_at >= DATE_SUB(NOW(), INTERVAL %s HOUR)
        GROUP BY event_type
        ORDER BY count DESC
    """, (hours,))
    return rows


@app.get("/api/events", response_model=PaginatedEvents, tags=["Events"])
def list_events(
    page:       int = Query(1,  ge=1),
    limit:      int = Query(20, ge=1, le=100),
    event_type: Optional[str] = None,
    category:   Optional[str] = None,
    sentiment:  Optional[str] = None,
    hours:      int = Query(24, ge=1, le=168),
) -> dict:
    """Paginated events list with optional filters."""
    offset = (page - 1) * limit

    where_clauses = ["created_at >= DATE_SUB(NOW(), INTERVAL %s HOUR)"]
    params: list[Any] = [hours]

    if event_type:
        where_clauses.append("event_type = %s")
        params.append(event_type)
    if category:
        where_clauses.append("category = %s")
        params.append(category)
    if sentiment:
        where_clauses.append("sentiment_label = %s")
        params.append(sentiment)

    where_sql = " AND ".join(where_clauses)

    count_row = db.fetchone(f"SELECT COUNT(*) AS cnt FROM events WHERE {where_sql}", tuple(params))
    total = int((count_row or {}).get("cnt", 0))

    rows = db.fetchall(f"""
        SELECT id, github_event_id, event_type, actor_login, actor_avatar,
               repo_name, repo_url, created_at, sentiment_score, sentiment_label,
               keywords, category, language, impact_score, commit_message
        FROM events
        WHERE {where_sql}
        ORDER BY created_at DESC
        LIMIT %s OFFSET %s
    """, tuple(params) + (limit, offset))

    # Decode JSON fields returned as strings
    for row in rows:
        if isinstance(row.get("keywords"), str):
            try:
                row["keywords"] = json.loads(row["keywords"])
            except Exception:
                row["keywords"] = []

    return {"total": total, "page": page, "limit": limit, "items": rows}


@app.get("/api/events/{event_id}", response_model=EventDetail, tags=["Events"])
def get_event(event_id: int) -> dict:
    """Single event detail including raw payload."""
    row = db.fetchone("SELECT * FROM events WHERE id = %s", (event_id,))
    if not row:
        raise HTTPException(status_code=404, detail="Event not found")
    if isinstance(row.get("keywords"), str):
        try:
            row["keywords"] = json.loads(row["keywords"])
        except Exception:
            row["keywords"] = []
    if isinstance(row.get("payload"), str):
        try:
            row["payload"] = json.loads(row["payload"])
        except Exception:
            pass
    return row


@app.get("/api/trending", response_model=list[TrendingRepo], tags=["Trending"])
def get_trending(limit: int = Query(10, ge=1, le=50)) -> list[dict]:
    """Top repositories by event count in the last 24 hours."""
    return db.fetchall("""
        SELECT r.repo_name, r.repo_url, r.total_events,
               r.push_count, r.star_count, r.fork_count,
               r.pr_count, r.primary_language, r.avg_sentiment, r.last_seen
        FROM trending_repos r
        WHERE r.last_seen >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
        ORDER BY r.total_events DESC
        LIMIT %s
    """, (limit,))


@app.get("/api/nlp/keywords", response_model=list[KeywordFreq], tags=["NLP"])
def get_keywords(limit: int = Query(20, ge=1, le=100),
                 category: Optional[str] = None) -> list[dict]:
    """Most frequent keywords extracted by the NLP pipeline."""
    if category:
        return db.fetchall("""
            SELECT keyword, frequency, category FROM keyword_freq
            WHERE category = %s
            ORDER BY frequency DESC LIMIT %s
        """, (category, limit))
    return db.fetchall("""
        SELECT keyword, frequency, category FROM keyword_freq
        ORDER BY frequency DESC LIMIT %s
    """, (limit,))


@app.get("/api/nlp/sentiment", response_model=SentimentBreakdown, tags=["NLP"])
def get_sentiment(hours: int = Query(24, ge=1, le=168)) -> dict:
    """Sentiment distribution for the last N hours."""
    row = db.fetchone("""
        SELECT
          SUM(sentiment_label = 'positive') AS positive,
          SUM(sentiment_label = 'neutral')  AS neutral,
          SUM(sentiment_label = 'negative') AS negative,
          COUNT(*) AS total
        FROM events
        WHERE created_at >= DATE_SUB(NOW(), INTERVAL %s HOUR)
    """, (hours,)) or {}

    total = max(int(row.get("total") or 0), 1)
    pos   = int(row.get("positive") or 0)
    neu   = int(row.get("neutral")  or 0)
    neg   = int(row.get("negative") or 0)

    return {
        "positive":     pos,
        "neutral":      neu,
        "negative":     neg,
        "positive_pct": round(pos / total * 100, 1),
        "neutral_pct":  round(neu / total * 100, 1),
        "negative_pct": round(neg / total * 100, 1),
    }


@app.get("/api/contributors", response_model=list[ContributorStat], tags=["Contributors"])
def get_contributors(
    limit: int = Query(10, ge=1, le=50),
    hours: int = Query(24, ge=1, le=168),
) -> list[dict]:
    """Top contributors by event count."""
    return db.fetchall("""
        SELECT actor_login, actor_avatar,
               COUNT(*) AS event_count,
               COUNT(DISTINCT repo_name) AS repos_touched
        FROM events
        WHERE created_at >= DATE_SUB(NOW(), INTERVAL %s HOUR)
        GROUP BY actor_login, actor_avatar
        ORDER BY event_count DESC
        LIMIT %s
    """, (hours, limit))


@app.get("/health", tags=["System"])
async def health_check() -> dict:
    """Service health endpoint."""
    r = await get_redis()
    redis_ok = False
    try:
        await r.ping()
        redis_ok = True
    except Exception:
        pass

    mysql_ok = False
    try:
        row = db.fetchone("SELECT 1 AS ok")
        mysql_ok = bool(row)
    except Exception:
        pass

    return {
        "status":       "ok" if (redis_ok and mysql_ok) else "degraded",
        "redis":        "ok" if redis_ok else "error",
        "mysql":        "ok" if mysql_ok else "error",
        "ws_clients":   manager.count,
        "timestamp":    datetime.now(timezone.utc).isoformat(),
    }