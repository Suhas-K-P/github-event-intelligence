#!/usr/bin/env python3
"""
ingestion/github_poller.py
───────────────────────────
GitHub Event Ingestion Service

Polls the GitHub public events API at a configurable interval, deduplicates
events using an ETag + in-memory seen-set, and pushes raw JSON payloads onto
a Redis list (simulating a Kafka topic).

Architecture role:
    GitHub REST API  →  [this service]  →  Redis Queue (github:queue:raw_events)

GitHub rate-limit awareness:
    - Uses ETag / If-None-Match to avoid re-downloading unchanged data
    - Reads X-RateLimit-Remaining; backs off automatically when < 10 calls left
    - Falls back gracefully on network errors (exponential back-off)

Run standalone:
    python ingestion/github_poller.py
"""

import json
import logging
import os
import sys
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import httpx
import redis

# ── Make root importable when running standalone ──────────────────────────────
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import config

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [POLLER]  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("poller")


# ─────────────────────────────────────────────────────────────────────────────
class GitHubPoller:
    """
    Polls the GitHub /events endpoint and publishes new events to Redis.

    Deduplication strategy
    ──────────────────────
    1. ETag  – GitHub returns an ETag; if nothing changed, it returns HTTP 304.
    2. Seen-set – a bounded deque of event IDs seen recently, so that even
       after a 304 is lifted we don't re-insert duplicates.
    """

    HEADERS_BASE = {
        "Accept":     "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "EventIntelligence/1.0",
    }

    def __init__(self) -> None:
        self._redis   = self._connect_redis()
        self._client  = httpx.Client(timeout=20.0, follow_redirects=True)
        self._etag: Optional[str]  = None
        self._seen_ids: deque      = deque(maxlen=2000)   # rolling window
        self._rate_remaining: int  = 60
        self._rate_reset: int      = 0
        self._poll_count: int      = 0
        self._total_pushed: int    = 0

        # Inject auth token if provided
        if config.GITHUB_TOKEN:
            self.HEADERS_BASE["Authorization"] = f"Bearer {config.GITHUB_TOKEN}"
            log.info("GitHub auth: token provided ✓")
        else:
            log.warning("GitHub auth: no token — limited to 60 req/hour")

    # ─── Redis ─────────────────────────────────────────────────────────────
    def _connect_redis(self) -> redis.Redis:
        """Establish Redis connection with retry."""
        for attempt in range(1, 6):
            try:
                r = redis.Redis(
                    host=config.REDIS_HOST,
                    port=config.REDIS_PORT,
                    db=config.REDIS_DB,
                    password=config.REDIS_PASSWORD,
                    decode_responses=True,
                    socket_connect_timeout=5,
                )
                r.ping()
                log.info("Redis connected at %s:%s", config.REDIS_HOST, config.REDIS_PORT)
                return r
            except redis.RedisError as exc:
                log.warning("Redis connection attempt %d/5 failed: %s", attempt, exc)
                time.sleep(attempt * 2)
        log.error("Cannot connect to Redis — aborting.")
        sys.exit(1)

    # ─── GitHub Fetch ───────────────────────────────────────────────────────
    def _fetch_events(self) -> list[dict]:
        """
        Call GitHub /events; returns list of new event dicts.
        Returns [] on 304 (no change) or error.
        """
        headers = dict(self.HEADERS_BASE)
        if self._etag:
            headers["If-None-Match"] = self._etag

        url = f"{config.GITHUB_EVENTS_URL}?per_page={config.GITHUB_MAX_EVENTS}"
        try:
            resp = self._client.get(url, headers=headers)
        except httpx.RequestError as exc:
            log.warning("Network error: %s", exc)
            return []

        # Update rate-limit counters
        self._rate_remaining = int(resp.headers.get("X-RateLimit-Remaining", 60))
        self._rate_reset      = int(resp.headers.get("X-RateLimit-Reset", 0))

        if resp.status_code == 304:
            log.debug("304 Not Modified — no new events")
            return []

        if resp.status_code == 401:
            log.error("GitHub 401 Unauthorized — check your GITHUB_TOKEN")
            return []

        if resp.status_code == 403:
            reset_in = max(0, self._rate_reset - int(time.time()))
            log.warning("GitHub 403 rate-limit hit — resets in %ds", reset_in)
            time.sleep(min(reset_in + 5, 120))
            return []

        if resp.status_code != 200:
            log.warning("GitHub returned HTTP %d", resp.status_code)
            return []

        # Store new ETag for next request
        self._etag = resp.headers.get("ETag", self._etag)

        try:
            events = resp.json()
        except json.JSONDecodeError:
            log.error("Failed to parse GitHub response JSON")
            return []

        return events if isinstance(events, list) else []

    # ─── Enrichment ────────────────────────────────────────────────────────
    def _enrich(self, raw: dict) -> dict:
        """Add ingestion metadata to raw event."""
        raw["_ingested_at"] = datetime.now(timezone.utc).isoformat()
        raw["_source"]      = "github_poller"
        return raw

    # ─── Push to Queue ──────────────────────────────────────────────────────
    def _push(self, events: list[dict]) -> int:
        """Push events to Redis queue; returns count pushed."""
        pushed = 0
        pipeline = self._redis.pipeline()

        for event in events:
            eid = event.get("id")
            if not eid or eid in self._seen_ids:
                continue

            self._seen_ids.append(eid)
            enriched = self._enrich(event)
            pipeline.lpush(config.QUEUE_RAW_EVENTS, json.dumps(enriched))
            pushed += 1

        if pushed:
            pipeline.execute()

        return pushed

    # ─── Stats ─────────────────────────────────────────────────────────────
    def _update_redis_stats(self, pushed: int) -> None:
        """Keep a lightweight live counter in Redis for the API."""
        if pushed:
            self._redis.hincrby(config.KEY_STATS, "total_ingested", pushed)
            self._redis.hset(config.KEY_STATS, "last_poll",
                             datetime.now(timezone.utc).isoformat())

    # ─── Main Loop ──────────────────────────────────────────────────────────
    def run(self) -> None:
        """Blocking event loop — polls GitHub and pushes to Redis."""
        log.info("═" * 60)
        log.info("  GitHub Event Intelligence — Poller Service")
        log.info("  Poll interval: %ds | Queue: %s",
                 config.GITHUB_POLL_INTERVAL, config.QUEUE_RAW_EVENTS)
        log.info("═" * 60)

        backoff = 1  # exponential backoff on errors

        while True:
            try:
                self._poll_count += 1
                events = self._fetch_events()

                pushed = self._push(events)
                self._total_pushed += pushed
                self._update_redis_stats(pushed)

                log.info(
                    "Poll #%d | fetched=%d  new=%d  total_pushed=%d  "
                    "rate_remaining=%d",
                    self._poll_count, len(events), pushed,
                    self._total_pushed, self._rate_remaining,
                )

                # Adaptive sleep — slow down when rate limit is low
                sleep_time = config.GITHUB_POLL_INTERVAL
                if self._rate_remaining < 10:
                    reset_in = max(0, self._rate_reset - int(time.time()))
                    sleep_time = max(sleep_time, reset_in + 5)
                    log.warning("Rate limit low (%d remaining) — sleeping %ds",
                                self._rate_remaining, sleep_time)

                backoff = 1  # reset backoff on success
                time.sleep(sleep_time)

            except redis.RedisError as exc:
                log.error("Redis error (attempt %d): %s — retrying in %ds",
                          backoff, exc, backoff * 5)
                time.sleep(backoff * 5)
                backoff = min(backoff * 2, 16)

            except KeyboardInterrupt:
                log.info("Graceful shutdown — %d total events pushed", self._total_pushed)
                break

            except Exception as exc:  # noqa: BLE001
                log.exception("Unexpected error: %s", exc)
                time.sleep(backoff * 3)
                backoff = min(backoff * 2, 16)


# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    GitHubPoller().run()