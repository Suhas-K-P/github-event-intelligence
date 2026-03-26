#!/usr/bin/env python3
"""
processor/event_processor.py
──────────────────────────────
Event Processing & NLP Service

Consumes raw GitHub events from the Redis queue, enriches each event with
NLP analysis (sentiment, keywords, categorisation, impact score), persists
results to MySQL, and publishes a lightweight summary to the Redis pub/sub
channel so the API WebSocket layer can broadcast it in real-time.

Architecture role:
    Redis Queue  →  [this service]  →  MySQL  +  Redis Pub/Sub

NLP pipeline
────────────
1. Text extraction – pulls commit messages, PR/issue titles, release bodies
2. Sentiment     – TextBlob polarity (float -1.0 … +1.0) → label
3. Subjectivity  – TextBlob subjectivity (float 0.0 … 1.0)
4. Keywords      – tf-simple top-N after stopword removal
5. Category      – mapped from event type (code / community / discussion …)
6. Language      – from GitHub repo metadata
7. Impact score  – weighted heuristic (stars, forks, commit count …)

Run standalone:
    python processor/event_processor.py
"""

import json
import logging
import re
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import mysql.connector
import redis

# ── nltk one-time setup ───────────────────────────────────────────────────────
import nltk

for corpus in ("punkt", "stopwords", "punkt_tab"):
    try:
        nltk.download(corpus, quiet=True)
    except Exception:
        pass

from nltk.corpus import stopwords as nltk_stopwords
from textblob import TextBlob

# ── Make root importable ──────────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import config

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [PROCESSOR]  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("processor")

# Pre-load NLTK stopwords once
try:
    _STOP = set(nltk_stopwords.words("english")) | config.EXTRA_STOPWORDS
except Exception:
    _STOP = config.EXTRA_STOPWORDS


# ─────────────────────────────────────────────────────────────────────────────
class NLPAnalyser:
    """Lightweight NLP pipeline for GitHub event text."""

    # Regex to strip URLs, SHA hashes, bracket tokens from commit messages
    _CLEAN_RE = re.compile(
        r"https?://\S+|[0-9a-f]{7,40}|\[[^\]]*\]|\(#\d+\)|#\d+", re.I
    )
    _WORD_RE  = re.compile(r"[a-zA-Z]{3,}")

    @classmethod
    def _clean(cls, text: str) -> str:
        text = cls._CLEAN_RE.sub(" ", text)
        text = re.sub(r"[^a-zA-Z\s]", " ", text)
        return " ".join(text.split())

    @classmethod
    def extract_text(cls, event: dict) -> str:
        """Pull human-readable text from an event payload."""
        payload   = event.get("payload", {}) or {}
        fragments = []

        etype = event.get("type", "")

        if etype == "PushEvent":
            commits = payload.get("commits", []) or []
            for c in commits[:5]:          # cap at 5 commits
                msg = (c.get("message") or "").split("\n")[0][:200]
                if msg:
                    fragments.append(msg)

        elif etype in ("PullRequestEvent", "PullRequestReviewEvent"):
            pr = payload.get("pull_request") or {}
            for field in ("title", "body"):
                val = (pr.get(field) or "")[:300]
                if val:
                    fragments.append(val)

        elif etype in ("IssuesEvent", "IssueCommentEvent"):
            obj = payload.get("issue") or payload.get("comment") or {}
            for field in ("title", "body"):
                val = (obj.get(field) or "")[:300]
                if val:
                    fragments.append(val)

        elif etype == "ReleaseEvent":
            rel = payload.get("release") or {}
            for field in ("name", "body"):
                val = (rel.get(field) or "")[:300]
                if val:
                    fragments.append(val)

        elif etype == "CreateEvent":
            desc = (payload.get("description") or "")[:200]
            ref  = (payload.get("ref") or "")[:100]
            if desc: fragments.append(desc)
            if ref:  fragments.append(ref)

        elif etype == "GollumEvent":
            for page in (payload.get("pages") or [])[:3]:
                summary = (page.get("summary") or page.get("title") or "")[:200]
                if summary:
                    fragments.append(summary)

        return " ".join(fragments)

    @classmethod
    def analyse(cls, event: dict) -> dict:
        """
        Return NLP fields:
          sentiment_score, sentiment_label, subjectivity,
          keywords, category, commit_message, impact_score
        """
        text = cls.extract_text(event)

        # ── Defaults ────────────────────────────────────────────────────────
        result: dict = {
            "sentiment_score":  0.0,
            "sentiment_label":  "neutral",
            "subjectivity":     0.0,
            "keywords":         [],
            "category":         config.EVENT_CATEGORIES.get(event.get("type", ""), "other"),
            "commit_message":   text[:500] if text else None,
            "impact_score":     0.0,
        }

        # ── Sentiment & Subjectivity ─────────────────────────────────────────
        if text.strip():
            try:
                blob = TextBlob(cls._clean(text))
                pol  = round(float(blob.sentiment.polarity),    3)
                sub  = round(float(blob.sentiment.subjectivity), 3)
                result["sentiment_score"] = pol
                result["subjectivity"]    = sub

                if pol >= config.SENTIMENT_POS_THRESHOLD:
                    result["sentiment_label"] = "positive"
                elif pol <= config.SENTIMENT_NEG_THRESHOLD:
                    result["sentiment_label"] = "negative"
                else:
                    result["sentiment_label"] = "neutral"
            except Exception as exc:
                log.debug("TextBlob error: %s", exc)

        # ── Keyword Extraction ───────────────────────────────────────────────
        if text.strip():
            words  = [w.lower() for w in cls._WORD_RE.findall(text)]
            words  = [w for w in words if w not in _STOP and len(w) > 3]
            top_kw = [w for w, _ in Counter(words).most_common(config.NLP_MAX_KEYWORDS)]
            result["keywords"] = top_kw

        # ── Impact Score ─────────────────────────────────────────────────────
        result["impact_score"] = cls._impact(event)

        return result

    @classmethod
    def _impact(cls, event: dict) -> float:
        """
        Heuristic impact score 0-100 based on:
          - Event type weight
          - Commit count (PushEvent)
          - PR/issue metadata (open source engagement signals)
        """
        weights = {
            "ReleaseEvent":      50,
            "PullRequestEvent":  35,
            "IssuesEvent":       25,
            "PushEvent":         20,
            "ForkEvent":         15,
            "WatchEvent":        10,
            "CreateEvent":       10,
            "IssueCommentEvent":  8,
            "MemberEvent":       12,
        }
        score = float(weights.get(event.get("type", ""), 5))

        payload = event.get("payload") or {}
        # Bonus for multiple commits
        commits = payload.get("commits") or []
        score += min(len(commits) * 2, 20)

        # Bonus for PR description richness
        pr = payload.get("pull_request") or {}
        if pr.get("body") and len(pr["body"]) > 100:
            score += 10
        if pr.get("changed_files", 0) > 5:
            score += 5

        return round(min(score, 100.0), 1)


# ─────────────────────────────────────────────────────────────────────────────
class EventProcessor:
    """Consumes Redis queue, enriches with NLP, persists to MySQL."""

    def __init__(self) -> None:
        self._redis = self._connect_redis()
        self._db    = self._connect_mysql()
        self._nlp   = NLPAnalyser()
        self._proc_count = 0
        self._err_count  = 0

    # ─── Connections ───────────────────────────────────────────────────────
    def _connect_redis(self) -> redis.Redis:
        for attempt in range(1, 6):
            try:
                r = redis.Redis(
                    host=config.REDIS_HOST, port=config.REDIS_PORT,
                    db=config.REDIS_DB,     password=config.REDIS_PASSWORD,
                    decode_responses=True,  socket_connect_timeout=5,
                )
                r.ping()
                log.info("Redis connected ✓")
                return r
            except redis.RedisError as exc:
                log.warning("Redis attempt %d/5: %s", attempt, exc)
                time.sleep(attempt * 2)
        log.error("Redis unavailable — aborting.")
        sys.exit(1)

    def _connect_mysql(self) -> mysql.connector.MySQLConnection:
        for attempt in range(1, 6):
            try:
                conn = mysql.connector.connect(
                    host=config.DB_HOST, port=config.DB_PORT,
                    user=config.DB_USER, password=config.DB_PASSWORD,
                    database=config.DB_NAME,
                    charset="utf8mb4",
                    autocommit=False,
                    connection_timeout=10,
                )
                log.info("MySQL connected ✓  (db=%s)", config.DB_NAME)
                return conn
            except mysql.connector.Error as exc:
                log.warning("MySQL attempt %d/5: %s", attempt, exc)
                time.sleep(attempt * 2)
        log.error("MySQL unavailable — aborting.")
        sys.exit(1)

    def _ensure_mysql(self) -> None:
        """Reconnect to MySQL if connection dropped."""
        try:
            self._db.ping(reconnect=True, attempts=3, delay=2)
        except mysql.connector.Error:
            log.warning("MySQL ping failed — reconnecting …")
            self._db = self._connect_mysql()

    # ─── MySQL Persistence ─────────────────────────────────────────────────
    _INSERT_EVENT = """
        INSERT IGNORE INTO events
          (github_event_id, event_type, actor_login, actor_avatar,
           repo_name, repo_url, payload, created_at, ingested_at,
           sentiment_score, sentiment_label, subjectivity,
           keywords, category, language, commit_message, impact_score)
        VALUES
          (%s, %s, %s, %s,
           %s, %s, %s, %s, %s,
           %s, %s, %s,
           %s, %s, %s, %s, %s)
    """

    _UPSERT_REPO = """
        INSERT INTO trending_repos
          (repo_name, repo_url, total_events, push_count, star_count,
           fork_count, pr_count, issue_count, primary_language,
           avg_sentiment, first_seen, last_seen)
        VALUES (%s, %s, 1, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
        ON DUPLICATE KEY UPDATE
          total_events      = total_events + 1,
          push_count        = push_count  + VALUES(push_count),
          star_count        = star_count  + VALUES(star_count),
          fork_count        = fork_count  + VALUES(fork_count),
          pr_count          = pr_count    + VALUES(pr_count),
          issue_count       = issue_count + VALUES(issue_count),
          primary_language  = COALESCE(VALUES(primary_language), primary_language),
          avg_sentiment     = (avg_sentiment + VALUES(avg_sentiment)) / 2,
          last_seen         = NOW()
    """

    def _persist_event(self, event: dict, nlp: dict) -> bool:
        """Write enriched event to MySQL; return True on success."""
        self._ensure_mysql()
        cursor = self._db.cursor()
        try:
            actor   = event.get("actor") or {}
            repo    = event.get("repo")  or {}
            etype   = event.get("type", "UnknownEvent")
            payload = event.get("payload") or {}

            # Detect language from payload forks / repo data
            language: Optional[str] = (
                (payload.get("forkee") or {}).get("language")
                or (payload.get("pull_request") or {}).get("head", {}).get("repo", {}).get("language")
            )

            created_raw = event.get("created_at", datetime.now(timezone.utc).isoformat())
            try:
                created_dt = datetime.fromisoformat(created_raw.replace("Z", "+00:00"))
            except ValueError:
                created_dt = datetime.now(timezone.utc)

            # Insert event
            cursor.execute(self._INSERT_EVENT, (
                event.get("id"),
                etype,
                actor.get("login", "unknown"),
                actor.get("avatar_url"),
                repo.get("name", "unknown/unknown"),
                f"https://github.com/{repo.get('name', '')}",
                json.dumps(payload)[:65535],     # MySQL TEXT cap guard
                created_dt,
                datetime.now(timezone.utc),
                nlp["sentiment_score"],
                nlp["sentiment_label"],
                nlp["subjectivity"],
                json.dumps(nlp["keywords"]),
                nlp["category"],
                language,
                nlp["commit_message"],
                nlp["impact_score"],
            ))

            # Upsert trending repo
            is_push   = 1 if etype == "PushEvent"          else 0
            is_star   = 1 if etype == "WatchEvent"          else 0
            is_fork   = 1 if etype == "ForkEvent"           else 0
            is_pr     = 1 if etype == "PullRequestEvent"    else 0
            is_issue  = 1 if etype == "IssuesEvent"         else 0

            cursor.execute(self._UPSERT_REPO, (
                repo.get("name", "unknown/unknown"),
                f"https://github.com/{repo.get('name', '')}",
                is_push, is_star, is_fork, is_pr, is_issue,
                language, nlp["sentiment_score"],
            ))

            # Update keyword frequency
            for kw in nlp["keywords"]:
                cursor.execute("""
                    INSERT INTO keyword_freq (keyword, frequency, category)
                    VALUES (%s, 1, %s)
                    ON DUPLICATE KEY UPDATE
                        frequency = frequency + 1,
                        category  = VALUES(category),
                        last_seen = NOW()
                """, (kw, nlp["category"]))

            self._db.commit()
            return True

        except mysql.connector.Error as exc:
            log.error("MySQL insert failed: %s", exc)
            self._db.rollback()
            return False
        finally:
            cursor.close()

    # ─── Pub/Sub Broadcast ─────────────────────────────────────────────────
    def _broadcast(self, event: dict, nlp: dict) -> None:
        """Publish lightweight event summary to Redis pub/sub."""
        actor = event.get("actor") or {}
        repo  = event.get("repo")  or {}

        summary = {
            "id":               event.get("id"),
            "type":             event.get("type"),
            "actor":            actor.get("login"),
            "actor_avatar":     actor.get("avatar_url"),
            "repo":             repo.get("name"),
            "created_at":       event.get("created_at"),
            "sentiment_label":  nlp["sentiment_label"],
            "sentiment_score":  nlp["sentiment_score"],
            "keywords":         nlp["keywords"][:5],
            "category":         nlp["category"],
            "impact_score":     nlp["impact_score"],
            "commit_message":   (nlp["commit_message"] or "")[:120],
        }
        try:
            self._redis.publish(config.PUBSUB_LIVE_EVENTS, json.dumps(summary))
        except redis.RedisError as exc:
            log.warning("Pub/sub publish failed: %s", exc)

    # ─── Stats ──────────────────────────────────────────────────────────────
    def _update_stats(self, nlp: dict) -> None:
        try:
            pipe = self._redis.pipeline()
            pipe.hincrby(config.KEY_STATS, "total_processed", 1)
            pipe.hincrby(config.KEY_STATS, f"cat:{nlp['category']}", 1)
            if nlp["sentiment_label"] == "positive":
                pipe.hincrby(config.KEY_STATS, "positive_count", 1)
            elif nlp["sentiment_label"] == "negative":
                pipe.hincrby(config.KEY_STATS, "negative_count", 1)
            pipe.hset(config.KEY_STATS, "last_processed",
                      datetime.now(timezone.utc).isoformat())
            pipe.execute()
        except redis.RedisError:
            pass

    # ─── Main Loop ──────────────────────────────────────────────────────────
    def run(self) -> None:
        log.info("═" * 60)
        log.info("  GitHub Event Intelligence — Processor Service")
        log.info("  Listening on queue: %s", config.QUEUE_RAW_EVENTS)
        log.info("═" * 60)

        backoff = 1

        while True:
            try:
                # BRPOP blocks up to 5 s then returns None — keeps loop alive
                item = self._redis.brpop(config.QUEUE_RAW_EVENTS, timeout=5)
                if item is None:
                    continue                # timeout — loop again

                _, raw_json = item
                try:
                    event = json.loads(raw_json)
                except json.JSONDecodeError:
                    log.warning("Malformed JSON in queue — skipping")
                    continue

                # ── NLP Pipeline ─────────────────────────────────────────
                nlp = self._nlp.analyse(event)

                # ── Persist ──────────────────────────────────────────────
                ok = self._persist_event(event, nlp)

                if ok:
                    self._proc_count += 1
                    self._broadcast(event, nlp)
                    self._update_stats(nlp)

                    if self._proc_count % 10 == 0:
                        log.info(
                            "Processed: %d  errors: %d  |  last: %-30s  "
                            "sentiment=%s",
                            self._proc_count, self._err_count,
                            event.get("repo", {}).get("name", "?")[:30],
                            nlp["sentiment_label"],
                        )
                else:
                    self._err_count += 1

                backoff = 1   # reset on success

            except redis.RedisError as exc:
                log.error("Redis error: %s — backing off %ds", exc, backoff * 5)
                time.sleep(backoff * 5)
                backoff = min(backoff * 2, 16)

            except KeyboardInterrupt:
                log.info("Graceful shutdown — %d events processed", self._proc_count)
                break

            except Exception as exc:    # noqa: BLE001
                log.exception("Unexpected error: %s", exc)
                self._err_count += 1
                time.sleep(backoff * 2)
                backoff = min(backoff * 2, 8)


# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    EventProcessor().run()