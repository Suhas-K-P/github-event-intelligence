"""
config.py
─────────
Central configuration for the GitHub Event Intelligence System.
All values are loaded from environment variables (or .env file).
"""
import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    # ── GitHub ──────────────────────────────────────────────────────────────
    GITHUB_TOKEN: str            = os.getenv("GITHUB_TOKEN", "")
    GITHUB_POLL_INTERVAL: int    = int(os.getenv("GITHUB_POLL_INTERVAL", "15"))
    GITHUB_MAX_EVENTS: int       = int(os.getenv("GITHUB_MAX_EVENTS_PER_POLL", "100"))
    GITHUB_EVENTS_URL: str       = "https://api.github.com/events"
    GITHUB_API_BASE: str         = "https://api.github.com"

    # ── Redis Queue Keys ─────────────────────────────────────────────────────
    REDIS_HOST: str              = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT: int              = int(os.getenv("REDIS_PORT", "6379"))
    REDIS_DB: int                = int(os.getenv("REDIS_DB", "0"))
    REDIS_PASSWORD: str          = os.getenv("REDIS_PASSWORD", "") or None

    # Queue (simulated Kafka topic — LPUSH / BRPOP pattern)
    QUEUE_RAW_EVENTS: str        = "github:queue:raw_events"
    # Pub/Sub channel for real-time WebSocket broadcasts
    PUBSUB_LIVE_EVENTS: str      = "github:pubsub:live_events"
    # Sorted set for trending repos (score = event count)
    KEY_TRENDING: str            = "github:trending:repos"
    # Hash for real-time stats
    KEY_STATS: str               = "github:stats:global"

    # ── MySQL ────────────────────────────────────────────────────────────────
    DB_HOST: str                 = os.getenv("DB_HOST", "localhost")
    DB_PORT: int                 = int(os.getenv("DB_PORT", "3306"))
    DB_USER: str                 = os.getenv("DB_USER", "root")
    DB_PASSWORD: str             = os.getenv("DB_PASSWORD", "")
    DB_NAME: str                 = os.getenv("DB_NAME", "event_intelligence")

    # ── API ──────────────────────────────────────────────────────────────────
    API_HOST: str                = os.getenv("API_HOST", "0.0.0.0")
    API_PORT: int                = int(os.getenv("API_PORT", "8000"))
    API_DEBUG: bool              = os.getenv("API_DEBUG", "true").lower() == "true"

    # ── NLP ──────────────────────────────────────────────────────────────────
    NLP_MAX_KEYWORDS: int        = int(os.getenv("NLP_MAX_KEYWORDS", "8"))
    SENTIMENT_POS_THRESHOLD: float = float(os.getenv("SENTIMENT_POSITIVE_THRESHOLD", "0.1"))
    SENTIMENT_NEG_THRESHOLD: float = float(os.getenv("SENTIMENT_NEGATIVE_THRESHOLD", "-0.1"))

    # ── Event Categories (for NLP classification) ────────────────────────────
    EVENT_CATEGORIES: dict = {
        "PushEvent":             "code",
        "PullRequestEvent":      "code",
        "PullRequestReviewEvent":"code",
        "WatchEvent":            "community",
        "ForkEvent":             "community",
        "StarEvent":             "community",
        "IssuesEvent":           "discussion",
        "IssueCommentEvent":     "discussion",
        "CommitCommentEvent":    "discussion",
        "ReleaseEvent":          "release",
        "CreateEvent":           "project",
        "DeleteEvent":           "project",
        "PublicEvent":           "project",
        "MemberEvent":           "team",
        "GollumEvent":           "docs",
    }

    # ── NLP Stopwords (additional domain-specific) ───────────────────────────
    EXTRA_STOPWORDS: set = {
        "fix", "fixes", "fixed", "add", "added", "adds", "update", "updated",
        "updates", "change", "changed", "changes", "remove", "removed", "removes",
        "new", "use", "using", "used", "improve", "improved", "get", "set",
        "make", "made", "move", "moved", "version", "bump", "merge", "merged",
        "ci", "cd", "pr", "ref", "via", "etc", "also", "just", "now",
    }


config = Config()