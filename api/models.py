"""
api/models.py
──────────────
Pydantic v2 models for all API request and response schemas.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field


# ─── Event Models ─────────────────────────────────────────────────────────────

class EventSummary(BaseModel):
    """Lightweight event — used in the live WebSocket feed."""
    model_config = ConfigDict(from_attributes=True)

    id:              Optional[int]   = None
    github_event_id: str
    event_type:      str
    actor_login:     str
    actor_avatar:    Optional[str]   = None
    repo_name:       str
    repo_url:        Optional[str]   = None
    created_at:      datetime
    sentiment_score:  Optional[float] = None
    sentiment_label:  str            = "neutral"
    keywords:        list[str]       = Field(default_factory=list)
    category:        Optional[str]   = None
    language:        Optional[str]   = None
    impact_score:    float           = 0.0
    commit_message:  Optional[str]   = None


class EventDetail(EventSummary):
    """Full event — includes raw payload."""
    payload:      Optional[Any]  = None
    subjectivity: Optional[float] = None
    ingested_at:  Optional[datetime] = None


# ─── Stats Models ─────────────────────────────────────────────────────────────

class GlobalStats(BaseModel):
    total_events:      int   = 0
    total_processed:   int   = 0
    unique_repos:      int   = 0
    unique_actors:     int   = 0
    positive_pct:      float = 0.0
    negative_pct:      float = 0.0
    neutral_pct:       float = 0.0
    events_last_hour:  int   = 0
    events_last_24h:   int   = 0
    ws_clients:        int   = 0
    queue_depth:       int   = 0
    last_processed:    Optional[str] = None


class EventTypeCount(BaseModel):
    event_type: str
    count:      int
    pct:        float = 0.0


class HourlyPoint(BaseModel):
    hour:  str    # "YYYY-MM-DD HH:00"
    count: int


# ─── Trending Models ──────────────────────────────────────────────────────────

class TrendingRepo(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    repo_name:        str
    repo_url:         Optional[str] = None
    total_events:     int           = 0
    push_count:       int           = 0
    star_count:       int           = 0
    fork_count:       int           = 0
    pr_count:         int           = 0
    primary_language: Optional[str] = None
    avg_sentiment:    Optional[float] = None
    last_seen:        Optional[datetime] = None


# ─── NLP Insight Models ───────────────────────────────────────────────────────

class KeywordFreq(BaseModel):
    keyword:   str
    frequency: int
    category:  Optional[str] = None


class SentimentBreakdown(BaseModel):
    positive: int = 0
    neutral:  int = 0
    negative: int = 0
    positive_pct: float = 0.0
    neutral_pct:  float = 0.0
    negative_pct: float = 0.0


class ContributorStat(BaseModel):
    actor_login:   str
    actor_avatar:  Optional[str] = None
    event_count:   int
    repos_touched: int


# ─── Pagination Wrapper ───────────────────────────────────────────────────────

class PaginatedEvents(BaseModel):
    total:  int
    page:   int
    limit:  int
    items:  list[EventSummary]


# ─── WebSocket Message ────────────────────────────────────────────────────────

class WSMessage(BaseModel):
    """Schema of messages sent over the WebSocket connection."""
    msg_type:  str                    # "event" | "stats" | "ping"
    data:      Any
    timestamp: str