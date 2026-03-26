#  GitHub Event Intelligence System

> Real-time GitHub event analytics pipeline — streamed, NLP-enriched, and visualised in a live dashboard.

---

## Architecture

```
GitHub Public Events API
         │  (polls every 15s, ETag deduplication)
         ▼
  ┌─────────────────┐
  │  GitHub Poller  │  ingestion/github_poller.py
  │  (Python worker)│
  └────────┬────────┘
           │  LPUSH (JSON)
           ▼
  ┌─────────────────────────────┐
  │   Redis Queue               │  github:queue:raw_events
  │   (simulated Kafka topic)   │  BRPOP pattern = consumer group
  └────────┬────────────────────┘
           │  BRPOP
           ▼
  ┌─────────────────────────────┐
  │   NLP Processor             │  processor/event_processor.py
  │   · Sentiment (TextBlob)    │
  │   · Keywords (NLTK)         │
  │   · Categorisation          │
  │   · Impact scoring          │
  └──────┬──────────────────────┘
         │                │
    MySQL INSERT     Redis PUBLISH
         │                │
         ▼                ▼
  ┌──────────┐    ┌──────────────────┐
  │  MySQL   │    │  Redis Pub/Sub   │
  │  (XAMPP) │    │  live_events     │
  └──────────┘    └────────┬─────────┘
                           │ subscribe
                           ▼
                  ┌─────────────────┐
                  │  FastAPI        │  api/main.py
                  │  REST + WS      │  :8000
                  └────────┬────────┘
                           │ WebSocket broadcast
                           ▼
                  ┌─────────────────┐
                  │  Dashboard      │  dashboard/index.html
                  │  (HTML/CSS/JS)  │  Chart.js · live feed
                  └─────────────────┘
```

---

## Features

| Feature | Detail |
|---|---|
| **Real-time ingestion** | Polls GitHub `/events` every 15 s with ETag caching |
| **Simulated Kafka** | Redis LPUSH/BRPOP queue pattern — drop-in Kafka compatible |
| **NLP pipeline** | Sentiment, subjectivity, keyword extraction, categorisation |
| **Impact scoring** | Heuristic score per event (commits, PR richness, event type) |
| **WebSocket** | Live push to browser — no polling needed on frontend |
| **REST API** | Paginated events, stats, trending, NLP insights, health |
| **MySQL persistence** | Full event storage with indexed, queryable schema |
| **Live dashboard** | Chart.js timeline, donut chart, keyword cloud, live feed |
| **Rate-limit aware** | Reads `X-RateLimit-Remaining`, backs off automatically |
| **Graceful shutdown** | All services handle SIGINT cleanly |

---

## Prerequisites

| Tool | Version | Notes |
|---|---|---|
| Python | 3.10+ | |
| Redis | 6+ | `redis-server` on Linux/macOS or Redis for Windows |
| MySQL | 5.7+ | XAMPP ships MySQL — start via XAMPP Control Panel |
| XAMPP | any | For MySQL on Windows |
| GitHub Token | optional | 5000 req/h (vs 60 without) — get at github.com/settings/tokens |

---

## Setup

### 1. Clone & install dependencies
```bash
git clone <your-repo-url>
cd event-intelligence
pip install -r requirements.txt

# Download TextBlob corpora (one-time)
python -m textblob.download_corpora
```

### 2. Configure environment
```bash
cp .env.example .env
# Edit .env — at minimum set GITHUB_TOKEN and DB_PASSWORD
```

### 3. Set up MySQL (XAMPP)
1. Start XAMPP → Start **MySQL**
2. Open phpMyAdmin → `http://localhost/phpmyadmin`
3. Run the schema:

```bash
mysql -u root -p event_intelligence < sql/schema.sql
# or paste sql/schema.sql into the phpMyAdmin SQL tab
```

### 4. Start Redis
```bash
# Linux / macOS
redis-server

# Windows — download from https://github.com/microsoftarchive/redis/releases
# or use WSL: wsl redis-server
```

### 5. Run the system
```bash
# Start everything at once
python run.py

# Or start services individually (in separate terminals)
python run.py --api
python run.py --processor
python run.py --poller
```

### 6. Open dashboard
Navigate to **http://localhost:8000** 🎉

---

## API Reference

| Method | Endpoint | Description |
|---|---|---|
| GET | `/` | Dashboard HTML |
| WS | `/ws` | Real-time event stream |
| GET | `/api/stats` | Global statistics |
| GET | `/api/stats/hourly?hours=24` | Events per hour |
| GET | `/api/stats/event-types` | Event type distribution |
| GET | `/api/events?page=1&limit=20` | Paginated events |
| GET | `/api/events/{id}` | Single event detail |
| GET | `/api/trending?limit=10` | Trending repositories |
| GET | `/api/nlp/keywords?limit=20` | Top keywords |
| GET | `/api/nlp/sentiment` | Sentiment breakdown |
| GET | `/api/contributors?limit=10` | Top contributors |
| GET | `/health` | Service health check |
| GET | `/docs` | Swagger UI |

---

## Project Structure

```
event-intelligence/
├── .env.example              # Environment template
├── config.py                 # Central configuration
├── requirements.txt
├── run.py                    # Orchestrator (starts all 3 services)
│
├── sql/
│   └── schema.sql            # MySQL tables + views
│
├── ingestion/
│   └── github_poller.py      # GitHub API → Redis queue
│
├── processor/
│   └── event_processor.py    # Redis queue → NLP → MySQL → Pub/Sub
│
├── api/
│   ├── main.py               # FastAPI (REST + WebSocket)
│   ├── database.py           # MySQL connection pool
│   └── models.py             # Pydantic schemas
│
└── dashboard/
    └── index.html            # Real-time dashboard (self-contained)
```

---

## How to extend

- **Scale horizontally** — run multiple `event_processor.py` workers (BRPOP is inherently load-balanced across consumers)
- **Replace Redis with real Kafka** — swap `redis.lpush/brpop` for `confluent_kafka` — the interface is identical
- **Add ML models** — slot a HuggingFace transformer in `NLPAnalyser.analyse()` for richer classification
- **Add alerts** — watch for negative-sentiment spikes and send to Slack/email
- **Dockerise** — `docker-compose up` (Dockerfile not included, but straightforward to add)

---

## Tech Stack

| Layer | Technology |
|---|---|
| Ingestion | Python, httpx, GitHub REST API |
| Queue | Redis (LPUSH/BRPOP) |
| NLP | TextBlob, NLTK |
| Database | MySQL 8 (XAMPP) |
| API | FastAPI, WebSocket, Pydantic v2 |
| Frontend | Vanilla JS, Chart.js, WebSocket API |
| Fonts | Space Mono, DM Sans |

---

*Built to demonstrate FAANG-style system design: separation of concerns, queue-based decoupling, real-time streaming, and NLP enrichment.*


