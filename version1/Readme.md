# KVM-OCR

Full-stack system for monitoring KVM HMI screens via OCR + LLM extraction pipeline.

---

## Architecture Overview

```
KVM-OCR/
├── backend/        # FastAPI + MongoDB + LLM pipeline
└── frontend/       # Next.js 14 dashboard
|__deploy/   #deploy by docker
|__prometheus+grafana # if need warning alert
```

---

## Backend (FastAPI + MongoDB)

### Structure

| File / Folder | Description |
|---|---|
| `main.py` | App entry point, startup/shutdown, background poller |
| `routers/api.py` | REST API for sources, screens, logs, timeseries |
| `routers/config_router.py` | System settings API (stored in MongoDB) |
| `cores/config.py` | Reads `.env`, LLM prompts, snapshot paths |
| `cores/dbconnection/mongo.py` | MongoDB connection + index management |
| `cores/pipeline.py` | Poll snapshot → classify → LLM → map entities → log |
| `cores/helpers/` | Standardized CRUD helpers for MongoDB |
| `utils/*` | Helpers: time, image features, KVM client, LLM client |

### Requirements

- Python 3.10+
- MongoDB accessible via `.env` variables
- KVM endpoint supporting paths: `connect`, `status`, `sendmouse`, `snapshot`, `disconnect`

### Setup

```bash
cd KVM-OCR/backend
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Run

```bash
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

### API Endpoints

| Method | Path | Description |
|---|---|---|
| GET | `/health` | Health check |
| GET | `/api/kvm-sources` | List all KVM sources |
| POST | `/api/kvm-sources` | Create a new KVM source |
| PATCH | `/api/kvm-sources/{id}/toggle?enabled=true` | Enable/disable source |
| POST | `/api/kvm-sources/{id}/run-once` | Trigger one-time snapshot poll |
| GET | `/api/screens?source_id=...` | List screen groups for a source |
| GET | `/api/logs?screen_group_id=...&hours=24&limit=500` | Query entity logs |
| GET | `/api/timeseries?screen_group_id=...&hours=24` | Get timeseries data |
| GET | `/api/snapshots/latest?source_id=...&limit=20` | Latest snapshots |
| GET | `/api/config` | Get system configuration |
| PUT | `/api/config` | Update system configuration |
| POST | `/api/config/reset` | Reset config to `.env` defaults |

### Pipeline Flow

1. Poller fetches KVM screenshots at `poll_seconds` interval per source
2. Duplicate snapshots (same image hash) are skipped
3. New snapshots are grouped by histogram/brightness similarity
4. LLM call: image → markdown
5. LLM call: markdown → JSON entities
6. Entities are mapped to `screen_entities` and logged in `entity_logs`

### Operation Notes

- `poll_seconds` is configurable per source in DB (default: 300s = 5 min)
- `monitor_keys` allows a single source to capture multiple monitors
- System config (LLM URL, API key, prompts, poll interval) can be managed via `/api/config` and stored in MongoDB

---

## Frontend (Next.js 14)

### Requirements

- Node.js 18+
- Backend running at `http://localhost:8000` (or configured URL)

### Setup

```bash
cd KVM-OCR/frontend
npm install
```

### Development

```bash
npm run dev
```

Open `http://localhost:3000`.

### Production Build

```bash
npm run build
npm run start
```

### Configure Backend URL

- **Option 1**: Enter directly in the UI (Backend URL input + Apply button)
- **Option 2**: Set environment variable:

```bash
NEXT_PUBLIC_BACKEND_URL=http://localhost:8000 npm run dev
```

### Pages

| Route | Description |
|---|---|
| `/` | Dashboard — select source, screen group, view timeseries chart and entity logs |
| `/settings` | System settings — configure LLM, pipeline, and prompt parameters |

### Features

- Select KVM source and screen group
- Filter data by hours range
- Multi-entity timeseries chart (Recharts)
- Entity logs table with status, color, direction
- System settings management with save/reset
