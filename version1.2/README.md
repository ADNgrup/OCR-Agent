# KVM-OCR_Alert system
---

## What it does

KVM-OCR periodically captures screenshots from KVM HMI screens, extracts structured data using an LLM (image → markdown → JSON entities), stores readings in MongoDB, and runs two layers of anomaly detection that alert your team via Email and Microsoft Teams.

```
KVM Screen  →  Snapshot  →  LLM → JSON  →  MongoDB (entity_logs)
                                                   ↓
                                      per_write_detector.py     ← every reading
                                      rolling_window_detector.py ← every 10 min
                                                   ↓
                                         anomaly_logs (MongoDB)
                                                   ↓
                                    notify.py → Email + Teams + Dashboard
```

---

## Architecture

```
KVM-OCR/
├── backend/
│   ├── main.py                          # FastAPI app, startup/shutdown, background tasks
│   ├── routers/
│   │   ├── api.py                       # REST API: sources, screens, logs, timeseries
│   │   └── config_router.py             # System settings API (stored in MongoDB)
│   ├── cores/
│   │   ├── config.py                    # Reads .env, LLM prompts, snapshot paths
│   │   ├── pipeline.py                  # Poll → classify → LLM → map entities → log
│   │   ├── dbconnection/
│   │   │   └── mongo.py                 # MongoDB connection + index management
│   │   ├── detectors/
│   │   │   ├── __init__.py
│   │   │   ├── per_write_detector.py    # Per-reading anomaly checks
│   │   │   └── rolling_window_detector.py # Trend analysis over 50–100 readings
│   │   └── notifiers/
│   │       ├── __init__.py
│   │       └── notify.py                # Email (Gmail SMTP) + Teams webhook alerts
│   └── utils/                           # Helpers: time, image features, KVM client, LLM client
└── frontend/
    └── ...                              # Next.js 14 dashboard
```

---

# Email alerts (Gmail)
ALERT_EMAIL_FROM=yourname@gmail.com
ALERT_EMAIL_PASSWORD=xxxx xxxx xxxx xxxx   # Gmail App Password — NOT your account password
ALERT_EMAIL_TO=engineer1@company.com,engineer2@company.com
ALERT_EMAIL_ENABLED=true

# Microsoft Teams alerts
ALERT_TEAMS_WEBHOOK_URL=https://your-org.webhook.office.com/...
ALERT_TEAMS_ENABLED=true
```

> **Gmail App Password setup**
> 1. Enable 2-Step Verification on your Google account
> 2. Go to [myaccount.google.com](https://myaccount.google.com) → Security → App Passwords
> 3. Generate a password for "Mail" and paste the 16-character code as `ALERT_EMAIL_PASSWORD`

> **Teams Webhook setup**
> In your Teams channel → Manage channel → Connectors → Incoming Webhook → Create

### Run

```bash
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```
---

## Anomaly detection

Two detectors run automatically as background tasks alongside the KVM poller.

### `per_write_detector.py` — per-reading checks

Runs on every single `entity_log` document written to MongoDB. Four checks:

| Check | Applies to | Description |
|---|---|---|
| `null_value` | `number` type | `numeric_value` is null despite `value_type == "number"` |
| `low_confidence` | all types | LLM reported `confidence == "Low"` |
| `impossible_value` | `number` type | Value violates hard domain limits (e.g. temperature < -50℃ or > 250℃) |
| `outlier` | `number` type | Value is more than 3σ from the entity's historical mean (requires 10+ readings) |

**Domain limits (configurable in code):**

| Metric | Min | Max |
|---|---|---|
| `temperature` | -50℃ | 250℃ |
| `flow_rate` | 0% | 100% |
| `level_percent` | 0% | 100% |
| `level` | 0 | — |
| `volume` | 0 | — |
| `status` | 0 | 1 |

### `rolling_window_detector.py` — trend checks

Runs every 10 minutes (configurable). Pulls the last 75 readings per entity and checks:

| Check | Description |
|---|---|
| `freeze` | All values in the window are identical — possible stuck sensor |
| `spike` | Latest value is more than 3σ from the window mean |
| `drift` | Recent half of window mean has shifted >20% vs. older half |

**Tunable via MongoDB `system_config`** (no redeploy needed):

| Key | Default | Description |
|---|---|---|
| `rolling_window_size` | 75 | Number of readings per analysis window |
| `rolling_run_interval_seconds` | 600 | How often the scan runs (seconds) |
| `rolling_spike_z_threshold` | 3.0 | Z-score threshold for spike detection |
| `rolling_drift_threshold_pct` | 0.20 | Mean shift threshold for drift detection (20%) |

---

## Alert routing

Not every anomaly sends a notification. The routing rules avoid noise from low-confidence one-off readings while ensuring confirmed trend anomalies always get through.

| Detector | Anomaly type | Severity | Email | Teams |
|---|---|---|---|---|
| `per_write` | `impossible_value` | 🔴 high | ✅ | ✅ |
| `per_write` | `outlier`, `null_value` | 🟡 medium | ❌ | ❌ |
| `per_write` | `low_confidence` | 🔵 low | ❌ | ❌ |
| `rolling_window` | `spike` | 🔴 high | ✅ | ✅ |
| `rolling_window` | `drift`, `freeze` | 🟡 medium | ✅ | ❌ |

All anomalies — including silent ones — are written to the `anomaly_logs` MongoDB collection for dashboard display.

### `anomaly_logs` document schema

```json
{
  "entity_log_id":   "<ObjectId or null>",
  "entity_id":       "<ObjectId>",
  "snapshot_id":     "<ObjectId or null>",
  "metric":          "温度_temperature",
  "metric_name":     "temperature",
  "indicator_label": "浴槽現在温度",
  "unit":            "℃",
  "anomaly_type":    "spike",
  "severity":        "high",
  "description":     "Latest value 95.0 ℃ is 4.21σ from window mean 41.5",
  "detected_at":     "2026-03-16T03:28:48.000Z",
  "resolved":        false,
  "detector":        "rolling_window",
  "z_score":         4.2100,
  "window_mean":     41.5000,
  "window_stdev":    1.2700
}
```

---

## Pipeline flow

1. **Poller** fetches KVM screenshots at `poll_seconds` interval per source
2. **Duplicate filter** — snapshots with identical image hash are skipped
3. **Grouping** — new snapshots are grouped by histogram/brightness similarity
4. **LLM call 1** — image → markdown transcription
5. **LLM call 2** — markdown → structured JSON entities
6. **Entity mapping** — entities written to `screen_entities` and `entity_logs`
7. **per_write_detector** — runs immediately on each new log entry
8. **rolling_window_detector** — runs every 10 min across all entities
9. **notify.py** — dispatches Email / Teams alerts based on routing rules

---



