"use client";

import Link from "next/link";
import { useEffect, useState } from "react";

import { DEFAULT_BACKEND } from "@/lib/api";
import { useBackendApi } from "@/lib/hooks/useBackendApi";

/* eslint-disable @typescript-eslint/no-explicit-any */

interface QueueStatsData {
  pending: number;
  processing: number;
  completed: number;
  failed: number;
  recent_errors?: {
    time?: string;
    source_id?: string;
    monitor_key?: string;
    error?: string;
  }[];
}

function formatDate(value: string | undefined): string {
  if (!value) return "";
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return "";
  return d.toLocaleString();
}

export default function QueueDetailsPage() {
  const [backendUrl] = useState(DEFAULT_BACKEND);
  const api = useBackendApi(backendUrl);
  const [queueStats, setQueueStats] = useState<QueueStatsData>({
    pending: 0,
    processing: 0,
    completed: 0,
    failed: 0,
    recent_errors: [],
  });
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  async function loadDetails() {
    setLoading(true);
    setError("");
    try {
      const stats = await api.getQueueStats<QueueStatsData>();
      setQueueStats(stats);
    } catch (err: any) {
      setError(err.message || "Failed to load queue details.");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    loadDetails();
    const iv = setInterval(loadDetails, 10000);
    return () => clearInterval(iv);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [api]);

  return (
    <main className="page">
      <section className="hero">
        <h1>Pipeline Queue & Failure Logs</h1>
        <p>Inspect why requests or snapshots failed in the OCR pipeline.</p>
        <Link
          href="/"
          className="btn-sm btn-secondary"
          style={{ marginTop: 12, display: "inline-block" }}
        >
          &larr; Back to Dashboard
        </Link>
      </section>

      <div className="row-2col">
        <section className="card">
          <h2>Current Queue State</h2>
          <div className="queue-stats">
            <div className="queue-stat">
              <span className="queue-count q-pending">
                {queueStats.pending}
              </span>
              <span className="queue-label">Pending</span>
            </div>
            <div className="queue-stat">
              <span className="queue-count q-processing">
                {queueStats.processing}
              </span>
              <span className="queue-label">Processing</span>
            </div>
            <div className="queue-stat">
              <span className="queue-count q-completed">
                {queueStats.completed}
              </span>
              <span className="queue-label">Completed</span>
            </div>
            <div className="queue-stat">
              <span className="queue-count q-failed text-error">
                {queueStats.failed}
              </span>
              <span className="queue-label">Failed Total</span>
            </div>
          </div>
          <button
            className="btn-sm"
            onClick={loadDetails}
            disabled={loading}
            style={{ marginTop: 16 }}
          >
            {loading ? "Refreshing..." : "Refresh Now"}
          </button>
        </section>

        <section className="card">
          <h2>Recent Errors</h2>
          {error && <p className="error">{error}</p>}
          {!queueStats.recent_errors?.length ? (
            <p className="muted">
              No recent errors recorded. Everything looks good!
            </p>
          ) : (
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Time</th>
                    <th>Source ID</th>
                    <th>Monitor</th>
                    <th>Error Message</th>
                  </tr>
                </thead>
                <tbody>
                  {queueStats.recent_errors.map((err, idx) => (
                    <tr key={idx}>
                      <td style={{ whiteSpace: "nowrap" }}>
                        {formatDate(err.time)}
                      </td>
                      <td>
                        <code>{err.source_id}</code>
                      </td>
                      <td>{err.monitor_key}</td>
                      <td
                        className="text-error"
                        style={{ fontSize: 13, maxWidth: 300 }}
                      >
                        {err.error}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </section>
      </div>

      <section className="card" style={{ marginTop: 20 }}>
        <h2>Troubleshooting Tips</h2>
        <ul style={{ lineHeight: 1.6 }}>
          <li>
            <strong>Connection Refused:</strong> Check if the KVM Source host and
            port are reachable from the backend.
          </li>
          <li>
            <strong>LLM Error:</strong> Ensure the LLM API (e.g., Kimi, Qwen)
            is online and the API key in <code>.env</code> is valid.
          </li>
          <li>
            <strong>OCR JSON Error:</strong> The LLM might have returned invalid
            Markdown or JSON. The system tries to self-correct, but persistent
            issues might need prompt tuning.
          </li>
          <li>
            <strong>Timeout:</strong> The pipeline might be overloaded or the LLM
            response is taking too long ({">"}30s).
          </li>
        </ul>
      </section>
    </main>
  );
}
