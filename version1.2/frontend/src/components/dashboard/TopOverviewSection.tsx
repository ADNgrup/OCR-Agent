import Link from "next/link";
import { Dispatch, SetStateAction } from "react";

import { KvmSource, QueueStats, Screen as DashboardScreen } from "@/types/dashboard";

import { getScreenSchemaStatus } from "./dashboardUtils";

interface TopOverviewSectionProps {
  backendUrl: string;
  setBackendUrl: Dispatch<SetStateAction<string>>;
  sources: KvmSource[];
  screens: DashboardScreen[];
  queueStats: QueueStats;
  sourceId: string;
  screenId: string;
  hours: number;
  loading: boolean;
  selectedScreen?: DashboardScreen;
  onApplyBackend: () => Promise<void>;
  onSourceChange: (sid: string) => Promise<void>;
  onScreenChange: (gid: string) => Promise<void>;
  onRefreshData: () => Promise<void>;
  toggleSource: (id: string, enabled: boolean) => Promise<void>;
  runOnce: (id: string) => Promise<void>;
  toggleScreenIgnore: (sid: string, ignored: boolean) => Promise<void>;
  setHours: Dispatch<SetStateAction<number>>;
}

export default function TopOverviewSection({
  backendUrl,
  setBackendUrl,
  sources,
  screens,
  queueStats,
  sourceId,
  screenId,
  hours,
  loading,
  selectedScreen,
  onApplyBackend,
  onSourceChange,
  onScreenChange,
  onRefreshData,
  toggleSource,
  runOnce,
  toggleScreenIgnore,
  setHours,
}: TopOverviewSectionProps) {
  return (
    <>
      <section
        className="hero"
        style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}
      >
        <div>
          <h1>KVM OCR Monitoring Dashboard</h1>
          <p>
            Monitor KVM screens, entities, indicators, and timeseries from the OCR
            + LLM pipeline.
          </p>
        </div>
        <div style={{ display: "flex", gap: "10px" }}>
          <Link
            href="/snapshots"
            className="btn-primary"
            style={{ padding: "8px 16px", textDecoration: "none", borderRadius: "4px" }}
          >
            Check Snapshots
          </Link>
          <Link
            href="/settings"
            className="btn-secondary"
            style={{ padding: "8px 16px", textDecoration: "none", borderRadius: "4px" }}
          >
            Settings
          </Link>
        </div>
      </section>

      <div className="row-2col">
        <section className="card">
          <h2>KVM Source Controls</h2>
          {sources.length ? (
            <div className="source-controls">
              {sources.map((src) => (
                <div
                  key={src.id}
                  className={`source-card ${src.enabled ? "source-enabled" : "source-disabled"}`}
                >
                  <div className="source-card-info">
                    <div className="source-card-title">
                      {src.enabled && <span className="pulse-dot" />}
                      <strong>{src.name}</strong>
                    </div>
                    <span className="muted">
                      {src.host}:{src.port}
                    </span>
                    <span className="muted source-meta">
                      {src.enabled ? `Polling every ${src.poll_seconds}s` : "Stopped"}
                      {src.last_polled_at
                        ? ` · Last: ${new Date(src.last_polled_at).toLocaleTimeString()}`
                        : ""}
                    </span>
                  </div>
                  <div className="source-card-actions">
                    <button
                      className={`btn-toggle ${src.enabled ? "btn-on" : "btn-off"}`}
                      onClick={() => toggleSource(src.id, !src.enabled)}
                    >
                      {src.enabled ? "ON" : "OFF"}
                    </button>
                    <button className="btn-sm" onClick={() => runOnce(src.id)}>
                      Run Once
                    </button>
                  </div>
                </div>
              ))}
            </div>
          ) : (
            <p className="muted">No sources. Go to Settings to add KVM sources.</p>
          )}
        </section>

        <div className="2row-1col">
          <section className="card">
            <div className="section-header">
              <h2>Pipeline Queue</h2>
              <Link
                href="/queue-details"
                className="btn-sm btn-link"
                style={{ fontSize: 11 }}
              >
                View Details &rarr;
              </Link>
            </div>
            <div className="queue-stats">
              <div className="queue-stat">
                <span className="queue-count q-pending">{queueStats.pending}</span>
                <span className="queue-label">Pending</span>
              </div>
              <div className="queue-stat">
                <span className="queue-count q-processing">{queueStats.processing}</span>
                <span className="queue-label">Processing</span>
              </div>
              <div className="queue-stat">
                <span className="queue-count q-completed">{queueStats.completed}</span>
                <span className="queue-label">Completed</span>
              </div>
              <div className="queue-stat">
                <span
                  className={`queue-count q-failed ${queueStats.failed > 0 ? "text-error" : ""}`}
                >
                  {queueStats.failed}
                </span>
                <span className="queue-label">Failed</span>
              </div>
            </div>
          </section>

          <section className="card controls">
            <div className="control-row backend-row">
              <label>
                Backend URL
                <input
                  value={backendUrl}
                  onChange={(e) => setBackendUrl(e.target.value)}
                  placeholder="http://localhost:8000"
                />
              </label>
              <button onClick={onApplyBackend} disabled={loading}>
                Apply
              </button>
            </div>
            <div className="control-grid">
              <label>
                KVM Source
                <select
                  value={sourceId}
                  onChange={(e) => onSourceChange(e.target.value)}
                  disabled={loading || !sources.length}
                >
                  {sources.map((s) => (
                    <option key={s.id} value={s.id}>
                      {s.name}
                    </option>
                  ))}
                </select>
              </label>
              <label>
                Screen Group
                <div style={{ display: "flex", alignItems: "center", gap: "8px" }}>
                  <select
                    value={screenId}
                    onChange={(e) => onScreenChange(e.target.value)}
                    disabled={loading || !screens.length}
                    style={{ flex: 1 }}
                  >
                    {screens.map((s) => (
                      <option key={s.id} value={s.id}>
                        {s.name} {getScreenSchemaStatus(s) === "classified" ? "(OCR ready)" : "(Queue)"}{" "}
                        {s.ignored ? "(Ignored)" : ""}
                      </option>
                    ))}
                  </select>
                  {screenId && (
                    <label
                      style={{
                        display: "inline-flex",
                        alignItems: "center",
                        gap: "4px",
                        fontSize: "12px",
                        cursor: "pointer",
                        whiteSpace: "nowrap",
                      }}
                    >
                      <input
                        type="checkbox"
                        checked={!!screens.find((s) => s.id === screenId)?.ignored}
                        onChange={(e) => toggleScreenIgnore(screenId, e.target.checked)}
                      />
                      Skip OCR
                    </label>
                  )}
                </div>
                {selectedScreen ? (
                  <p className="muted" style={{ marginTop: 6, fontSize: 12 }}>
                    {getScreenSchemaStatus(selectedScreen) === "classified"
                      ? "This screen already has a schema. New snapshots will run OCR automatically."
                      : "This screen is still in the review queue. Define schema segments in Settings to turn on OCR for future snapshots."}
                  </p>
                ) : null}
              </label>
              <label>
                Hours
                <input
                  type="number"
                  min={1}
                  max={168}
                  value={hours}
                  onChange={(e) =>
                    setHours(Math.max(1, Math.min(168, Number(e.target.value) || 24)))
                  }
                />
              </label>
              <button onClick={onRefreshData} disabled={loading || !screenId}>
                {loading ? "Loading..." : "Refresh"}
              </button>
            </div>
          </section>
        </div>
      </div>
    </>
  );
}
