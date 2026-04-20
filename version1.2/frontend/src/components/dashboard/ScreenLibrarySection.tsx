import { Screen as DashboardScreen } from "@/types/dashboard";

import {
  formatDate,
  getScreenSchemaStatus,
  screenStatusPillColor,
} from "./dashboardUtils";

interface ScreenLibrarySectionProps {
  importedScreens: DashboardScreen[];
  queuedScreens: DashboardScreen[];
  loading: boolean;
  onOpenScreen: (screenId: string) => Promise<void>;
  onToggleScreenIgnore: (screenId: string, ignored: boolean) => Promise<void>;
}

export default function ScreenLibrarySection({
  importedScreens,
  queuedScreens,
  loading,
  onOpenScreen,
  onToggleScreenIgnore,
}: ScreenLibrarySectionProps) {
  return (
    <section className="card" style={{ marginTop: 24 }}>
      <div className="section-header">
        <div>
          <h2 style={{ marginBottom: 4 }}>Screen Library</h2>
          <p className="muted" style={{ margin: 0 }}>
            Imported screens have a schema attached and will run OCR automatically.
            Unclassified screens are kept in a review queue until you define segments.
          </p>
        </div>
        <span
          className="type-pill"
          style={{ background: screenStatusPillColor("classified") }}
        >
          {importedScreens.length} Imported
        </span>
      </div>

      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fit, minmax(320px, 1fr))",
          gap: 16,
          marginTop: 16,
        }}
      >
        <div
          style={{
            border: "1px solid var(--border, #e5e7eb)",
            borderRadius: 12,
            padding: 16,
            background: "linear-gradient(180deg, rgba(4,120,87,0.05), rgba(4,120,87,0.01))",
          }}
        >
          <h3 style={{ marginTop: 0, marginBottom: 12 }}>Imported Screens</h3>
          {importedScreens.length ? (
            <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
              {importedScreens.map((screen) => {
                const status = getScreenSchemaStatus(screen);
                return (
                  <div
                    key={screen.id}
                    style={{
                      border: "1px solid var(--border, #e5e7eb)",
                      borderRadius: 10,
                      padding: 12,
                      background: "#fff",
                      display: "flex",
                      justifyContent: "space-between",
                      gap: 12,
                    }}
                  >
                    <div style={{ minWidth: 0 }}>
                      <strong>{screen.name}</strong>
                      <div className="muted" style={{ fontSize: 12, marginTop: 4 }}>
                        {screen.monitor_key} · {screen.snapshot_count ?? 0} snapshots ·{" "}
                        {screen.entity_count ?? 0} segments
                      </div>
                      <div
                        style={{
                          display: "flex",
                          gap: 6,
                          flexWrap: "wrap",
                          marginTop: 8,
                        }}
                      >
                        <span
                          className="type-pill"
                          style={{ background: screenStatusPillColor(status) }}
                        >
                          OCR Ready
                        </span>
                        {screen.ignored ? (
                          <span className="type-pill" style={{ background: "#7c2d12" }}>
                            Skipped
                          </span>
                        ) : null}
                      </div>
                    </div>
                    <div
                      style={{
                        display: "flex",
                        flexDirection: "column",
                        gap: 8,
                        flexShrink: 0,
                      }}
                    >
                      <button
                        className="btn-sm btn-primary"
                        onClick={() => onOpenScreen(screen.id)}
                        disabled={loading}
                      >
                        Open
                      </button>
                      <button
                        className="btn-sm btn-secondary"
                        onClick={() => onToggleScreenIgnore(screen.id, !screen.ignored)}
                      >
                        {screen.ignored ? "Resume" : "Skip OCR"}
                      </button>
                    </div>
                  </div>
                );
              })}
            </div>
          ) : (
            <p className="muted" style={{ marginBottom: 0 }}>
              No imported screens yet. Create a schema for a queued screen to move it
              here.
            </p>
          )}
        </div>

        <div
          style={{
            border: "1px solid var(--border, #e5e7eb)",
            borderRadius: 12,
            padding: 16,
            background: "linear-gradient(180deg, rgba(180,83,9,0.06), rgba(180,83,9,0.01))",
          }}
        >
          <h3 style={{ marginTop: 0, marginBottom: 12 }}>Unclassified Queue</h3>
          {queuedScreens.length ? (
            <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
              {queuedScreens.map((screen) => {
                const status = getScreenSchemaStatus(screen);
                return (
                  <div
                    key={screen.id}
                    style={{
                      border: "1px solid var(--border, #e5e7eb)",
                      borderRadius: 10,
                      padding: 12,
                      background: "#fff",
                      display: "flex",
                      justifyContent: "space-between",
                      gap: 12,
                    }}
                  >
                    <div style={{ minWidth: 0 }}>
                      <strong>{screen.name}</strong>
                      <div className="muted" style={{ fontSize: 12, marginTop: 4 }}>
                        {screen.monitor_key} · {screen.snapshot_count ?? 0} snapshots · waiting
                        for schema
                      </div>
                      <div
                        style={{
                          display: "flex",
                          gap: 6,
                          flexWrap: "wrap",
                          marginTop: 8,
                        }}
                      >
                        <span
                          className="type-pill"
                          style={{ background: screenStatusPillColor(status) }}
                        >
                          Review
                        </span>
                        {screen.last_snapshot_at ? (
                          <span className="type-pill" style={{ background: "#475467" }}>
                            {formatDate(screen.last_snapshot_at)}
                          </span>
                        ) : null}
                      </div>
                    </div>
                    <div
                      style={{
                        display: "flex",
                        flexDirection: "column",
                        gap: 8,
                        flexShrink: 0,
                      }}
                    >
                      <button
                        className="btn-sm btn-primary"
                        onClick={() => onOpenScreen(screen.id)}
                        disabled={loading}
                      >
                        Review
                      </button>
                      <button
                        className="btn-sm btn-secondary"
                        onClick={() => onToggleScreenIgnore(screen.id, !screen.ignored)}
                      >
                        {screen.ignored ? "Resume" : "Skip OCR"}
                      </button>
                    </div>
                  </div>
                );
              })}
            </div>
          ) : (
            <p className="muted" style={{ marginBottom: 0 }}>
              No unclassified screens in the queue right now.
            </p>
          )}
        </div>
      </div>
    </section>
  );
}
