import { ReactNode } from "react";

interface KvmSourceRow {
  id: string;
  name: string;
  host: string;
  port: number;
  poll_seconds: number;
  monitor_keys?: string[];
  mode?: string;
  enabled: boolean;
  last_polled_at?: string;
}

interface KvmSourcesSectionProps {
  sources: KvmSourceRow[];
  sourceLoading: boolean;
  showAddForm: boolean;
  editingId: string | null;
  renderSourceForm: () => ReactNode;
  onOpenAddForm: () => void;
  onRunOnce: (id: string) => Promise<void>;
  onOpenEditForm: (src: KvmSourceRow) => void;
  onDeleteSource: (id: string) => Promise<void>;
  onToggleSource: (id: string, enabled: boolean) => Promise<void>;
}

export default function KvmSourcesSection({
  sources,
  sourceLoading,
  showAddForm,
  editingId,
  renderSourceForm,
  onOpenAddForm,
  onRunOnce,
  onOpenEditForm,
  onDeleteSource,
  onToggleSource,
}: KvmSourcesSectionProps) {
  return (
    <section className="card settings-group">
      <div className="section-header">
        <h2>KVM Sources</h2>
        <button onClick={onOpenAddForm} disabled={sourceLoading}>
          + Add Source
        </button>
      </div>

      {showAddForm && renderSourceForm()}

      {sources.length ? (
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Name</th>
                <th>Host</th>
                <th>Port</th>
                <th>Poll (s)</th>
                <th>Monitors</th>
                <th>Mode</th>
                <th>Enabled</th>
                <th>Last Polled</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {sources.map((src) => (
                <tr key={src.id}>
                  <td>
                    <strong>{src.name}</strong>
                  </td>
                  <td>{src.host}</td>
                  <td>{src.port}</td>
                  <td>{src.poll_seconds}</td>
                  <td>{(src.monitor_keys || []).join(", ") || "default"}</td>
                  <td>
                    <span className="badge">{src.mode || "v2"}</span>
                  </td>
                  <td>
                    <button
                      className={`btn-toggle ${src.enabled ? "btn-on" : "btn-off"}`}
                      onClick={() => onToggleSource(src.id, !src.enabled)}
                    >
                      {src.enabled ? "ON" : "OFF"}
                    </button>
                  </td>
                  <td className="muted">
                    {src.last_polled_at
                      ? new Date(src.last_polled_at).toLocaleString()
                      : "Never"}
                  </td>
                  <td className="actions-cell">
                    <button
                      className="btn-sm"
                      onClick={() => onRunOnce(src.id)}
                      title="Run one snapshot now"
                    >
                      Run
                    </button>
                    <button
                      className="btn-sm btn-secondary"
                      onClick={() => onOpenEditForm(src)}
                      title="Edit source"
                    >
                      Edit
                    </button>
                    <button
                      className="btn-sm btn-danger"
                      onClick={() => onDeleteSource(src.id)}
                      title="Delete source"
                    >
                      Del
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <p className="muted">
          No KVM sources configured. Click &quot;+ Add Source&quot; to create one.
        </p>
      )}

      {editingId && (
        <div style={{ marginTop: 12 }}>
          <h3>Edit Source</h3>
          {renderSourceForm()}
        </div>
      )}
    </section>
  );
}
