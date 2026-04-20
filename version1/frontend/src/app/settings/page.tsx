"use client";

import { useCallback, useEffect, useState } from "react";
import {
  DEFAULT_BACKEND,
  fetchJSON,
  normalizeBaseUrl,
  postJSON,
  putJSON,
  patchFetch,
  deleteFetch,
} from "@/lib/api";

/* eslint-disable @typescript-eslint/no-explicit-any */

// ---- Config field metadata ----
interface FieldMeta {
  key: string;
  label: string;
  group: string;
  readOnly?: boolean;
  type: string;
  placeholder?: string;
  min?: number;
  max?: number;
  rows?: number;
}

  const FIELD_META: FieldMeta[] = [
    { key: "db_host", label: "Database Host", group: "database", readOnly: true, type: "text" },
    { key: "db_port", label: "Database Port", group: "database", readOnly: true, type: "number" },
    { key: "db_name", label: "Database Name", group: "database", readOnly: true, type: "text" },
    { key: "llm_base_api", label: "LLM Base API URL", group: "llm", type: "text", placeholder: "http://host:port/" },
    { key: "llm_model", label: "LLM Model", group: "llm", type: "text", placeholder: "e.g. qwen35, gpt-4o-mini" },
    { key: "api_key", label: "API Key", group: "llm", type: "password", placeholder: "Enter API key" },
    { key: "poll_interval", label: "Poll Interval (seconds)", group: "pipeline", type: "number", min: 60, max: 86400 },
  ];const GROUP_LABELS: Record<string, string> = {
  database: "Database Connection (read-only)",
  llm: "LLM Configuration",
  pipeline: "Pipeline Settings",
};
const GROUPS = ["database", "llm", "pipeline"];

interface SourceForm {
  name: string;
  host: string;
  port: string;
  base_path: string;
  poll_seconds: number;
  monitor_keys: string;
  similarity_threshold: number;
  mode: string;
}

const EMPTY_SOURCE: SourceForm = {
  name: "",
  host: "",
  port: "",
  base_path: "kx",
  poll_seconds: 300,
  monitor_keys: "default",
  similarity_threshold: 0.92,
  mode: "v1",
};

interface KvmSource {
  id: string;
  name: string;
  host: string;
  port: number;
  base_path?: string;
  poll_seconds: number;
  monitor_keys?: string[];
  similarity_threshold?: number;
  mode?: string;
  enabled: boolean;
  last_polled_at?: string;
}

interface QueueStatsData {
  pending: number;
  processing: number;
  completed: number;
  failed: number;
  recent_errors?: { monitor_key: string; error: string; time?: string; source_id?: string }[];
}

export default function SettingsPage() {
  const [backendUrl] = useState(DEFAULT_BACKEND);
  const base = normalizeBaseUrl(backendUrl);

  // ---- system config state ----
  const [config, setConfig] = useState<Record<string, any>>({});
  const [draft, setDraft] = useState<Record<string, any>>({});
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [status, setStatus] = useState("");
  const [error, setError] = useState("");
  const [showApiKey, setShowApiKey] = useState(false);

  // ---- KVM sources state ----
  const [sources, setSources] = useState<KvmSource[]>([]);
  const [showAddForm, setShowAddForm] = useState(false);
  const [editingId, setEditingId] = useState<string | null>(null);
  const [sourceForm, setSourceForm] = useState<SourceForm>({ ...EMPTY_SOURCE });
  const [sourceLoading, setSourceLoading] = useState(false);

  // ---- Queue state ----
  const [queueStats, setQueueStats] = useState<QueueStatsData>({
    pending: 0,
    processing: 0,
    completed: 0,
    failed: 0,
    recent_errors: [],
  });

  // ---- Load config ----
  const loadConfig = useCallback(async () => {
    setLoading(true);
    setError("");
    try {
      const data = await fetchJSON<Record<string, any>>(base, "/api/config");
      setConfig(data);
      setDraft(data);
      setStatus("Config loaded.");
    } catch (err: any) {
      setError(err.message || "Failed to load config.");
    } finally {
      setLoading(false);
    }
  }, [base]);

  // ---- Load sources ----
  const loadSources = useCallback(async () => {
    try {
      const data = await fetchJSON<KvmSource[]>(base, "/api/kvm-sources");
      setSources(data);
    } catch (err: any) {
      setError(err.message || "Failed to load sources.");
    }
  }, [base]);

  // ---- Load queue stats ----
  const loadQueue = useCallback(async () => {
    try {
      const data = await fetchJSON<QueueStatsData>(base, "/api/queue");
      setQueueStats(data);
    } catch {
      // silent
    }
  }, [base]);

  useEffect(() => {
    loadConfig();
    loadSources();
    loadQueue();
    const iv = setInterval(loadQueue, 5000);
    return () => clearInterval(iv);
  }, [loadConfig, loadSources, loadQueue]);

  // ---- Config handlers ----
  function handleChange(key: string, value: string) {
    setDraft((prev) => ({ ...prev, [key]: value }));
  }

  function hasChanges(): boolean {
    return FIELD_META.filter((f) => !f.readOnly).some(
      (f) => String(draft[f.key] ?? "") !== String(config[f.key] ?? ""),
    );
  }

  async function handleSave() {
    setSaving(true);
    setError("");
    setStatus("Saving...");
    try {
      const payload: Record<string, any> = {};
      for (const field of FIELD_META) {
        if (field.readOnly) continue;
        const newVal = draft[field.key];
        const oldVal = config[field.key];
        if (String(newVal ?? "") !== String(oldVal ?? "")) {
          payload[field.key] = field.type === "number" ? Number(newVal) : newVal;
        }
      }
      const data = await putJSON<Record<string, any>>(base, "/api/config", payload);
      setConfig(data);
      setDraft(data);
      setStatus("Settings saved successfully!");
    } catch (err: any) {
      setError(err.message || "Failed to save settings.");
      setStatus("");
    } finally {
      setSaving(false);
    }
  }

  async function handleReset() {
    if (!window.confirm("Reset all settings to .env defaults?")) return;
    setSaving(true);
    setError("");
    setStatus("Resetting...");
    try {
      const data = await postJSON<Record<string, any>>(base, "/api/config/reset");
      setConfig(data);
      setDraft(data);
      setStatus("Settings reset to defaults.");
    } catch (err: any) {
      setError(err.message || "Failed to reset settings.");
      setStatus("");
    } finally {
      setSaving(false);
    }
  }

  // ---- KVM source handlers ----
  function openAddForm() {
    setEditingId(null);
    setSourceForm({ ...EMPTY_SOURCE });
    setShowAddForm(true);
  }

  function openEditForm(src: KvmSource) {
    setShowAddForm(false);
    setEditingId(src.id);
    setSourceForm({
      name: src.name || "",
      host: src.host || "",
      port: String(src.port || ""),
      base_path: src.base_path || "kx",
      poll_seconds: src.poll_seconds || 300,
      monitor_keys: (src.monitor_keys || []).join(", "),
      similarity_threshold: src.similarity_threshold || 0.92,
      mode: src.mode || "v2",
    });
  }

  function cancelForm() {
    setShowAddForm(false);
    setEditingId(null);
    setSourceForm({ ...EMPTY_SOURCE });
  }

  async function handleSourceSubmit(e: React.FormEvent) {
    e.preventDefault();
    setSourceLoading(true);
    setError("");
    try {
      const body: Record<string, any> = {
        name: sourceForm.name.trim(),
        host: sourceForm.host.trim(),
        port: Number(sourceForm.port),
        base_path: sourceForm.base_path.trim() || "kx",
        poll_seconds: Math.max(5, Number(sourceForm.poll_seconds) || 300),
        monitor_keys: sourceForm.monitor_keys
          .split(",")
          .map((s) => s.trim())
          .filter(Boolean),
        similarity_threshold: Math.min(
          0.999,
          Math.max(0.5, Number(sourceForm.similarity_threshold) || 0.92),
        ),
        mode: sourceForm.mode || "v2",
      };

      if (editingId) {
        await putJSON(base, `/api/kvm-sources/${editingId}`, body);
        setStatus("Source updated.");
      } else {
        body.enabled = true;
        await postJSON(base, "/api/kvm-sources", body);
        setStatus("Source created.");
      }
      cancelForm();
      await loadSources();
    } catch (err: any) {
      setError(err.message || "Failed to save source.");
    } finally {
      setSourceLoading(false);
    }
  }

  async function toggleSource(id: string, enabled: boolean) {
    try {
      await patchFetch(base, `/api/kvm-sources/${id}/toggle?enabled=${enabled}`);
      await loadSources();
    } catch (err: any) {
      setError(err.message || "Toggle failed.");
    }
  }

  async function deleteSource(id: string) {
    if (!window.confirm("Delete this KVM source?")) return;
    try {
      await deleteFetch(base, `/api/kvm-sources/${id}`);
      await loadSources();
      setStatus("Source deleted.");
    } catch (err: any) {
      setError(err.message || "Delete failed.");
    }
  }

  async function runOnce(id: string) {
    try {
      setStatus("Running one-time snapshot...");
      await postJSON(base, `/api/kvm-sources/${id}/run-once`);
      setStatus("One-time snapshot completed.");
      loadQueue();
    } catch (err: any) {
      setError(err.message || "Run-once failed.");
    }
  }

  // ---- Render helpers ----
  function renderField(field: FieldMeta) {
    const value = draft[field.key] ?? "";
    const isDisabled = field.readOnly || saving;

    if (field.type === "textarea") {
      return (
        <label key={field.key} className="field-full">
          <span className="field-label">{field.label}</span>
          <textarea
            className="field-textarea"
            rows={field.rows || 6}
            value={value}
            disabled={isDisabled}
            onChange={(e) => handleChange(field.key, e.target.value)}
          />
        </label>
      );
    }

    const inputType =
      field.type === "password" && !showApiKey
        ? "password"
        : field.type === "password"
          ? "text"
          : field.type;

    return (
      <label key={field.key} className="field">
        <span className="field-label">{field.label}</span>
        <div className="field-input-row">
          <input
            type={inputType}
            value={value}
            disabled={isDisabled}
            placeholder={field.placeholder || ""}
            min={field.min}
            max={field.max}
            onChange={(e) => handleChange(field.key, e.target.value)}
          />
          {field.type === "password" && (
            <button
              type="button"
              className="btn-toggle-vis"
              onClick={() => setShowApiKey((v) => !v)}
              title={showApiKey ? "Hide" : "Show"}
            >
              {showApiKey ? "Hide" : "Show"}
            </button>
          )}
        </div>
      </label>
    );
  }

  function renderSourceForm() {
    return (
      <form className="source-form" onSubmit={handleSourceSubmit}>
        <div className="source-form-grid">
          <label className="field">
            <span className="field-label">Name</span>
            <input
              required
              value={sourceForm.name}
              onChange={(e) =>
                setSourceForm((f) => ({ ...f, name: e.target.value }))
              }
              placeholder="e.g. kvm-machine-1"
            />
          </label>
          <label className="field">
            <span className="field-label">Host</span>
            <input
              required
              value={sourceForm.host}
              onChange={(e) =>
                setSourceForm((f) => ({ ...f, host: e.target.value }))
              }
              placeholder="e.g. 10.128.0.4"
            />
          </label>
          <label className="field">
            <span className="field-label">Port</span>
            <input
              required
              type="number"
              min={1}
              max={65535}
              value={sourceForm.port}
              onChange={(e) =>
                setSourceForm((f) => ({ ...f, port: e.target.value }))
              }
              placeholder="9081"
            />
          </label>
          <label className="field">
            <span className="field-label">Base Path</span>
            <input
              value={sourceForm.base_path}
              onChange={(e) =>
                setSourceForm((f) => ({ ...f, base_path: e.target.value }))
              }
              placeholder="kx"
            />
          </label>
          <label className="field">
            <span className="field-label">Poll Interval (s)</span>
            <input
              type="number"
              min={5}
              max={86400}
              value={sourceForm.poll_seconds}
              onChange={(e) =>
                setSourceForm((f) => ({
                  ...f,
                  poll_seconds: Number(e.target.value),
                }))
              }
            />
          </label>
          <label className="field">
            <span className="field-label">Monitor Keys</span>
            <input
              value={sourceForm.monitor_keys}
              onChange={(e) =>
                setSourceForm((f) => ({
                  ...f,
                  monitor_keys: e.target.value,
                }))
              }
              placeholder="default (comma-separated)"
            />
          </label>
          <label className="field">
            <span className="field-label">Similarity</span>
            <input
              type="number"
              step="0.01"
              min={0.5}
              max={0.999}
              value={sourceForm.similarity_threshold}
              onChange={(e) =>
                setSourceForm((f) => ({
                  ...f,
                  similarity_threshold: Number(e.target.value),
                }))
              }
            />
          </label>
          <label className="field">
            <span className="field-label">Pipeline Mode</span>
            <select
              value={sourceForm.mode}
              onChange={(e) =>
                setSourceForm((f) => ({ ...f, mode: e.target.value }))
              }
            >
              <option value="v1">v1 (Markdown + JSON)</option>
              <option value="v2">v2 (Direct JSON)</option>
            </select>
          </label>
        </div>
        <div className="source-form-actions">
          <button type="submit" disabled={sourceLoading}>
            {sourceLoading
              ? "Saving..."
              : editingId
                ? "Update Source"
                : "Add Source"}
          </button>
          <button type="button" className="btn-secondary" onClick={cancelForm}>
            Cancel
          </button>
        </div>
      </form>
    );
  }

  if (loading) {
    return (
      <main className="page">
        <section className="hero">
          <h1>Settings</h1>
          <p>Loading configuration...</p>
        </section>
      </main>
    );
  }

  return (
    <main className="page">
      <section className="hero">
        <h1>System Settings</h1>
        <p>
          Configure LLM, pipeline, and prompt parameters. Database settings are
          read-only (set via <code>.env</code>).
        </p>
      </section>

      {(status || error) && (
        <section className="card status-card">
          {status && (
            <div>
              <strong>Status:</strong> {status}
            </div>
          )}
          {error && <p className="error">{error}</p>}
        </section>
      )}

      {/* ======== KVM Sources Management ======== */}
      <section className="card settings-group">
        <div className="section-header">
          <h2>KVM Sources</h2>
          <button onClick={openAddForm} disabled={sourceLoading}>
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
                    <td>
                      {(src.monitor_keys || []).join(", ") || "default"}
                    </td>
                    <td>
                      <span className="badge">{src.mode || "v2"}</span>
                    </td>
                    <td>
                      <button
                        className={`btn-toggle ${src.enabled ? "btn-on" : "btn-off"}`}
                        onClick={() => toggleSource(src.id, !src.enabled)}
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
                        onClick={() => runOnce(src.id)}
                        title="Run one snapshot now"
                      >
                        Run
                      </button>
                      <button
                        className="btn-sm btn-secondary"
                        onClick={() => openEditForm(src)}
                        title="Edit source"
                      >
                        Edit
                      </button>
                      <button
                        className="btn-sm btn-danger"
                        onClick={() => deleteSource(src.id)}
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
            No KVM sources configured. Click &quot;+ Add Source&quot; to create
            one.
          </p>
        )}

        {editingId && (
          <div style={{ marginTop: 12 }}>
            <h3>Edit Source</h3>
            {renderSourceForm()}
          </div>
        )}
      </section>

      {/* ======== Pipeline Queue ======== */}
      <section className="card settings-group">
        <div className="section-header">
          <h2>Pipeline Queue</h2>
          <button className="btn-secondary" onClick={loadQueue}>
            Refresh
          </button>
        </div>
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
            <span className="queue-count q-failed">{queueStats.failed}</span>
            <span className="queue-label">Failed</span>
          </div>
        </div>

        {queueStats.recent_errors && queueStats.recent_errors.length > 0 && (
          <details style={{ marginTop: 10 }}>
            <summary className="muted" style={{ cursor: "pointer" }}>
              Recent errors ({queueStats.recent_errors.length})
            </summary>
            <ul className="error-list">
              {queueStats.recent_errors.map((e, i) => (
                <li key={i} className="error">
                  {e.monitor_key} — {e.error}
                </li>
              ))}
            </ul>
          </details>
        )}
      </section>

      {/* ======== Existing config groups ======== */}
      {GROUPS.map((group) => {
        const groupFields = FIELD_META.filter((f) => f.group === group);
        if (!groupFields.length) return null;

        return (
          <section key={group} className="card settings-group">
            <h2>{GROUP_LABELS[group]}</h2>
            <div className="settings-fields">
              {groupFields.map(renderField)}
            </div>
          </section>
        );
      })}

      <section className="card settings-actions">
        <button onClick={handleSave} disabled={saving || !hasChanges()}>
          {saving ? "Saving..." : "Save Changes"}
        </button>
        <button className="btn-secondary" onClick={handleReset} disabled={saving}>
          Reset to Defaults
        </button>
        <button className="btn-secondary" onClick={loadConfig} disabled={saving}>
          Reload
        </button>
      </section>
    </main>
  );
}
