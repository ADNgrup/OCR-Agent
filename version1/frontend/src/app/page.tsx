"use client";

import Link from "next/link";
import { useCallback, useEffect, useMemo, useState } from "react";

import ColorSequenceChart from "@/components/ColorSequenceChart";
import EntityModal from "@/components/EntityModal";
import TimeseriesChart from "@/components/TimeseriesChart";
import {
  DEFAULT_BACKEND,
  fetchJSON,
  normalizeBaseUrl,
  patchFetch,
  postJSON,
  putJSON,
  deleteFetch,
} from "@/lib/api";

/* eslint-disable @typescript-eslint/no-explicit-any */

import {
  KvmSource,
  Screen,
  Entity,
  LogEntry,
  QueueStats,
  Preview
} from "@/types/dashboard";

function formatDate(value: string | undefined): string {
  if (!value) return "";
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return "";
  return d.toLocaleString();
}

/** Badge color for value type */
function typeColor(vtype: string | undefined): string {
  if (vtype === "number") return "#2563eb";
  if (vtype === "color") return "#7c3aed";
  if (vtype === "bool") return "#059669";
  if (vtype === "text") return "#c2410c";
  return "#667085";
}

export default function DashboardPage() {
  const [backendUrl, setBackendUrl] = useState(DEFAULT_BACKEND);
  const [sources, setSources] = useState<KvmSource[]>([]);
  const [screens, setScreens] = useState<Screen[]>([]);
  const [entities, setEntities] = useState<Entity[]>([]);
  const [selectedEntityIds, setSelectedEntityIds] = useState<string[]>([]);
  const [logs, setLogs] = useState<LogEntry[]>([]);
  const [series, setSeries] = useState<Record<string, any>>({});
  const [preview, setPreview] = useState<Preview | null>(null);

  const [showEntityModal, setShowEntityModal] = useState(false);
  const [editingEntityId, setEditingEntityId] = useState<string | null>(null);
  const [entityForm, setEntityForm] = useState({
    main_entity_name: "",
    type: "HMI Object",
    region: "center",
    indicators: [{ _id: "new", label: "", metric: "", value_type: "text", unit: "" }] as any[],
    subentities: [{ _id: "new", col: "", row: "", value_type: "text", unit: "" }] as any[]
  });

  const [sourceId, setSourceId] = useState("");
  const [screenId, setScreenId] = useState("");
  const [hours, setHours] = useState(24);

  const [loading, setLoading] = useState(false);
  const [status, setStatus] = useState("Ready");
  const [error, setError] = useState("");

  // Queue stats
  const [queueStats, setQueueStats] = useState<QueueStats>({
    pending: 0,
    processing: 0,
    completed: 0,
    failed: 0,
  });

  const selectedSource = useMemo(
    () => sources.find((s) => s.id === sourceId),
    [sources, sourceId],
  );
  const selectedScreen = useMemo(
    () => screens.find((s) => s.id === screenId),
    [screens, screenId],
  );
  void selectedSource;
  void selectedScreen;

  const base = useMemo(() => normalizeBaseUrl(backendUrl), [backendUrl]);

  // ---- Load queue stats (auto-refresh) ----
  async function loadQueue() {
    try {
      setQueueStats(await fetchJSON<QueueStats>(base, "/api/queue"));
    } catch {
      /* silent */
    }
  }

  async function refreshSourcesQuiet() {
    try {
      setSources(await fetchJSON<KvmSource[]>(base, "/api/kvm-sources"));
    } catch {
      /* silent */
    }
  }

  useEffect(() => {
    loadQueue();
    refreshSourcesQuiet();
    const iv = setInterval(() => {
      loadQueue();
      refreshSourcesQuiet();
    }, 5000);
    return () => clearInterval(iv);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [base]);

  // ---- Load entities for current screen ----
  const loadEntities = useCallback(
    async (gid: string) => {
      if (!gid) {
        setEntities([]);
        return [];
      }
      try {
        const data = await fetchJSON<Entity[]>(
          base,
          `/api/entities?screen_group_id=${gid}`,
        );
        setEntities(data);
        return data;
      } catch {
        setEntities([]);
        return [];
      }
    },
    [base],
  );

  // ---- Load preview image for current screen ----
  const loadPreview = useCallback(
    async (gid: string) => {
      if (!gid) {
        setPreview(null);
        return;
      }
      try {
        const data = await fetchJSON<Preview>(
          base,
          `/api/screens/${gid}/preview`,
        );
        setPreview(data);
      } catch {
        setPreview(null);
      }
    },
    [base],
  );

  // ---- Load logs + timeseries (optionally filtered by entity IDs) ----
  const loadData = useCallback(
    async (gid: string, rangeHours: number, eids: string[]) => {
      if (!gid) return;
      const safeHours = Math.max(1, Math.min(168, Number(rangeHours) || 24));
      let entityParam = "";
      if (eids && eids.length > 0) {
        entityParam = `&entity_ids=${eids.join(",")}`;
      }
      const [logsData, seriesData] = await Promise.all([
        fetchJSON<LogEntry[]>(
          base,
          `/api/logs?screen_group_id=${gid}&hours=${safeHours}&limit=500${entityParam}`,
        ),
        fetchJSON<Record<string, any>>(
          base,
          `/api/timeseries?screen_group_id=${gid}&hours=${safeHours}${entityParam}`,
        ),
      ]);
      setLogs(logsData);
      setSeries(seriesData);
    },
    [base],
  );

  // ---- When a screen is selected ----
  async function onScreenSelected(gid: string) {
    setSelectedEntityIds([]);
    await Promise.all([loadEntities(gid), loadPreview(gid)]);
    // load all data initially (no entity filter)
    await loadData(gid, hours, []);
  }

  // ---- Load screens for a source ----
  async function loadScreens(sid: string) {
    if (!sid) return;
    const data = await fetchJSON<Screen[]>(
      base,
      `/api/screens?source_id=${sid}`,
    );
    setScreens(data);
    if (!data.length) {
      setScreenId("");
      setEntities([]);
      setSelectedEntityIds([]);
      setLogs([]);
      setSeries({});
      setPreview(null);
      return;
    }
    const nextScreenId = data.some((s) => s.id === screenId)
      ? screenId
      : data[0].id;
    setScreenId(nextScreenId);
    await onScreenSelected(nextScreenId);
  }

  // ---- Load sources ----
  async function loadSources() {
    const data = await fetchJSON<KvmSource[]>(base, "/api/kvm-sources");
    setSources(data);
    if (!data.length) {
      setSourceId("");
      setScreens([]);
      setScreenId("");
      setEntities([]);
      setSelectedEntityIds([]);
      setLogs([]);
      setSeries({});
      setPreview(null);
      return;
    }
    const nextSourceId = data.some((s) => s.id === sourceId)
      ? sourceId
      : data[0].id;
    setSourceId(nextSourceId);
    await loadScreens(nextSourceId);
  }

  async function refreshAll(msg = "Refreshing dashboard...") {
    setLoading(true);
    setError("");
    setStatus(msg);
    try {
      await loadSources();
      setStatus("Dashboard loaded.");
    } catch (err: any) {
      setError(err.message || "Unknown error");
      setStatus("Failed to load dashboard.");
    } finally {
      setLoading(false);
    }
  }

  // eslint-disable-next-line react-hooks/exhaustive-deps
  useEffect(() => {
    refreshAll("Loading dashboard...");
  }, []);

  // ---- Handlers ----
  async function onApplyBackend() {
    await refreshAll("Applying backend URL...");
  }

  async function onSourceChange(sid: string) {
    setSourceId(sid);
    setLoading(true);
    setError("");
    setStatus("Loading screens...");
    try {
      await loadScreens(sid);
      setStatus("Screens loaded.");
    } catch (err: any) {
      setError(err.message);
      setStatus("Failed.");
    } finally {
      setLoading(false);
    }
  }

  async function onScreenChange(gid: string) {
    setScreenId(gid);
    setLoading(true);
    setError("");
    setStatus("Loading screen data...");
    try {
      await onScreenSelected(gid);
      setStatus("Screen data loaded.");
    } catch (err: any) {
      setError(err.message);
      setStatus("Failed.");
    } finally {
      setLoading(false);
    }
  }

  function toggleEntitySelection(entityId: string) {
    setSelectedEntityIds((prev) =>
      prev.includes(entityId)
        ? prev.filter((id) => id !== entityId)
        : [...prev, entityId],
    );
  }

  function openAddEntityModal() {
    setEntityForm({
      main_entity_name: "",
      type: "HMI Object",
      region: "center",
      indicators: [{ _id: Math.random().toString(36).substring(2, 9), label: "", metric: "", value_type: "text", unit: "" }],
      subentities: [{ _id: Math.random().toString(36).substring(2, 9), col: "", row: "", value_type: "text", unit: "" }]
    });
    setEditingEntityId(null);
    setShowEntityModal(true);
  }

  function openEditEntityModal(ent: Entity) {
    const indsObj = ent.indicators || ent.metrics || {};
    let inds = Object.keys(indsObj).map((mk) => ({
      _id: Math.random().toString(36).substring(2, 9),
      label: indsObj[mk].indicator_label || indsObj[mk].label || "",
      metric: indsObj[mk].metric || indsObj[mk].metric_key || mk,
      value_type: indsObj[mk].value_type || "text",
      unit: indsObj[mk].unit || ""
    }));

    if (!inds.length && ent.indicators && Array.isArray(ent.indicators)) {
       inds = ent.indicators.map(ind => ({
         _id: Math.random().toString(36).substring(2, 9),
         label: ind.label || "",
         metric: ind.metric || "",
         value_type: ind.value_type || "text",
         unit: ind.unit || ""
       }));
    }

    const subs = ent.subentities && Array.isArray(ent.subentities) ? ent.subentities.map(sub => ({
       _id: Math.random().toString(36).substring(2, 9),
       col: sub.col || "",
       row: sub.row || "",
       value_type: sub.value_type || "text",
       unit: sub.unit || ""
    })) : [];
    
    setEntityForm({
      main_entity_name: ent.display_name || ent.main_entity_name || "",
      type: ent.entity_type || ent.type || "HMI Object",
      region: ent.region || "center",
      indicators: inds.length ? inds : [{ _id: Math.random().toString(36).substring(2, 9), label: "", metric: "", value_type: "text", unit: "" }],
      subentities: subs.length ? subs : [{ _id: Math.random().toString(36).substring(2, 9), col: "", row: "", value_type: "text", unit: "" }]
    });
    setEditingEntityId(ent.id);
    setShowEntityModal(true);
  }

  async function saveEntityForm() {
    if (!screenId) return;
    setStatus("Saving entity...");
    try {
      const payload: any = {
        screen_group_id: screenId,
        main_entity_name: entityForm.main_entity_name,
        type: entityForm.type,
        region: entityForm.region,
      };

      if (entityForm.type.toLowerCase() === "table") {
        payload.subentities = entityForm.subentities.map((sub: any) => ({
          col: sub.col,
          row: sub.row,
          value_type: sub.value_type,
          unit: sub.unit
        }));
      } else if (entityForm.type.toLowerCase() === "log/alert" || entityForm.type.toLowerCase() === "log") {
        // Log doesn't need indicators/subentities
      } else {
        payload.indicators = entityForm.indicators.map((ind: any) => ({
          label: ind.label,
          metric: ind.metric,
          value_type: ind.value_type,
          unit: ind.unit
        }));
      }

      if (editingEntityId) {
        await putJSON(base, `/api/entities/${editingEntityId}`, payload);
      } else {
        await postJSON(base, `/api/entities`, payload);
      }
      setShowEntityModal(false);
      await loadEntities(screenId);
      setStatus("Entity saved.");
    } catch (err: any) {
      setError(err.message || "Failed to save entity.");
      setStatus("Save failed.");
    }
  }

  async function deleteEntity(id: string) {
    if (!confirm("Are you sure you want to delete this entity?")) return;
    setStatus("Deleting entity...");
    try {
      await deleteFetch(base, `/api/entities/${id}?screen_group_id=${screenId}`);
      await loadEntities(screenId);
      setStatus("Entity deleted.");
    } catch (err: any) {
      setError(err.message || "Failed to delete entity.");
      setStatus("Delete failed.");
    }
  }

  async function deleteSelectedEntities() {
    if (!selectedEntityIds.length) return;
    if (
      !confirm(
        `Are you sure you want to delete ${selectedEntityIds.length} selected entities?`
      )
    )
      return;

    setStatus(`Deleting ${selectedEntityIds.length} entities...`);
    try {
      await postJSON(base, "/api/entities/batch-delete", { 
        entity_ids: selectedEntityIds,
        screen_group_id: screenId
      });
      setSelectedEntityIds([]);
      await loadEntities(screenId);
      setStatus("Selected entities deleted.");
    } catch (err: any) {
      setError(err.message || "Failed to delete selected entities.");
      setStatus("Delete failed.");
    }
  }

  async function toggleScreenIgnore(sid: string, ignored: boolean) {
    if (!sid) return;
    setStatus(ignored ? "Ignoring screen..." : "Unignoring screen...");
    try {
      await postJSON(base, `/api/screens/${sid}/toggle-ignore`, { ignored });
      // Update local screens state
      setScreens(prev => prev.map(s => s.id === sid ? { ...s, ignored } : s));
      setStatus(ignored ? "Screen ignored." : "Screen unignored.");
    } catch (err: any) {
      setError(err.message || "Failed to toggle screen ignore.");
      setStatus("Failed.");
    }
  }

  function selectAllEntities() {
    setSelectedEntityIds(entities.map((e) => e.id));
  }

  function clearEntitySelection() {
    setSelectedEntityIds([]);
  }

  async function onMonitorSelected() {
    if (!screenId) return;
    setLoading(true);
    setError("");
    setStatus("Loading data for selected entities...");
    try {
      await loadData(screenId, hours, selectedEntityIds);
      setStatus("Data loaded.");
    } catch (err: any) {
      setError(err.message);
      setStatus("Failed.");
    } finally {
      setLoading(false);
    }
  }

  async function onRefreshData() {
    if (!screenId) return;
    setLoading(true);
    setError("");
    setStatus("Refreshing...");
    try {
      await Promise.all([loadEntities(screenId), loadPreview(screenId)]);
      await loadData(screenId, hours, selectedEntityIds);
      setStatus("Data refreshed.");
    } catch (err: any) {
      setError(err.message);
      setStatus("Failed.");
    } finally {
      setLoading(false);
    }
  }

  // ---- Source toggle & run-once ----
  async function toggleSource(id: string, enabled: boolean) {
    try {
      await patchFetch(
        base,
        `/api/kvm-sources/${id}/toggle?enabled=${enabled}`,
      );
      await loadSources();
    } catch (err: any) {
      setError(err.message || "Toggle failed.");
    }
  }

  async function runOnce(id: string) {
    try {
      setStatus("Running one-time snapshot...");
      await postJSON(base, `/api/kvm-sources/${id}/run-once`);
      setStatus("Snapshot queued.");
      loadQueue();
    } catch (err: any) {
      setError(err.message || "Run-once failed.");
    }
  }

  // ---- Snapshot image URL helper ----
  function snapshotImgUrl(imageUrl: string | undefined): string | null {
    if (!imageUrl) return null;
    if (imageUrl.startsWith("data:")) return imageUrl;
    return `${base}${imageUrl}`;
  }

  return (
    <main className="page">
      <section className="hero" style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
        <div>
          <h1>KVM OCR Monitoring Dashboard</h1>
          <p>
            Monitor KVM screens, entities, indicators, and timeseries from the OCR
            + LLM pipeline.
          </p>
        </div>
        <div style={{ display: "flex", gap: "10px" }}>
          <Link href="/snapshots" className="btn-primary" style={{ padding: "8px 16px", textDecoration: "none", borderRadius: "4px" }}>
            Check Snapshots
          </Link>
          <Link href="/settings" className="btn-secondary" style={{ padding: "8px 16px", textDecoration: "none", borderRadius: "4px" }}>
            Settings
          </Link>
        </div>
      </section>

      {/* ======== Source Controls + Queue ======== */}
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
                      {src.enabled
                        ? `Polling every ${src.poll_seconds}s`
                        : "Stopped"}
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
                    <button
                      className="btn-sm"
                      onClick={() => runOnce(src.id)}
                    >
                      Run Once
                    </button>
                  </div>
                </div>
              ))}
            </div>
          ) : (
            <p className="muted">
              No sources. Go to Settings to add KVM sources.
            </p>
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
                <span
                  className={`queue-count q-failed ${queueStats.failed > 0 ? "text-error" : ""}`}
                >
                  {queueStats.failed}
                </span>
                <span className="queue-label">Failed</span>
              </div>
            </div>    
          </section>
          {/* ======== Controls ======== */}
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
                        {s.name} {s.ignored ? "(Ignored)" : ""}
                      </option>
                    ))}
                  </select>
                  {screenId && (
                    <label style={{ display: "inline-flex", alignItems: "center", gap: "4px", fontSize: "12px", cursor: "pointer", whiteSpace: "nowrap" }}>
                      <input
                        type="checkbox"
                        checked={!!screens.find(s => s.id === screenId)?.ignored}
                        onChange={(e) => toggleScreenIgnore(screenId, e.target.checked)}
                      />
                      Skip OCR
                    </label>
                  )}
                </div>
              </label>
              <label>
                Hours
                <input
                  type="number"
                  min={1}
                  max={168}
                  value={hours}
                  onChange={(e) =>
                    setHours(
                      Math.max(1, Math.min(168, Number(e.target.value) || 24)),
                    )
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



      {/* ======== Scene Preview + Entities ======== */}
      <div className="row-2col row-2col-stretch">
        <section className="card">
          <h2>Scene Preview</h2>
          {preview ? (
            <div className="scene-preview">
              <img
                src={snapshotImgUrl(preview.image_url) ?? undefined}
                alt="Latest snapshot"
                className="preview-img"
              />
              <p
                className="muted"
                style={{ marginTop: 8, fontSize: 12 }}
              >
                Captured: {formatDate(preview.created_at)}
              </p>
            </div>
          ) : (
            <p className="muted">
              No snapshots available for this screen.
            </p>
          )}
        </section>

        <section className="card">
          <div className="section-header">
            <h2>Entities & Indicators ({entities.length})</h2>
            <div style={{ display: "flex", gap: 6 }}>
              <button
                className="btn-sm btn-primary"
                onClick={openAddEntityModal}
                disabled={!screenId || loading}
              >
                + Add Entity
              </button>
              <button
                className="btn-sm btn-secondary"
                onClick={selectAllEntities}
                disabled={!entities.length}
              >
                Select All
              </button>
              <button
                className="btn-sm btn-secondary"
                onClick={clearEntitySelection}
                disabled={!selectedEntityIds.length}
              >
                Clear
              </button>
              <button
                className="btn-sm"
                style={{
                  background: selectedEntityIds.length ? "#ef4444" : undefined,
                  color: selectedEntityIds.length ? "white" : undefined,
                }}
                onClick={deleteSelectedEntities}
                disabled={!selectedEntityIds.length || loading}
              >
                Delete Selected
              </button>
              <button
                className="btn-sm"
                onClick={onMonitorSelected}
                disabled={!selectedEntityIds.length || loading}
              >
                Monitor Selected
              </button>
            </div>
          </div>
          {entities.length ? (
            <div className="entity-list">
              {entities.map((ent) => {
                const selected = selectedEntityIds.includes(ent.id);
                const indicatorsObj = ent.indicators || ent.metrics || {};
                const indicatorKeys = Object.keys(indicatorsObj);
                return (
                  <div
                    key={ent.id}
                    className={`entity-row ${selected ? "entity-selected" : ""}`}
                    onClick={() => toggleEntitySelection(ent.id)}
                  >
                    <div className="entity-check">
                      <input
                        type="checkbox"
                        checked={selected}
                        readOnly
                        tabIndex={-1}
                      />
                    </div>
                    <div className="entity-info">
                      <div
                        style={{
                          display: "flex",
                          alignItems: "center",
                          gap: 8,
                          flexWrap: "wrap",
                        }}
                      >
                        <strong>{ent.display_name}</strong>
                        {ent.entity_type ? (
                          <span
                            className="type-pill"
                            style={{ background: "#475467" }}
                          >
                            {ent.entity_type}
                          </span>
                        ) : null}
                        {ent.region ? (
                          <span
                            className="type-pill"
                            style={{ background: "#10b981" }}
                          >
                            {ent.region}
                          </span>
                        ) : null}
                        <div style={{ flexGrow: 1 }} />
                        <button
                          className="btn-sm btn-secondary"
                          onClick={(e) => {
                            e.stopPropagation();
                            openEditEntityModal(ent);
                          }}
                        >
                          Edit
                        </button>
                        <button
                          className="btn-sm btn-secondary"
                          style={{ color: "red" }}
                          onClick={(e) => {
                            e.stopPropagation();
                            deleteEntity(ent.id);
                          }}
                        >
                          Delete
                        </button>
                      </div>
                      <div className="entity-metrics">
                        {ent.entity_type?.toLowerCase() === "table" ? (
                          <div style={{width: "100%", overflowX: "auto", marginTop: "1rem"}}>
                            {(() => {
                              const hasSubentities = ent.subentities && ent.subentities.length > 0;
                              let cols: string[] = [];
                              let rows: string[] = [];
                              const valueMap: Record<string, Record<string, any>> = {};

                              if (hasSubentities && ent.subentities) {
                                const colSet = new Set<string>();
                                const rowSet = new Set<string>();
                                ent.subentities.forEach((sub: any) => {
                                  if (sub.col) colSet.add(sub.col);
                                  if (sub.row) rowSet.add(sub.row);
                                });
                                cols = Array.from(colSet);
                                rows = Array.from(rowSet);

                                ent.subentities.forEach((sub: any) => {
                                  if (!valueMap[sub.row]) valueMap[sub.row] = {};
                                  valueMap[sub.row][sub.col] = sub;
                                });
                              }

                              return hasSubentities ? (
                                <table style={{width: "100%", borderCollapse: "collapse", fontSize: "0.85rem", textAlign: "center"}}>
                                  <thead>
                                    <tr>
                                      <th style={{borderBottom: "1px solid #ccc", padding: "4px", borderRight: "1px solid #ccc"}}></th>
                                      {cols.map(c => (
                                        <th key={c} style={{borderBottom: "1px solid #ccc", padding: "4px"}}>{c}</th>
                                      ))}
                                    </tr>
                                  </thead>
                                  <tbody>
                                    {rows.map(r => (
                                      <tr key={r}>
                                        <td style={{borderBottom: "1px solid #eee", padding: "4px", borderRight: "1px solid #eee", fontWeight: "bold", textAlign: "left"}}>{r}</td>
                                        {cols.map(c => {
                                          const sub = valueMap[r]?.[c];
                                          if (!sub) return <td key={c} style={{borderBottom: "1px solid #eee", padding: "4px"}}>-</td>;
                                          return (
                                            <td key={c} style={{borderBottom: "1px solid #eee", padding: "4px", color: typeColor(sub.value_type)}}>
                                              {sub.value_raw ?? "-"} {sub.unit || ""}
                                            </td>
                                          );
                                        })}
                                      </tr>
                                    ))}
                                  </tbody>
                                </table>
                              ) : (
                                <table style={{width: "100%", borderCollapse: "collapse", fontSize: "0.85rem"}}>
                                  <thead>
                                    <tr>
                                      <th style={{borderBottom: "1px solid #ccc", textAlign: "left", padding: "4px"}}>Col</th>
                                      <th style={{borderBottom: "1px solid #ccc", textAlign: "left", padding: "4px"}}>Row</th>
                                      <th style={{borderBottom: "1px solid #ccc", textAlign: "left", padding: "4px"}}>Value</th>
                                    </tr>
                                  </thead>
                                  <tbody>
                                    {indicatorKeys.map((mk) => {
                                      const m = indicatorsObj[mk];
                                      const indicatorName = m.indicator_label || m.display_name || mk;
                                      return (
                                        <tr key={mk}>
                                          <td style={{borderBottom: "1px solid #eee", padding: "4px"}}>{indicatorName}</td>
                                          <td style={{borderBottom: "1px solid #eee", padding: "4px"}}>-</td>
                                          <td style={{borderBottom: "1px solid #eee", padding: "4px", color: typeColor(m.value_type)}}>
                                            {m.last_value || "-"} {m.unit || ""}
                                          </td>
                                        </tr>
                                      );
                                    })}
                                  </tbody>
                                </table>
                              );
                            })()}
                          </div>
                        ) : ent.entity_type?.toLowerCase() === "log/alert" || ent.entity_type?.toLowerCase() === "log" ? (
                          <div style={{width: "100%", marginTop: "1rem", display: "flex", flexDirection: "column", gap: "4px"}}>
                            {ent.logs && ent.logs.length > 0 ? ent.logs.map((lg, idx) => (
                              <div key={idx} style={{padding: "6px", background: "#fee2e2", borderLeft: "3px solid #ef4444", borderRadius: "4px", fontSize: "0.85rem"}}>
                                <strong>{lg.time}</strong> - {lg.name}: {lg.desc}
                              </div>
                            )) : (
                              <p className="muted" style={{fontSize: "0.85rem"}}>No logs found.</p>
                            )}
                          </div>
                        ) : (
                          indicatorKeys.map((mk) => {
                            const m = indicatorsObj[mk];
                            const indicatorName =
                              m.indicator_label || m.display_name || mk;
                            return (
                              <span
                                key={mk}
                                className="metric-badge"
                                style={{
                                  borderColor: typeColor(m.value_type),
                                }}
                              >
                                <span className="metric-name">
                                  {indicatorName}
                                </span>
                                <span
                                  className="metric-val"
                                  style={{
                                    color: typeColor(m.value_type),
                                  }}
                                >
                                  {m.last_value}
                                  {m.unit ? ` ${m.unit}` : ""}
                                </span>
                                <span
                                  className="metric-type"
                                  style={{
                                    background: typeColor(m.value_type),
                                  }}
                                >
                                  {m.value_type}
                                </span>
                              </span>
                            );
                          })
                        )}
                      </div>
                    </div>
                  </div>
                );
              })}
            </div>
          ) : (
            <p className="muted">
              No entities detected yet for this screen.
            </p>
          )}
        </section>
      </div>

      {/* ======== Timeseries + Logs ======== */}
      <div className="row-2col row-2col-stretch">
        <section className="card">
          <h2>Timeseries & Status</h2>
          {(() => {
            const allKeys = Object.keys(series || {});

            // --- Numeric charts grouped by individual metric to avoid mixed scales ---
            const entityGroups: Record<string, { numeric: Record<string, Record<string, any>>, color: { t: string; value: string; metric: string }[] }> = {};
            for (const key of allKeys) {
              const item = series[key];
              const entityName = item?.entity_name || "Unknown";
              const metricLabel = item?.metric_label || item?.metric || key;
              const unit = item?.unit ? ` (${item.unit})` : "";
              const chartName = `${metricLabel}${unit}`;

              if (!entityGroups[entityName]) {
                entityGroups[entityName] = { numeric: {}, color: [] };
              }
              if (!entityGroups[entityName].numeric[chartName]) {
                entityGroups[entityName].numeric[chartName] = {};
              }
              entityGroups[entityName].numeric[chartName][key] = item;
            }

            // --- Color sequences grouped by entity (from logs) ---
            for (const log of logs) {
              if (log.value_type !== "color") continue;
              const eName = log.entity_name || log.entity_key || "Unknown";
              if (!entityGroups[eName]) {
                entityGroups[eName] = { numeric: {}, color: [] };
              }
              entityGroups[eName].color.push({
                t: log.recorded_at,
                value: (log.value ?? "").toString(),
                metric: log.metric || "status",
              });
            }

            // Sort each group by time
            for (const eName of Object.keys(entityGroups)) {
              entityGroups[eName].color.sort(
                (a, b) => new Date(a.t).getTime() - new Date(b.t).getTime(),
              );
            }

            const entityEntries = Object.entries(entityGroups);
            const hasData = entityEntries.some(([, group]) => Object.keys(group.numeric).length > 0 || group.color.length > 0);

            if (!hasData)
              return (
                <p className="muted">
                  No data found for selected range.
                </p>
              );

            return (
              <div className="timeseries-entity-groups" style={{ display: "flex", flexDirection: "column", gap: "24px" }}>
                {entityEntries.map(([entityName, groupData]) => {
                  const numEntries = Object.entries(groupData.numeric);
                  const hasGroupData = numEntries.length > 0 || groupData.color.length > 0;
                  if (!hasGroupData) return null;

                  return (
                    <div key={entityName} className="entity-chart-card" style={{ border: "1px solid var(--border, #e5e7eb)", borderRadius: "8px", padding: "16px", background: "var(--bg, #fff)", boxShadow: "0 1px 3px rgba(0,0,0,0.05)" }}>
                      <h3 style={{ marginTop: 0, marginBottom: "16px", fontSize: "1.1rem", borderBottom: "1px solid var(--border, #e5e7eb)", paddingBottom: "8px" }}>
                        📍 Thực thể: <strong style={{ color: "var(--primary, #2563eb)"}}>{entityName}</strong>
                      </h3>
                      <div className="timeseries-grid">
                        {numEntries.map(([chartName, subset]) => (
                          <div
                            key={`num-${entityName}-${chartName}`}
                            className="timeseries-panel"
                          >
                            <h4 className="ts-panel-title">{chartName}</h4>
                            <TimeseriesChart series={subset} />
                          </div>
                        ))}
                        {groupData.color.length > 0 && (
                          <div
                            key={`clr-${entityName}`}
                            className="timeseries-panel color-panel"
                          >
                            <h4 className="ts-panel-title">
                              <span
                                className="type-pill"
                                style={{
                                  background: typeColor("color"),
                                  fontSize: 10,
                                  marginRight: 6,
                                }}
                              >
                                COLOR
                              </span>
                              Trạng thái / Màu sắc
                            </h4>
                            <ColorSequenceChart entries={groupData.color} />
                          </div>
                        )}
                      </div>
                    </div>
                  );
                })}
              </div>
            );
          })()}
        </section>

        <section className="card">
          <h2>Entity Logs</h2>
          {logs.length ? (
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Time</th>
                    <th>Entity</th>
                    <th>Indicator</th>
                    <th>Value</th>
                    <th>Type</th>
                  </tr>
                </thead>
                <tbody>
                  {logs.map((log) => (
                    <tr key={log.log_id}>
                      <td>{formatDate(log.recorded_at)}</td>
                      <td>
                        {log.entity_name || log.entity_key}
                      </td>
                      <td>{log.metric ?? ""}</td>
                      <td>
                        {(log.value ?? "").toString()}
                        {log.unit ? ` ${log.unit}` : ""}
                      </td>
                      <td>
                        <span
                          className="type-pill"
                          style={{
                            background: typeColor(log.value_type),
                          }}
                        >
                          {log.value_type}
                        </span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <p className="muted">
              No logs found for selected screen/time range.
            </p>
          )}
        </section>
      </div>

      <EntityModal
        show={showEntityModal}
        onClose={() => setShowEntityModal(false)}
        entityForm={entityForm}
        setEntityForm={setEntityForm}
        editingEntityId={editingEntityId}
        onSave={saveEntityForm}
      />

    </main>
  );
}
