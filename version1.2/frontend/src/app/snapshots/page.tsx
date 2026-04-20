"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { DEFAULT_BACKEND } from "@/lib/api";
import { useBackendApi } from "@/lib/hooks/useBackendApi";
import { KvmSource } from "@/types/dashboard";

function formatDate(value: string | undefined): string {
  if (!value) return "";
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return "";
  return d.toLocaleString();
}

function parseCsvLine(line: string): string[] {
  const out: string[] = [];
  let curr = "";
  let inQuotes = false;

  for (let i = 0; i < line.length; i += 1) {
    const ch = line[i];
    if (ch === '"') {
      if (inQuotes && line[i + 1] === '"') {
        curr += '"';
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }
    if (ch === "," && !inQuotes) {
      out.push(curr.trim());
      curr = "";
      continue;
    }
    curr += ch;
  }
  out.push(curr.trim());
  return out;
}

function getIndicatorDisplayValue(ind: any): string {
  if (ind?.value_raw !== undefined && ind?.value_raw !== null && String(ind.value_raw).trim() !== "") {
    return String(ind.value_raw);
  }
  if (ind?.value !== undefined && ind?.value !== null && String(ind.value).trim() !== "") {
    return String(ind.value);
  }
  if (ind?.value_number !== undefined && ind?.value_number !== null) {
    return String(ind.value_number);
  }
  return "[missing]";
}

function getEntityValuePayload(ent: any): any {
  if (ent?.value && typeof ent.value === "object") {
    return ent.value;
  }
  return ent;
}

function getTableSubentities(ent: any): any[] {
  const valuePayload = getEntityValuePayload(ent);

  const metadata = valuePayload?.metadata && typeof valuePayload.metadata === "object"
    ? valuePayload.metadata
    : (ent?.metadata && typeof ent.metadata === "object" ? ent.metadata : {});

  const valueColumns: string[] = Array.isArray(metadata.value_columns)
    ? metadata.value_columns.map((x: any) => String(x).trim()).filter(Boolean)
    : Array.isArray(valuePayload?.columns)
      ? valuePayload.columns.map((x: any) => String(x).trim()).filter(Boolean)
      : [];

  const rowsMeta: string[] = Array.isArray(metadata.rows)
    ? metadata.rows.map((x: any) => String(x).trim()).filter(Boolean)
    : Array.isArray(valuePayload?.rows)
      ? valuePayload.rows.map((x: any) => String(x).trim()).filter(Boolean)
      : ["value"];

  const unitParts = typeof metadata.unit === "string"
    ? metadata.unit.split("|").map((x: string) => x.trim())
    : [];

  const typeParts = typeof metadata.value_type === "string"
    ? metadata.value_type.split("|").map((x: string) => x.trim())
    : [];

  if (Array.isArray(valuePayload?.cells) && valuePayload.cells.length > 0) {
    const compactOut: any[] = [];
    for (const cell of valuePayload.cells) {
      if (!cell || typeof cell !== "object") continue;
      const r = Number(cell.r);
      const c = Number(cell.c);
      if (!Number.isInteger(r) || !Number.isInteger(c)) continue;
      if (r < 0 || c < 0 || r >= rowsMeta.length || c >= valueColumns.length) continue;

      const rawVal =
        cell.v ?? cell.value_number ?? cell.value_raw ?? cell.value ?? "";
      if (String(rawVal).trim() === "") continue;

      compactOut.push({
        col: valueColumns[c],
        row: rowsMeta[r],
        value_raw: String(rawVal),
        value_number: Number(rawVal),
        unit: unitParts[c] || "",
        value_type: typeParts[c] || "number",
      });
    }
    if (compactOut.length > 0) {
      return compactOut;
    }
  }

  if (Array.isArray(valuePayload?.subentities) && valuePayload.subentities.length > 0) {
    return valuePayload.subentities;
  }

  if (Array.isArray(ent?.subentities) && ent.subentities.length > 0) {
    return ent.subentities;
  }

  const rawCsvFromValue =
    typeof valuePayload?.raw_csv_table === "string" ? valuePayload.raw_csv_table.trim() : "";
  const rawCsv = rawCsvFromValue || (typeof ent?.raw_csv_table === "string" ? ent.raw_csv_table.trim() : "");
  if (!rawCsv) {
    return [];
  }

  const lines = rawCsv.split(/\r?\n/).map((l: string) => l.trim()).filter(Boolean);
  if (lines.length < 2) {
    return [];
  }

  const header = parseCsvLine(lines[0]);
  if (header.length < 2) {
    return [];
  }

  const fallbackColumns = valueColumns.length > 0 ? valueColumns : header.slice(1);

  const colIndex = new Map<string, number>();
  header.forEach((h: string, idx: number) => colIndex.set(h, idx));

  const out: any[] = [];
  for (let i = 1; i < lines.length; i += 1) {
    const row = parseCsvLine(lines[i]);
    const rowName = row[0] || "Unknown";
    fallbackColumns.forEach((col: string, idx: number) => {
      const pos = colIndex.get(col);
      if (pos === undefined || pos >= row.length) {
        return;
      }
      const valueRaw = row[pos] ?? "";
      if (!String(valueRaw).trim()) {
        return;
      }
      out.push({
        col,
        row: rowName,
        value_raw: valueRaw,
        unit: unitParts[idx] || "",
        value_type: typeParts[idx] || "text",
      });
    });
  }

  return out;
}

function asNonEmptyString(value: any): string {
  if (value === undefined || value === null) return "";
  const text = String(value).trim();
  return text;
}

function getLogMessageFromObject(lg: any): string {
  const directMessage = asNonEmptyString(lg?.message);
  if (directMessage) return directMessage;

  const msg = asNonEmptyString(lg?.msg);
  if (msg) return msg;

  const name = asNonEmptyString(lg?.name);
  const desc = asNonEmptyString(lg?.desc);
  if (name && desc) return `${name}: ${desc}`;
  if (name) return name;
  if (desc) return desc;

  if (lg && typeof lg === "object") {
    const entries = Object.entries(lg);
    const preferred = entries.find(([k, v]) => /message|msg|desc|name/i.test(k) && asNonEmptyString(v));
    if (preferred) return asNonEmptyString(preferred[1]);

    const fallback = entries.find(([k, v]) => k.toLowerCase() !== "time" && asNonEmptyString(v));
    if (fallback) return asNonEmptyString(fallback[1]);
  }

  return "";
}

function parseLogRowsFromCsv(rawCsv: string): any[] {
  if (!rawCsv) {
    return [];
  }

  const lines = rawCsv.split(/\r?\n/).map((l: string) => l.trim()).filter(Boolean);
  if (lines.length < 2) {
    return [];
  }

  const header = parseCsvLine(lines[0]).map((h: string) => h.toLowerCase());
  const timeIdx = header.indexOf("time") >= 0 ? header.indexOf("time") : 0;
  let messageIdx = header.indexOf("message");
  if (messageIdx < 0) messageIdx = header.indexOf("desc");
  if (messageIdx < 0) messageIdx = header.indexOf("name");
  if (messageIdx < 0) messageIdx = header.length > 1 ? 1 : 0;

  const out: any[] = [];
  for (let i = 1; i < lines.length; i += 1) {
    const row = parseCsvLine(lines[i]);
    if (!row.length) continue;
    out.push({
      time: row[timeIdx] ?? "",
      message: row[messageIdx] ?? "",
    });
  }
  return out;
}

function getLogRows(ent: any): any[] {
  const valuePayload = getEntityValuePayload(ent);
  const rawCsvFromValue =
    typeof valuePayload?.raw_csv_table === "string" ? valuePayload.raw_csv_table.trim() : "";
  const rawCsv = rawCsvFromValue || (typeof ent?.raw_csv_table === "string" ? ent.raw_csv_table.trim() : "");
  const csvRows = parseLogRowsFromCsv(rawCsv);

  const logsPayload = Array.isArray(valuePayload?.logs)
    ? valuePayload.logs
    : (Array.isArray(ent?.logs) ? ent.logs : []);

  if (logsPayload.length > 0) {
    return logsPayload.map((lg: any, idx: number) => {
      const message = getLogMessageFromObject(lg) || asNonEmptyString(csvRows[idx]?.message) || "[missing]";
      return {
        time: asNonEmptyString(lg?.time) || asNonEmptyString(csvRows[idx]?.time),
        message,
      };
    });
  }

  return csvRows;
}

function getEntityIndicators(ent: any): any[] {
  const valuePayload = getEntityValuePayload(ent);
  const indicators = valuePayload?.indicators ?? ent?.indicators;
  if (Array.isArray(indicators)) {
    return indicators;
  }
  if (indicators && typeof indicators === "object") {
    return Object.values(indicators);
  }
  return [];
}

export default function SnapshotsPage() {
  const [backendUrl, setBackendUrl] = useState(DEFAULT_BACKEND);
  const api = useBackendApi(backendUrl);
  const [sources, setSources] = useState<KvmSource[]>([]);
  const [sourceId, setSourceId] = useState("");
  
  const [snapshots, setSnapshots] = useState<any[]>([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const limit = 3;

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  async function loadSources() {
    try {
      const data = await api.listSources<KvmSource[]>();
      setSources(data);
    } catch (err: any) {
      console.error("Failed to load sources:", err);
    }
  }

  async function loadSnapshots(currentPage: number, currentSourceId: string) {
    setLoading(true);
    setError("");
    try {
      const skip = (currentPage - 1) * limit;
      const data = await api.listSnapshots<any>({
        limit,
        skip,
        sourceId: currentSourceId || undefined,
      });
      setSnapshots(data.items || []);
      setTotal(data.total || 0);
    } catch (err: any) {
      setError(err.message || "Failed to load snapshots");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    loadSources();
    loadSnapshots(page, sourceId);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [api, page, sourceId]);

  async function handleEvaluate(id: string, evaluation: string) {
    try {
      await api.updateSnapshotEvaluation(id, evaluation);
      // Update local state
      setSnapshots(prev => prev.map(s => s.id === id ? { ...s, evaluation } : s));
    } catch (err: any) {
      alert("Failed to save evaluation: " + err.message);
    }
  }

  function snapshotImgUrl(imageUrl: string | undefined): string | null {
    return api.resolveImageUrl(imageUrl);
  }

  const totalPages = Math.ceil(total / limit) || 1;

  return (
    <main className="page" style={{ padding: "20px" }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "20px" }}>
        <h1>Snapshot Evaluation</h1>
        <Link href="/" className="btn-sm btn-secondary">
          &larr; Back to Dashboard
        </Link>
      </div>

      <section className="card controls">
        <div className="control-grid" style={{ marginBottom: "1rem" }}>
          <label>
            Backend URL
            <input
              value={backendUrl}
              onChange={(e) => setBackendUrl(e.target.value)}
              style={{ padding: "8px", marginLeft: "10px" }}
            />
          </label>

          <label>
            Filter by Source:
            <select
              value={sourceId}
              onChange={(e) => {
                setSourceId(e.target.value);
                setPage(1);
              }}
              style={{ padding: "8px", marginLeft: "10px" }}
            >
              <option value="">-- All Sources --</option>
              {sources.map(s => (
                <option key={s.id} value={s.id}>{s.name}</option>
              ))}
            </select>
          </label>
        </div>
      </section>

      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "20px" }}>
        <div style={{ display: "flex", gap: "5px", flexWrap: "wrap", alignItems: "center" }}>
          <button className="btn-sm btn-secondary" disabled={page <= 1 || loading} onClick={() => setPage(p => p - 1)}>&laquo;</button>
          {Array.from({ length: Math.min(5, totalPages) }, (_, i) => {
             let start = Math.max(1, page - 2);
             if (start + 4 > totalPages) start = Math.max(1, totalPages - 4);
             const p = start + i;
             if (p < 1 || p > totalPages) return null;
             return (
              <button
                key={p}
                className={`btn-sm ${page === p ? 'btn-primary' : 'btn-secondary'}`}
                onClick={() => setPage(p)}
                disabled={loading}
              >
                {p}
              </button>
             );
          })}
          <button className="btn-sm btn-secondary" disabled={page >= totalPages || loading} onClick={() => setPage(p => p + 1)}>&raquo;</button>
        </div>
        <span>Total: {total} items</span>
      </div>

      {error ? <p className="error">{error}</p> : null}

      <div style={{ display: "flex", flexDirection: "column", gap: "20px" }}>
        {loading ? (
          <p>Loading snapshots...</p>
        ) : snapshots.length === 0 ? (
          <p className="muted card">No snapshots found.</p>
        ) : (
          snapshots.map(snap => (
            <div key={snap.id} className="card" style={{ display: "flex", gap: "20px", alignItems: "flex-start" }}>
              <div style={{ flex: "0 0 900px" }}>
                <img 
                  src={snapshotImgUrl(snap.image_url) ?? undefined} 
                  alt="Snapshot" 
                  style={{ width: "100%", borderRadius: "4px", border: "1px solid #ccc" }} 
                />
                <p className="muted" style={{ marginTop: "10px", fontSize: "0.85rem" }}>
                  <strong>ID:</strong> {snap.id} <br />
                  <strong>Time:</strong> {formatDate(snap.created_at)} <br />
                  <strong>Monitor Key:</strong> {snap.monitor_key} <br />
                  <strong>Processing Time:</strong> {snap.processing_time_ms ? `${(snap.processing_time_ms / 1000).toFixed(2)}s` : 'N/A'}
                </p>
                <div style={{ marginTop: "10px", padding: "10px", background: "#f9fafb", borderRadius: "4px", border: "1px solid #e5e7eb" }}>
                  <strong>Evaluation:</strong>
                  <div style={{ display: "flex", gap: "8px", marginTop: "8px" }}>
                    <button 
                      className={`btn-sm ${snap.evaluation === 'accurate' ? 'btn-primary' : 'btn-secondary'}`}
                      onClick={() => handleEvaluate(snap.id, 'accurate')}
                      style={{ background: snap.evaluation === 'accurate' ? '#10b981' : undefined }}
                    >
                      Accurate
                    </button>
                    <button 
                      className={`btn-sm ${snap.evaluation === 'inaccurate' ? 'btn-primary' : 'btn-secondary'}`}
                      onClick={() => handleEvaluate(snap.id, 'inaccurate')}
                      style={{ background: snap.evaluation === 'inaccurate' ? '#ef4444' : undefined }}
                    >
                      Inaccurate
                    </button>
                    <button 
                      className={`btn-sm ${snap.evaluation === 'unreadable' ? 'btn-primary' : 'btn-secondary'}`}
                      onClick={() => handleEvaluate(snap.id, 'unreadable')}
                      style={{ background: snap.evaluation === 'unreadable' ? '#f59e0b' : undefined }}
                    >
                      Unreadable
                    </button>
                  </div>
                  {snap.evaluation && (
                    <div style={{ marginTop: '8px', fontSize: '0.85rem', color: '#6b7280' }}>
                      Current status: <strong>{snap.evaluation}</strong>
                      <br/>
                      <button 
                        className="btn-link" 
                        style={{ fontSize: "0.8rem", padding: 0, marginTop: "4px" }}
                        onClick={() => handleEvaluate(snap.id, '')}
                      >
                        Clear Evaluation
                      </button>
                    </div>
                  )}
                </div>
              </div>
              
              <div style={{ flex: 1, overflowX: "auto" }}>
                <h3 style={{ marginTop: 0, fontSize: "1rem" }}>Extracted Entities</h3>
                {snap.llm_parse_error ? (
                  <p style={{ color: "red", fontSize: "0.85rem", fontWeight: "bold" }}>LLM Parse Error Occurred!</p>
                ) : null}
                
                {(!snap.entities_values || snap.entities_values.length === 0) ? (
                  <p className="muted" style={{ fontSize: "0.85rem" }}>No entities extracted.</p>
                ) : (
                  <div style={{ display: "flex", flexDirection: "column", gap: "10px" }}>
                    {snap.entities_values.map((ent: any, i: number) => (
                      <div key={i} style={{ border: "1px solid #e5e7eb", borderRadius: "4px", padding: "10px" }}>
                        <h4 style={{ margin: "0 0 8px 0", fontSize: "0.95rem" }}>{ent.main_entity_name} ({ent.type})</h4>
                        
                        {/* Table-like entities (fixed table + scada 1xN) */}
                        {ent.type?.toLowerCase() !== "log/alert" && ent.type?.toLowerCase() !== "log" && (() => {
                          const tableSubentities = getTableSubentities(ent);
                          if (!tableSubentities.length) return null;

                          const cols = Array.from(new Set(tableSubentities.map((sub: any) => sub.col)));
                          const rows = Array.from(new Set(tableSubentities.map((sub: any) => sub.row)));
                          const cellMap = new Map();
                          tableSubentities.forEach((sub: any) => {
                            cellMap.set(`${sub.row}-${sub.col}`, sub);
                          });

                          return (
                            <div style={{ overflowX: "auto" }}>
                              <table style={{ width: "100%", fontSize: "0.8rem", borderCollapse: "collapse" }}>
                                <thead>
                                  <tr>
                                    <th style={{ borderBottom: "1px solid #ccc", textAlign: "left", padding: "4px" }}>Row</th>
                                    {cols.map((col: any, j: number) => (
                                      <th key={j} style={{ borderBottom: "1px solid #ccc", textAlign: "left", padding: "4px" }}>{col}</th>
                                    ))}
                                  </tr>
                                </thead>
                                <tbody>
                                  {rows.map((row: any, i: number) => (
                                    <tr key={i}>
                                      <td style={{ borderBottom: "1px solid #eee", padding: "4px", fontWeight: "bold", whiteSpace: "nowrap" }}>{row}</td>
                                      {cols.map((col: any, j: number) => {
                                        const cell = cellMap.get(`${row}-${col}`);
                                        return (
                                          <td key={j} style={{ borderBottom: "1px solid #eee", padding: "4px" }}>
                                            {cell ? `${cell.value_raw ?? cell.value ?? ""} ${cell.unit || ""}`.trim() : ""}
                                          </td>
                                        );
                                      })}
                                    </tr>
                                  ))}
                                </tbody>
                              </table>
                            </div>
                          );
                        })()}

                        {/* Log/Alert type */}
                        {(ent.type?.toLowerCase() === "log/alert" || ent.type?.toLowerCase() === "log") && (() => {
                          const logRows = getLogRows(ent);
                          if (!logRows.length) return null;
                          return (
                            <div style={{ maxHeight: "250px", overflowY: "auto", border: "1px solid #e5e7eb", borderRadius: "4px" }}>
                              <table style={{ width: "100%", fontSize: "0.8rem", borderCollapse: "collapse" }}>
                                <thead style={{ position: "sticky", top: 0, background: "#f9fafb" }}>
                                  <tr>
                                    <th style={{ borderBottom: "1px solid #ccc", textAlign: "left", padding: "6px" }}>Time</th>
                                    <th style={{ borderBottom: "1px solid #ccc", textAlign: "left", padding: "6px" }}>Message</th>
                                  </tr>
                                </thead>
                                <tbody>
                                  {logRows.map((lg: any, j: number) => (
                                    <tr key={j}>
                                      <td style={{ borderBottom: "1px solid #eee", padding: "6px", whiteSpace: "nowrap", verticalAlign: "top" }}>{lg.time}</td>
                                      <td style={{ borderBottom: "1px solid #eee", padding: "6px" }}>{lg.message}</td>
                                    </tr>
                                  ))}
                                </tbody>
                              </table>
                            </div>
                          );
                        })()}

                        {/* Indicator fallback (legacy payloads without table cells) */}
                        {ent.type?.toLowerCase() !== "log/alert" && ent.type?.toLowerCase() !== "log" && (() => {
                          const tableSubentities = getTableSubentities(ent);
                          if (tableSubentities.length > 0) return null;

                          const indicators = getEntityIndicators(ent);
                          if (!indicators.length) return null;

                          return (
                            <table style={{ width: "100%", fontSize: "0.8rem", borderCollapse: "collapse" }}>
                              <thead>
                                <tr>
                                  <th style={{ borderBottom: "1px solid #ccc", textAlign: "left", padding: "4px" }}>Indicator</th>
                                  <th style={{ borderBottom: "1px solid #ccc", textAlign: "left", padding: "4px" }}>Value</th>
                                </tr>
                              </thead>
                              <tbody>
                                {indicators.map((ind: any, j: number) => (
                                  <tr key={j}>
                                    <td style={{ borderBottom: "1px solid #eee", padding: "4px" }}>{ind.indicator_label || ind.label}</td>
                                    <td style={{ borderBottom: "1px solid #eee", padding: "4px" }}>
                                      {getIndicatorDisplayValue(ind)} {ind.unit || ""}
                                    </td>
                                  </tr>
                                ))}
                              </tbody>
                            </table>
                          );
                        })()}

                      </div>
                    ))}
                  </div>
                )}
              </div>
            </div>
          ))
        )}
      </div>

      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginTop: "20px" }}>
        <div style={{ display: "flex", gap: "5px", flexWrap: "wrap", alignItems: "center" }}>
          <button className="btn-sm btn-secondary" disabled={page <= 1 || loading} onClick={() => setPage(p => p - 1)}>&laquo;</button>
          {Array.from({ length: Math.min(5, totalPages) }, (_, i) => {
             let start = Math.max(1, page - 2);
             if (start + 4 > totalPages) start = Math.max(1, totalPages - 4);
             const p = start + i;
             if (p < 1 || p > totalPages) return null;
             return (
              <button
                key={p}
                className={`btn-sm ${page === p ? 'btn-primary' : 'btn-secondary'}`}
                onClick={() => setPage(p)}
                disabled={loading}
              >
                {p}
              </button>
             );
          })}
          <button className="btn-sm btn-secondary" disabled={page >= totalPages || loading} onClick={() => setPage(p => p + 1)}>&raquo;</button>
        </div>
        <span>Total: {total} items</span>
      </div>
    </main>
  );
}
