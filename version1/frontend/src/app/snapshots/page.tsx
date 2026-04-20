"use client";

import { useEffect, useState, useMemo } from "react";
import Link from "next/link";
import { DEFAULT_BACKEND, fetchJSON, normalizeBaseUrl, putJSON } from "@/lib/api";
import { KvmSource } from "@/types/dashboard";

function formatDate(value: string | undefined): string {
  if (!value) return "";
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return "";
  return d.toLocaleString();
}

export default function SnapshotsPage() {
  const [backendUrl, setBackendUrl] = useState(DEFAULT_BACKEND);
  const [sources, setSources] = useState<KvmSource[]>([]);
  const [sourceId, setSourceId] = useState("");
  
  const [snapshots, setSnapshots] = useState<any[]>([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const limit = 20;

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  
  const base = useMemo(() => normalizeBaseUrl(backendUrl), [backendUrl]);

  async function loadSources() {
    try {
      const data = await fetchJSON<KvmSource[]>(base, "/api/kvm-sources");
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
      let url = `/api/snapshots?limit=${limit}&skip=${skip}`;
      if (currentSourceId) {
        url += `&source_id=${currentSourceId}`;
      }
      const data = await fetchJSON<any>(base, url);
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
  }, [base, page, sourceId]);

  async function handleEvaluate(id: string, evaluation: string) {
    try {
      await putJSON(base, `/api/snapshots/${id}/evaluation`, { evaluation });
      // Update local state
      setSnapshots(prev => prev.map(s => s.id === id ? { ...s, evaluation } : s));
    } catch (err: any) {
      alert("Failed to save evaluation: " + err.message);
    }
  }

  function snapshotImgUrl(imageUrl: string | undefined): string | null {
    if (!imageUrl) return null;
    if (imageUrl.startsWith("data:")) return imageUrl;
    return `${base}${imageUrl}`;
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

      {error ? <p className="error">{error}</p> : null}

      <div style={{ display: "flex", flexDirection: "column", gap: "20px" }}>
        {loading ? (
          <p>Loading snapshots...</p>
        ) : snapshots.length === 0 ? (
          <p className="muted card">No snapshots found.</p>
        ) : (
          snapshots.map(snap => (
            <div key={snap.id} className="card" style={{ display: "flex", gap: "20px", alignItems: "flex-start" }}>
              <div style={{ flex: "0 0 300px" }}>
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
                        
                        {/* Table type */}
                        {ent.type?.toLowerCase() === "table" && ent.subentities && ent.subentities.length > 0 && (
                          <table style={{ width: "100%", fontSize: "0.8rem", borderCollapse: "collapse" }}>
                            <thead>
                              <tr>
                                <th style={{ borderBottom: "1px solid #ccc", textAlign: "left", padding: "4px" }}>Row</th>
                                <th style={{ borderBottom: "1px solid #ccc", textAlign: "left", padding: "4px" }}>Col</th>
                                <th style={{ borderBottom: "1px solid #ccc", textAlign: "left", padding: "4px" }}>Value</th>
                              </tr>
                            </thead>
                            <tbody>
                              {ent.subentities.map((sub: any, j: number) => (
                                <tr key={j}>
                                  <td style={{ borderBottom: "1px solid #eee", padding: "4px" }}>{sub.row}</td>
                                  <td style={{ borderBottom: "1px solid #eee", padding: "4px" }}>{sub.col}</td>
                                  <td style={{ borderBottom: "1px solid #eee", padding: "4px" }}>
                                    {sub.value_raw ?? sub.value} {sub.unit || ""}
                                  </td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        )}

                        {/* Indicators type (Log/Alert usually) */}
                        {ent.type?.toLowerCase() !== "table" && ent.indicators && ent.indicators.length > 0 && (
                          <table style={{ width: "100%", fontSize: "0.8rem", borderCollapse: "collapse" }}>
                            <thead>
                              <tr>
                                <th style={{ borderBottom: "1px solid #ccc", textAlign: "left", padding: "4px" }}>Indicator</th>
                                <th style={{ borderBottom: "1px solid #ccc", textAlign: "left", padding: "4px" }}>Value</th>
                              </tr>
                            </thead>
                            <tbody>
                              {ent.indicators.map((ind: any, j: number) => (
                                <tr key={j}>
                                  <td style={{ borderBottom: "1px solid #eee", padding: "4px" }}>{ind.indicator_label || ind.label}</td>
                                  <td style={{ borderBottom: "1px solid #eee", padding: "4px" }}>
                                    {ind.value_raw ?? ind.value} {ind.unit || ""}
                                  </td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        )}

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
        <button 
          className="btn-secondary" 
          disabled={page <= 1 || loading}
          onClick={() => setPage(p => p - 1)}
        >
          Previous
        </button>
        <span>Page {page} of {totalPages} (Total: {total})</span>
        <button 
          className="btn-secondary" 
          disabled={page >= totalPages || loading}
          onClick={() => setPage(p => p + 1)}
        >
          Next
        </button>
      </div>
    </main>
  );
}
