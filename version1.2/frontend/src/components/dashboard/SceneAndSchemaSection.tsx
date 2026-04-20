import Link from "next/link";

import { Entity, Preview } from "@/types/dashboard";

import {
  asNonEmptyString,
  formatDate,
  getEntityIndicators,
  getEntityLogRows,
  getEntityTableGrid,
  metricDisplayValue,
  normalizeSchemaEntityType,
  schemaTypeColor,
  typeColor,
} from "./dashboardUtils";

interface SceneAndSchemaSectionProps {
  preview: Preview | null;
  entities: Entity[];
  selectedEntityIds: string[];
  loading: boolean;
  snapshotImgUrl: (imageUrl: string | undefined) => string | null;
  onToggleEntitySelection: (entityId: string) => void;
  onSelectAllEntities: () => void;
  onClearEntitySelection: () => void;
  onMonitorSelected: () => Promise<void>;
}

export default function SceneAndSchemaSection({
  preview,
  entities,
  selectedEntityIds,
  loading,
  snapshotImgUrl,
  onToggleEntitySelection,
  onSelectAllEntities,
  onClearEntitySelection,
  onMonitorSelected,
}: SceneAndSchemaSectionProps) {
  return (
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
            <p className="muted" style={{ marginTop: 8, fontSize: 12 }}>
              Captured: {formatDate(preview.created_at)}
            </p>
          </div>
        ) : (
          <p className="muted">No snapshots available for this screen.</p>
        )}
      </section>

      <section className="card">
        <div className="section-header">
          <div>
            <h2 style={{ marginBottom: 4 }}>Schema Segments ({entities.length})</h2>
            <p className="muted" style={{ margin: 0, fontSize: 12 }}>
              Read-only view for new schema structure (scada object, fixed table,
              log tables). Edit schema in Settings page.
            </p>
          </div>
          <div style={{ display: "flex", gap: 6 }}>
            <button
              className="btn-sm btn-secondary"
              onClick={onSelectAllEntities}
              disabled={!entities.length}
            >
              Select All
            </button>
            <button
              className="btn-sm btn-secondary"
              onClick={onClearEntitySelection}
              disabled={!selectedEntityIds.length}
            >
              Clear
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
              const schemaType = normalizeSchemaEntityType(ent.entity_type || ent.type);
              return (
                <div
                  key={ent.id}
                  className={`entity-row ${selected ? "entity-selected" : ""}`}
                  onClick={() => onToggleEntitySelection(ent.id)}
                >
                  <div className="entity-check">
                    <input type="checkbox" checked={selected} readOnly tabIndex={-1} />
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
                      {schemaType ? (
                        <span
                          className="type-pill"
                          style={{ background: schemaTypeColor(schemaType) }}
                        >
                          {schemaType}
                        </span>
                      ) : null}
                      {ent.region ? (
                        <span className="type-pill" style={{ background: "#10b981" }}>
                          {ent.region}
                        </span>
                      ) : null}
                    </div>
                    <div className="entity-metrics">
                      {(() => {
                        const entityType = normalizeSchemaEntityType(ent.entity_type || ent.type);

                        if (entityType === "fixed table") {
                          const { cols, rows, valueMap } = getEntityTableGrid(ent);

                          if (!cols.length || !rows.length) {
                            return (
                              <p
                                className="muted"
                                style={{ fontSize: "0.85rem", marginTop: "0.75rem" }}
                              >
                                No table values found.
                              </p>
                            );
                          }

                          return (
                            <div style={{ width: "100%", overflowX: "auto", marginTop: "1rem" }}>
                              <table
                                style={{
                                  width: "100%",
                                  borderCollapse: "collapse",
                                  fontSize: "0.85rem",
                                  textAlign: "center",
                                }}
                              >
                                <thead>
                                  <tr>
                                    <th
                                      style={{
                                        borderBottom: "1px solid #ccc",
                                        padding: "4px",
                                        borderRight: "1px solid #ccc",
                                        textAlign: "left",
                                      }}
                                    >
                                      Row
                                    </th>
                                    {cols.map((c) => (
                                      <th
                                        key={c}
                                        style={{ borderBottom: "1px solid #ccc", padding: "4px" }}
                                      >
                                        {c}
                                      </th>
                                    ))}
                                  </tr>
                                </thead>
                                <tbody>
                                  {rows.map((r) => (
                                    <tr key={r}>
                                      <td
                                        style={{
                                          borderBottom: "1px solid #eee",
                                          padding: "4px",
                                          borderRight: "1px solid #eee",
                                          fontWeight: 600,
                                          textAlign: "left",
                                          whiteSpace: "nowrap",
                                        }}
                                      >
                                        {r}
                                      </td>
                                      {cols.map((c) => {
                                        const cell = valueMap[r]?.[c];
                                        if (!cell) {
                                          return (
                                            <td
                                              key={`${r}-${c}`}
                                              style={{
                                                borderBottom: "1px solid #eee",
                                                padding: "4px",
                                              }}
                                            >
                                              -
                                            </td>
                                          );
                                        }

                                        const valueText = metricDisplayValue(cell);
                                        const shownValue = valueText === "[missing]" ? "-" : valueText;
                                        const unit = asNonEmptyString(cell?.unit);

                                        return (
                                          <td
                                            key={`${r}-${c}`}
                                            style={{
                                              borderBottom: "1px solid #eee",
                                              padding: "4px",
                                              color: typeColor(cell?.value_type),
                                            }}
                                          >
                                            {shownValue}
                                            {unit ? ` ${unit}` : ""}
                                          </td>
                                        );
                                      })}
                                    </tr>
                                  ))}
                                </tbody>
                              </table>
                            </div>
                          );
                        }

                        if (entityType === "log tables") {
                          const logRows = getEntityLogRows(ent);
                          if (!logRows.length) {
                            return (
                              <p
                                className="muted"
                                style={{ fontSize: "0.85rem", marginTop: "0.75rem" }}
                              >
                                No alert logs found.
                              </p>
                            );
                          }

                          return (
                            <div style={{ width: "100%", overflowX: "auto", marginTop: "1rem" }}>
                              <table
                                style={{ width: "100%", borderCollapse: "collapse", fontSize: "0.85rem" }}
                              >
                                <thead>
                                  <tr>
                                    <th
                                      style={{
                                        borderBottom: "1px solid #ccc",
                                        textAlign: "left",
                                        padding: "4px",
                                        width: "30%",
                                      }}
                                    >
                                      Time
                                    </th>
                                    <th
                                      style={{
                                        borderBottom: "1px solid #ccc",
                                        textAlign: "left",
                                        padding: "4px",
                                      }}
                                    >
                                      Message
                                    </th>
                                  </tr>
                                </thead>
                                <tbody>
                                  {logRows.map((row, idx) => {
                                    const rawTime = asNonEmptyString(row?.time);
                                    const displayTime = formatDate(rawTime) || rawTime || "-";
                                    const messageText = asNonEmptyString(row?.message) || "[missing]";

                                    return (
                                      <tr key={`${ent.id}-log-${idx}`}>
                                        <td
                                          style={{
                                            borderBottom: "1px solid #eee",
                                            padding: "4px",
                                            color: "#b42318",
                                            whiteSpace: "nowrap",
                                          }}
                                        >
                                          {displayTime}
                                        </td>
                                        <td
                                          style={{
                                            borderBottom: "1px solid #eee",
                                            padding: "4px",
                                            color: "#912018",
                                          }}
                                        >
                                          {messageText}
                                        </td>
                                      </tr>
                                    );
                                  })}
                                </tbody>
                              </table>
                            </div>
                          );
                        }

                        const indicatorEntries = getEntityIndicators(ent);
                        if (!indicatorEntries.length) {
                          return (
                            <p
                              className="muted"
                              style={{ fontSize: "0.85rem", marginTop: "0.75rem" }}
                            >
                              No indicator values found.
                            </p>
                          );
                        }

                        return (
                          <div
                            style={{
                              width: "100%",
                              marginTop: "0.75rem",
                              display: "flex",
                              flexWrap: "wrap",
                              gap: "6px",
                            }}
                          >
                            {indicatorEntries.map(({ key, indicator }) => {
                              const indicatorName =
                                asNonEmptyString(
                                  indicator?.indicator_label ||
                                    indicator?.display_name ||
                                    indicator?.label ||
                                    indicator?.metric_key ||
                                    indicator?.metric ||
                                    key,
                                ) || key;
                              const valueText = metricDisplayValue(indicator);
                              const shownValue = valueText === "[missing]" ? "-" : valueText;
                              const unit = asNonEmptyString(indicator?.unit);
                              const valueType = asNonEmptyString(indicator?.value_type) || "text";

                              return (
                                <span
                                  key={`${ent.id}-${key}`}
                                  className="metric-badge"
                                  style={{ borderColor: typeColor(valueType) }}
                                >
                                  <span className="metric-name">{indicatorName}</span>
                                  <span
                                    className="metric-val"
                                    style={{ color: typeColor(valueType) }}
                                  >
                                    {shownValue}
                                    {unit ? ` ${unit}` : ""}
                                  </span>
                                  <span
                                    className="metric-type"
                                    style={{ background: typeColor(valueType) }}
                                  >
                                    {valueType}
                                  </span>
                                </span>
                              );
                            })}
                          </div>
                        );
                      })()}
                    </div>
                  </div>
                </div>
              );
            })}
          </div>
        ) : (
          <p className="muted">No entities detected yet for this screen.</p>
        )}
      </section>
    </div>
  );
}
