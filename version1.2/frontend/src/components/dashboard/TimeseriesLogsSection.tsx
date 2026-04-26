import ColorSequenceChart from "@/components/ColorSequenceChart";
import TimeseriesChart from "@/components/TimeseriesChart";
import { LogEntry } from "@/types/dashboard";

import { typeColor } from "./dashboardUtils";

interface TimeseriesLogsSectionProps {
  series: Record<string, any>;
  logs: LogEntry[];
}

export default function TimeseriesLogsSection({
  series,
  logs,
}: TimeseriesLogsSectionProps) {
  return (
    <section className="card" style={{ marginTop: 24 }}>
      <h2>Timeseries & Status</h2>
      {(() => {
        const allKeys = Object.keys(series || {});

        const entityGroups: Record<
          string,
          {
            numeric: Record<string, Record<string, any>>;
            color: { t: string; value: string; metric: string }[];
          }
        > = {};
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

        for (const eName of Object.keys(entityGroups)) {
          entityGroups[eName].color.sort(
            (a, b) => new Date(a.t).getTime() - new Date(b.t).getTime(),
          );
        }

        const entityEntries = Object.entries(entityGroups);
        const hasData = entityEntries.some(
          ([, group]) =>
            Object.keys(group.numeric).length > 0 || group.color.length > 0,
        );

        if (!hasData)
          return <p className="muted">No data found for selected range.</p>;

        return (
          <div
            className="timeseries-entity-groups"
            style={{ display: "flex", flexDirection: "column", gap: "24px" }}
          >
            {entityEntries.map(([entityName, groupData]) => {
              const numEntries = Object.entries(groupData.numeric);
              const hasGroupData =
                numEntries.length > 0 || groupData.color.length > 0;
              if (!hasGroupData) return null;

              return (
                <div
                  key={entityName}
                  className="entity-chart-card"
                  style={{
                    border: "1px solid var(--border, #e5e7eb)",
                    borderRadius: "8px",
                    padding: "16px",
                    background: "var(--bg, #fff)",
                    boxShadow: "0 1px 3px rgba(0,0,0,0.05)",
                  }}
                >
                  <h3
                    style={{
                      marginTop: 0,
                      marginBottom: "16px",
                      fontSize: "1.1rem",
                      borderBottom: "1px solid var(--border, #e5e7eb)",
                      paddingBottom: "8px",
                    }}
                  >
                    Entity: <strong style={{ color: "var(--primary, #2563eb)" }}>{entityName}</strong>
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
                          Status / Color Sequence
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
  );
}
