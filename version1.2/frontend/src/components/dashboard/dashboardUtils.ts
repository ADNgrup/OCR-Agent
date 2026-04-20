/* eslint-disable @typescript-eslint/no-explicit-any */

import { Screen as DashboardScreen } from "@/types/dashboard";

export function formatDate(value: string | undefined): string {
  if (!value) return "";
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return "";
  return d.toLocaleString();
}

export function typeColor(vtype: string | undefined): string {
  if (vtype === "number") return "#2563eb";
  if (vtype === "color") return "#7c3aed";
  if (vtype === "bool") return "#059669";
  if (vtype === "text") return "#c2410c";
  return "#667085";
}

export function metricDisplayValue(v: any): string {
  const candidates = [
    v?.value_raw,
    v?.last_value,
    v?.value,
    v?.value_number,
    v?.last_number,
    v?.numeric_value,
  ];
  for (const candidate of candidates) {
    if (candidate === null || candidate === undefined) continue;
    const text = String(candidate).trim();
    if (text !== "") return text;
  }
  return "[missing]";
}

function logDisplayMessage(log: any): string {
  const direct = log?.message;
  if (direct !== undefined && direct !== null && String(direct).trim() !== "") {
    return String(direct).trim();
  }

  const msg = log?.msg;
  if (msg !== undefined && msg !== null && String(msg).trim() !== "") {
    return String(msg).trim();
  }

  const name = String(log?.name ?? "").trim();
  const desc = String(log?.desc ?? "").trim();
  if (name && desc) return `${name}: ${desc}`;
  if (name) return name;
  if (desc) return desc;

  if (log && typeof log === "object") {
    const entries = Object.entries(log);
    const preferred = entries.find(
      ([k, v]) =>
        /message|msg|desc|name/i.test(k) &&
        v !== undefined &&
        v !== null &&
        String(v).trim() !== "",
    );
    if (preferred) return String(preferred[1]).trim();

    const fallback = entries.find(
      ([k, v]) =>
        k.toLowerCase() !== "time" &&
        v !== undefined &&
        v !== null &&
        String(v).trim() !== "",
    );
    if (fallback) return String(fallback[1]).trim();
  }

  return "[missing]";
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

export function asNonEmptyString(value: any): string {
  if (value === undefined || value === null) return "";
  return String(value).trim();
}

function parseEntityLogRowsFromCsv(rawCsv: string): Array<{ time: string; message: string }> {
  if (!rawCsv) return [];

  const lines = rawCsv
    .split(/\r?\n/)
    .map((l: string) => l.trim())
    .filter(Boolean);
  if (lines.length < 2) return [];

  const header = parseCsvLine(lines[0]).map((h) => h.toLowerCase());
  const timeIdx = header.indexOf("time") >= 0 ? header.indexOf("time") : 0;
  let messageIdx = header.indexOf("message");
  if (messageIdx < 0) messageIdx = header.indexOf("desc");
  if (messageIdx < 0) messageIdx = header.indexOf("name");
  const messageFallbackIndexes = header
    .map((_, idx) => idx)
    .filter((idx) => idx !== timeIdx);

  const out: Array<{ time: string; message: string }> = [];
  for (let i = 1; i < lines.length; i += 1) {
    const row = parseCsvLine(lines[i]);
    if (!row.length) continue;

    const directMessage = messageIdx >= 0 ? asNonEmptyString(row[messageIdx]) : "";
    const mergedMessage = messageFallbackIndexes
      .map((idx) => asNonEmptyString(row[idx]))
      .filter(Boolean)
      .join(" _ ");

    out.push({
      time: asNonEmptyString(row[timeIdx]),
      message: directMessage || mergedMessage,
    });
  }
  return out;
}

export function getEntityLogRows(ent: any): Array<{ time: string; message: string }> {
  const rawCsv = typeof ent?.raw_csv_table === "string" ? ent.raw_csv_table.trim() : "";
  const csvRows = parseEntityLogRowsFromCsv(rawCsv);

  if (Array.isArray(ent?.logs) && ent.logs.length > 0) {
    return ent.logs.map((lg: any, idx: number) => {
      const message = logDisplayMessage(lg);
      return {
        time: asNonEmptyString(lg?.time) || asNonEmptyString(csvRows[idx]?.time),
        message:
          message !== "[missing]"
            ? message
            : asNonEmptyString(csvRows[idx]?.message) || "[missing]",
      };
    });
  }

  return csvRows;
}

export function getEntityTableGrid(ent: any): {
  cols: string[];
  rows: string[];
  valueMap: Record<string, Record<string, any>>;
} {
  const valueMap: Record<string, Record<string, any>> = {};
  const colSet = new Set<string>();
  const rowSet = new Set<string>();

  const addCell = (row: string, col: string, cell: any) => {
    const rowName = asNonEmptyString(row) || "Unknown";
    const colName = asNonEmptyString(col);
    if (!colName) return;
    rowSet.add(rowName);
    colSet.add(colName);
    if (!valueMap[rowName]) valueMap[rowName] = {};
    valueMap[rowName][colName] = cell;
  };

  if (Array.isArray(ent?.subentities) && ent.subentities.length > 0) {
    ent.subentities.forEach((sub: any) => {
      addCell(sub?.row, sub?.col, sub);
    });
  }

  if (rowSet.size === 0) {
    const rawCsv = typeof ent?.raw_csv_table === "string" ? ent.raw_csv_table.trim() : "";
    if (rawCsv) {
      const lines = rawCsv
        .split(/\r?\n/)
        .map((l: string) => l.trim())
        .filter(Boolean);
      if (lines.length >= 2) {
        const header = parseCsvLine(lines[0]);
        const metadata = ent?.metadata && typeof ent.metadata === "object" ? ent.metadata : {};
        const valueColumns: string[] = Array.isArray(metadata.value_columns)
          ? metadata.value_columns.map((x: any) => String(x).trim()).filter(Boolean)
          : header.slice(1);
        const unitParts =
          typeof metadata.unit === "string"
            ? metadata.unit.split("|").map((x: string) => x.trim())
            : [];
        const typeParts =
          typeof metadata.value_type === "string"
            ? metadata.value_type.split("|").map((x: string) => x.trim())
            : [];

        const colIndex = new Map<string, number>();
        header.forEach((h: string, idx: number) => colIndex.set(h, idx));

        for (let i = 1; i < lines.length; i += 1) {
          const row = parseCsvLine(lines[i]);
          const rowName = row[0] || "Unknown";
          valueColumns.forEach((col: string, idx: number) => {
            const pos = colIndex.get(col);
            if (pos === undefined || pos >= row.length) return;
            const valueRaw = row[pos] ?? "";
            if (!String(valueRaw).trim()) return;
            addCell(rowName, col, {
              row: rowName,
              col,
              value_raw: valueRaw,
              unit: unitParts[idx] || "",
              value_type: typeParts[idx] || "text",
            });
          });
        }
      }
    }
  }

  return {
    cols: Array.from(colSet),
    rows: Array.from(rowSet),
    valueMap,
  };
}

export function getEntityIndicators(ent: any): Array<{ key: string; indicator: any }> {
  const indicatorsObj =
    ent?.indicators && typeof ent.indicators === "object" && !Array.isArray(ent.indicators)
      ? ent.indicators
      : {};
  const metricsObj =
    ent?.metrics && typeof ent.metrics === "object" && !Array.isArray(ent.metrics)
      ? ent.metrics
      : {};

  const sourceObj = Object.keys(indicatorsObj).length ? indicatorsObj : metricsObj;
  const objectEntries = Object.entries(sourceObj).map(([key, indicator]) => ({
    key,
    indicator,
  }));
  if (objectEntries.length) return objectEntries;

  if (Array.isArray(ent?.indicators)) {
    return ent.indicators.map((indicator: any, idx: number) => {
      const key =
        asNonEmptyString(
          indicator?.metric_key ||
            indicator?.metric ||
            indicator?.label ||
            indicator?.indicator_label ||
            indicator?.display_name,
        ) || `metric_${idx + 1}`;
      return { key, indicator };
    });
  }

  return [];
}

export function getScreenSchemaStatus(screen: DashboardScreen | undefined): string {
  if (!screen) return "unclassified";
  const status = String(screen.schema_status || "").trim().toLowerCase();
  if (status === "classified") return "classified";
  return "unclassified";
}

export function screenStatusPillColor(status: string): string {
  return status === "classified" ? "#047857" : "#b45309";
}

export type SchemaEntityType = "scada object" | "fixed table" | "log tables";

export function normalizeSchemaEntityType(value: any): SchemaEntityType {
  const text = String(value || "").trim().toLowerCase();
  if (text === "table" || text === "fixed table" || text === "fixed table object") {
    return "fixed table";
  }
  if (text === "log" || text === "log/alert" || text === "log table" || text === "log tables") {
    return "log tables";
  }
  return "scada object";
}

export function schemaTypeColor(value: SchemaEntityType): string {
  if (value === "fixed table") return "#0f766e";
  if (value === "log tables") return "#9a3412";
  return "#334155";
}
