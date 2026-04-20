"use client";

interface ColorEntry {
  t?: string;
  value?: string;
  metric?: string;
}

interface ColorSequenceChartProps {
  entries: ColorEntry[] | null;
}

const KNOWN_CSS: Record<string, string> = {
  red: "#ef4444",
  green: "#22c55e",
  blue: "#3b82f6",
  yellow: "#eab308",
  cyan: "#06b6d4",
  magenta: "#d946ef",
  orange: "#f97316",
  white: "#e5e7eb",
  black: "#1e1e1e",
  gray: "#6b7280",
  grey: "#6b7280",
  purple: "#8b5cf6",
  pink: "#ec4899",
};

function hashColor(str: string): string {
  let h = 0;
  for (let i = 0; i < str.length; i++)
    h = str.charCodeAt(i) + ((h << 5) - h);
  return `hsl(${Math.abs(h) % 360}, 60%, 48%)`;
}

function colorFor(value: string | undefined): string {
  const low = (value || "").trim().toLowerCase();
  return KNOWN_CSS[low] || hashColor(value || "?");
}

function formatTime(value: string | undefined): string {
  if (!value) return "";
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return "";
  return d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
}

function formatDateTime(value: string | undefined): string {
  if (!value) return "";
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return "";
  return d.toLocaleString();
}

export default function ColorSequenceChart({
  entries,
}: ColorSequenceChartProps) {
  if (!entries || entries.length === 0) {
    return <p className="text-sm text-gray-400">No color data available.</p>;
  }

  const byMetric: Record<string, ColorEntry[]> = {};
  for (const entry of entries) {
    const mk = entry.metric || "status";
    if (!byMetric[mk]) byMetric[mk] = [];
    byMetric[mk].push(entry);
  }

  const metricKeys = Object.keys(byMetric);

  return (
    <div className="flex flex-col gap-3">
      {metricKeys.map((mk) => {
        const items = byMetric[mk];
        return (
          <div key={mk}>
            {metricKeys.length > 1 && (
              <div className="mb-1 text-[11px] font-semibold uppercase text-gray-400">
                {mk}
              </div>
            )}
            <div className="flex flex-wrap items-end gap-1.5">
              {items.map((item, idx) => {
                const bg = colorFor(item.value);
                return (
                  <div
                    key={idx}
                    className="flex min-w-[48px] cursor-default flex-col items-center gap-0.5"
                    title={`${formatDateTime(item.t)}\n${mk}: ${item.value}`}
                  >
                    <div
                      className="h-6 w-6 shrink-0 rounded border-2 border-black/10"
                      style={{ background: bg }}
                    />
                    <span className="max-w-[64px] truncate text-center text-[10px] font-semibold text-gray-800">
                      {item.value}
                    </span>
                    <span className="whitespace-nowrap text-[9px] text-gray-400">
                      {formatTime(item.t)}
                    </span>
                  </div>
                );
              })}
            </div>
          </div>
        );
      })}
    </div>
  );
}
