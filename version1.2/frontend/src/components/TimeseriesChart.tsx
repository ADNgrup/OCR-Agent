"use client";

import {
  Line,
  LineChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
  Legend,
} from "recharts";

interface TimeseriesPoint {
  t: string | number;
  y: number | string;
}

interface TimeseriesItem {
  points: TimeseriesPoint[];
  name?: string;
  entity_name?: string;
}

interface TimeseriesChartProps {
  series: Record<string, TimeseriesItem> | null;
}

function colorFromKey(key: string): string {
  const total = key.split("").reduce((acc, ch) => acc + ch.charCodeAt(0), 0);
  return `hsl(${Math.abs(total) % 360}, 70%, 45%)`;
}

function toTimestamp(value: string | number): number | null {
  const ts = new Date(value).getTime();
  return Number.isFinite(ts) ? ts : null;
}

export default function TimeseriesChart({ series }: TimeseriesChartProps) {
  const keys = Object.keys(series || {});

  if (!keys.length) {
    return (
      <p className="text-sm text-gray-400">
        No timeseries found for selected range.
      </p>
    );
  }

  const rowsMap = new Map<number, Record<string, number>>();
  const legendMap: Record<string, string> = {};

  for (const key of keys) {
    const item = series![key] || { points: [], name: key };
    legendMap[key] = item.name || key;

    for (const point of item.points || []) {
      const t = toTimestamp(point.t);
      const y =
        typeof point.y === "number" ? point.y : Number.parseFloat(point.y);
      if (t === null || !Number.isFinite(y)) {
        continue;
      }
      if (!rowsMap.has(t)) {
        rowsMap.set(t, { t } as Record<string, number>);
      }
      rowsMap.get(t)![key] = y;
    }
  }

  const data = Array.from(rowsMap.values()).sort(
    (a, b) => (a.t as number) - (b.t as number),
  );

  if (!data.length) {
    return (
      <p className="text-sm text-gray-400">
        No numeric values in selected range.
      </p>
    );
  }

  return (
    <div className="w-full min-h-[240px]">
      <ResponsiveContainer width="100%" height={240}>
        <LineChart
          data={data}
          margin={{ top: 10, right: 24, bottom: 10, left: 0 }}
        >
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis
            dataKey="t"
            type="number"
            domain={["dataMin", "dataMax"]}
            tickFormatter={(v: number) =>
              new Date(v).toLocaleString(undefined, {
                month: "2-digit",
                day: "2-digit",
                hour: "2-digit",
                minute: "2-digit",
              })
            }
          />
          <YAxis />
          <Tooltip
            labelFormatter={(v: number) => new Date(v).toLocaleString()}
          />
          <Legend formatter={(value: string) => legendMap[value] || value} />
          {keys.map((key) => (
            <Line
              key={key}
              type="monotone"
              dataKey={key}
              dot={false}
              strokeWidth={2}
              stroke={colorFromKey(key)}
              connectNulls
            />
          ))}
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}
