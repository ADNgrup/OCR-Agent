"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import KvmSourcesSection from "@/components/settings/KvmSourcesSection";
import SettingsHeader from "@/components/settings/SettingsHeader";
import SettingsStatusCard from "@/components/settings/SettingsStatusCard";
import { DEFAULT_BACKEND } from "@/lib/api";
import { useBackendApi } from "@/lib/hooks/useBackendApi";

/* eslint-disable @typescript-eslint/no-explicit-any */

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
];

const GROUP_LABELS: Record<string, string> = {
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
  mode: "v2",
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

interface ScreenLibraryItem {
  id: string;
  source_id?: string;
  monitor_key?: string;
  name: string;
  ignored?: boolean;
  schema_status?: string;
  entity_count?: number;
  sample_count?: number;
  sample_image_url?: string;
  classified_at?: string;
  updated_at?: string;
}

interface ScreenSample {
  id: string;
  filename?: string;
  content_type?: string;
  image_hash?: string;
  image_base64: string;
  width?: number;
  height?: number;
  created_at?: string;
}

interface SegmentBBox {
  x: number;
  y: number;
  w: number;
  h: number;
}

interface SegmentPoint {
  x: number;
  y: number;
}

type DrawTool = "rectangle";
type SegmentObjectType = "scada object" | "fixed table" | "log tables";

interface SegmentItem {
  id: string;
  name: string;
  type: SegmentObjectType;
  shape: DrawTool;
  points: SegmentPoint[];
  bbox: SegmentBBox;
  sample_id?: string;
  columns: string[];
  rows: string[];
  color?: string;
  confidence?: number | null;
}

interface ScreenSchemaEditorData extends ScreenLibraryItem {
  samples: ScreenSample[];
  segmentation_schema?: SegmentItem[];
}

function clampPct(value: number): number {
  if (!Number.isFinite(value)) return 0;
  return Math.max(0, Math.min(100, value));
}

function normalizeBBox(input: any): SegmentBBox {
  const src = input && typeof input === "object" ? input : {};
  return {
    x: clampPct(Number(src.x ?? 10)),
    y: clampPct(Number(src.y ?? 10)),
    w: clampPct(Number(src.w ?? 20)),
    h: clampPct(Number(src.h ?? 20)),
  };
}

function asDrawTool(value: any): DrawTool {
  return "rectangle";
}

function asSegmentObjectType(value: any): SegmentObjectType {
  const text = String(value || "scada object").trim().toLowerCase();
  if (text === "log" || text === "log table" || text === "log tables" || text === "log/alert") {
    return "log tables";
  }
  if (text === "table" || text === "fixed table" || text === "fixed table object") {
    return "fixed table";
  }
  return "scada object";
}

function normalizeColumnsForType(type: SegmentObjectType, columns: string[] | undefined): string[] {
  if (type === "log tables") {
    return ["time", "message"];
  }
  return Array.isArray(columns)
    ? columns.map((col) => String(col).trim()).filter(Boolean)
    : [];
}

function applyObjectTypeRules(
  type: SegmentObjectType,
  columns: string[] | undefined,
  rows: string[] | undefined,
): { columns: string[]; rows: string[] } {
  const cleanColumns = Array.isArray(columns)
    ? columns.map((col) => String(col).trim()).filter(Boolean)
    : [];
  const cleanRows = Array.isArray(rows)
    ? rows.map((row) => String(row).trim()).filter(Boolean)
    : [];

  if (type === "log tables") {
    return { columns: ["time", "message"], rows: [] };
  }
  if (type === "fixed table") {
    return {
      columns: cleanColumns.length ? cleanColumns : ["column_1"],
      rows: cleanRows.length ? cleanRows : ["row_1"],
    };
  }
  return {
    columns: cleanColumns,
    rows: [],
  };
}

function parseSegmentListInput(value: string): string[] {
  return value
    .split(/[;,]/)
    .map((item) => item.trim())
    .filter(Boolean);
}

function formatSegmentListInput(values: string[] | undefined): string {
  if (!Array.isArray(values)) return "";
  return values.map((item) => String(item).trim()).filter(Boolean).join("; ");
}

function normalizePoints(input: any): SegmentPoint[] {
  if (!Array.isArray(input)) return [];
  return input
    .map((pt) => {
      const src = pt && typeof pt === "object" ? pt : {};
      return {
        x: clampPct(Number(src.x ?? 0)),
        y: clampPct(Number(src.y ?? 0)),
      };
    })
    .filter((pt) => Number.isFinite(pt.x) && Number.isFinite(pt.y));
}

function rectanglePointsFromBBox(bbox: SegmentBBox): SegmentPoint[] {
  return [
    { x: clampPct(bbox.x), y: clampPct(bbox.y) },
    { x: clampPct(bbox.x + bbox.w), y: clampPct(bbox.y) },
    { x: clampPct(bbox.x + bbox.w), y: clampPct(bbox.y + bbox.h) },
    { x: clampPct(bbox.x), y: clampPct(bbox.y + bbox.h) },
  ];
}

function randomSegId(): string {
  return Math.random().toString(36).slice(2, 10);
}

function formatDate(value: string | undefined): string {
  if (!value) return "";
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return "";
  return d.toLocaleString();
}

export default function SettingsPage() {
  const [backendUrl] = useState(DEFAULT_BACKEND);
  const api = useBackendApi(backendUrl);

  const [config, setConfig] = useState<Record<string, any>>({});
  const [draft, setDraft] = useState<Record<string, any>>({});
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [status, setStatus] = useState("");
  const [error, setError] = useState("");
  const [showApiKey, setShowApiKey] = useState(false);

  const [sources, setSources] = useState<KvmSource[]>([]);
  const [showAddForm, setShowAddForm] = useState(false);
  const [editingId, setEditingId] = useState<string | null>(null);
  const [sourceForm, setSourceForm] = useState<SourceForm>({ ...EMPTY_SOURCE });
  const [sourceLoading, setSourceLoading] = useState(false);

  const [queueStats, setQueueStats] = useState<QueueStatsData>({
    pending: 0,
    processing: 0,
    completed: 0,
    failed: 0,
    recent_errors: [],
  });

  const [schemaSourceId, setSchemaSourceId] = useState("");
  const [screenLibrary, setScreenLibrary] = useState<ScreenLibraryItem[]>([]);
  const [selectedScreenId, setSelectedScreenId] = useState("");
  const [screenEditorData, setScreenEditorData] = useState<ScreenSchemaEditorData | null>(null);

  const [targetScreenIdForUpload, setTargetScreenIdForUpload] = useState("");
  const [uploadScreenName, setUploadScreenName] = useState("");
  const [uploadMonitorKey, setUploadMonitorKey] = useState("default");
  const [sampleFile, setSampleFile] = useState<File | null>(null);
  const [uploadingSample, setUploadingSample] = useState(false);
  const [deletingScreenId, setDeletingScreenId] = useState("");

  const [segments, setSegments] = useState<SegmentItem[]>([]);
  const [selectedSegmentId, setSelectedSegmentId] = useState("");
  const [activeSampleId, setActiveSampleId] = useState("");
  const [savingSegments, setSavingSegments] = useState(false);
  const [drawTool] = useState<DrawTool>("rectangle");
  const [zoomPct, setZoomPct] = useState(100);
  const [showJsonOutput, setShowJsonOutput] = useState(true);
  const [movingSourceByScreen, setMovingSourceByScreen] = useState<Record<string, boolean>>({});
  const [columnsInput, setColumnsInput] = useState("");
  const [rowsInput, setRowsInput] = useState("");

  const drawAreaRef = useRef<HTMLDivElement | null>(null);
  const [drawingStart, setDrawingStart] = useState<{ x: number; y: number } | null>(null);
  const [draftBbox, setDraftBbox] = useState<SegmentBBox | null>(null);

  const schemaSource = useMemo(
    () => sources.find((src) => src.id === schemaSourceId),
    [sources, schemaSourceId],
  );

  const sourceNameById = useMemo(() => {
    const out = new Map<string, string>();
    for (const source of sources) {
      out.set(source.id, source.name);
    }
    return out;
  }, [sources]);

  const activeSample = useMemo(() => {
    const sampleList = screenEditorData?.samples || [];
    if (!sampleList.length) return null;
    return sampleList.find((s) => s.id === activeSampleId) || sampleList[0];
  }, [screenEditorData, activeSampleId]);

  const selectedSegment = useMemo(
    () => segments.find((seg) => seg.id === selectedSegmentId) || null,
    [segments, selectedSegmentId],
  );

  useEffect(() => {
    if (!selectedSegment) {
      setColumnsInput("");
      setRowsInput("");
      return;
    }

    setColumnsInput(formatSegmentListInput(selectedSegment.columns));
    setRowsInput(formatSegmentListInput(selectedSegment.rows));
  }, [selectedSegment]);

  function commitSelectedSegmentColumns(value: string) {
    if (!selectedSegment) return;
    const parsed = parseSegmentListInput(value);
    setColumnsInput(formatSegmentListInput(parsed));
    updateSelectedSegment({ columns: parsed });
  }

  function commitSelectedSegmentRows(value: string) {
    if (!selectedSegment) return;
    const parsed = parseSegmentListInput(value);
    setRowsInput(formatSegmentListInput(parsed));
    updateSelectedSegment({ rows: parsed });
  }

  function resolveScreenThumbUrl(imageUrl: string | undefined): string | null {
    return api.resolveImageUrl(imageUrl);
  }

  function handleSchemaSourceChange(sourceId: string) {
    setSchemaSourceId(sourceId);
    setTargetScreenIdForUpload("");

    const source = sources.find((src) => src.id === sourceId);
    if (source?.monitor_keys?.length) {
      setUploadMonitorKey(source.monitor_keys[0]);
    }
  }

  const annotationJson = useMemo(() => {
    const sampleName = activeSample?.filename || `sample_${activeSample?.id || "unknown"}`;
    return {
      image_path: sampleName,
      image_width: activeSample?.width || null,
      image_height: activeSample?.height || null,
      total_segments: segments.length,
      segments: segments.map((seg, idx) => ({
        id: seg.id || `seg_${idx + 1}`,
        name: seg.name,
        type: seg.type,
        shape: seg.shape,
        bbox: seg.bbox,
        columns: seg.columns || [],
        rows: seg.rows || [],
        confidence: seg.confidence,
        color: seg.color || "#55cF6f",
        screen_id: selectedScreenId,
        sample_id: seg.sample_id || activeSample?.id || "",
      })),
    };
  }, [activeSample, segments, selectedScreenId]);

  const loadConfig = useCallback(async () => {
    setLoading(true);
    setError("");
    try {
      const data = await api.getConfig<Record<string, any>>();
      setConfig(data);
      setDraft(data);
      setStatus("Config loaded.");
    } catch (err: any) {
      setError(err.message || "Failed to load config.");
    } finally {
      setLoading(false);
    }
  }, [api]);

  const loadSources = useCallback(async () => {
    try {
      const data = await api.listSources<KvmSource[]>();
      setSources(data);
      setSchemaSourceId((prev) => {
        if (prev && data.some((src) => src.id === prev)) return prev;
        return data[0]?.id || "";
      });
    } catch (err: any) {
      setError(err.message || "Failed to load sources.");
    }
  }, [api]);

  const loadQueue = useCallback(async () => {
    try {
      const data = await api.getQueueStats<QueueStatsData>();
      setQueueStats(data);
    } catch {
      // silent
    }
  }, [api]);

  const loadScreenLibrary = useCallback(async () => {
    if (!sources.length) {
      setScreenLibrary([]);
      return;
    }

    try {
      const allScreens = await Promise.all(
        sources.map((src) => api.listScreens<ScreenLibraryItem[]>(src.id)),
      );

      const mergedById = new Map<string, ScreenLibraryItem>();
      for (const screen of allScreens.flat()) {
        const existing = mergedById.get(screen.id);
        if (!existing) {
          mergedById.set(screen.id, screen);
          continue;
        }

        const existingTs = Date.parse(existing.updated_at || "") || 0;
        const currentTs = Date.parse(screen.updated_at || "") || 0;
        if (currentTs >= existingTs) {
          mergedById.set(screen.id, screen);
        }
      }

      const merged = Array.from(mergedById.values()).sort((a, b) => {
        const aTs = Date.parse(a.updated_at || "") || 0;
        const bTs = Date.parse(b.updated_at || "") || 0;
        if (aTs !== bTs) return bTs - aTs;
        return (a.name || "").localeCompare(b.name || "");
      });

      setScreenLibrary(merged);
    } catch (err: any) {
      setError(err.message || "Failed to load screen library.");
    }
  }, [api, sources]);

  const loadScreenEditor = useCallback(
    async (screenId: string) => {
      if (!screenId) {
        setScreenEditorData(null);
        setSegments([]);
        setSelectedSegmentId("");
        setActiveSampleId("");
        return;
      }

      try {
        const data = await api.getScreenSchemaEditor<ScreenSchemaEditorData>(screenId);
        setScreenEditorData(data);

        const initialSegments =
          Array.isArray(data.segmentation_schema)
            ? data.segmentation_schema.map((seg: any) => {
                const bbox = normalizeBBox(seg.bbox);
                const shape = asDrawTool(seg.shape);
                const parsedPoints = normalizePoints(seg.points);
                const points = parsedPoints.length
                  ? parsedPoints
                  : rectanglePointsFromBBox(bbox);
                const segType = asSegmentObjectType(seg.type);
                const normalized = applyObjectTypeRules(
                  segType,
                  Array.isArray(seg.columns)
                    ? seg.columns.map((col: any) => String(col).trim()).filter(Boolean)
                    : [],
                  Array.isArray(seg.rows)
                    ? seg.rows.map((row: any) => String(row).trim()).filter(Boolean)
                    : [],
                );

                return {
                  id: String(seg.id || randomSegId()),
                  name: String(seg.name || "Unnamed segment"),
                  type: segType,
                  shape,
                  points,
                  bbox,
                  sample_id: String(seg.sample_id || ""),
                  columns: normalized.columns,
                  rows: normalized.rows,
                  color: String(seg.color || "#55cF6f"),
                  confidence: seg.confidence ?? null,
                } as SegmentItem;
              })
            : [];

        setSegments(initialSegments);
        setSelectedSegmentId(initialSegments[0]?.id || "");
        setActiveSampleId(data.samples?.[0]?.id || "");
        setDraftBbox(null);
        setDrawingStart(null);
      } catch (err: any) {
        setError(err.message || "Failed to load screen schema editor.");
      }
    },
    [api],
  );

  useEffect(() => {
    loadConfig();
    loadSources();
    loadQueue();
    const iv = setInterval(loadQueue, 5000);
    return () => clearInterval(iv);
  }, [loadConfig, loadSources, loadQueue]);

  useEffect(() => {
    loadScreenLibrary();
  }, [loadScreenLibrary]);

  useEffect(() => {
    const monitorKeys = schemaSource?.monitor_keys || [];
    if (!monitorKeys.length) return;
    setUploadMonitorKey((prev) => (prev && monitorKeys.includes(prev) ? prev : monitorKeys[0]));
  }, [schemaSource]);

  useEffect(() => {
    if (!screenLibrary.length) {
      setSelectedScreenId("");
      setScreenEditorData(null);
      return;
    }
    if (selectedScreenId && screenLibrary.some((scr) => scr.id === selectedScreenId)) {
      return;
    }
    setSelectedScreenId(screenLibrary[0].id);
  }, [screenLibrary, selectedScreenId]);

  useEffect(() => {
    if (!selectedScreenId) {
      setScreenEditorData(null);
      setSegments([]);
      setSelectedSegmentId("");
      setActiveSampleId("");
      return;
    }
    loadScreenEditor(selectedScreenId);
  }, [selectedScreenId, loadScreenEditor]);

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
      const data = await api.updateConfig<Record<string, any>>(payload);
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
      const data = await api.resetConfig<Record<string, any>>();
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
        await api.updateSource(editingId, body);
        setStatus("Source updated.");
      } else {
        body.enabled = true;
        await api.createSource(body);
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
      await api.toggleSource(id, enabled);
      await loadSources();
    } catch (err: any) {
      setError(err.message || "Toggle failed.");
    }
  }

  async function deleteSource(id: string) {
    if (!window.confirm("Delete this KVM source?")) return;
    try {
      await api.deleteSource(id);
      await loadSources();
      setStatus("Source deleted.");
    } catch (err: any) {
      setError(err.message || "Delete failed.");
    }
  }

  async function runOnce(id: string) {
    try {
      setStatus("Running one-time snapshot...");
      await api.runSourceOnce(id);
      setStatus("One-time snapshot completed.");
      loadQueue();
    } catch (err: any) {
      setError(err.message || "Run-once failed.");
    }
  }

  async function changeScreenSource(screenId: string, nextSourceId: string) {
    if (!screenId || !nextSourceId) return;

    setMovingSourceByScreen((prev) => ({ ...prev, [screenId]: true }));
    setError("");
    try {
      const data = await api.updateScreenSource<{ screen?: Record<string, any> }>(
        screenId,
        nextSourceId,
      );

      const nextUpdatedAt = String(data?.screen?.updated_at || new Date().toISOString());
      const resolvedSourceId = String(data?.screen?.source_id || nextSourceId);
      setScreenLibrary((prev) =>
        prev.map((screen) =>
          screen.id === screenId
            ? {
                ...screen,
                source_id: resolvedSourceId,
                updated_at: nextUpdatedAt,
              }
            : screen,
        ),
      );

      setStatus("Screen source updated.");
      await loadScreenLibrary();

      if (selectedScreenId === screenId) {
        await loadScreenEditor(screenId);
      }
    } catch (err: any) {
      setError(err.message || "Failed to update screen source.");
    } finally {
      setMovingSourceByScreen((prev) => ({ ...prev, [screenId]: false }));
    }
  }

  async function deleteScreenExample(screenId: string) {
    if (!screenId) return;
    if (!window.confirm("Delete this screen example? This will remove related snapshots too.")) return;

    setDeletingScreenId(screenId);
    setError("");
    try {
      await api.deleteScreen(screenId);
      setStatus("Screen example deleted.");

      if (selectedScreenId === screenId) {
        setSelectedScreenId("");
        setScreenEditorData(null);
        setSegments([]);
        setSelectedSegmentId("");
        setActiveSampleId("");
      }

      await loadScreenLibrary();
    } catch (err: any) {
      setError(err.message || "Failed to delete screen example.");
    } finally {
      setDeletingScreenId("");
    }
  }

  async function uploadScreenSample() {
    if (!schemaSourceId) {
      setError("Please select a source first.");
      return;
    }
    if (!sampleFile) {
      setError("Please choose an image file to upload.");
      return;
    }

    setUploadingSample(true);
    setError("");
    setStatus("Uploading screen sample...");
    try {
      const formData = new FormData();
      formData.append("source_id", schemaSourceId);
      formData.append("file", sampleFile);

      if (targetScreenIdForUpload) {
        formData.append("screen_group_id", targetScreenIdForUpload);
      } else {
        formData.append("monitor_key", uploadMonitorKey || "default");
        formData.append("screen_name", uploadScreenName || `imported_${Date.now()}`);
      }

      const data = await api.uploadScreenSample<{ screen?: { id?: string } }>(formData);

      setStatus("Screen sample uploaded.");
      setSampleFile(null);
      await loadScreenLibrary();

      const newScreenId = data?.screen?.id || targetScreenIdForUpload;
      if (newScreenId) {
        setSelectedScreenId(newScreenId);
        await loadScreenEditor(newScreenId);
      }
    } catch (err: any) {
      setError(err.message || "Upload failed.");
      setStatus("");
    } finally {
      setUploadingSample(false);
    }
  }

  function updateSelectedSegment(patch: Partial<SegmentItem>) {
    if (!selectedSegmentId) return;
    setSegments((prev) =>
      prev.map((seg) => {
        if (seg.id !== selectedSegmentId) return seg;
        const nextType = asSegmentObjectType(patch.type ?? seg.type);
        const merged = { ...seg, ...patch, type: nextType, shape: "rectangle" as DrawTool };
        const nextBbox = normalizeBBox(merged.bbox);
        const normalized = applyObjectTypeRules(nextType, merged.columns, merged.rows);

        return {
          ...merged,
          bbox: nextBbox,
          points: rectanglePointsFromBBox(nextBbox),
          columns: normalized.columns,
          rows: normalized.rows,
        };
      }),
    );
  }

  function updateSelectedSegmentBbox(patch: Partial<SegmentBBox>) {
    if (!selectedSegmentId) return;
    setSegments((prev) =>
      prev.map((seg) => {
        if (seg.id !== selectedSegmentId) return seg;

        const nextBbox = {
          x: clampPct(Number(patch.x ?? seg.bbox.x)),
          y: clampPct(Number(patch.y ?? seg.bbox.y)),
          w: clampPct(Number(patch.w ?? seg.bbox.w)),
          h: clampPct(Number(patch.h ?? seg.bbox.h)),
        };
        return {
          ...seg,
          bbox: nextBbox,
          points: rectanglePointsFromBBox(nextBbox),
        };
      }),
    );
  }

  function createRectangleSegment(bboxInput: SegmentBBox) {
    const bbox = normalizeBBox(bboxInput);
    const nextId = `seg_${randomSegId()}`;

    setSegments((prev) => {
      const nextIndex = prev.length + 1;
      const type: SegmentObjectType = "scada object";
      const normalized = applyObjectTypeRules(type, [], []);
      const next: SegmentItem = {
        id: nextId,
        name: `Segment ${nextIndex}`,
        type,
        shape: "rectangle",
        points: rectanglePointsFromBBox(bbox),
        bbox,
        sample_id: activeSample?.id || "",
        columns: normalized.columns,
        rows: normalized.rows,
        color: "#55cF6f",
        confidence: null,
      };
      setSelectedSegmentId(next.id);
      return [...prev, next];
    });
  }

  function addManualSegment() {
    const bbox = { x: 10, y: 10, w: 20, h: 20 };
    createRectangleSegment(bbox);
  }

  function removeSelectedSegment() {
    if (!selectedSegmentId) return;
    setSegments((prev) => {
      const remaining = prev.filter((seg) => seg.id !== selectedSegmentId);
      setSelectedSegmentId(remaining[0]?.id || "");
      return remaining;
    });
  }

  function getPointerPercent(clientX: number, clientY: number): { x: number; y: number } {
    const rect = drawAreaRef.current?.getBoundingClientRect();
    if (!rect) return { x: 0, y: 0 };
    return {
      x: clampPct(((clientX - rect.left) / rect.width) * 100),
      y: clampPct(((clientY - rect.top) / rect.height) * 100),
    };
  }

  function onDrawStart(e: React.PointerEvent<HTMLDivElement>) {
    if (drawTool !== "rectangle") return;
    if (!activeSample) return;
    if (e.button !== 0) return;
    e.preventDefault();
    const start = getPointerPercent(e.clientX, e.clientY);
    e.currentTarget.setPointerCapture(e.pointerId);
    setDrawingStart(start);
    setDraftBbox({ x: start.x, y: start.y, w: 0, h: 0 });
  }

  function onDrawMove(e: React.PointerEvent<HTMLDivElement>) {
    if (drawTool !== "rectangle") return;
    if (!drawingStart) return;
    e.preventDefault();
    const pos = getPointerPercent(e.clientX, e.clientY);
    const x = Math.min(drawingStart.x, pos.x);
    const y = Math.min(drawingStart.y, pos.y);
    const w = Math.abs(pos.x - drawingStart.x);
    const h = Math.abs(pos.y - drawingStart.y);
    setDraftBbox({ x, y, w, h });
  }

  function finishRectangleDraw() {
    if (!drawingStart || !draftBbox) {
      setDrawingStart(null);
      setDraftBbox(null);
      return;
    }

    if (draftBbox.w >= 1 && draftBbox.h >= 1) {
      const bbox = {
        x: clampPct(draftBbox.x),
        y: clampPct(draftBbox.y),
        w: clampPct(draftBbox.w),
        h: clampPct(draftBbox.h),
      };
      createRectangleSegment(bbox);
    }

    setDrawingStart(null);
    setDraftBbox(null);
  }

  function onDrawEnd(e: React.PointerEvent<HTMLDivElement>) {
    if (drawTool !== "rectangle") return;
    if (e.currentTarget.hasPointerCapture(e.pointerId)) {
      e.currentTarget.releasePointerCapture(e.pointerId);
    }
    finishRectangleDraw();
  }

  function onDrawCancel(e: React.PointerEvent<HTMLDivElement>) {
    if (drawTool !== "rectangle") return;
    if (e.currentTarget.hasPointerCapture(e.pointerId)) {
      e.currentTarget.releasePointerCapture(e.pointerId);
    }
    setDrawingStart(null);
    setDraftBbox(null);
  }

  async function saveSegmentsToSchema() {
    if (!selectedScreenId) {
      setError("Please select a screen before saving schema.");
      return;
    }

    setSavingSegments(true);
    setError("");
    setStatus("Saving segmentation schema...");
    try {
      const parsedColumns = parseSegmentListInput(columnsInput);
      const parsedRows = parseSegmentListInput(rowsInput);
      const normalizedColumns = formatSegmentListInput(parsedColumns);
      const normalizedRows = formatSegmentListInput(parsedRows);

      const segmentsToSave = segments.map((seg) => {
        if (seg.id !== selectedSegmentId) return seg;

        if (seg.type === "fixed table") {
          return {
            ...seg,
            columns: parsedColumns,
            rows: parsedRows,
          };
        }

        if (seg.type === "scada object") {
          return {
            ...seg,
            columns: parsedColumns,
            rows: [],
          };
        }

        return {
          ...seg,
          columns: ["time", "message"],
          rows: [],
        };
      });

      const payload = {
        sample_id: activeSample?.id || "",
        segments: segmentsToSave.map((seg) => ({
          id: seg.id,
          name: seg.name,
          type: seg.type,
          bbox: seg.bbox,
          sample_id: seg.sample_id || activeSample?.id || "",
          columns: seg.columns || [],
          rows: seg.rows || [],
        })),
      };

      setSegments(segmentsToSave);
      setColumnsInput(normalizedColumns);
      setRowsInput(normalizedRows);

      await api.saveScreenSchemaSegments(selectedScreenId, payload as Record<string, unknown>);
      setStatus("Segmentation schema saved.");
      await loadScreenLibrary();
      await loadScreenEditor(selectedScreenId);
    } catch (err: any) {
      setError(err.message || "Failed to save schema.");
      setStatus("");
    } finally {
      setSavingSegments(false);
    }
  }

  async function copyAnnotationJson() {
    try {
      await navigator.clipboard.writeText(JSON.stringify(annotationJson, null, 2));
      setStatus("Annotation JSON copied.");
    } catch {
      setError("Failed to copy JSON.");
    }
  }

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
              onChange={(e) => setSourceForm((f) => ({ ...f, name: e.target.value }))}
              placeholder="e.g. kvm-machine-1"
            />
          </label>
          <label className="field">
            <span className="field-label">Host</span>
            <input
              required
              value={sourceForm.host}
              onChange={(e) => setSourceForm((f) => ({ ...f, host: e.target.value }))}
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
              onChange={(e) => setSourceForm((f) => ({ ...f, port: e.target.value }))}
              placeholder="9081"
            />
          </label>
          <label className="field">
            <span className="field-label">Base Path</span>
            <input
              value={sourceForm.base_path}
              onChange={(e) => setSourceForm((f) => ({ ...f, base_path: e.target.value }))}
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
              onChange={(e) => setSourceForm((f) => ({ ...f, poll_seconds: Number(e.target.value) }))}
            />
          </label>
          <label className="field">
            <span className="field-label">Monitor Keys</span>
            <input
              value={sourceForm.monitor_keys}
              onChange={(e) => setSourceForm((f) => ({ ...f, monitor_keys: e.target.value }))}
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
              onChange={(e) => setSourceForm((f) => ({ ...f, similarity_threshold: Number(e.target.value) }))}
            />
          </label>
          <label className="field">
            <span className="field-label">Pipeline Mode</span>
            <select
              value={sourceForm.mode}
              onChange={(e) => setSourceForm((f) => ({ ...f, mode: e.target.value }))}
            >
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
        <h1>Settings</h1>
        <p>Loading configuration...</p>
      </main>
    );
  }

  return (
    <main className="page">
      <SettingsHeader />

      <SettingsStatusCard status={status} error={error} />

      <KvmSourcesSection
        sources={sources}
        sourceLoading={sourceLoading}
        showAddForm={showAddForm}
        editingId={editingId}
        renderSourceForm={renderSourceForm}
        onOpenAddForm={openAddForm}
        onRunOnce={runOnce}
        onOpenEditForm={openEditForm}
        onDeleteSource={deleteSource}
        onToggleSource={toggleSource}
      />

      <section className="card settings-group">
        <div className="section-header">
          <h2>Screen Samples &amp; Segmentation Schema</h2>
          <button className="btn-secondary" onClick={() => loadScreenLibrary()} disabled={!sources.length}>
            Refresh Queue
          </button>
        </div>

        <p className="muted" style={{ marginTop: 0 }}>
          Upload screen samples, define segmentation areas, and save schema to MongoDB.
          Screens with schema are highlighted in green; unclassified screens are highlighted in red.
        </p>

        <div style={{ marginBottom: 14 }}>
          <div className="section-header" style={{ marginBottom: 8 }}>
            <h3 style={{ margin: 0 }}>Sample Queue (All Screens)</h3>
          </div>
          <p className="muted" style={{ marginTop: 0, marginBottom: 10, fontSize: 12 }}>
            Queue hiển thị toàn bộ screen, không chia theo source.
          </p>

          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(300px, 1fr))", gap: 12, marginBottom: 16 }}>
          {screenLibrary.map((screen) => {
            const status = (screen.schema_status || "").toLowerCase();
            const isClassified = status === "classified" || Number(screen.entity_count || 0) > 0;
            const isSelected = selectedScreenId === screen.id;
            const currentSourceId = String(screen.source_id || "");
            const sourceChanging = Boolean(movingSourceByScreen[screen.id]);
            const sourceLabel = sourceNameById.get(currentSourceId) || currentSourceId || "Unknown";
            const deletingThisScreen = deletingScreenId === screen.id;
            const thumbnailUrl = resolveScreenThumbUrl(screen.sample_image_url);
            return (
              <div
                key={screen.id}
                style={{
                  textAlign: "left",
                  borderRadius: 10,
                  border: `2px solid ${isClassified ? "#16a34a" : "#dc2626"}`,
                  padding: 10,
                  background: isSelected ? "#f8fafc" : "#fff",
                }}
              >
                <button
                  type="button"
                  onClick={() => {
                    setSelectedScreenId(screen.id);
                  }}
                  style={{
                    width: "100%",
                    textAlign: "left",
                    border: "none",
                    background: "transparent",
                    padding: 0,
                    cursor: "pointer",
                  }}
                >
                <div style={{ display: "flex", justifyContent: "space-between", gap: 8, alignItems: "center", width: "100%" }}>
                  <strong>{screen.name}</strong>
                  <span className="badge" style={{ background: isClassified ? "#16a34a" : "#dc2626", color: "#fff" }}>
                    {isClassified ? "Schema Ready" : "No Schema"}
                  </span>
                </div>
                <div className="muted" style={{ marginTop: 6, fontSize: 12 }}>
                  Monitor: {screen.monitor_key || "default"}
                </div>
                <div className="muted" style={{ marginTop: 2, fontSize: 12 }}>
                  Samples: {screen.sample_count ?? 0} · Entities: {screen.entity_count ?? 0}
                </div>

                {thumbnailUrl ? (
                  <img
                    src={thumbnailUrl}
                    alt={screen.name}
                    style={{
                      marginTop: 8,
                      width: "78%",
                      maxWidth: 220,
                      height: 64,
                      objectFit: "cover",
                      borderRadius: 6,
                      border: "1px solid #e5e7eb",
                      display: "block",
                      marginLeft: "auto",
                      marginRight: "auto",
                    }}
                  />
                ) : null}
                </button>

                {isSelected ? (
                  <div style={{ marginTop: 8, paddingTop: 8, borderTop: "1px solid #e5e7eb", display: "grid", gap: 8 }}>
                    <label className="field" style={{ marginBottom: 0 }}>
                      <span className="field-label">Source</span>
                      <select
                        value={currentSourceId}
                        disabled={sourceChanging || deletingThisScreen || !sources.length}
                        onChange={(e) => {
                          const nextSourceId = e.target.value;
                          if (!nextSourceId || nextSourceId === currentSourceId) return;
                          changeScreenSource(screen.id, nextSourceId);
                        }}
                      >
                        {currentSourceId && !sources.some((src) => src.id === currentSourceId) ? (
                          <option value={currentSourceId}>{sourceLabel}</option>
                        ) : null}
                        {sources.map((src) => (
                          <option key={src.id} value={src.id}>
                            {src.name}
                          </option>
                        ))}
                      </select>
                    </label>

                    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", gap: 8 }}>
                      <button
                        type="button"
                        className="btn-sm btn-danger"
                        disabled={deletingThisScreen}
                        onClick={() => deleteScreenExample(screen.id)}
                        title="Delete screen example"
                      >
                        {deletingThisScreen ? "Deleting..." : "Delete Screen"}
                      </button>
                      {sourceChanging ? (
                        <div className="muted" style={{ fontSize: 11 }}>
                          Updating source...
                        </div>
                      ) : null}
                    </div>
                  </div>
                ) : (
                  <p className="muted" style={{ marginTop: 8, marginBottom: 0, fontSize: 11 }}>
                    Select this screen to edit source or delete.
                  </p>
                )}
              </div>
            );
          })}
          {!screenLibrary.length && (
            <p className="muted" style={{ margin: 0 }}>
              No screens in queue yet. Upload your first sample below.
            </p>
          )}
        </div>

        <div style={{ border: "1px solid #e5e7eb", borderRadius: 10, padding: 12, marginBottom: 14 }}>
          <h3 style={{ marginTop: 0 }}>Upload Screen Sample</h3>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))", gap: 10, alignItems: "end" }}>
            <label className="field">
              <span className="field-label">Source</span>
              <select
                value={schemaSourceId}
                onChange={(e) => handleSchemaSourceChange(e.target.value)}
              >
                {sources.map((src) => (
                  <option key={src.id} value={src.id}>
                    {src.name}
                  </option>
                ))}
              </select>
            </label>

            <label className="field">
              <span className="field-label">Attach To Existing Screen</span>
              <select value={targetScreenIdForUpload} onChange={(e) => setTargetScreenIdForUpload(e.target.value)}>
                <option value="">Create New Screen</option>
                {screenLibrary.map((screen) => (
                  <option key={screen.id} value={screen.id}>
                    {screen.name} ({screen.monitor_key || "default"})
                  </option>
                ))}
              </select>
            </label>

            {!targetScreenIdForUpload && (
              <label className="field">
                <span className="field-label">New Screen Name</span>
                <input value={uploadScreenName} onChange={(e) => setUploadScreenName(e.target.value)} placeholder="e.g. PLC Main Screen" />
              </label>
            )}

            {!targetScreenIdForUpload && (
              <label className="field">
                <span className="field-label">Monitor Key</span>
                <input
                  value={uploadMonitorKey}
                  onChange={(e) => setUploadMonitorKey(e.target.value)}
                  placeholder={schemaSource?.monitor_keys?.[0] || "default"}
                />
              </label>
            )}

            <label className="field">
              <span className="field-label">Sample Image</span>
              <input type="file" accept="image/*" onChange={(e) => setSampleFile(e.target.files?.[0] || null)} />
            </label>

            <button className="btn-sm" onClick={uploadScreenSample} disabled={!schemaSourceId || !sampleFile || uploadingSample}>
              {uploadingSample ? "Uploading..." : "Upload Sample"}
            </button>
          </div>
        </div>
        </div>

        <div style={{ borderTop: "1px solid #e5e7eb", paddingTop: 14 }}>
          <div className="section-header">
            <h3>Segmentation Tool</h3>
            <div style={{ display: "flex", gap: 8, flexWrap: "wrap", justifyContent: "flex-end" }}>
              <button className="btn-secondary" onClick={addManualSegment} disabled={!selectedScreenId}>
                + Add Segment
              </button>
              <button className="btn-danger" onClick={removeSelectedSegment} disabled={!selectedSegmentId}>
                Delete Segment
              </button>
              <button onClick={saveSegmentsToSchema} disabled={!selectedScreenId || savingSegments}>
                {savingSegments ? "Saving..." : "Save Schema"}
              </button>
              <button className="btn-secondary" onClick={() => setShowJsonOutput((prev) => !prev)}>
                {showJsonOutput ? "Hide JSON" : "Show JSON"}
              </button>
            </div>
          </div>

          {!selectedScreenId ? (
            <p className="muted">Select a screen above to define segmentation schema.</p>
          ) : (
            <div
              style={{
                display: "grid",
                gridTemplateColumns: showJsonOutput
                  ? "minmax(720px, 1fr) minmax(280px, 360px)"
                  : "minmax(780px, 1fr)",
                gap: 14,
                alignItems: "start",
              }}
            >
              <div style={{ border: "1px solid #e5e7eb", borderRadius: 10, padding: 10 }}>
                <div style={{ display: "flex", justifyContent: "space-between", gap: 8, alignItems: "center", marginBottom: 8 }}>
                  <h4 style={{ margin: 0 }}>Label Canvas</h4>
                  <div style={{ display: "inline-flex", gap: 6, alignItems: "center", flexWrap: "wrap" }}>
                    <span className="muted" style={{ fontSize: 12 }}>Tool: Rectangle</span>
                    <span className="muted" style={{ fontSize: 12, marginLeft: 4 }}>Zoom:</span>
                    <button className="btn-sm" onClick={() => setZoomPct((z) => Math.max(40, z - 10))}>-</button>
                    <span style={{ minWidth: 42, textAlign: "center", fontSize: 12 }}>{zoomPct}%</span>
                    <button className="btn-sm" onClick={() => setZoomPct((z) => Math.min(200, z + 10))}>+</button>
                    <button className="btn-sm btn-secondary" onClick={() => setZoomPct(100)}>Reset</button>
                  </div>
                </div>

                <div style={{ display: "flex", gap: 8, alignItems: "center", marginBottom: 8, flexWrap: "wrap" }}>
                  <span className="muted" style={{ fontSize: 12 }}>
                    Drag to draw a rectangle. After drawing, fill metadata in Segment Properties.
                  </span>
                </div>

                {activeSample ? (
                  <div style={{ border: "1px solid #d1d5db", borderRadius: 8, overflow: "hidden" }}>
                    <div style={{ maxHeight: 560, overflow: "auto", background: "#f8fafc" }}>
                      <div
                        ref={drawAreaRef}
                        role="presentation"
                        onPointerDown={onDrawStart}
                        onPointerMove={onDrawMove}
                        onPointerUp={onDrawEnd}
                        onPointerCancel={onDrawCancel}
                        style={{
                          position: "relative",
                          width: `${zoomPct}%`,
                          minWidth: 360,
                          margin: "0 auto",
                          cursor: "crosshair",
                          touchAction: "none",
                          userSelect: "none",
                        }}
                      >
                        <img
                          src={activeSample.image_base64}
                          alt="Active sample"
                          draggable={false}
                          onDragStart={(e) => e.preventDefault()}
                          style={{ display: "block", width: "100%", height: "auto" }}
                        />

                        {segments.map((seg) => {
                          const active = seg.id === selectedSegmentId;
                          return (
                            <button
                              key={seg.id}
                              type="button"
                              onPointerDown={(e) => e.stopPropagation()}
                              onClick={(e) => {
                                e.stopPropagation();
                                setSelectedSegmentId(seg.id);
                              }}
                              style={{
                                position: "absolute",
                                left: `${seg.bbox.x}%`,
                                top: `${seg.bbox.y}%`,
                                width: `${seg.bbox.w}%`,
                                height: `${seg.bbox.h}%`,
                                border: active ? "2px solid #2563eb" : `2px solid ${seg.color || "#16a34a"}`,
                                background: active ? "rgba(37, 99, 235, 0.12)" : "rgba(22,163,74,0.10)",
                                borderRadius: 4,
                                padding: 0,
                                cursor: "pointer",
                              }}
                              title={seg.name}
                            >
                              <span
                                style={{
                                  position: "absolute",
                                  left: 2,
                                  top: 2,
                                  fontSize: 11,
                                  color: "#0f172a",
                                  background: "rgba(255,255,255,0.9)",
                                  borderRadius: 4,
                                  padding: "0 4px",
                                }}
                              >
                                {seg.name}
                              </span>
                            </button>
                          );
                        })}

                        {draftBbox ? (
                          <div
                            style={{
                              position: "absolute",
                              left: `${draftBbox.x}%`,
                              top: `${draftBbox.y}%`,
                              width: `${draftBbox.w}%`,
                              height: `${draftBbox.h}%`,
                              border: "2px dashed #f59e0b",
                              background: "rgba(245, 158, 11, 0.12)",
                              borderRadius: 4,
                              pointerEvents: "none",
                            }}
                          />
                        ) : null}
                      </div>
                    </div>
                  </div>
                ) : (
                  <p className="muted" style={{ marginBottom: 0 }}>
                    No sample image found. Upload at least one sample to start segmentation.
                  </p>
                )}

                <div style={{ marginTop: 12, borderTop: "1px solid #e5e7eb", paddingTop: 10 }}>
                  <h4 style={{ marginTop: 0 }}>Segment Properties</h4>
                  {selectedSegment ? (
                    <div style={{ display: "grid", gap: 8 }}>
                      <div style={{ display: "grid", gridTemplateColumns: "repeat(2, 1fr)", gap: 8 }}>
                        <label className="field">
                          <span className="field-label">Name</span>
                          <input
                            value={selectedSegment.name}
                            onChange={(e) => updateSelectedSegment({ name: e.target.value })}
                          />
                        </label>

                        <label className="field">
                          <span className="field-label">Object Type</span>
                          <select
                            value={selectedSegment.type}
                            onChange={(e) => {
                              const nextType = asSegmentObjectType(e.target.value);
                              const normalized = applyObjectTypeRules(nextType, selectedSegment.columns, selectedSegment.rows);
                              updateSelectedSegment({
                                type: nextType,
                                columns: normalized.columns,
                                rows: normalized.rows,
                              });
                            }}
                          >
                            <option value="scada object">scada object</option>
                            <option value="fixed table">fixed table</option>
                            <option value="log tables">log tables</option>
                          </select>
                        </label>
                      </div>

                      {selectedSegment.type === "scada object" ? (
                        <label className="field">
                          <span className="field-label">Terms (semicolon separated)</span>
                          <input
                            value={columnsInput}
                            onChange={(e) => setColumnsInput(e.target.value)}
                            onBlur={(e) => commitSelectedSegmentColumns(e.target.value)}
                            onKeyDown={(e) => {
                              if (e.key === "Enter") {
                                e.preventDefault();
                                commitSelectedSegmentColumns((e.target as HTMLInputElement).value);
                              }
                            }}
                            placeholder="pressure; temperature; flow"
                          />
                        </label>
                      ) : null}

                      {selectedSegment.type === "fixed table" ? (
                        <div style={{ display: "grid", gridTemplateColumns: "repeat(2, 1fr)", gap: 8 }}>
                          <label className="field">
                            <span className="field-label">Columns (semicolon separated)</span>
                            <input
                              value={columnsInput}
                              onChange={(e) => setColumnsInput(e.target.value)}
                              onBlur={(e) => commitSelectedSegmentColumns(e.target.value)}
                              onKeyDown={(e) => {
                                if (e.key === "Enter") {
                                  e.preventDefault();
                                  commitSelectedSegmentColumns((e.target as HTMLInputElement).value);
                                }
                              }}
                              placeholder="phase_a; phase_b"
                            />
                          </label>

                          <label className="field">
                            <span className="field-label">Rows (semicolon separated)</span>
                            <input
                              value={rowsInput}
                              onChange={(e) => setRowsInput(e.target.value)}
                              onBlur={(e) => commitSelectedSegmentRows(e.target.value)}
                              onKeyDown={(e) => {
                                if (e.key === "Enter") {
                                  e.preventDefault();
                                  commitSelectedSegmentRows((e.target as HTMLInputElement).value);
                                }
                              }}
                              placeholder="pump_1; pump_2"
                            />
                          </label>
                        </div>
                      ) : null}

                      {selectedSegment.type === "log tables" ? (
                        <label className="field">
                          <span className="field-label">Columns</span>
                          <input value="time; message" readOnly />
                        </label>
                      ) : null}

                      <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 8 }}>
                        <label className="field">
                          <span className="field-label">X (%)</span>
                          <input type="number" min={0} max={100} value={selectedSegment.bbox.x} onChange={(e) => updateSelectedSegmentBbox({ x: Number(e.target.value) })} />
                        </label>
                        <label className="field">
                          <span className="field-label">Y (%)</span>
                          <input type="number" min={0} max={100} value={selectedSegment.bbox.y} onChange={(e) => updateSelectedSegmentBbox({ y: Number(e.target.value) })} />
                        </label>
                        <label className="field">
                          <span className="field-label">W (%)</span>
                          <input type="number" min={0} max={100} value={selectedSegment.bbox.w} onChange={(e) => updateSelectedSegmentBbox({ w: Number(e.target.value) })} />
                        </label>
                        <label className="field">
                          <span className="field-label">H (%)</span>
                          <input type="number" min={0} max={100} value={selectedSegment.bbox.h} onChange={(e) => updateSelectedSegmentBbox({ h: Number(e.target.value) })} />
                        </label>
                      </div>

                      <div className="muted" style={{ fontSize: 12 }}>
                        Schema preview: {selectedSegment.type} | columns: {selectedSegment.columns.length} | rows: {selectedSegment.rows.length}
                      </div>
                    </div>
                  ) : (
                    <p className="muted" style={{ marginBottom: 0 }}>
                      Draw on canvas or select an annotation to edit properties.
                    </p>
                  )}

                  <div style={{ marginTop: 12 }}>
                    <h4 style={{ marginBottom: 6 }}>Defined Segments ({segments.length})</h4>
                    <div style={{ display: "flex", flexDirection: "column", gap: 6, maxHeight: 220, overflow: "auto" }}>
                      {segments.map((seg) => (
                        <button
                          key={seg.id}
                          type="button"
                          onClick={() => setSelectedSegmentId(seg.id)}
                          style={{
                            textAlign: "left",
                            border: seg.id === selectedSegmentId ? "1px solid #2563eb" : "1px solid #d1d5db",
                            borderRadius: 6,
                            padding: "6px 8px",
                            background: seg.id === selectedSegmentId ? "#eff6ff" : "#fff",
                            cursor: "pointer",
                          }}
                        >
                          <strong>{seg.name}</strong>
                          <div className="muted" style={{ fontSize: 12 }}>
                            {seg.shape} · {seg.type} · cols {seg.columns.length} · rows {seg.rows.length}
                          </div>
                        </button>
                      ))}
                      {!segments.length && <p className="muted" style={{ marginBottom: 0 }}>No segments yet.</p>}
                    </div>
                  </div>
                </div>
              </div>

              {showJsonOutput ? (
                <div style={{ border: "1px solid #e5e7eb", borderRadius: 10, padding: 10 }}>
                  <div className="section-header" style={{ marginBottom: 8 }}>
                    <h4 style={{ margin: 0 }}>JSON Output</h4>
                    <button className="btn-sm btn-secondary" onClick={copyAnnotationJson}>
                      Copy
                    </button>
                  </div>
                  <pre
                    style={{
                      margin: 0,
                      maxHeight: 620,
                      overflow: "auto",
                      background: "#f8fafc",
                      border: "1px solid #e2e8f0",
                      borderRadius: 8,
                      padding: 10,
                      fontSize: 12,
                    }}
                  >
{JSON.stringify(annotationJson, null, 2)}
                  </pre>
                </div>
              ) : null}
            </div>
          )}

          {screenEditorData?.classified_at ? (
            <p className="muted" style={{ marginTop: 10, marginBottom: 0 }}>
              Schema last classified at: {formatDate(screenEditorData.classified_at)}
            </p>
          ) : null}
        </div>
      </section>



      {GROUPS.map((group) => {
        const groupFields = FIELD_META.filter((f) => f.group === group);
        if (!groupFields.length) return null;

        return (
          <section key={group} className="card settings-group">
            <h2>{GROUP_LABELS[group]}</h2>
            <div className="settings-fields">{groupFields.map(renderField)}</div>
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
