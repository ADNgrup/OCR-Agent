"use client";

import { useCallback, useEffect, useMemo, useState } from "react";

import SceneAndSchemaSection from "@/components/dashboard/SceneAndSchemaSection";
import ScreenLibrarySection from "@/components/dashboard/ScreenLibrarySection";
import TimeseriesLogsSection from "@/components/dashboard/TimeseriesLogsSection";
import TopOverviewSection from "@/components/dashboard/TopOverviewSection";
import { getScreenSchemaStatus } from "@/components/dashboard/dashboardUtils";
import { DEFAULT_BACKEND } from "@/lib/api";
import { useBackendApi } from "@/lib/hooks/useBackendApi";
import {
  Entity,
  KvmSource,
  LogEntry,
  Preview,
  QueueStats,
  Screen as DashboardScreen,
} from "@/types/dashboard";

/* eslint-disable @typescript-eslint/no-explicit-any */

export default function DashboardPage() {
  const [backendUrl, setBackendUrl] = useState(DEFAULT_BACKEND);
  const api = useBackendApi(backendUrl);
  const [sources, setSources] = useState<KvmSource[]>([]);
  const [screens, setScreens] = useState<DashboardScreen[]>([]);
  const [entities, setEntities] = useState<Entity[]>([]);
  const [selectedEntityIds, setSelectedEntityIds] = useState<string[]>([]);
  const [logs, setLogs] = useState<LogEntry[]>([]);
  const [series, setSeries] = useState<Record<string, any>>({});
  const [preview, setPreview] = useState<Preview | null>(null);

  const [sourceId, setSourceId] = useState("");
  const [screenId, setScreenId] = useState("");
  const [hours, setHours] = useState(24);

  const [loading, setLoading] = useState(false);
  const [status, setStatus] = useState("Ready");
  const [error, setError] = useState("");

  const [queueStats, setQueueStats] = useState<QueueStats>({
    pending: 0,
    processing: 0,
    completed: 0,
    failed: 0,
  });

  const selectedScreen = useMemo(
    () => screens.find((s) => s.id === screenId),
    [screens, screenId],
  );
  const importedScreens = useMemo(
    () => screens.filter((screen) => getScreenSchemaStatus(screen) === "classified"),
    [screens],
  );
  const queuedScreens = useMemo(
    () => screens.filter((screen) => getScreenSchemaStatus(screen) !== "classified"),
    [screens],
  );

  useEffect(() => {
    setSelectedEntityIds((prev) =>
      prev.filter((id) => entities.some((ent) => ent.id === id)),
    );
  }, [entities]);

  async function loadQueue() {
    try {
      setQueueStats(await api.getQueueStats<QueueStats>());
    } catch {
      // silent
    }
  }

  async function refreshSourcesQuiet() {
    try {
      setSources(await api.listSources<KvmSource[]>());
    } catch {
      // silent
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
  }, [api]);

  const loadEntities = useCallback(
    async (gid: string) => {
      if (!gid) {
        setEntities([]);
        return [];
      }
      try {
        const data = await api.listEntities<Entity[]>(gid);
        setEntities(data);
        return data;
      } catch {
        setEntities([]);
        return [];
      }
    },
    [api],
  );

  const loadPreview = useCallback(
    async (gid: string) => {
      if (!gid) {
        setPreview(null);
        return;
      }
      try {
        const data = await api.getScreenPreview<Preview>(gid);
        setPreview(data);
      } catch {
        setPreview(null);
      }
    },
    [api],
  );

  const loadData = useCallback(
    async (gid: string, rangeHours: number, eids: string[]) => {
      if (!gid) return;
      const safeHours = Math.max(1, Math.min(168, Number(rangeHours) || 24));
      const [logsData, seriesData] = await Promise.all([
        api.listLogs<LogEntry[]>(gid, {
          hours: safeHours,
          limit: 500,
          entityIds: eids,
        }),
        api.getTimeseries<Record<string, any>>(gid, {
          hours: safeHours,
          entityIds: eids,
        }),
      ]);
      setLogs(logsData);
      setSeries(seriesData);
    },
    [api],
  );

  async function onScreenSelected(gid: string) {
    setSelectedEntityIds([]);
    await Promise.all([loadEntities(gid), loadPreview(gid)]);
    await loadData(gid, hours, []);
  }

  async function loadScreens(sid: string) {
    if (!sid) return;
    const data = await api.listScreens<DashboardScreen[]>(sid);
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
    const nextScreenId = data.some((s) => s.id === screenId) ? screenId : data[0].id;
    setScreenId(nextScreenId);
    await onScreenSelected(nextScreenId);
  }

  async function loadSources() {
    const data = await api.listSources<KvmSource[]>();
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
    const nextSourceId = data.some((s) => s.id === sourceId) ? sourceId : data[0].id;
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

  async function toggleScreenIgnore(sid: string, ignored: boolean) {
    if (!sid) return;
    setStatus(ignored ? "Ignoring screen..." : "Unignoring screen...");
    try {
      await api.toggleScreenIgnore(sid, ignored);
      setScreens((prev) => prev.map((s) => (s.id === sid ? { ...s, ignored } : s)));
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

  async function toggleSource(id: string, enabled: boolean) {
    try {
      await api.toggleSource(id, enabled);
      await loadSources();
    } catch (err: any) {
      setError(err.message || "Toggle failed.");
    }
  }

  async function runOnce(id: string) {
    try {
      setStatus("Running one-time snapshot...");
      await api.runSourceOnce(id);
      setStatus("Snapshot queued.");
      loadQueue();
    } catch (err: any) {
      setError(err.message || "Run-once failed.");
    }
  }

  function snapshotImgUrl(imageUrl: string | undefined): string | null {
    return api.resolveImageUrl(imageUrl);
  }

  return (
    <main className="page">
      <TopOverviewSection
        backendUrl={backendUrl}
        setBackendUrl={setBackendUrl}
        sources={sources}
        screens={screens}
        queueStats={queueStats}
        sourceId={sourceId}
        screenId={screenId}
        hours={hours}
        loading={loading}
        selectedScreen={selectedScreen}
        onApplyBackend={onApplyBackend}
        onSourceChange={onSourceChange}
        onScreenChange={onScreenChange}
        onRefreshData={onRefreshData}
        toggleSource={toggleSource}
        runOnce={runOnce}
        toggleScreenIgnore={toggleScreenIgnore}
        setHours={setHours}
      />

      <SceneAndSchemaSection
        preview={preview}
        entities={entities}
        selectedEntityIds={selectedEntityIds}
        loading={loading}
        snapshotImgUrl={snapshotImgUrl}
        onToggleEntitySelection={toggleEntitySelection}
        onSelectAllEntities={selectAllEntities}
        onClearEntitySelection={clearEntitySelection}
        onMonitorSelected={onMonitorSelected}
      />

      <ScreenLibrarySection
        importedScreens={importedScreens}
        queuedScreens={queuedScreens}
        loading={loading}
        onOpenScreen={onScreenChange}
        onToggleScreenIgnore={toggleScreenIgnore}
      />

      <TimeseriesLogsSection series={series} logs={logs} />
    </main>
  );
}
