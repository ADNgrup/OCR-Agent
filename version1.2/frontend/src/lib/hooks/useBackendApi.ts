import { useMemo } from "react";

import {
  DEFAULT_BACKEND,
  deleteFetch,
  fetchJSON,
  normalizeBaseUrl,
  patchFetch,
  postJSON,
  putJSON,
} from "@/lib/api";
import { API_ENDPOINTS } from "@/lib/endpoints";

type JsonBody = Record<string, unknown>;

interface ScreenLogsOptions {
  hours?: number;
  limit?: number;
  entityIds?: string[];
}

interface ScreenTimeseriesOptions {
  hours?: number;
  entityIds?: string[];
}

interface SnapshotsListOptions {
  limit?: number;
  skip?: number;
  sourceId?: string;
}

async function patchJSON<T = unknown>(
  baseUrl: string,
  path: string,
  body: JsonBody = {},
): Promise<T> {
  const response = await fetch(`${normalizeBaseUrl(baseUrl)}${path}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
    cache: "no-store",
  });

  if (!response.ok) {
    const payload = await response.json().catch(() => null);
    throw new Error(payload?.detail || `${path} failed (${response.status})`);
  }

  return response.json() as Promise<T>;
}

async function postForm<T = unknown>(
  baseUrl: string,
  path: string,
  formData: FormData,
): Promise<T> {
  const response = await fetch(`${normalizeBaseUrl(baseUrl)}${path}`, {
    method: "POST",
    body: formData,
    cache: "no-store",
  });

  const payload = await response.json().catch(() => null);
  if (!response.ok) {
    throw new Error(payload?.detail || `${path} failed (${response.status})`);
  }

  return payload as T;
}

export function useBackendApi(backendUrl?: string) {
  const base = useMemo(
    () => normalizeBaseUrl(backendUrl || DEFAULT_BACKEND),
    [backendUrl],
  );

  return useMemo(
    () => ({
      base,
      resolveImageUrl(imageUrl: string | undefined | null): string | null {
        if (!imageUrl) return null;
        if (imageUrl.startsWith("data:")) return imageUrl;
        if (/^https?:\/\//i.test(imageUrl)) return imageUrl;
        if (imageUrl.startsWith("/")) return `${base}${imageUrl}`;
        return `${base}/${imageUrl}`;
      },

      getQueueStats<T = unknown>() {
        return fetchJSON<T>(base, API_ENDPOINTS.QUEUE.STATS);
      },

      getConfig<T = unknown>() {
        return fetchJSON<T>(base, API_ENDPOINTS.CONFIG.GET);
      },

      updateConfig<T = unknown>(payload: JsonBody) {
        return putJSON<T>(base, API_ENDPOINTS.CONFIG.UPDATE, payload);
      },

      resetConfig<T = unknown>() {
        return postJSON<T>(base, API_ENDPOINTS.CONFIG.RESET);
      },

      listSources<T = unknown>() {
        return fetchJSON<T>(base, API_ENDPOINTS.KVM_SOURCES.LIST);
      },

      createSource<T = unknown>(payload: JsonBody) {
        return postJSON<T>(base, API_ENDPOINTS.KVM_SOURCES.CREATE, payload);
      },

      updateSource<T = unknown>(sourceId: string, payload: JsonBody) {
        return putJSON<T>(base, API_ENDPOINTS.KVM_SOURCES.UPDATE(sourceId), payload);
      },

      toggleSource<T = unknown>(sourceId: string, enabled: boolean) {
        return patchFetch<T>(base, API_ENDPOINTS.KVM_SOURCES.TOGGLE(sourceId, enabled));
      },

      deleteSource<T = unknown>(sourceId: string) {
        return deleteFetch<T>(base, API_ENDPOINTS.KVM_SOURCES.DELETE(sourceId));
      },

      runSourceOnce<T = unknown>(sourceId: string) {
        return postJSON<T>(base, API_ENDPOINTS.KVM_SOURCES.RUN_ONCE(sourceId));
      },

      listScreens<T = unknown>(sourceId: string) {
        return fetchJSON<T>(base, API_ENDPOINTS.SCREENS.LIST_BY_SOURCE(sourceId));
      },

      getScreenPreview<T = unknown>(screenGroupId: string) {
        return fetchJSON<T>(base, API_ENDPOINTS.SCREENS.PREVIEW(screenGroupId));
      },

      getScreenSchemaEditor<T = unknown>(screenGroupId: string) {
        return fetchJSON<T>(base, API_ENDPOINTS.SCREENS.SCHEMA_EDITOR(screenGroupId));
      },

      saveScreenSchemaSegments<T = unknown>(screenGroupId: string, payload: JsonBody) {
        return putJSON<T>(base, API_ENDPOINTS.SCREENS.SCHEMA_SEGMENTS(screenGroupId), payload);
      },

      toggleScreenIgnore<T = unknown>(screenGroupId: string, ignored: boolean) {
        return postJSON<T>(base, API_ENDPOINTS.SCREENS.TOGGLE_IGNORE(screenGroupId), {
          ignored,
        });
      },

      updateScreenSource<T = unknown>(screenGroupId: string, sourceId: string) {
        return patchJSON<T>(base, API_ENDPOINTS.SCREENS.UPDATE_SOURCE(screenGroupId), {
          source_id: sourceId,
        });
      },

      deleteScreen<T = unknown>(screenGroupId: string) {
        return deleteFetch<T>(base, API_ENDPOINTS.SCREENS.DELETE(screenGroupId));
      },

      uploadScreenSample<T = unknown>(formData: FormData) {
        return postForm<T>(base, API_ENDPOINTS.SCREENS.SAMPLES_UPLOAD, formData);
      },

      listEntities<T = unknown>(screenGroupId: string) {
        return fetchJSON<T>(base, API_ENDPOINTS.ENTITIES.BY_SCREEN(screenGroupId));
      },

      listLogs<T = unknown>(screenGroupId: string, options: ScreenLogsOptions = {}) {
        return fetchJSON<T>(base, API_ENDPOINTS.LOGS.BY_SCREEN(screenGroupId, options));
      },

      getTimeseries<T = unknown>(
        screenGroupId: string,
        options: ScreenTimeseriesOptions = {},
      ) {
        return fetchJSON<T>(base, API_ENDPOINTS.TIMESERIES.BY_SCREEN(screenGroupId, options));
      },

      listSnapshots<T = unknown>(options: SnapshotsListOptions = {}) {
        return fetchJSON<T>(base, API_ENDPOINTS.SNAPSHOTS.LIST_PAGED(options));
      },

      updateSnapshotEvaluation<T = unknown>(snapshotId: string, evaluation: string) {
        return putJSON<T>(base, API_ENDPOINTS.SNAPSHOTS.EVALUATION(snapshotId), {
          evaluation,
        });
      },
    }),
    [base],
  );
}
