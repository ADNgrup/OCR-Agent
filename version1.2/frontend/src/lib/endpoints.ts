/**
 * All backend API endpoint definitions.
 * Static endpoints as strings, dynamic endpoints as functions.
 */
type QueryValue = string | number | boolean | null | undefined;

function withQuery(path: string, query: Record<string, QueryValue>): string {
  const params = new URLSearchParams();
  for (const [key, value] of Object.entries(query)) {
    if (value === undefined || value === null || value === "") continue;
    params.set(key, String(value));
  }
  const suffix = params.toString();
  return suffix ? `${path}?${suffix}` : path;
}

const API_V2 = "/api/v2";

export const API_ENDPOINTS = {
  HEALTH: "/health",

  KVM_SOURCES: {
    LIST: `${API_V2}/kvm-sources`,
    CREATE: `${API_V2}/kvm-sources`,
    UPDATE: (id: string) => `${API_V2}/kvm-sources/${id}`,
    DELETE: (id: string) => `${API_V2}/kvm-sources/${id}`,
    TOGGLE: (id: string, enabled: boolean) =>
      withQuery(`${API_V2}/kvm-sources/${id}/toggle`, { enabled }),
    RUN_ONCE: (id: string) => `${API_V2}/kvm-sources/${id}/run-once`,
  },

  SCREENS: {
    LIST: `${API_V2}/screens`,
    LIST_BY_SOURCE: (sourceId: string) =>
      withQuery(`${API_V2}/screens`, { source_id: sourceId }),
    PREVIEW: (screenGroupId: string) => `${API_V2}/screens/${screenGroupId}/preview`,
    TOGGLE_IGNORE: (screenGroupId: string) =>
      `${API_V2}/screens/${screenGroupId}/toggle-ignore`,
    UPDATE_SOURCE: (screenGroupId: string) =>
      `${API_V2}/screens/${screenGroupId}/source`,
    DELETE: (screenGroupId: string) => `${API_V2}/screens/${screenGroupId}`,
    SCHEMA_EDITOR: (screenGroupId: string) =>
      `${API_V2}/screens/${screenGroupId}/schema-editor`,
    SCHEMA_SEGMENTS: (screenGroupId: string) =>
      `${API_V2}/screens/${screenGroupId}/schema-segments`,
    SAMPLES_UPLOAD: `${API_V2}/screens/samples/upload`,
  },

  ENTITIES: {
    LIST: `${API_V2}/entities`,
    BY_SCREEN: (screenGroupId: string) =>
      withQuery(`${API_V2}/entities`, { screen_group_id: screenGroupId }),
  },

  LOGS: {
    LIST: `${API_V2}/logs`,
    BY_SCREEN: (
      screenGroupId: string,
      options?: { hours?: number; limit?: number; entityIds?: string[] },
    ) =>
      withQuery(`${API_V2}/logs`, {
        screen_group_id: screenGroupId,
        hours: options?.hours,
        limit: options?.limit,
        entity_ids: options?.entityIds?.length
          ? options.entityIds.join(",")
          : undefined,
      }),
  },

  TIMESERIES: {
    GET: `${API_V2}/timeseries`,
    BY_SCREEN: (
      screenGroupId: string,
      options?: { hours?: number; entityIds?: string[] },
    ) =>
      withQuery(`${API_V2}/timeseries`, {
        screen_group_id: screenGroupId,
        hours: options?.hours,
        entity_ids: options?.entityIds?.length
          ? options.entityIds.join(",")
          : undefined,
      }),
  },

  SNAPSHOTS: {
    LIST: `${API_V2}/snapshots`,
    LIST_PAGED: (options: { limit?: number; skip?: number; sourceId?: string }) =>
      withQuery(`${API_V2}/snapshots`, {
        limit: options.limit,
        skip: options.skip,
        source_id: options.sourceId,
      }),
    LATEST: `${API_V2}/snapshots/latest`,
    LATEST_BY_SOURCE: (sourceId: string, limit?: number) =>
      withQuery(`${API_V2}/snapshots/latest`, {
        source_id: sourceId,
        limit,
      }),
    IMAGE: (snapshotId: string) => `${API_V2}/snapshots/${snapshotId}/image`,
    EVALUATION: (snapshotId: string) => `${API_V2}/snapshots/${snapshotId}/evaluation`,
  },

  QUEUE: {
    STATS: `${API_V2}/queue`,
  },

  CONFIG: {
    GET: `${API_V2}/config`,
    UPDATE: `${API_V2}/config`,
    RESET: `${API_V2}/config/reset`,
  },

  BACKFILL: `${API_V2}/backfill`,
} as const;
