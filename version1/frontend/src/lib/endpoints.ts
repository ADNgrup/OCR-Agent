/**
 * All backend API endpoint definitions.
 * Static endpoints as strings, dynamic endpoints as functions.
 */
export const API_ENDPOINTS = {
  HEALTH: "/health",

  KVM_SOURCES: {
    LIST: "/api/kvm-sources",
    CREATE: "/api/kvm-sources",
    UPDATE: (id: string) => `/api/kvm-sources/${id}`,
    DELETE: (id: string) => `/api/kvm-sources/${id}`,
    TOGGLE: (id: string, enabled: boolean) =>
      `/api/kvm-sources/${id}/toggle?enabled=${enabled}`,
    RUN_ONCE: (id: string) => `/api/kvm-sources/${id}/run-once`,
  },

  SCREENS: {
    LIST: "/api/screens",
    PREVIEW: (screenGroupId: string) => `/api/screens/${screenGroupId}/preview`,
  },

  ENTITIES: {
    LIST: "/api/entities",
  },

  LOGS: {
    LIST: "/api/logs",
  },

  TIMESERIES: {
    GET: "/api/timeseries",
  },

  SNAPSHOTS: {
    LATEST: "/api/snapshots/latest",
    IMAGE: (snapshotId: string) => `/api/snapshots/${snapshotId}/image`,
  },

  QUEUE: {
    STATS: "/api/queue",
  },

  CONFIG: {
    GET: "/api/config",
    UPDATE: "/api/config",
    RESET: "/api/config/reset",
  },

  BACKFILL: "/api/backfill",
} as const;
