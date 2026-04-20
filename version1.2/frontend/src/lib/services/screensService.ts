import { get } from "@/lib/apiClient";
import { API_ENDPOINTS } from "@/lib/endpoints";

interface LogsOptions {
  hours?: number;
  entityIds?: string[];
  limit?: number;
}

interface TimeseriesOptions {
  hours?: number;
  entityIds?: string[];
}

export const screensService = {
  list: (sourceId: string) =>
    get(API_ENDPOINTS.SCREENS.LIST_BY_SOURCE(sourceId)),

  preview: (screenGroupId: string) =>
    get(API_ENDPOINTS.SCREENS.PREVIEW(screenGroupId)),

  entities: (screenGroupId: string) =>
    get(API_ENDPOINTS.ENTITIES.BY_SCREEN(screenGroupId)),

  logs: (
    screenGroupId: string,
    { hours = 24, entityIds, limit = 500 }: LogsOptions = {},
  ) =>
    get(
      API_ENDPOINTS.LOGS.BY_SCREEN(screenGroupId, {
        hours,
        limit,
        entityIds,
      }),
    ),

  timeseries: (
    screenGroupId: string,
    { hours = 24, entityIds }: TimeseriesOptions = {},
  ) =>
    get(
      API_ENDPOINTS.TIMESERIES.BY_SCREEN(screenGroupId, {
        hours,
        entityIds,
      }),
    ),

  latestSnapshots: (sourceId: string, limit = 20) =>
    get(API_ENDPOINTS.SNAPSHOTS.LATEST_BY_SOURCE(sourceId, limit)),
};
