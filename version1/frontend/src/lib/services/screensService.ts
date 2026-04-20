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
    get(`${API_ENDPOINTS.SCREENS.LIST}?source_id=${sourceId}`),

  preview: (screenGroupId: string) =>
    get(API_ENDPOINTS.SCREENS.PREVIEW(screenGroupId)),

  entities: (screenGroupId: string) =>
    get(`${API_ENDPOINTS.ENTITIES.LIST}?screen_group_id=${screenGroupId}`),

  logs: (
    screenGroupId: string,
    { hours = 24, entityIds, limit = 500 }: LogsOptions = {},
  ) => {
    let path = `${API_ENDPOINTS.LOGS.LIST}?screen_group_id=${screenGroupId}&hours=${hours}&limit=${limit}`;
    if (entityIds?.length) path += `&entity_ids=${entityIds.join(",")}`;
    return get(path);
  },

  timeseries: (
    screenGroupId: string,
    { hours = 24, entityIds }: TimeseriesOptions = {},
  ) => {
    let path = `${API_ENDPOINTS.TIMESERIES.GET}?screen_group_id=${screenGroupId}&hours=${hours}`;
    if (entityIds?.length) path += `&entity_ids=${entityIds.join(",")}`;
    return get(path);
  },

  latestSnapshots: (sourceId: string, limit = 20) =>
    get(
      `${API_ENDPOINTS.SNAPSHOTS.LATEST}?source_id=${sourceId}&limit=${limit}`,
    ),
};
