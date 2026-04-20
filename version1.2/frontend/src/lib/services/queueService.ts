import { get } from "@/lib/apiClient";
import { API_ENDPOINTS } from "@/lib/endpoints";

export const queueService = {
  stats: () => get(API_ENDPOINTS.QUEUE.STATS),
};
