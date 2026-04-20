import { get, put, post } from "@/lib/apiClient";
import { API_ENDPOINTS } from "@/lib/endpoints";

export const configService = {
  get: () => get(API_ENDPOINTS.CONFIG.GET),

  update: (payload: Record<string, unknown>) =>
    put(API_ENDPOINTS.CONFIG.UPDATE, payload),

  reset: () => post(API_ENDPOINTS.CONFIG.RESET),
};
