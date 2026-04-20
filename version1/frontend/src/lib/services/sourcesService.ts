import { get, post, put, patch, del } from "@/lib/apiClient";
import { API_ENDPOINTS } from "@/lib/endpoints";

export const sourcesService = {
  list: () => get(API_ENDPOINTS.KVM_SOURCES.LIST),

  create: (body: Record<string, unknown>) =>
    post(API_ENDPOINTS.KVM_SOURCES.CREATE, body),

  update: (id: string, body: Record<string, unknown>) =>
    put(API_ENDPOINTS.KVM_SOURCES.UPDATE(id), body),

  remove: (id: string) => del(API_ENDPOINTS.KVM_SOURCES.DELETE(id)),

  toggle: (id: string, enabled: boolean) =>
    patch(API_ENDPOINTS.KVM_SOURCES.TOGGLE(id, enabled)),

  runOnce: (id: string) => post(API_ENDPOINTS.KVM_SOURCES.RUN_ONCE(id)),
};
