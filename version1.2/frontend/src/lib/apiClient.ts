/**
 * API client — thin wrapper around fetch.
 * All service modules use this instead of raw fetch.
 */

export const DEFAULT_BACKEND =
  process.env.NEXT_PUBLIC_BACKEND_URL || "http://localhost:8000";

function normalizeBaseUrl(value?: string): string {
  return (value || DEFAULT_BACKEND).trim().replace(/\/$/, "");
}

let _base = normalizeBaseUrl(DEFAULT_BACKEND);

/** Update the base URL at runtime (e.g. from Settings UI). */
export function setBaseUrl(url: string): void {
  _base = normalizeBaseUrl(url);
}

export function getBaseUrl(): string {
  return _base;
}

async function handleResponse<T = unknown>(res: Response): Promise<T> {
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(text || `Request failed (${res.status})`);
  }
  return res.json() as Promise<T>;
}

export async function get<T = unknown>(path: string): Promise<T> {
  const res = await fetch(`${_base}${path}`, { cache: "no-store" });
  return handleResponse<T>(res);
}

export async function post<T = unknown>(
  path: string,
  body: Record<string, unknown> = {},
): Promise<T> {
  const res = await fetch(`${_base}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
    cache: "no-store",
  });
  return handleResponse<T>(res);
}

export async function put<T = unknown>(
  path: string,
  body: Record<string, unknown> = {},
): Promise<T> {
  const res = await fetch(`${_base}${path}`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
    cache: "no-store",
  });
  return handleResponse<T>(res);
}

export async function patch<T = unknown>(path: string): Promise<T> {
  const res = await fetch(`${_base}${path}`, {
    method: "PATCH",
    cache: "no-store",
  });
  return handleResponse<T>(res);
}

export async function del<T = unknown>(path: string): Promise<T> {
  const res = await fetch(`${_base}${path}`, {
    method: "DELETE",
    cache: "no-store",
  });
  return handleResponse<T>(res);
}

/** Build a full URL for image src attributes. */
export function imageUrl(path: string | null | undefined): string | null {
  if (!path) return null;
  return `${_base}${path}`;
}
