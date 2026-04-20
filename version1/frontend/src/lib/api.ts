const DEFAULT_BACKEND =
  process.env.NEXT_PUBLIC_BACKEND_URL || "http://localhost:8000";

export function normalizeBaseUrl(value?: string): string {
  const raw = (value || DEFAULT_BACKEND).trim();
  return raw.replace(/\/$/, "");
}

export async function fetchJSON<T = unknown>(
  baseUrl: string,
  path: string,
): Promise<T> {
  const response = await fetch(`${normalizeBaseUrl(baseUrl)}${path}`, {
    cache: "no-store",
  });
  if (!response.ok) {
    throw new Error(`${path} failed (${response.status})`);
  }
  return response.json() as Promise<T>;
}

export async function postJSON<T = unknown>(
  baseUrl: string,
  path: string,
  body: Record<string, unknown> = {},
): Promise<T> {
  const response = await fetch(`${normalizeBaseUrl(baseUrl)}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
    cache: "no-store",
  });
  if (!response.ok) throw new Error(`${path} failed (${response.status})`);
  return response.json() as Promise<T>;
}

export async function putJSON<T = unknown>(
  baseUrl: string,
  path: string,
  body: Record<string, unknown> = {},
): Promise<T> {
  const response = await fetch(`${normalizeBaseUrl(baseUrl)}${path}`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
    cache: "no-store",
  });
  if (!response.ok) throw new Error(`${path} failed (${response.status})`);
  return response.json() as Promise<T>;
}

export async function patchFetch<T = unknown>(
  baseUrl: string,
  path: string,
): Promise<T> {
  const response = await fetch(`${normalizeBaseUrl(baseUrl)}${path}`, {
    method: "PATCH",
    cache: "no-store",
  });
  if (!response.ok) throw new Error(`${path} failed (${response.status})`);
  return response.json() as Promise<T>;
}

export async function deleteFetch<T = unknown>(
  baseUrl: string,
  path: string,
): Promise<T> {
  const response = await fetch(`${normalizeBaseUrl(baseUrl)}${path}`, {
    method: "DELETE",
    cache: "no-store",
  });
  if (!response.ok) throw new Error(`${path} failed (${response.status})`);
  return response.json() as Promise<T>;
}

export { DEFAULT_BACKEND };
