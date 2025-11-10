// frontend/src/lib/api.ts
export type SummaryKpis = {
  total_ops: number | null;
  porto_top: string | null;
  porto_low: string | null;
  atrasos_coleta: number | null;
  atrasos_entrega: number | null;
  reagendamentos: number | null;
};

/**
 * Resolve a URL base da API de forma inteligente
 * Prioridade:
 * 1) window.__ENV__.API_BASE_URL (runtime, sem rebuild)
 * 2) VITE_API_BASE
 * 3) VITE_API_BASE_URL
 * 4) localhost (dev)
 * 5) fallback Koyeb (prod)
 */
function resolveApiBase(): string {
  const runtime = (window as any)?.__ENV__?.API_BASE_URL;
  if (runtime && String(runtime).trim()) {
    return String(runtime).trim().replace(/\/+$/, "");
  }

  const fromEnv = (import.meta as any)?.env?.VITE_API_BASE;
  if (fromEnv && String(fromEnv).trim()) {
    return String(fromEnv).trim().replace(/\/+$/, "");
  }

  const fromEnv2 = (import.meta as any)?.env?.VITE_API_BASE_URL;
  if (fromEnv2 && String(fromEnv2).trim()) {
    return String(fromEnv2).trim().replace(/\/+$/, "");
  }

  // fallback só para dev local
  if (typeof window !== "undefined") {
    const { hostname, port } = window.location;
    const isDev =
      port === "5173" || port === "5174" || port === "3000" || hostname === "localhost";
    if (isDev) return "http://127.0.0.1:8000";
  }

  // produção (Fly.io - Diário Operacional)
  return "https://diario-bordo-api-kaue.fly.dev";
}

const API_BASE = resolveApiBase();
const DEFAULT_TIMEOUT = Number((import.meta as any)?.env?.VITE_API_TIMEOUT_MS ?? 120000);

console.log(`[API Client] Base URL: ${API_BASE}`);

/** Monta URL com segurança (sem // duplicado) */
function buildUrl(path: string, query?: Record<string, string | number | boolean>) {
  const clean = String(path || "").replace(/^\/+/, "");
  const u = new URL(clean, API_BASE + "/");
  if (query) {
    for (const [k, v] of Object.entries(query)) {
      if (v !== undefined && v !== null) u.searchParams.set(k, String(v));
    }
  }
  return u.toString();
}

/**
 * Helper para verificar resposta HTTP e lançar erro descritivo
 */
async function ensureOk(res: Response): Promise<void> {
  if (!res.ok) {
    let errorMessage = `HTTP ${res.status} - ${res.statusText}`;
    try {
      const ct = res.headers.get("content-type") || "";
      if (ct.includes("application/json")) {
        const errorData = await res.json().catch(() => null);
        if (errorData?.detail) errorMessage += `: ${errorData.detail}`;
        else if (errorData?.error) errorMessage += `: ${errorData.error}`;
      } else {
        const txt = await res.text().catch(() => "");
        if (txt) errorMessage += `: ${txt.slice(0, 300)}`;
      }
    } catch {}
    throw new Error(errorMessage);
  }
}

/**
 * Wrapper fetch com timeout
 */
async function fetchWithTimeout(
  url: string,
  options: RequestInit = {},
  timeout: number = DEFAULT_TIMEOUT
): Promise<Response> {
  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(new Error(`timeout:${timeout}`)), timeout);
  try {
    const res = await fetch(url, {
      cache: "no-store",
      ...options,
      signal: controller.signal,
    });
    clearTimeout(id);
    return res;
  } catch (error: any) {
    clearTimeout(id);
    if (error?.name === "AbortError" || String(error?.message || "").startsWith("timeout:")) {
      throw new Error("Request timeout - servidor demorou muito para responder");
    }
    throw error instanceof Error ? error : new Error("Erro desconhecido na requisição");
  }
}

/**
 * Upload de arquivos para processamento
 */
export async function uploadFiles(
  client: string,
  bookingFile: File,
  multiFile: File,
  transpFile: File
): Promise<{
  periods: string[];
  embarcadores: string[];
  inserted?: any[];
  skipped?: any[];
}> {
  const form = new FormData();
  form.append("client", client);
  form.append("booking", bookingFile);
  form.append("multimodal", multiFile);
  form.append("transportes", transpFile);

  const res = await fetchWithTimeout(
    buildUrl("/api/upload"),
    { method: "POST", body: form },
    120000 // 2 min
  );
  await ensureOk(res);
  return res.json();
}

/**
 * Buscar resumo/KPIs por período e embarcadores
 */
export async function getSummaryBy(
  client: string,
  yms: string[],
  embarcadores: string[]
): Promise<{ kpis: SummaryKpis; debug: any }> {
  const res = await fetchWithTimeout(
    buildUrl("/api/summary", {
      client,
      ym: yms.join(","),
      embarcador: embarcadores.join(","),
    }),
    {},
    120000
  );
  await ensureOk(res);
  return res.json();
}

/**
 * Gerar email com análise de IA
 */
export async function generateEmailBy(payload: {
  client: string;
  yms: string[];
  embarcadores: string[];
}): Promise<{ email: string; email_html: string }> {
  const res = await fetchWithTimeout(
    buildUrl("/api/generate-email"),
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    },
    90000
  );
  await ensureOk(res);
  return res.json();
}

/**
 * Gerar arquivo .EML para download
 */
export async function generateEmlBy(payload: {
  client: string;
  yms: string[];
  embarcadores: string[];
}): Promise<{ filename: string; file_b64: string }> {
  const res = await fetchWithTimeout(
    buildUrl("/api/generate-eml-by"),
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    },
    90000
  );
  await ensureOk(res);
  return res.json();
}

/**
 * Buscar dados disponíveis no banco (períodos e embarcadores)
 * Para auto-carregar ao abrir a aplicação
 */
export async function getAvailableData(
  client: string
): Promise<{
  has_data: boolean;
  periods: string[];
  embarcadores: string[];
  error?: string;
}> {
  const res = await fetchWithTimeout(
    buildUrl("/api/available-data", { client }),
    {},
    60000 // ↑ 60s (estava curto e estava estourando)
  );
  await ensureOk(res);
  return res.json();
}

/**
 * Limpar banco de dados (flush)
 */
export async function clearDatabase(
  client: string,
  ym?: string
): Promise<{ status: string; deleted: number; detail: any }> {
  const res = await fetchWithTimeout(
    buildUrl("/api/flush", { client, ...(ym ? { ym } : {}) }),
    { method: "DELETE" }
  );
  await ensureOk(res);
  return res.json();
}

/**
 * Health check - verificar se API está online
 */
export async function healthCheck(): Promise<{ ok: boolean; cache_size?: number }> {
  try {
    const res = await fetchWithTimeout(buildUrl("/api/health"), {}, 5000);
    if (res.ok) return res.json();
    return { ok: false };
  } catch {
    return { ok: false };
  }
}