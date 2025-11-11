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
 * Resolve a URL base da API
 * CONFIGURAÇÃO PARA DESENVOLVIMENTO LOCAL:
 * - Se estiver rodando localmente (localhost/127.0.0.1), usa backend local
 * - Se estiver em produção (Netlify), usa variável de ambiente ou fallback
 */
function resolveApiBase(): string {
  // 1. Verificar variável de ambiente (prioritário)
  const fromEnv = (import.meta as any)?.env?.VITE_API_BASE;
  if (fromEnv && String(fromEnv).trim()) {
    return String(fromEnv).trim().replace(/\/$/, "");
  }

  // 2. Detectar ambiente
  if (typeof window !== "undefined") {
    const { hostname, port } = window.location;
    
    // Se estiver rodando em localhost/127.0.0.1, usar backend local
    const isLocalDev =
      hostname === "localhost" ||
      hostname === "127.0.0.1" ||
      port === "5173" ||
      port === "5174" ||
      port === "3000";

    if (isLocalDev) {
      console.log("[API Client] Modo de desenvolvimento local detectado");
      return "http://127.0.0.1:8000"; // Backend local
    }
  }

  // 3. Produção (Netlify) - usar variável de ambiente ou configuração
  console.log("[API Client] Modo de produção detectado");
  return "https://devisable-fissirostral-rylee.ngrok-free.dev"; // ⚠️ SUBSTITUA pela URL do seu backend em produção
}

const API_BASE = resolveApiBase();
console.log(`[API Client] Base URL configurada: ${API_BASE}`);

/** Monta URL com query params */
function buildUrl(path: string, params?: Record<string, string | number | boolean | undefined>) {
  const base = API_BASE.replace(/\/+$/, "");
  const p = path.startsWith("/") ? path : `/${path}`;
  const url = new URL(base + p);
  if (params) {
    Object.entries(params).forEach(([k, v]) => {
      if (v === undefined || v === null) return;
      url.searchParams.set(k, String(v));
    });
  }
  return url.toString();
}

/** Verifica resposta HTTP */
async function ensureOk(res: Response): Promise<void> {
  if (!res.ok) {
    let msg = `HTTP ${res.status} - ${res.statusText}`;
    try {
      const data = await res.json();
      if ((data as any)?.detail) msg += `: ${(data as any).detail}`;
    } catch {}
    throw new Error(msg);
  }

  const ct = res.headers.get('content-type') || '';
  if (!ct.includes('application/json')) {
    throw new Error(`Resposta inesperada do servidor (Content-Type=${ct}). Possível aviso do ngrok.`);
  }
}


/** fetch com timeout */
async function fetchWithTimeout(
  url: string,
  options: RequestInit = {},
  timeout = 60000
): Promise<Response> {
  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), timeout);
  try {
    const headers = new Headers((options && (options as any).headers) || {});
    if (!headers.has("ngrok-skip-browser-warning")) headers.set("ngrok-skip-browser-warning", "true");
    const res = await fetch(url, { ...options, headers, signal: controller.signal });
    clearTimeout(id);
    return res;
  } catch (err: any) {
    clearTimeout(id);
    if (err?.name === "AbortError") {
      throw new Error("Request timeout - servidor demorou muito para responder");
    }
    throw err instanceof Error ? err : new Error("Erro desconhecido na requisição");
  }
}

/** Upload de arquivos */
export async function uploadFiles(
  client: string,
  bookingFile: File,
  multiFile: File,
  transpFile: File
): Promise<{ periods: string[]; embarcadores: string[]; inserted?: any[]; skipped?: any[] }> {
  const form = new FormData();
  form.append("client", client);
  form.append("booking", bookingFile);
  form.append("multimodal", multiFile);
  form.append("transportes", transpFile);

  const res = await fetchWithTimeout(buildUrl("/api/upload"), { method: "POST", body: form }, 120000);
  await ensureOk(res);
  return res.json();
}

/** Buscar resumo/KPIs */
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

/** Gerar email com IA */
export async function generateEmailBy(payload: {
  client: string;
  yms: string[];
  embarcadores: string[];
}): Promise<{ email: string; email_html: string }> {
  const res = await fetchWithTimeout(
    buildUrl("/api/generate-email"),
    { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload) },
    90000
  );
  await ensureOk(res);
  return res.json();
}

/** Gerar .EML */
export async function generateEmlBy(payload: {
  client: string;
  yms: string[];
  embarcadores: string[];
}): Promise<{ filename: string; file_b64: string }> {
  const res = await fetchWithTimeout(
    buildUrl("/api/generate-eml-by"),
    { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload) },
    90000
  );
  await ensureOk(res);
  return res.json();
}

/** Dados disponíveis (períodos/embarcadores) */
export async function getAvailableData(
  client: string
): Promise<{ has_data: boolean; periods: string[]; embarcadores: string[]; error?: string }> {
  const res = await fetchWithTimeout(buildUrl("/api/available-data", { client }), {}, 60000);
  await ensureOk(res);
  return res.json();
}

/** Flush */
export async function clearDatabase(
  client: string,
  ym?: string
): Promise<{ status: string; deleted: number; detail: any }> {
  const res = await fetchWithTimeout(buildUrl("/api/flush", { client, ...(ym ? { ym } : {}) }), {
    method: "DELETE",
  });
  await ensureOk(res);
  return res.json();
}

/** Health */
export async function healthCheck(): Promise<{ ok: boolean; cache_size?: number }> {
  try {
    const res = await fetchWithTimeout(buildUrl("/api/health"), {}, 5000);
    if (res.ok) return res.json();
    return { ok: false };
  } catch {
    return { ok: false };
  }
}