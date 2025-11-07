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
 * Prioridade: VITE_API_BASE > window.location (produção) > localhost (dev)
 */
function resolveApiBase(): string {
  // 1. Variável de ambiente tem prioridade
  const fromEnv = (import.meta as any)?.env?.VITE_API_BASE;
  if (fromEnv && String(fromEnv).trim()) {
    return String(fromEnv).trim();
  }

  if (typeof window !== "undefined") {
    const { protocol, hostname, port } = window.location;
    
    // 2. Detectar ambiente de desenvolvimento
    const isDev = port === "5173" || port === "5174" || port === "3000" || hostname === "localhost";
    
    if (isDev) {
      // Em dev, usa localhost:8000
      return "http://127.0.0.1:8000";
    }
    
    // 3. Em produção, usa o mesmo domínio/protocolo
    // Exemplo: se frontend está em https://app.netlify.com, tenta https://app.netlify.com/api
    // Mas isso geralmente não funciona, então assuma que VITE_API_BASE está configurado
    return `${protocol}//${hostname}`;
  }
  
  // Fallback final
  return "http://127.0.0.1:8000";
}

const API_BASE = resolveApiBase();

console.log(`[API Client] Base URL: ${API_BASE}`);

/**
 * Helper para verificar resposta HTTP e lançar erro descritivo
 */
async function ensureOk(res: Response): Promise<void> {
  if (!res.ok) {
    let errorMessage = `HTTP ${res.status} - ${res.statusText}`;
    
    try {
      const errorData = await res.json();
      if (errorData.detail) {
        errorMessage += `: ${errorData.detail}`;
      }
    } catch {
      // Se não conseguir parsear JSON, usa statusText
    }
    
    throw new Error(errorMessage);
  }
}

/**
 * Wrapper fetch com timeout e retry
 */
async function fetchWithTimeout(
  url: string, 
  options: RequestInit = {}, 
  timeout: number = 60000
): Promise<Response> {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeout);
  
  try {
    const response = await fetch(url, {
      ...options,
      signal: controller.signal
    });
    clearTimeout(timeoutId);
    return response;
  } catch (error) {
    clearTimeout(timeoutId);
    if (error instanceof Error) {
      if (error.name === 'AbortError') {
        throw new Error('Request timeout - servidor demorou muito para responder');
      }
      throw error;
    }
    throw new Error('Erro desconhecido na requisição');
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
  skipped?: any[] 
}> {
  const form = new FormData();
  form.append("client", client);
  form.append("booking", bookingFile);
  form.append("multimodal", multiFile);
  form.append("transportes", transpFile);

  const res = await fetchWithTimeout(
    `${API_BASE}/api/upload`, 
    { method: "POST", body: form },
    120000 // 2 minutos de timeout para upload
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
  const params = new URLSearchParams();
  params.set("client", client);
  params.set("ym", yms.join(","));
  params.set("embarcador", embarcadores.join(","));
  
  const res = await fetchWithTimeout(
    `${API_BASE}/api/summary?${params.toString()}`
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
    `${API_BASE}/api/generate-email`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    },
    90000 // 90 segundos - IA pode demorar
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
    `${API_BASE}/api/generate-eml-by`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    },
    90000 // 90 segundos
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
  const params = new URLSearchParams();
  params.set("client", client);
  if (ym) params.set("ym", ym);
  
  const res = await fetchWithTimeout(
    `${API_BASE}/api/flush?${params.toString()}`, 
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
    const res = await fetchWithTimeout(
      `${API_BASE}/api/health`,
      {},
      5000 // 5 segundos timeout
    );
    
    if (res.ok) {
      return res.json();
    }
    return { ok: false };
  } catch {
    return { ok: false };
  }
}