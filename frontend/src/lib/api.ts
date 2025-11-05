// frontend/src/lib/api.ts
export type SummaryKpis = {
  total_ops: number | null;
  porto_top: string | null;
  porto_low: string | null;
  atrasos_coleta: number | null;
  atrasos_entrega: number | null;
  reagendamentos: number | null;
};

function resolveApiBase(): string {
  const fromEnv = (import.meta as any)?.env?.VITE_API_BASE;
  if (fromEnv && String(fromEnv).trim()) return String(fromEnv).trim();

  if (typeof window !== "undefined") {
    const port = window.location.port;
    // Vite / CRA em dev → a API costuma rodar em 8000
    if (port === "5173" || port === "5174" || port === "3000") {
      return "http://127.0.0.1:8000";
    }
    return `${window.location.protocol}//${window.location.host}`;
  }
  return "http://127.0.0.1:8000";
}

const API_BASE = resolveApiBase();

function ensureOk(res: Response) {
  if (!res.ok) throw new Error(`HTTP ${res.status} - ${res.statusText}`);
}

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

  const res = await fetch(`${API_BASE}/api/upload`, { method: "POST", body: form });
  ensureOk(res);
  return res.json();
}

export async function getSummaryBy(
  client: string,
  yms: string[],
  embarcadores: string[]
): Promise<{ kpis: SummaryKpis; debug: any }> {
  const params = new URLSearchParams();
  params.set("client", client);
  params.set("ym", yms.join(","));
  params.set("embarcador", embarcadores.join(","));
  const res = await fetch(`${API_BASE}/api/summary?${params.toString()}`);
  ensureOk(res);
  return res.json();
}

export async function generateEmailBy(payload: {
  client: string;
  yms: string[];
  embarcadores: string[];
}): Promise<{ email: string; email_html: string }> {
  const res = await fetch(`${API_BASE}/api/generate-email`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  ensureOk(res);
  return res.json();
}

export async function generateEmlBy(payload: {
  client: string;
  yms: string[];
  embarcadores: string[];
}): Promise<{ filename: string; file_b64: string }> {
  const res = await fetch(`${API_BASE}/api/generate-eml-by`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  ensureOk(res);
  return res.json();
}

export async function clearDatabase(
  client: string,
  ym?: string
): Promise<{ status: string; deleted: number; detail: any }> {
  const params = new URLSearchParams();
  params.set("client", client);
  if (ym) params.set("ym", ym);
  const res = await fetch(`${API_BASE}/api/flush?${params.toString()}`, { method: "DELETE" });
  ensureOk(res);
  return res.json();
}