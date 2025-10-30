// frontend/src/lib/api.ts

import { API_BASE } from "../config";

export type UploadResult = {
  status: string;
  periods: string[];        // ["2025-07","2025-08", ...]
  embarcadores: string[];   // ["AMBEV", "AMAZONIA...", ...]
};

export type SummaryKpis = {
  total_ops: number;
  porto_top: string | null;
  porto_low: string | null;
  atrasos_coleta: number;
  atrasos_entrega: number;
  reagendamentos: number;
};

export type EmailTemplate = {
  status: string;
  email: string;        // texto pronto pra copiar/colar
  email_html: string;   // html rico
  conclusao?: string;   // conclusões / próximos passos
};

export type EmlFileResp = {
  status: string;
  filename: string;
  file_b64: string;     // base64 do .eml
};

// 1. Upload das 3 planilhas
export async function uploadFiles(
  client: string,
  bookingFile: File,
  multiFile: File,
  transpFile: File
): Promise<UploadResult> {
  const fd = new FormData();
  fd.append("client", client);
  fd.append("booking", bookingFile);
  fd.append("multimodal", multiFile);
  fd.append("transportes", transpFile);

  const res = await fetch(`${API_BASE}/api/upload`, {
    method: "POST",
    body: fd,
  });

  if (!res.ok) {
    throw new Error(`Erro upload ${res.status} - ${await res.text()}`);
  }

  return res.json() as Promise<UploadResult>;
}

// 2. Pede os KPIs/resumo
export async function getSummaryBy(
  client: string,
  yms: string[],          // ["2025-08","2025-09"]
  embarcador: string
): Promise<{ kpis: SummaryKpis; debug: any }> {
  const url = new URL(`${API_BASE}/api/summary`);
  url.searchParams.set("client", client);
  url.searchParams.set("ym", yms.join(","));
  url.searchParams.set("embarcador", embarcador);

  const res = await fetch(url.toString(), {
    method: "GET",
  });

  if (!res.ok) {
    throw new Error(`Erro summary ${res.status} - ${await res.text()}`);
  }
  return res.json() as Promise<{ kpis: SummaryKpis; debug: any }>;
}

// 3. Gera corpo de e-mail (texto e HTML)
export async function generateEmailBy(body: {
  client: string;
  yms: string[];
  embarcador: string;
}): Promise<EmailTemplate> {
  const res = await fetch(`${API_BASE}/api/generate-email`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });

  if (!res.ok) {
    throw new Error(`Erro email ${res.status} - ${await res.text()}`);
  }

  return res.json() as Promise<EmailTemplate>;
}

// 4. Gera arquivo .eml pra baixar
export async function generateEmlBy(body: {
  client: string;
  yms: string[];
  embarcador: string;
}): Promise<EmlFileResp> {
  const res = await fetch(`${API_BASE}/api/generate-eml-by`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });

  if (!res.ok) {
    throw new Error(`Erro eml ${res.status} - ${await res.text()}`);
  }

  return res.json() as Promise<EmlFileResp>;
}