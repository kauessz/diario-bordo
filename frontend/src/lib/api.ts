// frontend/src/api.ts

const API_BASE = "http://localhost:8000/api";

export type UploadResult = {
  status: string;
  periods: string[];        // ["2025-07","2025-08",...]
  embarcadores: string[];   // ["AMAZONIA ...", ...]
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
  email_html: string;   // html rico (lista, negrito, etc)
  conclusao?: string;   // IA / próximos passos
};

export type EmlFileResp = {
  status: string;
  filename: string;
  file_b64: string;
};

// faz upload dos 3 arquivos e retorna períodos detectados + embarcadores
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

  const res = await fetch(`${API_BASE}/upload`, {
    method: "POST",
    body: fd,
  });

  if (!res.ok) {
    throw new Error(await res.text());
  }

  return res.json() as Promise<UploadResult>;
}

// pede os KPIs pro(s) período(s) e embarcador selecionados
export async function getSummaryBy(
  client: string,
  yms: string[],          // ["2025-08","2025-09"]
  embarcador: string
): Promise<{ kpis: SummaryKpis }> {
  const res = await fetch(
    `${API_BASE}/summary?client=${encodeURIComponent(client)}&ym=${encodeURIComponent(
      yms.join(",")
    )}&embarcador=${encodeURIComponent(embarcador)}`
  );
  if (!res.ok) throw new Error(await res.text());
  return res.json() as Promise<{ kpis: SummaryKpis }>;
}

// gera o corpo de e-mail (texto e html) com base na seleção
export async function generateEmailBy(body: {
  client: string;
  yms: string[];        // ["2025-08","2025-09"]
  embarcador: string;
}): Promise<EmailTemplate> {
  const res = await fetch(`${API_BASE}/generate-email`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(await res.text());
  return res.json() as Promise<EmailTemplate>;
}

// gera .eml pronto pra baixar (com html e imagens inline)
export async function generateEmlBy(body: {
  client: string;
  yms: string[];
  embarcador: string;
}): Promise<EmlFileResp> {
  const res = await fetch(`${API_BASE}/generate-eml-by`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(await res.text());
  return res.json() as Promise<EmlFileResp>;
}