// frontend/src/App.tsx
import React, { useState } from "react";
import {
  uploadFiles,
  getSummaryBy,
  generateEmailBy,
  generateEmlBy,
  SummaryKpis,
} from "./lib/api";
import "./styles.css"; // tema atualizado

// nome fixo de "cliente" no backend (pode mudar depois pra dropdown cliente se você quiser multi-cliente)
const CLIENT_BUCKET = "GLOBAL";

// util pra mostrar "2025-08" como "ago/2025"
function formatYmLabel(ym: string): string {
  // ym vem "YYYY-MM"
  const [yyyy, mm] = ym.split("-");
  const meses = [
    "jan", "fev", "mar", "abr", "mai", "jun",
    "jul", "ago", "set", "out", "nov", "dez",
  ];
  const idx = parseInt(mm, 10) - 1;
  const mes = meses[idx] ?? mm;
  return `${mes}/${yyyy}`;
}

export default function App() {
  // arquivos selecionados pra upload
  const [bookingFile, setBookingFile] = useState<File | null>(null);
  const [multiFile, setMultiFile] = useState<File | null>(null);
  const [transpFile, setTranspFile] = useState<File | null>(null);

  // depois do upload:
  const [periodOptions, setPeriodOptions] = useState<string[]>([]); // ["2025-08","2025-09",...]
  const [embarcadoresOptions, setEmbarcadoresOptions] = useState<string[]>([]);

  // escolha do usuário:
  const [selectedPeriods, setSelectedPeriods] = useState<string[]>([]);
  const [embarcador, setEmbarcador] = useState<string>("");

  // KPIs / resumo
  const [kpis, setKpis] = useState<SummaryKpis | null>(null);

  // texto/HTML do e-mail
  const [emailTxt, setEmailTxt] = useState("");
  const [emailHtml, setEmailHtml] = useState("");

  // controla loading de botões
  const [isUploading, setIsUploading] = useState(false);
  const [isLoadingSummary, setIsLoadingSummary] = useState(false);
  const [isGeneratingEmail, setIsGeneratingEmail] = useState(false);
  const [isGeneratingEml, setIsGeneratingEml] = useState(false);

  // 1. Envia planilhas → backend devolve períodos e embarcadores detectados
  async function handleUpload() {
    if (!bookingFile || !multiFile || !transpFile) {
      alert("Envie as 3 planilhas antes de processar.");
      return;
    }
    setIsUploading(true);
    try {
      const resp = await uploadFiles(
        CLIENT_BUCKET,
        bookingFile,
        multiFile,
        transpFile
      );
      // resp.periods -> ["2025-07","2025-08",...]
      setPeriodOptions(resp.periods || []);
      setEmbarcadoresOptions(resp.embarcadores || []);
      setSelectedPeriods(resp.periods || []); // opcional: já marcar todos de cara
    } catch (err: any) {
      console.error(err);
      alert("Falha no upload/processamento: " + err.message);
    } finally {
      setIsUploading(false);
    }
  }

  // 2. Carregar resumo (KPIs) baseado no período(s) selecionado(s) + embarcador
  async function handleResumo() {
    if (!selectedPeriods.length) {
      alert("Selecione pelo menos um período.");
      return;
    }
    if (!embarcador) {
      alert("Selecione o embarcador.");
      return;
    }
    setIsLoadingSummary(true);
    try {
      const resp = await getSummaryBy(CLIENT_BUCKET, selectedPeriods, embarcador);
      setKpis(resp.kpis);
    } catch (err: any) {
      console.error(err);
      alert("Erro ao carregar resumo: " + err.message);
    } finally {
      setIsLoadingSummary(false);
    }
  }

  // 3. Gerar e-mail (texto + html com gráficos)
  async function handleGenerateEmail() {
    if (!selectedPeriods.length || !embarcador) {
      alert("Selecione período(s) e embarcador primeiro.");
      return;
    }
    setIsGeneratingEmail(true);
    try {
      const resp = await generateEmailBy({
        client: CLIENT_BUCKET,
        yms: selectedPeriods,
        embarcador,
      });
      setEmailTxt(resp.email || "");
      setEmailHtml(resp.email_html || "");
    } catch (err: any) {
      console.error(err);
      alert("Erro ao gerar e-mail: " + err.message);
    } finally {
      setIsGeneratingEmail(false);
    }
  }

  // 4. Gerar .EML pra Outlook/Gmail Desktop
  async function handleGenerateEml() {
    if (!selectedPeriods.length || !embarcador) {
      alert("Selecione período(s) e embarcador primeiro.");
      return;
    }
    setIsGeneratingEml(true);
    try {
      const resp = await generateEmlBy({
        client: CLIENT_BUCKET,
        yms: selectedPeriods,
        embarcador,
      });

      // cria um blob .eml pra baixar
      const blob = b64ToBlob(resp.file_b64, "message/rfc822");
      const a = document.createElement("a");
      a.href = URL.createObjectURL(blob);
      a.download = resp.filename || "diario_operacional.eml";
      a.click();
      URL.revokeObjectURL(a.href);
    } catch (err: any) {
      console.error(err);
      alert("Erro ao gerar EML: " + err.message);
    } finally {
      setIsGeneratingEml(false);
    }
  }

  // helper converter base64 -> Blob pra download
  function b64ToBlob(b64Data: string, contentType: string) {
    const byteChars = atob(b64Data);
    const byteNums = new Array(byteChars.length);
    for (let i = 0; i < byteChars.length; i++) {
      byteNums[i] = byteChars.charCodeAt(i);
    }
    const byteArray = new Uint8Array(byteNums);
    return new Blob([byteArray], { type: contentType });
  }

  // liga/desliga checkbox de período
  function togglePeriod(ym: string) {
    setSelectedPeriods((prev) =>
      prev.includes(ym)
        ? prev.filter((p) => p !== ym)
        : [...prev, ym]
    );
  }

  return (
    <div className="page-wrap">
      <h1 className="page-title">Diário Operacional — MVP</h1>
      <p className="page-sub">
        Upload das 3 planilhas (.xlsx) → períodos/embarcador → KPIs → E-mail
      </p>

      {/* BLOCO 1: Upload & Preparação */}
      <section className="card-block">
        <h2 className="card-title">1. Upload & Preparação</h2>

        <div className="form-row">
          <div className="form-col">
            <label className="lbl">Detalhamento Booking (.xlsx)</label>
            <input
              type="file"
              accept=".xlsx"
              onChange={(e) => {
                if (e.target.files?.[0]) setBookingFile(e.target.files[0]);
              }}
            />
          </div>

          <div className="form-col">
            <label className="lbl">Detalhamento Multimodal (.xlsx)</label>
            <input
              type="file"
              accept=".xlsx"
              onChange={(e) => {
                if (e.target.files?.[0]) setMultiFile(e.target.files[0]);
              }}
            />
          </div>

          <div className="form-col">
            <label className="lbl">Programações de Transportes (.xlsx)</label>
            <input
              type="file"
              accept=".xlsx"
              onChange={(e) => {
                if (e.target.files?.[0]) setTranspFile(e.target.files[0]);
              }}
            />
          </div>
        </div>

        <div className="form-row">
          <button
            className="btn-primary"
            disabled={isUploading || !bookingFile || !multiFile || !transpFile}
            onClick={handleUpload}
          >
            {isUploading ? "Processando..." : "Enviar planilhas"}
          </button>
        </div>

        {/* depois do upload bem-sucedido */}
        {periodOptions.length > 0 && (
          <div className="after-upload-grid">
            <div className="after-upload-col">
              <label className="lbl">Período(s) disponíveis</label>
              <p className="hint">Selecione um ou mais meses:</p>

              <div className="period-grid">
                {periodOptions.map((ym) => (
                  <label key={ym} className="period-check">
                    <input
                      type="checkbox"
                      checked={selectedPeriods.includes(ym)}
                      onChange={() => togglePeriod(ym)}
                    />
                    <span>{formatYmLabel(ym)}</span>
                  </label>
                ))}
              </div>
            </div>

            <div className="after-upload-col">
              <label className="lbl">Embarcador</label>
              <p className="hint">Carregado a partir do Detalhamento Booking (coluna NOME_FANTASIA / cliente).</p>
              <select
                className="input-select"
                value={embarcador}
                onChange={(e) => setEmbarcador(e.target.value)}
              >
                <option value="">— selecione —</option>
                {embarcadoresOptions.map((emb) => (
                  <option key={emb} value={emb}>
                    {emb}
                  </option>
                ))}
              </select>

              <button
                className="btn-secondary"
                disabled={
                  isLoadingSummary ||
                  !selectedPeriods.length ||
                  !embarcador
                }
                onClick={handleResumo}
              >
                {isLoadingSummary ? "Carregando..." : "Carregar Resumo"}
              </button>
            </div>
          </div>
        )}
      </section>

      {/* BLOCO 2: Resumo do período */}
      <section className="card-block">
        <h2 className="card-title">2. Resumo do Período</h2>

        {kpis ? (
          <div className="kpi-grid">
            <div className="kpi-card">
              <div className="kpi-label">Total operações</div>
              <div className="kpi-value">{kpis.total_ops ?? "—"}</div>
            </div>

            <div className="kpi-card">
              <div className="kpi-label">Porto com mais operações</div>
              <div className="kpi-value">{kpis.porto_top || "—"}</div>
            </div>

            <div className="kpi-card">
              <div className="kpi-label">Porto com menos operações</div>
              <div className="kpi-value">{kpis.porto_low || "—"}</div>
            </div>

            <div className="kpi-card">
              <div className="kpi-label">Atrasos Coleta</div>
              <div className="kpi-value">{kpis.atrasos_coleta ?? "—"}</div>
            </div>

            <div className="kpi-card">
              <div className="kpi-label">Atrasos Entrega</div>
              <div className="kpi-value">{kpis.atrasos_entrega ?? "—"}</div>
            </div>

            <div className="kpi-card">
              <div className="kpi-label">Reagendamentos (Mercosul)</div>
              <div className="kpi-value">{kpis.reagendamentos ?? "—"}</div>
            </div>
          </div>
        ) : (
          <p className="hint">
            KPIs não carregados ainda. Faça upload, selecione período(s) e
            embarcador e clique em "Carregar Resumo".
          </p>
        )}
      </section>

      {/* BLOCO 3: E-mail */}
      <section className="card-block">
        <h2 className="card-title">3. E-mail</h2>

        <div className="form-row">
          <button
            className="btn-primary"
            disabled={isGeneratingEmail || !kpis}
            onClick={handleGenerateEmail}
          >
            {isGeneratingEmail
              ? "Gerando texto..."
              : "Gerar (texto + HTML c/ gráficos)"}
          </button>

          <button
            className="btn-secondary"
            disabled={isGeneratingEml || !kpis}
            onClick={handleGenerateEml}
          >
            {isGeneratingEml ? "Gerando .eml..." : "Baixar .EML (Outlook)"}
          </button>
        </div>

        <div className="email-panels">
          <div className="email-block">
            <label className="lbl">Texto (copiar e colar)</label>
            <textarea
              className="textarea-out"
              value={emailTxt}
              onChange={(e) => setEmailTxt(e.target.value)}
              rows={10}
            />
          </div>

          <div className="email-block">
            <label className="lbl">
              HTML
            </label>
            <textarea
              className="textarea-out"
              value={emailHtml}
              onChange={(e) => setEmailHtml(e.target.value)}
              rows={10}
            />
          </div>
        </div>

        <p className="hint footnote">
        </p>
      </section>
    </div>
  );
}