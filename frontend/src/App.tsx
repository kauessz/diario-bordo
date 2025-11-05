// frontend/src/App.tsx
import React, { useState } from "react";
import {
  uploadFiles,
  getSummaryBy,
  generateEmailBy,
  generateEmlBy,
  SummaryKpis,
  clearDatabase, // NOVO
} from "./lib/api";
import MultiSelect from "./components/MultiSelect";
import "./styles.css";

const CLIENT_BUCKET = "GLOBAL";

function formatYmLabel(ym: string): string {
  const [yyyy, mm] = ym.split("-");
  const meses = [
    "jan", "fev", "mar", "abr", "mai", "jun",
    "jul", "ago", "set", "out", "nov", "dez",
  ];
  const idx = parseInt(mm, 10) - 1;
  const mes = meses[idx] ?? mm;
  return `${mes}/${yyyy}`;
}

// (opcional) helper para limpar estado
function resetState(setters: {
  setBookingFile: (f: File | null) => void;
  setMultiFile: (f: File | null) => void;
  setTranspFile: (f: File | null) => void;
  setPeriodOptions: (v: string[]) => void;
  setEmbarcadoresOptions: (v: string[]) => void;
  setSelectedPeriods: (v: string[]) => void;
  setSelectedEmbarcadores: (v: string[]) => void;
  setKpis: (k: SummaryKpis | null) => void;
  setEmailTxt: (s: string) => void;
  setEmailHtml: (s: string) => void;
}) {
  const {
    setBookingFile, setMultiFile, setTranspFile,
    setPeriodOptions, setEmbarcadoresOptions,
    setSelectedPeriods, setSelectedEmbarcadores,
    setKpis, setEmailTxt, setEmailHtml
  } = setters;
  setBookingFile(null);
  setMultiFile(null);
  setTranspFile(null);
  setPeriodOptions([]);
  setEmbarcadoresOptions([]);
  setSelectedPeriods([]);
  setSelectedEmbarcadores([]);
  setKpis(null);
  setEmailTxt("");
  setEmailHtml("");
}

export default function App() {
  const [bookingFile, setBookingFile] = useState<File | null>(null);
  const [multiFile, setMultiFile] = useState<File | null>(null);
  const [transpFile, setTranspFile] = useState<File | null>(null);

  const [periodOptions, setPeriodOptions] = useState<string[]>([]);
  const [embarcadoresOptions, setEmbarcadoresOptions] = useState<string[]>([]);

  const [selectedPeriods, setSelectedPeriods] = useState<string[]>([]);
  const [selectedEmbarcadores, setSelectedEmbarcadores] = useState<string[]>([]);

  const [kpis, setKpis] = useState<SummaryKpis | null>(null);

  const [emailTxt, setEmailTxt] = useState("");
  const [emailHtml, setEmailHtml] = useState("");

  const [isUploading, setIsUploading] = useState(false);
  const [isLoadingSummary, setIsLoadingSummary] = useState(false);
  const [isGeneratingEmail, setIsGeneratingEmail] = useState(false);
  const [isGeneratingEml, setIsGeneratingEml] = useState(false);
  const [isFlushing, setIsFlushing] = useState(false); // NOVO

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
      setPeriodOptions(resp.periods || []);
      setEmbarcadoresOptions(resp.embarcadores || []);
      setSelectedPeriods(resp.periods || []);
      setSelectedEmbarcadores([]);
      if (resp.skipped?.length) {
        console.log("Deduplicação:", resp.skipped);
      }
    } catch (err: any) {
      console.error(err);
      alert("Falha no upload/processamento: " + err.message);
    } finally {
      setIsUploading(false);
    }
  }

  async function handleResumo() {
    if (!selectedPeriods.length) {
      alert("Selecione pelo menos um período.");
      return;
    }
    if (!selectedEmbarcadores.length) {
      alert("Selecione pelo menos um embarcador.");
      return;
    }
    setIsLoadingSummary(true);
    try {
      const resp = await getSummaryBy(CLIENT_BUCKET, selectedPeriods, selectedEmbarcadores);
      setKpis(resp.kpis);
    } catch (err: any) {
      console.error(err);
      alert("Erro ao carregar resumo: " + err.message);
    } finally {
      setIsLoadingSummary(false);
    }
  }

  async function handleGenerateEmail() {
    if (!selectedPeriods.length || !selectedEmbarcadores.length) {
      alert("Selecione período(s) e embarcador(es) primeiro.");
      return;
    }
    setIsGeneratingEmail(true);
    try {
      const resp = await generateEmailBy({
        client: CLIENT_BUCKET,
        yms: selectedPeriods,
        embarcadores: selectedEmbarcadores,
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

  async function handleGenerateEml() {
    if (!selectedPeriods.length || !selectedEmbarcadores.length) {
      alert("Selecione período(s) e embarcador(es) primeiro.");
      return;
    }
    setIsGeneratingEml(true);
    try {
      const resp = await generateEmlBy({
        client: CLIENT_BUCKET,
        yms: selectedPeriods,
        embarcadores: selectedEmbarcadores,
      });
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

  // NOVO: limpar banco (por client)
  async function handleFlush() {
    if (!confirm("Tem certeza que deseja limpar todos os dados do cliente atual? Esta ação não pode ser desfeita.")) {
      return;
    }
    setIsFlushing(true);
    try {
      const resp = await clearDatabase(CLIENT_BUCKET);
      alert(`Banco limpo (${resp.deleted} registros removidos).`);
      resetState({
        setBookingFile, setMultiFile, setTranspFile,
        setPeriodOptions, setEmbarcadoresOptions,
        setSelectedPeriods, setSelectedEmbarcadores,
        setKpis, setEmailTxt, setEmailHtml
      });
    } catch (err: any) {
      console.error(err);
      alert("Erro ao limpar banco: " + err.message);
    } finally {
      setIsFlushing(false);
    }
  }

  function b64ToBlob(b64Data: string, contentType: string) {
    const byteChars = atob(b64Data);
    const byteNums = new Array(byteChars.length);
    for (let i = 0; i < byteChars.length; i++) {
      byteNums[i] = byteChars.charCodeAt(i);
    }
    const byteArray = new Uint8Array(byteNums);
    return new Blob([byteArray], { type: contentType });
  }

  function togglePeriod(ym: string) {
    setSelectedPeriods((prev) =>
      prev.includes(ym) ? prev.filter((p) => p !== ym) : [...prev, ym]
    );
  }

  function selectAllPeriods() {
    setSelectedPeriods(periodOptions);
  }

  function deselectAllPeriods() {
    setSelectedPeriods([]);
  }

  function formatKpiValue(value: any): string {
    if (value === null || value === undefined || value === "" || (typeof value === 'number' && isNaN(value))) {
      return "Sem evidência";
    }
    return String(value);
  }

  return (
    <div className="page-wrap">
      <h1 className="page-title">Diário Operacional – MVP v2</h1>
      <p className="page-sub">
        Upload das 3 planilhas (.xlsx) → períodos/embarcadores → KPIs → E-mail com gráficos detalhados
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

        <div className="form-row" style={{ gap: 12, flexWrap: "wrap" }}>
          <button
            className="btn-primary"
            disabled={isUploading || !bookingFile || !multiFile || !transpFile}
            onClick={handleUpload}
          >
            {isUploading ? "Processando..." : "Enviar planilhas"}
          </button>

          {/* NOVO: Limpar banco */}
          <button
            className="btn-danger"
            onClick={handleFlush}
            disabled={isFlushing}
            title="Remove todos os registros do cliente atual"
          >
            {isFlushing ? "Limpando..." : "Limpar banco (cliente)"}
          </button>
        </div>

        {periodOptions.length > 0 && (
          <div className="after-upload-grid">
            <div className="after-upload-col">
              <label className="lbl">Período(s) disponíveis</label>
              <p className="hint">Selecione um ou mais meses:</p>

              <div className="selection-controls">
                <button className="btn-mini" onClick={selectAllPeriods}>
                  Selecionar todos
                </button>
                <button className="btn-mini" onClick={deselectAllPeriods}>
                  Limpar seleção
                </button>
              </div>

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
              <MultiSelect
                label="Embarcador(es)"
                options={embarcadoresOptions}
                selected={selectedEmbarcadores}
                onChange={setSelectedEmbarcadores}
                placeholder="Selecione os clientes..."
              />

              <button
                className="btn-secondary"
                disabled={
                  isLoadingSummary ||
                  !selectedPeriods.length ||
                  !selectedEmbarcadores.length
                }
                onClick={handleResumo}
                style={{ marginTop: "16px" }}
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
              <div className="kpi-value">{formatKpiValue(kpis.total_ops)}</div>
            </div>

            <div className="kpi-card">
              <div className="kpi-label">Porto com mais operações</div>
              <div className="kpi-value">{formatKpiValue(kpis.porto_top)}</div>
            </div>

            <div className="kpi-card">
              <div className="kpi-label">Porto com menos operações</div>
              <div className="kpi-value">{formatKpiValue(kpis.porto_low)}</div>
            </div>

            <div className="kpi-card">
              <div className="kpi-label">Atrasos Coleta</div>
              <div className="kpi-value">{formatKpiValue(kpis.atrasos_coleta)}</div>
            </div>

            <div className="kpi-card">
              <div className="kpi-label">Atrasos Entrega</div>
              <div className="kpi-value">{formatKpiValue(kpis.atrasos_entrega)}</div>
            </div>

            <div className="kpi-card">
              <div className="kpi-label">Reagendamentos (Mercosul)</div>
              <div className="kpi-value">{formatKpiValue(kpis.reagendamentos)}</div>
            </div>
          </div>
        ) : (
          <p className="hint">
            KPIs não carregados ainda. Faça upload, selecione período(s) e
            embarcador(es) e clique em "Carregar Resumo".
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
            <label className="lbl">HTML (preview)</label>
            <textarea
              className="textarea-out"
              value={emailHtml}
              onChange={(e) => setEmailHtml(e.target.value)}
              rows={10}
            />
          </div>
        </div>

        {emailHtml && (
          <div className="email-preview">
            <h3 className="lbl">Preview do E-mail</h3>
            <div
              className="preview-container"
              dangerouslySetInnerHTML={{ __html: emailHtml }}
            />
          </div>
        )}
      </section>
    </div>
  );
}