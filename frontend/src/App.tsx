// frontend/src/App.tsx - CORRIGIDO: Inputs dentro dos labels + Auto-load de dados
import React, { useState, useEffect } from "react";
import {
  uploadFiles,
  getSummaryBy,
  generateEmailBy,
  generateEmlBy,
  SummaryKpis,
  clearDatabase,
  getAvailableData,
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
  
  // Limpar localStorage
  localStorage.removeItem('selectedPeriods');
  localStorage.removeItem('selectedEmbarcadores');
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
  const [isFlushing, setIsFlushing] = useState(false);

  const [uploadProgress, setUploadProgress] = useState<string | null>(null);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [successMessage, setSuccessMessage] = useState<string | null>(null);

  const [isInitialLoading, setIsInitialLoading] = useState(true);

  // 🚀 AUTO-LOAD: Carregar dados existentes ao montar o componente
  useEffect(() => {
    async function loadExistingData() {
      try {
        const data = await getAvailableData(CLIENT_BUCKET);
        
        if (data.has_data && data.periods.length > 0) {
          setPeriodOptions(data.periods);
          setEmbarcadoresOptions(data.embarcadores);
          
          // Recuperar seleções do localStorage se existirem
          const savedPeriods = localStorage.getItem('selectedPeriods');
          const savedEmbarcadores = localStorage.getItem('selectedEmbarcadores');
          
          if (savedPeriods) {
            const parsed = JSON.parse(savedPeriods);
            // Filtrar apenas períodos que ainda existem
            const validPeriods = parsed.filter((p: string) => data.periods.includes(p));
            if (validPeriods.length > 0) {
              setSelectedPeriods(validPeriods);
            }
          }
          
          if (savedEmbarcadores) {
            const parsed = JSON.parse(savedEmbarcadores);
            // Filtrar apenas embarcadores que ainda existem
            const validEmbs = parsed.filter((e: string) => data.embarcadores.includes(e));
            if (validEmbs.length > 0) {
              setSelectedEmbarcadores(validEmbs);
            }
          }
          
          console.log(`✅ Dados carregados: ${data.periods.length} período(s), ${data.embarcadores.length} embarcador(es)`);
        } else {
          console.log('ℹ️ Nenhum dado encontrado no banco. Faça upload das planilhas.');
        }
      } catch (err) {
        console.error('Erro ao carregar dados existentes:', err);
      } finally {
        setIsInitialLoading(false);
      }
    }
    
    loadExistingData();
  }, []);

  // Salvar seleções no localStorage sempre que mudarem
  useEffect(() => {
    if (selectedPeriods.length > 0) {
      localStorage.setItem('selectedPeriods', JSON.stringify(selectedPeriods));
    }
  }, [selectedPeriods]);

  useEffect(() => {
    if (selectedEmbarcadores.length > 0) {
      localStorage.setItem('selectedEmbarcadores', JSON.stringify(selectedEmbarcadores));
    }
  }, [selectedEmbarcadores]);

  async function handleUpload() {
    if (!bookingFile || !multiFile || !transpFile) {
      setErrorMessage("Envie as 3 planilhas antes de processar.");
      return;
    }
    setIsUploading(true);
    setErrorMessage(null);
    setUploadProgress("Enviando arquivos...");
    
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
      
      setSuccessMessage(`✅ Arquivos processados com sucesso! ${resp.periods?.length || 0} período(s) encontrado(s).`);
      setUploadProgress(null);
      
      if (resp.skipped?.length) {
        console.log("Deduplicação:", resp.skipped);
      }
    } catch (err: any) {
      console.error(err);
      setErrorMessage(`❌ Falha no upload: ${err.message}`);
      setUploadProgress(null);
    } finally {
      setIsUploading(false);
    }
  }

  async function handleResumo() {
    if (!selectedPeriods.length) {
      setErrorMessage("Selecione pelo menos um período.");
      return;
    }
    if (!selectedEmbarcadores.length) {
      setErrorMessage("Selecione pelo menos um embarcador.");
      return;
    }
    setIsLoadingSummary(true);
    setErrorMessage(null);
    
    try {
      const resp = await getSummaryBy(CLIENT_BUCKET, selectedPeriods, selectedEmbarcadores);
      setKpis(resp.kpis);
      setSuccessMessage("✅ Resumo carregado com sucesso!");
    } catch (err: any) {
      console.error(err);
      setErrorMessage(`❌ Erro ao carregar resumo: ${err.message}`);
    } finally {
      setIsLoadingSummary(false);
    }
  }

  async function handleGenerateEmail() {
    if (!selectedPeriods.length || !selectedEmbarcadores.length) {
      setErrorMessage("Selecione período(s) e embarcador(es) primeiro.");
      return;
    }
    setIsGeneratingEmail(true);
    setErrorMessage(null);
    
    try {
      const resp = await generateEmailBy({
        client: CLIENT_BUCKET,
        yms: selectedPeriods,
        embarcadores: selectedEmbarcadores,
      });
      setEmailTxt(resp.email || "");
      setEmailHtml(resp.email_html || "");
      setSuccessMessage("✅ E-mail gerado com análise de IA!");
    } catch (err: any) {
      console.error(err);
      setErrorMessage(`❌ Erro ao gerar e-mail: ${err.message}`);
    } finally {
      setIsGeneratingEmail(false);
    }
  }

  async function handleGenerateEml() {
    if (!selectedPeriods.length || !selectedEmbarcadores.length) {
      setErrorMessage("Selecione período(s) e embarcador(es) primeiro.");
      return;
    }
    setIsGeneratingEml(true);
    setErrorMessage(null);
    
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
      setSuccessMessage("✅ Arquivo .EML baixado com sucesso!");
    } catch (err: any) {
      console.error(err);
      setErrorMessage(`❌ Erro ao gerar EML: ${err.message}`);
    } finally {
      setIsGeneratingEml(false);
    }
  }

  async function handleFlush() {
    if (!confirm("⚠️ Tem certeza que deseja limpar todos os dados do cliente atual? Esta ação não pode ser desfeita.")) {
      return;
    }
    setIsFlushing(true);
    setErrorMessage(null);
    
    try {
      const resp = await clearDatabase(CLIENT_BUCKET);
      setSuccessMessage(`✅ Banco limpo (${resp.deleted} registros removidos).`);
      resetState({
        setBookingFile, setMultiFile, setTranspFile,
        setPeriodOptions, setEmbarcadoresOptions,
        setSelectedPeriods, setSelectedEmbarcadores,
        setKpis, setEmailTxt, setEmailHtml
      });
    } catch (err: any) {
      console.error(err);
      setErrorMessage(`❌ Erro ao limpar banco: ${err.message}`);
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
      {/* Header */}
      <div className="page-header">
        <div className="header-content">
          <div className="header-icon">📊</div>
          <div>
            <h1 className="page-title">Diário Operacional</h1>
            <p className="page-version">MVP v2.0 • Powered by AI</p>
          </div>
        </div>
        <p className="page-sub">
          Sistema integrado de análise operacional com insights gerados por inteligência artificial
        </p>
      </div>

      {/* Loading inicial */}
      {isInitialLoading && (
        <div className="alert alert-info">
          <span className="spinner"></span>
          <span>Carregando dados existentes...</span>
        </div>
      )}

      {/* Alertas */}
      {errorMessage && (
        <div className="alert alert-error">
          <span className="alert-icon">⚠️</span>
          <span>{errorMessage}</span>
          <button className="alert-close" onClick={() => setErrorMessage(null)}>×</button>
        </div>
      )}

      {successMessage && (
        <div className="alert alert-success">
          <span className="alert-icon">✓</span>
          <span>{successMessage}</span>
          <button className="alert-close" onClick={() => setSuccessMessage(null)}>×</button>
        </div>
      )}

      {uploadProgress && (
        <div className="progress-bar">
          <div className="progress-fill"></div>
          <span className="progress-text">{uploadProgress}</span>
        </div>
      )}

      {/* BLOCO 1: Upload */}
      <section className="card-block">
        <div className="card-header">
          <h2 className="card-title">
            <span className="card-number">1</span>
            Upload & Preparação
          </h2>
          <p className="card-subtitle">Envie as planilhas para processamento automático</p>
        </div>

        <div className="upload-grid">
          {/* Booking - Input DENTRO do label */}
          <div className="upload-item">
            <label className="upload-label" htmlFor="booking-upload">
              <input
                id="booking-upload"
                type="file"
                accept=".xlsx"
                className="upload-input"
                onChange={(e) => {
                  if (e.target.files?.[0]) setBookingFile(e.target.files[0]);
                }}
              />
              <span className="upload-icon">📋</span>
              <span className="upload-text">
                <strong>Detalhamento Booking</strong>
                <small>.xlsx required</small>
              </span>
            </label>
            {bookingFile && <div className="file-badge">✓ {bookingFile.name}</div>}
          </div>

          {/* Multimodal - Input DENTRO do label */}
          <div className="upload-item">
            <label className="upload-label" htmlFor="multi-upload">
              <input
                id="multi-upload"
                type="file"
                accept=".xlsx"
                className="upload-input"
                onChange={(e) => {
                  if (e.target.files?.[0]) setMultiFile(e.target.files[0]);
                }}
              />
              <span className="upload-icon">🚛</span>
              <span className="upload-text">
                <strong>Detalhamento Multimodal</strong>
                <small>.xlsx required</small>
              </span>
            </label>
            {multiFile && <div className="file-badge">✓ {multiFile.name}</div>}
          </div>

          {/* Transportes - Input DENTRO do label */}
          <div className="upload-item">
            <label className="upload-label" htmlFor="transp-upload">
              <input
                id="transp-upload"
                type="file"
                accept=".xlsx"
                className="upload-input"
                onChange={(e) => {
                  if (e.target.files?.[0]) setTranspFile(e.target.files[0]);
                }}
              />
              <span className="upload-icon">📅</span>
              <span className="upload-text">
                <strong>Programações de Transportes</strong>
                <small>.xlsx required</small>
              </span>
            </label>
            {transpFile && <div className="file-badge">✓ {transpFile.name}</div>}
          </div>
        </div>

        <div className="button-group">
          <button
            className="btn btn-primary btn-large"
            disabled={isUploading || !bookingFile || !multiFile || !transpFile}
            onClick={handleUpload}
          >
            {isUploading ? (
              <>
                <span className="spinner"></span> Processando...
              </>
            ) : (
              <>
                <span>▶</span> Enviar e Processar Planilhas
              </>
            )}
          </button>

          <button
            className="btn btn-danger"
            onClick={handleFlush}
            disabled={isFlushing}
          >
            {isFlushing ? (
              <>
                <span className="spinner"></span> Limpando...
              </>
            ) : (
              <>
                <span>🗑️</span> Limpar Banco de Dados
              </>
            )}
          </button>
        </div>

        {periodOptions.length > 0 && (
          <div className="selection-panel">
            <div className="selection-col">
              <label className="section-label">Períodos Disponíveis</label>
              <p className="hint">Selecione um ou mais meses:</p>

              <div className="selection-controls">
                <button className="btn-mini" onClick={selectAllPeriods}>Todos</button>
                <button className="btn-mini" onClick={deselectAllPeriods}>Limpar</button>
              </div>

              <div className="period-grid">
                {periodOptions.map((ym) => (
                  <label key={ym} className="period-check">
                    <input
                      type="checkbox"
                      checked={selectedPeriods.includes(ym)}
                      onChange={() => togglePeriod(ym)}
                    />
                    <span className="period-label">{formatYmLabel(ym)}</span>
                  </label>
                ))}
              </div>
            </div>

            <div className="selection-col">
              <MultiSelect
                label="Embarcadores/Clientes"
                options={embarcadoresOptions}
                selected={selectedEmbarcadores}
                onChange={setSelectedEmbarcadores}
                placeholder="Selecione os clientes..."
              />

              <button
                className="btn btn-secondary btn-large"
                disabled={isLoadingSummary || !selectedPeriods.length || !selectedEmbarcadores.length}
                onClick={handleResumo}
                style={{ marginTop: "20px" }}
              >
                {isLoadingSummary ? (
                  <>
                    <span className="spinner"></span> Carregando...
                  </>
                ) : (
                  <>
                    <span>📊</span> Carregar Resumo e KPIs
                  </>
                )}
              </button>
            </div>
          </div>
        )}
      </section>

      {/* BLOCO 2: KPIs */}
      <section className="card-block">
        <div className="card-header">
          <h2 className="card-title">
            <span className="card-number">2</span>
            Resumo do Período
          </h2>
          <p className="card-subtitle">Indicadores-chave de performance</p>
        </div>

        {kpis ? (
          <div className="kpi-grid">
            <div className="kpi-card kpi-primary">
              <div className="kpi-icon">📦</div>
              <div className="kpi-content">
                <div className="kpi-label">Total operações</div>
                <div className="kpi-value">{formatKpiValue(kpis.total_ops)}</div>
                <div className="kpi-unit">TEUs</div>
              </div>
            </div>

            <div className="kpi-card kpi-success">
              <div className="kpi-icon">🔝</div>
              <div className="kpi-content">
                <div className="kpi-label">Porto TOP</div>
                <div className="kpi-value">{formatKpiValue(kpis.porto_top)}</div>
                <div className="kpi-unit">mais operações</div>
              </div>
            </div>

            <div className="kpi-card kpi-info">
              <div className="kpi-icon">📉</div>
              <div className="kpi-content">
                <div className="kpi-label">Porto MENOR</div>
                <div className="kpi-value">{formatKpiValue(kpis.porto_low)}</div>
                <div className="kpi-unit">menos operações</div>
              </div>
            </div>

            <div className="kpi-card kpi-warning">
              <div className="kpi-icon">🚚</div>
              <div className="kpi-content">
                <div className="kpi-label">Atrasos Coleta</div>
                <div className="kpi-value">{formatKpiValue(kpis.atrasos_coleta)}</div>
                <div className="kpi-unit">ocorrências</div>
              </div>
            </div>

            <div className="kpi-card kpi-warning">
              <div className="kpi-icon">📦</div>
              <div className="kpi-content">
                <div className="kpi-label">Atrasos Entrega</div>
                <div className="kpi-value">{formatKpiValue(kpis.atrasos_entrega)}</div>
                <div className="kpi-unit">ocorrências</div>
              </div>
            </div>

            <div className="kpi-card kpi-danger">
              <div className="kpi-icon">🔄</div>
              <div className="kpi-content">
                <div className="kpi-label">Reagendamentos</div>
                <div className="kpi-value">{formatKpiValue(kpis.reagendamentos)}</div>
                <div className="kpi-unit">Mercosul</div>
              </div>
            </div>
          </div>
        ) : (
          <div className="empty-state">
            <div className="empty-icon">📊</div>
            <p className="empty-text">
              KPIs não carregados. Faça upload e clique em "Carregar Resumo".
            </p>
          </div>
        )}
      </section>

      {/* BLOCO 3: E-mail */}
      <section className="card-block">
        <div className="card-header">
          <h2 className="card-title">
            <span className="card-number">3</span>
            Geração de E-mail
          </h2>
          <p className="card-subtitle">Relatório com análise de IA</p>
        </div>

        <div className="button-group">
          <button
            className="btn btn-primary btn-large"
            disabled={isGeneratingEmail || !kpis}
            onClick={handleGenerateEmail}
          >
            {isGeneratingEmail ? (
              <>
                <span className="spinner"></span> Gerando...
              </>
            ) : (
              <>
                <span>🤖</span> Gerar E-mail com IA
              </>
            )}
          </button>

          <button
            className="btn btn-secondary btn-large"
            disabled={isGeneratingEml || !kpis}
            onClick={handleGenerateEml}
          >
            {isGeneratingEml ? (
              <>
                <span className="spinner"></span> Gerando .eml...
              </>
            ) : (
              <>
                <span>📧</span> Baixar .EML
              </>
            )}
          </button>
        </div>

        <div className="email-tabs">
          <div className="email-panel">
            <label className="section-label">
              <span>📝</span> Texto
            </label>
            <textarea
              className="textarea-out"
              value={emailTxt}
              onChange={(e) => setEmailTxt(e.target.value)}
              rows={12}
              placeholder="Texto do e-mail..."
            />
          </div>

          <div className="email-panel">
            <label className="section-label">
              <span>🎨</span> HTML
            </label>
            <textarea
              className="textarea-out"
              value={emailHtml}
              onChange={(e) => setEmailHtml(e.target.value)}
              rows={12}
              placeholder="HTML do e-mail..."
            />
          </div>
        </div>

        {emailHtml && (
          <div className="email-preview-section">
            <h3 className="section-label">
              <span>👁️</span> Preview
            </h3>
            <div className="email-preview">
              <div
                className="preview-container"
                dangerouslySetInnerHTML={{ __html: emailHtml }}
              />
            </div>
          </div>
        )}
      </section>

      {/* Footer */}
      <footer className="page-footer">
        <p>
          Sistema desenvolvido com <span className="heart">❤️</span> • 
          Powered by <strong>Google Gemini AI</strong>
        </p>
        <p className="footer-version">v2.0.1 • 2024</p>
      </footer>
    </div>
  );
}