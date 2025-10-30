# Ops Report MVP — Híbrido (Upload Excel → KPIs → E-mail)

MVP para automatizar o "Diário das Operações": upload de **3 planilhas (.xlsx)**, cálculo de KPIs, exibição de gráficos e **geração de e-mail** (texto pronto para copiar e colar). Modo **híbrido**: template automático + botão "Gerar com IA" opcional (stub no MVP).

## 📦 Stack
- **Backend:** FastAPI + Pandas + SQLite
- **Frontend:** Vite + React + TypeScript
- **Gráficos:** (placeholder no MVP), foco na geração do texto do e-mail
- **IA (opcional):** botão para futura integração (não necessário para rodar)

---

## ▶️ Como rodar (local)

### 1) Backend
```bash
cd backend
python -m venv .venv
# Windows
.venv\Scripts\activate
# macOS/Linux
# source .venv/bin/activate

pip install -r requirements.txt
uvicorn app:app --reload --port 8000
```

A API sobe em: http://localhost:8000

Endpoints principais:
- `POST /api/upload` — form-data: `client`, `ym` (YYYY-MM), e arquivos: `booking`, `multimodal`, `transportes`
- `GET /api/summary?client=Totalplast&ym=2025-08`
- `POST /api/generate-email` — body JSON: `{"client":"Totalplast","ym":"2025-08"}`

### 2) Frontend
```bash
cd frontend
npm i
npm run dev
```
App em: http://localhost:5173

---

## 🧪 Fluxo de teste rápido
1. Abra o frontend → faça upload das 3 planilhas (`.xlsx`) + informe **Cliente** e **Ano/Mês (YYYY-MM)**.
2. Clique em **Processar**.
3. Selecione o mesmo Cliente + Período na seção **Resumo**.
4. Clique em **Gerar e-mail (template)** → o texto aparecerá pronto para **copiar e colar**.
5. (Opcional) O botão **Gerar com IA** fica desativado no MVP (stub).

---

## 📁 Banco de dados
- Arquivo SQLite: `backend/data.db`
- Tabelas:
  - `uploads(client, ym, ... bytes ...)`
  - `metrics(client, ym, data_json)`

---

## 🔌 Próximos passos
- Adicionar gráficos (Chart.js) com os dados do `/api/summary`
- Habilitar "Gerar com IA" via chave na interface (env) e provider de IA
- Exportar PDF/Docx
- Migrar para Postgres (Neon/Supabase) se necessário
