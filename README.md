# 📊 Diário Operacional - Sistema de Análise Logística

Sistema integrado de análise operacional com geração automática de relatórios e insights utilizando inteligência artificial (Google Gemini).

![Version](https://img.shields.io/badge/version-2.0.0-blue.svg)
![Python](https://img.shields.io/badge/python-3.11-blue.svg)
![FastAPI](https://img.shields.io/badge/fastapi-0.111.0-green.svg)
![React](https://img.shields.io/badge/react-18.x-blue.svg)

## 🎯 Funcionalidades Principais

- ✅ **Upload Múltiplo de Planilhas**: Processamento de 3 tipos de planilhas Excel
- 📊 **KPIs Automáticos**: Cálculo de indicadores operacionais em tempo real
- 🤖 **Análise com IA**: Insights gerados automaticamente usando Google Gemini
- 📧 **Geração de E-mails**: Relatórios profissionais em texto e HTML
- 📥 **Exportação .EML**: Download direto para Outlook/email clients
- 🎨 **Dashboard Interativo**: Interface moderna e responsiva
- 💾 **Deduplicação Inteligente**: Evita processamento duplicado de dados
- 🔄 **Cache Otimizado**: Performance melhorada com cache de 30 minutos

## 📋 Estrutura das Planilhas

### 1️⃣ Detalhamento Booking (.xlsx)

Planilha principal com informações de bookings/operações.

**Colunas Obrigatórias:**

| Coluna | Variações Aceitas | Descrição | Exemplo |
|--------|------------------|-----------|---------|
| **Data** | `DATA_BOOKING`, `data_booking`, `DATA`, `data` | Data da operação | 01/01/2024 |
| **Cliente/Embarcador** | `NOME_FANTASIA`, `Cliente`, `cliente`, `Embarcador`, `embarcador` | Nome do cliente | ACME Logistics |
| **Quantidade** | `QTDE_CONTAINER`, `QTDE_CONT`, `QTD_CONTAINER` | Quantidade de containers | 5 |
| **Status** | `DESC_STATUS`, `Status da Operação`, `STATUS` | Status da operação | Ativo |
| **Booking ID** | `BOOKING`, `NUM_BOOKING`, `BOOKING_ID` | Número do booking | BK12345 |
| **Porto Origem** | `SIGLA_PORTO_ORIGEM`, `Porto da Operação` | Porto de origem | SANTOS |
| **Porto Destino** | `SIGLA_PORTO_DESTINO`, `Porto de destino` | Porto de destino | BUENOS AIRES |

**Exemplo de Estrutura:**

```
| DATA_BOOKING | NOME_FANTASIA | QTDE_CONTAINER | DESC_STATUS | BOOKING | SIGLA_PORTO_ORIGEM | SIGLA_PORTO_DESTINO |
|--------------|---------------|----------------|-------------|---------|-------------------|-------------------|
| 01/01/2024   | ACME SA       | 5              | Ativo       | BK001   | SANTOS            | BUENOS AIRES     |
| 02/01/2024   | Beta Corp     | 3              | Ativo       | BK002   | RIO GRANDE        | MONTEVIDEO       |
```

### 2️⃣ Detalhamento Multimodal (.xlsx)

Planilha com informações de reagendamentos e operações multimodais.

**Colunas Obrigatórias:**

| Coluna | Variações Aceitas | Descrição |
|--------|------------------|-----------|
| **Cliente** | `Cliente`, `cliente`, `NOME_FANTASIA`, `Embarcador` | Nome do cliente |
| **Causador Reagenda** | `Causador Reagenda`, `Causador reagenda` | Causador (filtrar "Mercosul") |
| **Área Responsável** | `Área Responsável`, `Area Responsável` | Área (excluir "CUS", "TRA") |
| **Justificativa** | `Justificativa Reagendamento`, `Justificativa` | Motivo do reagendamento |
| **Data Agendamento** | `Agendamento`, `Data Agendamento`, `Última Alteração` | Data do agendamento |
| **Porto** | `Porto da Operação`, `Porto da Operacao` | Porto |
| **Tipo Operação** | `Tipo de Operação`, `TIPO_OP_ESP_UNIF` | Tipo |

**Regras de Filtro:**
- ⚠️ Apenas registros com `Causador Reagenda = "Mercosul"` são contabilizados
- ⚠️ Registros com `Área Responsável = "CUS"` ou `"TRA"` são excluídos
- ⚠️ Justificativas vazias ou com apenas "-" são normalizadas para "Sem justificativa"

### 3️⃣ Programações de Transportes (.xlsx)

Planilha com dados de atrasos e programações.

**Colunas Obrigatórias:**

| Coluna | Variações Aceitas | Descrição |
|--------|------------------|-----------|
| **Embarcador** | `Embarcador`, `embarcador`, `Cliente`, `NOME_FANTASIA` | Nome do cliente |
| **Situação Programação** | `Situação programação`, `Situação Programação` | Status |
| **Situação Prazo** | `Situação prazo programação`, `Status prazo` | Status do prazo |
| **Tipo Programação** | `Tipo de programação`, `Tipo de programacao` | Coleta/Entrega |
| **Data Referência** | `Previsão início atendimento (BRA)`, `Data referência` | Data |
| **Justificativa** | `Justificativa de atraso de programação` | Motivo atraso |
| **Porto Origem** | `Porto de origem`, `SIGLA_PORTO_ORIGEM` | Porto |

**Regras de Processamento:**
- ✅ Identifica automaticamente atrasos quando prazo está "Fora" ou "Atrasado"
- ✅ Separa atrasos de COLETA vs ENTREGA automaticamente
- ✅ Normaliza tipos de programação (coleta, entrega, desconsolidação, etc.)

## 🚀 Setup e Instalação

### Pré-requisitos

- Python 3.11+
- Node.js 18+ (para frontend)
- Conta no [Supabase](https://supabase.com) (PostgreSQL)
- Chave API do [Google Gemini](https://makersuite.google.com/app/apikey)

### Backend Setup

```bash
# Clone o repositório
git clone <seu-repo>
cd diario-operacional

# Crie ambiente virtual
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# ou
.venv\Scripts\activate  # Windows

# Instale dependências
pip install -r requirements.txt

# Configure variáveis de ambiente
cp .env.example .env
# Edite .env com suas credenciais

# Execute o servidor
uvicorn app:app --reload --port 8000
```

### Frontend Setup

```bash
# Entre na pasta do frontend
cd frontend

# Instale dependências
npm install

# Configure API base URL
# Crie arquivo frontend/.env
echo "VITE_API_BASE=http://127.0.0.1:8000" > .env

# Execute o dev server
npm run dev
```

Acesse: `http://localhost:5173`

## ☁️ Deploy em Produção

### Opção 1: Fly.io (Recomendado) ⭐

**Por que Fly.io?**
- ✅ Free tier generoso (512MB RAM, 1 máquina sempre ligada)
- ✅ Datacenter em São Paulo (GRU) - baixa latência
- ✅ Deploy simples e rápido
- ✅ Melhor performance que Render no free tier
- ⚠️ Requer cartão de crédito (não cobra se ficar no free tier)

**Passo a passo:**

```bash
# 1. Instale o flyctl
# Windows (PowerShell):
iwr https://fly.io/install.ps1 -useb | iex

# Linux/Mac:
curl -L https://fly.io/install.sh | sh

# 2. Faça login
flyctl auth login

# 3. Lance a aplicação
flyctl launch
# Escolha:
# - Region: gru (São Paulo)
# - PostgreSQL: No (já tem Supabase)

# 4. Configure secrets
flyctl secrets set SUPABASE_DB_URL="postgresql://postgres.xxxxx:[PASSWORD]@aws-0-sa-east-1.pooler.supabase.com:6543/postgres"
flyctl secrets set GEMINI_API_KEY="AIzaSy..."
flyctl secrets set FRONTEND_ORIGIN="https://seu-frontend.netlify.app"

# 5. Deploy!
flyctl deploy

# 6. Abra a aplicação
flyctl open
```

**URL final:** `https://diario-operacional.fly.dev`

### Opção 2: Render (Alternativa Gratuita)

```bash
# No render.com:
# 1. Connect repository
# 2. New Web Service
# 3. Configure:
#    - Build Command: pip install -r requirements.txt
#    - Start Command: gunicorn app:app --bind 0.0.0.0:$PORT --workers 2 --worker-class uvicorn.workers.UvicornWorker
# 4. Add Environment Variables (do .env.example)
```

⚠️ **Render no free tier:**
- Hiberna após 15min sem uso
- Primeira requisição leva ~30s para "acordar"
- Performance inferior ao Fly.io

### Frontend no Netlify

```bash
# Na pasta frontend:

# 1. Build
npm run build

# 2. No netlify.com:
# - Connect repository
# - Build command: npm run build
# - Publish directory: dist
# - Environment variables:
#   VITE_API_BASE=https://diario-operacional.fly.dev
```

## 🔧 Variáveis de Ambiente

### Backend (.env)

```env
# Database
SUPABASE_DB_URL=postgresql://...

# AI
GEMINI_API_KEY=AIzaSy...
GEMINI_MODEL=gemini-pro

# CORS
FRONTEND_ORIGIN=http://localhost:5173
# ou para produção:
FRONTEND_ORIGIN_REGEX=https://.*\.netlify\.app,https://.*\.fly\.dev
```

### Frontend (.env)

```env
# API Base URL
VITE_API_BASE=http://127.0.0.1:8000
# ou para produção:
VITE_API_BASE=https://diario-operacional.fly.dev
```

## 🤖 Recursos de IA (Gemini)

A aplicação usa Google Gemini para gerar análises automáticas com:

### 📈 Análise Geral do Período
- Desempenho operacional geral
- Tendências de volume (crescimento/redução)
- Distribuição entre portos
- Comparação com períodos anteriores

### ⚠️ Pontos Críticos Identificados
- 3-5 problemas principais detectados
- Impacto quantificado
- Padrões observados nos dados

### 💡 Recomendações e Ações
- 3-5 recomendações práticas
- Ações específicas e acionáveis
- Potencial de melhoria mensurável

### ✅ Conclusão Executiva
- Resumo do panorama geral
- Próximos passos prioritários

**Exemplo de Prompt usado:**

```
Você é um analista sênior de operações logísticas com 15 anos de experiência.
Analise os dados operacionais abaixo:

=== INDICADORES ===
• Total de operações: 1,245 TEUs
• Porto mais movimentado: SANTOS (685 TEUs)
• Atrasos de coleta: 23
• Atrasos de entrega: 15
• Reagendamentos: 8

=== TENDÊNCIA DE VOLUME ===
Variação: +12.5% (1,107 → 1,245 TEUs)
JAN/24: 1,107 → FEV/24: 1,245

=== PRINCIPAIS MOTIVOS DE ATRASO - COLETA ===
• Atraso na chegada do caminhão: 12 ocorrências
• Falta de container disponível: 8 ocorrências
...

[Análise detalhada gerada pela IA]
```

**Fallback sem IA:**
Se a API do Gemini não estiver disponível, a aplicação usa análises padrão baseadas em regras.

## 📊 KPIs Calculados

| KPI | Descrição | Cálculo |
|-----|-----------|---------|
| **Total Operações** | Soma de containers movimentados | SUM(qtde) WHERE status='Ativo' |
| **Porto TOP** | Porto com mais operações | MAX(COUNT BY porto_origem) |
| **Porto MENOR** | Porto com menos operações | MIN(COUNT BY porto_origem) |
| **Atrasos Coleta** | Total de atrasos na coleta | COUNT WHERE tipo='coleta' AND atrasado=true |
| **Atrasos Entrega** | Total de atrasos na entrega | COUNT WHERE tipo='entrega' AND atrasado=true |
| **Reagendamentos** | Reagendamentos Mercosul | COUNT WHERE causador='Mercosul' |

## 🎨 Gráficos Gerados

1. **📈 Movimentação Mensal** - Comparativo por porto
2. **📊 Tabela de Variação** - Percentuais mês a mês
3. **🗺️ Matriz Origem × Destino** - Heatmap de rotas
4. **⏱️ Atrasos por Motivo e Porto** - Coleta e Entrega
5. **🔄 Reagendamentos** - Por causa e porto
6. **📉 Tendências** - Análise temporal

## 🔐 Segurança

- ✅ CORS configurável por ambiente
- ✅ Validação de inputs
- ✅ SQL Injection protection (SQLAlchemy)
- ✅ Deduplicação por hash SHA256
- ✅ Rate limiting recomendado em produção

## 📦 Estrutura do Projeto

```
.
├── app.py                      # Backend FastAPI
├── requirements.txt            # Dependências Python
├── Dockerfile                  # Container config
├── fly.toml                    # Fly.io config
├── .env.example                # Template de variáveis
├── README.md                   # Esta documentação
│
└── frontend/
    ├── src/
    │   ├── App.tsx            # Componente principal
    │   ├── main.tsx           # Entry point
    │   ├── styles.css         # Estilos modernos
    │   ├── lib/
    │   │   └── api.ts         # Cliente API
    │   └── components/
    │       └── MultiSelect.tsx # Seletor múltiplo
    ├── package.json
    └── vite.config.ts
```

## 🐛 Troubleshooting

### Erro: "Failed to fetch" / CORS

**Problema:** Frontend não consegue conectar ao backend

**Soluções:**
1. Verifique se o backend está rodando
2. Confirme a URL em `VITE_API_BASE`
3. Configure `FRONTEND_ORIGIN` no backend
4. Para produção, use `FRONTEND_ORIGIN_REGEX`

### Erro: "Coluna não encontrada"

**Problema:** Planilha não tem as colunas esperadas

**Soluções:**
1. Confira a seção "Estrutura das Planilhas" acima
2. Verifique variações aceitas de nomes
3. Console do backend mostra colunas disponíveis

### Banco de dados muito grande

**Problema:** Supabase atingindo limite de storage

**Soluções:**
1. Use o botão "Limpar Banco de Dados"
2. Limpe períodos antigos: `DELETE FROM uploads WHERE ym < '2024-01'`
3. A deduplicação por hash evita duplicatas

### IA não está gerando análises

**Problema:** Análises estão genéricas ou padrão

**Soluções:**
1. Verifique se `GEMINI_API_KEY` está configurada
2. Confirme quota da API do Google
3. Logs do backend mostram erros da IA
4. Fallback automático para análises padrão

## 📈 Melhorias Futuras

- [ ] Autenticação com JWT
- [ ] Multi-tenancy (múltiplos clientes isolados)
- [ ] Exportação para PDF
- [ ] Dashboard de tendências históricas
- [ ] Alertas automáticos por email
- [ ] Integração com WhatsApp Business
- [ ] Modo offline com sync

## 🤝 Contribuindo

Pull requests são bem-vindos! Para mudanças importantes:

1. Fork o projeto
2. Crie uma branch (`git checkout -b feature/AmazingFeature`)
3. Commit suas mudanças (`git commit -m 'Add: nova feature'`)
4. Push para a branch (`git push origin feature/AmazingFeature`)
5. Abra um Pull Request

## 📝 Licença

Este projeto é proprietário. Todos os direitos reservados.

## 👤 Autor

**Kauê** - Full Stack Developer
- Sistema desenvolvido com foco em performance e UX

## 🙏 Agradecimentos

- Google Gemini AI pela análise inteligente
- Supabase pelo banco de dados PostgreSQL
- Fly.io pela infraestrutura de hosting
- FastAPI e React pela stack moderna

---

**Versão:** 2.0.0  
**Última atualização:** Novembro 2024  
**Status:** ✅ Em produção
