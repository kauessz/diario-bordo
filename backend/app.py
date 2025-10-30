import io
import base64
import math
from datetime import datetime, date
from typing import List, Optional, Dict
from collections import defaultdict

import pandas as pd
import os
from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import JSONResponse
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator
from textwrap import fill as _wrap


# =============================================================================
# CONFIG
# =============================================================================

import os
from fastapi.middleware.cors import CORSMiddleware

DB_PATH = "data.db"
engine: Engine = create_engine(f"sqlite:///{DB_PATH}", future=True)

app = FastAPI()

# Aceita várias origens separadas por vírgula
origins_env = os.getenv("FRONTEND_ORIGIN", "http://localhost:5173,http://127.0.0.1:5173")
origin_list = [o.strip() for o in origins_env.split(",") if o.strip()]
origin_regex = os.getenv("FRONTEND_ORIGIN_REGEX", "")  # ex.: r"^https?://(localhost:5173|.*\.netlify\.app)$"

if origin_regex:
    app.add_middleware(
        CORSMiddleware,
        allow_origin_regex=origin_regex,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
else:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origin_list,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

with engine.begin() as conn:
    conn.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS uploads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client TEXT NOT NULL,
                ym TEXT NOT NULL,
                kind TEXT NOT NULL,         -- "booking" | "multi" | "transp"
                data BLOB NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
    )
    conn.execute(
        text(
            "CREATE INDEX IF NOT EXISTS idx_uploads_client_ym_kind "
            "ON uploads (client, ym, kind)"
        )
    )

# ---- LISTAS "PLUGGÁVEIS" DE NOMES DE COLUNAS (adapte aqui quando mudar no Excel)
CANDS_BOOKING_DT   = ["DATA_BOOKING", "data_booking", "DATA", "data", "DATA_BOOKING "]
CANDS_BOOKING_EMB  = ["NOME_FANTASIA","Cliente","cliente","Embarcador","embarcador",
                      "NOME_FANTASIA ","cliente embarcador","cliente_embarcador",
                      "nome embarcador","nome_embarcador","shipper","remetente"]
CANDS_BOOKING_QTD  = ["QTDE_CONTAINER","QTDE_CONT","QTD_CONTAINER","QUANTIDADE_BOX_EMBARCADOS"]
CANDS_BOOKING_ID   = ["BOOKING","Booking","NUM_BOOKING","NUM_BOOKING ","BOOKING_ID","ID_BOOKING"]
CANDS_BOOKING_PORT = ["SIGLA_PORTO_ORIGEM","Porto da Operação","SIGLA_PORTO_ORIGEM ",
                      "SIGLA_PORTO_ORIGEM_x","SIGLA_PORTO_ORIGEM_y"]
CANDS_BOOKING_STAT = ["DESC_STATUS", "Status da Operação", "STATUS", "desc_status", "status"]

CANDS_MULTI_CLIENTE   = ["Cliente","cliente","NOME_FANTASIA","Embarcador","embarcador"]
CANDS_MULTI_CAUSADOR  = ["Causador Reagenda","Causador reagenda","Causador da Reagenda"]
CANDS_MULTI_AREA      = ["Área Responsável","Area Responsável","AREA RESPONSÁVEL","Area Responsavel","AREA RESPONSAVEL"]
CANDS_MULTI_JUST      = ["Justificativa Reagendamento","Justificativa de Reagendamento","Justificativa"]
CANDS_MULTI_DT        = ["Agendamento","Data Agendamento","Agendamento.1","Última Alteração da Agenda","ultima alteracao"]
CANDS_MULTI_PORTO     = ["Porto da Operação","Porto da Operacao"]
CANDS_MULTI_TIPO_OP   = ["Tipo de Operação","Tipo de Operacao","TIPO_OP_ESP_UNIF"]

CANDS_TRANSP_EMB      = ["Embarcador","embarcador","Cliente","cliente","NOME_FANTASIA"]
CANDS_TRANSP_SIT_PROG  = [
    "Situação programação","Situação programacao","Situacao programação",
    "Situação Programação","Situação de programação","Situacao de programacao",
    "Situação da programação","Situacao da programacao","Status programação","Status programacao",
    "Sit prog","Situacao prog"
]

CANDS_TRANSP_SIT_PRAZO = [
    "Situação prazo programação","Situacao prazo programacao","Situação prazo programação ",
    "Situacao Prazo Programacao","Status prazo programação","Status prazo programacao",
    "Situação do prazo","Status do prazo"
]

CANDS_TRANSP_TIPO      = ["Tipo de programação","tipo de programação","Tipo de programacao","tipo de programacao"]
CANDS_TRANSP_DT_REF    = ["Previsão início atendimento (BRA)","Previsao inicio atendimento (BRA)",
                           "Previsão início atendimento","Previsao inicio atendimento","Data referência","Data referencia"]
CANDS_TRANSP_JUST      = ["Justificativa de atraso de programação","Campo Digitável Justificativa",
                           "Justificativa atraso","Justificativa","Justificativa de atraso de programação "]


# =============================================================================
# HELPERS
# =============================================================================

def normalize_str(v: str) -> str:
    import unicodedata
    if v is None:
        return ""
    s = str(v).replace("\u00a0", " ")  # NBSP -> espaço normal
    s = unicodedata.normalize("NFKD", s).encode("ASCII", "ignore").decode("ASCII")
    s = s.lower().strip()
    for ch in [",", ".", ";", ":", "/", "\\", "|", "_", "(", ")", "[", "]", "{", "}", "'", '"', "-"]:
        s = s.replace(ch, " ")
    s = " ".join(s.split())
    return s


def parse_excel_bytes(xlsx_bytes: bytes) -> Dict[str, pd.DataFrame]:
    dfs = {}
    with io.BytesIO(xlsx_bytes) as fh:
        xls = pd.ExcelFile(fh)
        for sheet in xls.sheet_names:
            fh.seek(0)
            df = pd.read_excel(fh, sheet_name=sheet)
            df.columns = [str(c).strip() for c in df.columns]
            if len(df) > 0:
                dfs[sheet] = df
    return dfs


def first_existing_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    """Procura coluna por igualdade (normalizada) e, se falhar, por 'contains'."""
    norm_map = {c: normalize_str(c) for c in df.columns}
    # 1) igualdade
    for wanted in candidates:
        wn = normalize_str(wanted)
        for col, normed in norm_map.items():
            if normed == wn:
                return col
    # 2) contains
    for col in df.columns:
        cn = normalize_str(col)
        if any(normalize_str(w) in cn for w in candidates):
            return col
    return None


def ensure_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    """Wrapper seguro: só retorna se realmente existe no DataFrame."""
    col = first_existing_col(df, candidates)
    if col and col in df.columns:
        return col
    # debug útil no terminal
    print("[WARN] Nenhuma coluna encontrada p/ candidatos:", candidates)
    print("       Colunas disponíveis:", list(df.columns)[:20], "...")
    return None


def extract_period_ym(dt_raw) -> Optional[str]:
    """Converte diversos formatos para YYYY-MM (datetime, string, serial Excel)."""
    if pd.isna(dt_raw):
        return None

    if isinstance(dt_raw, (datetime, pd.Timestamp, date)):
        d = pd.to_datetime(dt_raw, errors="coerce")
        return f"{d.year:04d}-{d.month:02d}" if pd.notna(d) else None

    if isinstance(dt_raw, (int, float)):
        if not (isinstance(dt_raw, float) and math.isnan(dt_raw)):
            if 10000 <= float(dt_raw) <= 60000:
                d = pd.to_datetime(dt_raw, unit="d", origin="1899-12-30", errors="coerce")
                return f"{d.year:04d}-{d.month:02d}" if pd.notna(d) else None
            return None

    cand = str(dt_raw).strip()
    fmts = ["%d/%m/%Y %H:%M:%S","%d/%m/%Y %H:%M","%d/%m/%Y","%Y-%m-%d %H:%M:%S","%Y-%m-%d"]
    for f in fmts:
        try:
            d = datetime.strptime(cand, f)
            return f"{d.year:04d}-{d.month:02d}"
        except ValueError:
            pass
    try:
        d2 = pd.to_datetime(dt_raw, dayfirst=True, errors="raise")
        return f"{d2.year:04d}-{d2.month:02d}"
    except Exception:
        return None


def safe_int(x) -> int:
    try:
        return int(x)
    except Exception:
        return 0


def pick_best_datetime_col(df: pd.DataFrame, name_contains_list: List[str]) -> Optional[str]:
    """Escolhe a coluna mais 'data' pela taxa de parse para __ym."""
    candidates = []
    for c in df.columns:
        cn = normalize_str(c)
        if any(term in cn for term in name_contains_list):
            candidates.append(c)
    best, best_rate = None, -1.0
    for c in candidates:
        parsed = df[c].apply(extract_period_ym)
        rate = parsed.notna().mean()
        if rate > best_rate:
            best, best_rate = c, rate
    return best


# --- Aliases de cliente -------------------------------------------------------

STOPWORDS_CORP = {
    "sa", "s.a", "s.a.", "s", "a", "s/a",
    "ltda", "me", "epp", "industria", "industria de", "ind",
    "comercio", "comercio de", "comercio e", "com", "grupo",
    "holdings", "brasil", "do", "da", "de", "the", "company",
    "co", "co.", "corp", "inc"
}

def canonical_client_root(name: str) -> str:
    if name is None:
        return ""
    raw = str(name).split(" - ", 1)[0].strip()
    s = normalize_str(raw)
    tokens = [t for t in s.split() if t not in STOPWORDS_CORP]
    return " ".join(tokens).strip()

def client_match(selected: str, value: str) -> bool:
    s_root = canonical_client_root(selected)
    v_root = canonical_client_root(value)
    if not s_root or not v_root:
        return False
    return (s_root in v_root) or (v_root in s_root)

def _wrap_label(s: str, width: int = 22) -> str:
    if not s:
        return s
    return "\n".join(_wrap(str(s), width=width).splitlines())


# =============================================================================
# BOOKING
# =============================================================================

def load_booking_df(
    xlsx_bytes: bytes,
    selected_ym_list: Optional[List[str]] = None,
    selected_embarcador: Optional[str] = None
) -> pd.DataFrame:
    sheets = parse_excel_bytes(xlsx_bytes)
    df_all = pd.concat(sheets.values(), ignore_index=True)

    col_status    = ensure_col(df_all, CANDS_BOOKING_STAT)
    col_dt        = ensure_col(df_all, CANDS_BOOKING_DT)
    col_emb       = ensure_col(df_all, CANDS_BOOKING_EMB)
    col_qtd       = ensure_col(df_all, CANDS_BOOKING_QTD)
    col_booking_id= ensure_col(df_all, CANDS_BOOKING_ID)
    col_porto     = ensure_col(df_all, CANDS_BOOKING_PORT)

    if not col_dt or not col_emb or not col_qtd:
        return pd.DataFrame(columns=["ym","embarcador","booking_id","porto_origem","qtde"])

    df_all["__ym"] = df_all[col_dt].apply(extract_period_ym)

    if col_status:
        df_all = df_all[df_all[col_status].astype(str).str.strip().str.lower() == "ativo"]

    if selected_embarcador:
        df_all = df_all[df_all[col_emb].apply(lambda v: client_match(selected_embarcador, str(v)))]

    if selected_ym_list:
        df_all = df_all[df_all["__ym"].isin(selected_ym_list)]

    df_all = df_all[~df_all["__ym"].isna()]
    df_all["__qtde"] = df_all[col_qtd].apply(safe_int)

    if not col_booking_id:
        col_booking_id = "__generated_booking_id"
        df_all[col_booking_id] = (
            df_all["__ym"].astype(str) + "|" + df_all["__qtde"].astype(str) + "|row" + df_all.index.astype(str)
        )

    if not col_porto:
        col_porto = "__porto_dummy"
        df_all[col_porto] = ""

    tmp = (
        df_all
        .groupby(["__ym", col_booking_id, col_porto], dropna=False, as_index=False)["__qtde"]
        .sum()
    ).rename(columns={"__ym":"ym", col_booking_id:"booking_id", col_porto:"porto_origem", "__qtde":"qtde_parcial"})

    records = []
    for (ym_val, bid), sub in tmp.groupby(["ym", "booking_id"]):
        total_qtde = sub["qtde_parcial"].sum()
        best_row = sub.sort_values("qtde_parcial", ascending=False).iloc[0]
        records.append({
            "ym": ym_val,
            "booking_id": bid,
            "porto_origem": str(best_row["porto_origem"]).strip(),
            "qtde": int(total_qtde),
            "embarcador": selected_embarcador or ""
        })

    return pd.DataFrame(records, columns=["ym","booking_id","porto_origem","qtde","embarcador"]).reset_index(drop=True)


# =============================================================================
# MULTIMODAL / REAGENDAMENTO
# =============================================================================

def load_multi_df(
    xlsx_bytes: bytes,
    selected_ym_list: Optional[List[str]] = None,
    selected_embarcador: Optional[str] = None
) -> pd.DataFrame:
    sheets = parse_excel_bytes(xlsx_bytes)
    if not sheets:
        return pd.DataFrame(columns=["__ym", "porto_op", "tipo_operacao", "motivo_reagenda", "flag"])

    df_all = pd.concat(sheets.values(), ignore_index=True).replace("-", "").fillna("")

    col_cliente   = ensure_col(df_all, CANDS_MULTI_CLIENTE)
    col_causador  = ensure_col(df_all, CANDS_MULTI_CAUSADOR)
    col_area_resp = ensure_col(df_all, CANDS_MULTI_AREA)
    col_just      = ensure_col(df_all, CANDS_MULTI_JUST)

    # melhor coluna de data
    col_agendamento = pick_best_datetime_col(df_all, ["agendamento","data agendamento","ultima alteracao"]) \
                      or ensure_col(df_all, CANDS_MULTI_DT)

    col_porto   = ensure_col(df_all, CANDS_MULTI_PORTO)
    col_tipoop  = ensure_col(df_all, CANDS_MULTI_TIPO_OP)

    if col_causador:
        print("Valores únicos CAUSADOR:", df_all[col_causador].astype(str).unique()[:10])

    if col_agendamento:
        df_all["__ym"] = df_all[col_agendamento].apply(extract_period_ym)
    else:
        df_all["__ym"] = None

    if selected_ym_list:
        df_all = df_all[df_all["__ym"].isin(selected_ym_list)]

    if selected_embarcador and col_cliente:
        df_all["__cliente_norm"] = df_all[col_cliente].astype(str)
        df_all = df_all[df_all["__cliente_norm"].apply(lambda v: client_match(selected_embarcador, v))]

    mask_causador_ok = pd.Series([True]*len(df_all), index=df_all.index)
    if col_causador:
        mask_causador_ok = df_all[col_causador].astype(str).str.strip().str.lower().eq("mercosul")

    mask_area_ok = pd.Series([True]*len(df_all), index=df_all.index)
    if col_area_resp:
        mask_area_ok = ~df_all[col_area_resp].astype(str).str.strip().str.upper().isin(["CUS","TRA"])

    just_norm_series = pd.Series([""]*len(df_all), index=df_all.index)
    mask_just_ok = pd.Series([True]*len(df_all), index=df_all.index)
    if col_just:
        just_norm_series = df_all[col_just].astype(str).str.strip()
        mask_just_ok = just_norm_series.apply(lambda x: len(x) > 1)

    df_valid = df_all[mask_causador_ok & mask_area_ok & mask_just_ok].copy()

    df_valid["motivo_reagenda"] = just_norm_series.loc[df_valid.index].astype(str).str.strip()
    df_valid["porto_op"] = df_valid[col_porto].astype(str).str.strip() if col_porto else ""
    df_valid["tipo_operacao"] = df_valid[col_tipoop].astype(str).str.strip() if col_tipoop else ""
    df_valid["flag"] = 1

    return df_valid[["__ym", "porto_op", "tipo_operacao", "motivo_reagenda", "flag"]].reset_index(drop=True)


# =============================================================================
# TRANSP / ATRASO
# =============================================================================

def load_transp_df(
    xlsx_bytes: bytes,
    selected_ym_list: Optional[List[str]] = None,
    selected_embarcador: Optional[str] = None
) -> pd.DataFrame:
    sheets = parse_excel_bytes(xlsx_bytes)
    if not sheets:
        return pd.DataFrame(columns=["tipo_norm","justificativa_atraso","__ym"])

    df_all = pd.concat(sheets.values(), ignore_index=True)

    # Descoberta de colunas (robusta)
    col_embarc        = ensure_col(df_all, CANDS_TRANSP_EMB)
    col_situacao_prog = ensure_col(df_all, CANDS_TRANSP_SIT_PROG)
    col_situacao_prazo= ensure_col(df_all, CANDS_TRANSP_SIT_PRAZO)
    col_tipo_prog     = ensure_col(df_all, CANDS_TRANSP_TIPO)
    col_dt_ref        = ensure_col(df_all, CANDS_TRANSP_DT_REF)
    col_just_transp   = ensure_col(df_all, CANDS_TRANSP_JUST)

    print("[TRANSP] colunas:", list(df_all.columns)[:25], "...")
    print("[TRANSP] resolvidas:",
          "embarc:", col_embarc,
          "sit_prog:", col_situacao_prog,
          "sit_prazo:", col_situacao_prazo,
          "tipo:", col_tipo_prog,
          "data_ref:", col_dt_ref,
          "just:", col_just_transp)

    # __ym
    if col_dt_ref:
        df_all["__ym"] = df_all[col_dt_ref].apply(extract_period_ym)
    else:
        df_all["__ym"] = None

    # mês
    if selected_ym_list:
        df_all = df_all[df_all["__ym"].isin(selected_ym_list)]

    # cliente (aliases)
    if selected_embarcador and col_embarc:
        df_all["__emb_norm"] = df_all[col_embarc].astype(str)
        df_all = df_all[df_all["__emb_norm"].apply(lambda v: client_match(selected_embarcador, v))]

    # filtros só se as colunas existirem de fato
    if col_situacao_prog and col_situacao_prog in df_all.columns:
        df_all = df_all[~df_all[col_situacao_prog].astype(str).str.lower().str.contains("cancelad", na=False)]
    else:
        print("[TRANSP] sem coluna de 'situação programação' — filtro pulado.")

    if col_situacao_prazo and col_situacao_prazo in df_all.columns:
        df_all = df_all[df_all[col_situacao_prazo].astype(str).str.strip().str.lower() == "atrasado"]
    else:
        print("[TRANSP] sem coluna de 'situação prazo' — filtro pulado.")

    # tipo obrigatório
    if not (col_tipo_prog and col_tipo_prog in df_all.columns):
        print("[TRANSP] sem coluna de 'tipo de programação' — retornando vazio.")
        return pd.DataFrame(columns=["tipo_norm","justificativa_atraso","__ym"])

    df_all["tipo_norm"] = df_all[col_tipo_prog].astype(str).str.strip().str.lower()

    # justificativa (opcional)
    if col_just_transp and col_just_transp in df_all.columns:
        df_all["justificativa_atraso"] = df_all[col_just_transp].astype(str).str.strip()
    else:
        df_all["justificativa_atraso"] = ""

    df_all = df_all.drop_duplicates()

    return df_all[["tipo_norm","justificativa_atraso","__ym"]].reset_index(drop=True)


# =============================================================================
# KPIs
# =============================================================================

def compute_kpis(booking_df: pd.DataFrame, multi_df: pd.DataFrame, transp_df: pd.DataFrame) -> Dict[str, object]:
    total_ops = int(booking_df["qtde"].sum()) if len(booking_df) else 0

    porto_stats = defaultdict(int)
    for _, row in booking_df.iterrows():
        porto_stats[row["porto_origem"]] += safe_int(row["qtde"])
    if porto_stats:
        porto_top = max(porto_stats.items(), key=lambda x: x[1])[0]
        porto_low = min(porto_stats.items(), key=lambda x: x[1])[0]
    else:
        porto_top = None
        porto_low = None

    reagendamentos = int(multi_df["flag"].sum()) if len(multi_df) else 0

    if len(transp_df):
        coleta_total = (transp_df["tipo_norm"] == "coleta").sum()
        entrega_total = (transp_df["tipo_norm"] == "entrega").sum()
    else:
        coleta_total = 0
        entrega_total = 0

    return {
        "total_ops": total_ops,
        "porto_top": porto_top,
        "porto_low": porto_low,
        "atrasos_coleta": int(coleta_total),
        "atrasos_entrega": int(entrega_total),
        "reagendamentos": reagendamentos,
    }


# =============================================================================
# GRÁFICOS (ajustados para e-mail)
# =============================================================================

def _save_fig_to_b64(fig) -> str:
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=160, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode("ascii")


def build_origem_destino_chart(booking_df: pd.DataFrame, yms: List[str]) -> str:
    if booking_df.empty:
        return ""
    pivot = booking_df.groupby(["porto_origem", "ym"])["qtde"].sum().unstack(fill_value=0)
    pivot = pivot.reindex(columns=yms, fill_value=0)

    fig, ax = plt.subplots(figsize=(8, 4.5), dpi=160)
    x = range(len(pivot.index))
    width = 0.8 / max(len(pivot.columns), 1)

    for i, col in enumerate(pivot.columns):
        vals = pivot[col].values
        offs = [xx + i*width for xx in x]
        bars = ax.bar(offs, vals, width=width, label=col)
        try: ax.bar_label(bars, padding=3, fontsize=9)
        except: 
            for xx, val in zip(offs, vals):
                if val > 0:
                    ax.text(xx, val, str(int(val)), ha="center", va="bottom", fontsize=8)

    ax.set_xticks([xx + width*(len(pivot.columns)-1)/2 for xx in x])
    ax.set_xticklabels([_wrap_label(i) for i in pivot.index], rotation=0)
    ax.set_ylabel("Qtd contêiner")
    ax.set_title("Origem x Destino")
    ax.legend(fontsize=9, ncols=min(len(pivot.columns), 3))
    ax.yaxis.set_major_locator(MaxNLocator(integer=True))
    fig.tight_layout()
    return _save_fig_to_b64(fig)


def chart_ops_por_mes(booking_df: pd.DataFrame) -> str:
    if booking_df.empty:
        return ""

    serie = booking_df.groupby("ym")["qtde"].sum().sort_index()

    # cores diferentes por mês (usa o ciclo padrão do Matplotlib)
    colors = plt.rcParams["axes.prop_cycle"].by_key().get("color", ["#1f77b4"])

    fig, ax = plt.subplots(figsize=(8, 4.5), dpi=160)
    xs = range(len(serie.index))
    vals = serie.values
    bar_colors = [colors[i % len(colors)] for i in xs]

    bars = ax.bar(xs, vals, color=bar_colors)

    # rótulos no topo
    try:
        ax.bar_label(bars, padding=3, fontsize=10)
    except Exception:
        for x, val in zip(xs, vals):
            ax.text(x, val, str(int(val)), ha="center", va="bottom", fontsize=9)

    ax.set_xticks(list(xs))
    ax.set_xticklabels(list(serie.index))
    ax.set_ylabel("Qtd contêineres")
    ax.set_title("Operações por mês / Evolução de volume")
    ax.yaxis.set_major_locator(MaxNLocator(integer=True))
    fig.tight_layout()
    return _save_fig_to_b64(fig)


def chart_atrasos_por_motivo(transp_df: pd.DataFrame, tipo: str) -> str:
    """
    tipo: "coleta" ou "entrega"
    Agora em barras HORIZONTAIS (motivos no eixo Y) com cores distintas.
    """
    if transp_df.empty:
        return ""
    df_tipo = transp_df[transp_df["tipo_norm"] == tipo]
    if df_tipo.empty:
        return ""

    serie = (
        df_tipo
        .groupby("justificativa_atraso")["tipo_norm"]
        .count()
        .sort_values(ascending=False)
        .head(12)        # top 12
        [::-1]           # invertido p/ maior ficar no topo
    )

    labels = [_wrap_label(lbl, 28) for lbl in serie.index]
    y = range(len(serie))

    colors = plt.rcParams["axes.prop_cycle"].by_key().get("color", ["#1f77b4"])
    bar_colors = [colors[i % len(colors)] for i in y]

    fig, ax = plt.subplots(figsize=(8.8, 5.2), dpi=160)
    bars = ax.barh(list(y), serie.values, color=bar_colors)

    # rótulos à direita das barras
    for i, v in enumerate(serie.values):
        ax.text(v + 0.3, i, str(int(v)), va="center", fontsize=9)

    ax.set_yticks(list(y))
    ax.set_yticklabels(labels, fontsize=9)
    ax.set_xlabel("Ocorrências")
    ax.set_title(f"Atrasos {tipo.capitalize()} por motivo")
    ax.xaxis.set_major_locator(MaxNLocator(integer=True))
    fig.tight_layout()
    return _save_fig_to_b64(fig)


def chart_reagendamentos(multi_df: pd.DataFrame) -> str:
    if multi_df.empty:
        return ""

    serie = (
        multi_df
        .groupby("motivo_reagenda")["flag"]
        .sum()
        .sort_values(ascending=False)
        .head(12)        # top 12
        [::-1]           # invertido p/ maior ficar no topo
    )

    labels = [_wrap_label(lbl, 28) for lbl in serie.index]
    y = range(len(serie))

    colors = plt.rcParams["axes.prop_cycle"].by_key().get("color", ["#1f77b4"])
    bar_colors = [colors[i % len(colors)] for i in y]

    fig, ax = plt.subplots(figsize=(8.8, 5.2), dpi=160)
    bars = ax.barh(list(y), serie.values, color=bar_colors)

    for i, v in enumerate(serie.values):
        ax.text(v + 0.3, i, str(int(v)), va="center", fontsize=9)

    ax.set_yticks(list(y))
    ax.set_yticklabels(labels, fontsize=9)
    ax.set_xlabel("Qtd reagendamentos")
    ax.set_title("Reagendamentos por motivo")
    ax.xaxis.set_major_locator(MaxNLocator(integer=True))
    fig.tight_layout()
    return _save_fig_to_b64(fig)


# =============================================================================
# EMAIL (texto + HTML)
# =============================================================================

def default_conclusao(kpis: Dict[str, object]) -> str:
    msgs = []
    total_ops = kpis.get("total_ops", 0)
    atrasos_coleta = kpis.get("atrasos_coleta", 0)
    atrasos_entrega = kpis.get("atrasos_entrega", 0)
    reag = kpis.get("reagendamentos", 0)

    if total_ops and total_ops > 0:
        msgs.append(f"Volume operacional relevante ({total_ops} operações no período), demonstrando continuidade de demanda.")
    if (atrasos_coleta or atrasos_entrega):
        msgs.append("Persistem atrasos de coleta/entrega, sugerindo foco em documentação antecipada e disponibilidade de janela.")
    if reag:
        msgs.append("Houve reagendamentos atribuídos ao Mercosul; recomenda-se revisar janelas e comunicação entre transportador e terminal.")
    if not msgs:
        msgs = ["Operação estável, sem registros críticos de atraso ou reagendamento relevantes neste período."]
    return " ".join(msgs)


def format_periodos_label(yms: List[str]) -> str:
    meses_pt = ["JANEIRO","FEVEREIRO","MARÇO","ABRIL","MAIO","JUNHO","JULHO","AGOSTO","SETEMBRO","OUTUBRO","NOVEMBRO","DEZEMBRO"]
    out_parts = []
    for ym in yms:
        yyyy, mm = ym.split("-")
        m_idx = int(mm)-1
        nome_mes = meses_pt[m_idx] if 0 <= m_idx < 12 else mm
        out_parts.append(f"{nome_mes}/{yyyy[-2:]}")
    return ", ".join(out_parts)


def build_email_v2(kpis: Dict[str, object], yms: List[str], embarcador: str,
                   booking_df: pd.DataFrame, transp_df: pd.DataFrame, multi_df: pd.DataFrame):
    label = format_periodos_label(yms)

    graf_ops_mes_b64    = chart_ops_por_mes(booking_df)
    graf_atraso_col_b64 = chart_atrasos_por_motivo(transp_df, "coleta")
    graf_atraso_ent_b64 = chart_atrasos_por_motivo(transp_df, "entrega")
    graf_reag_b64       = chart_reagendamentos(multi_df)

    total_ops = kpis["total_ops"]
    porto_top = kpis["porto_top"] or "N/D"
    porto_low = kpis["porto_low"] or "N/D"
    atrasos_coleta = kpis["atrasos_coleta"]
    atrasos_entrega = kpis["atrasos_entrega"]
    reag_total = kpis["reagendamentos"]

    conclusao_txt = default_conclusao(kpis)

    txt_lines = [
        "Boa tarde,\n",
        f"Para garantirmos um serviço cada vez mais alinhado com as necessidades de nossos clientes, "
        f"gostaria de compartilhar com vocês o Diário das Operações de {label}, referente ao cliente {embarcador}.\n",
        "O objetivo é proporcionar visibilidade sobre a operação, identificar oportunidades e priorizar ações.\n",
        "(FONTE: QLIK + OPENTECH)\n",
        "📈 Dados Mensais – Análise Operacional",
        f"Total de operações no período: {total_ops}",
        f"Porto mais movimentado: {porto_top}",
        f"Porto com menor movimentação: {porto_low}",
        "\n⏱️ Atrasos",
        f"Coletas: {atrasos_coleta}",
        f"Entregas: {atrasos_entrega}",
        "\n🔄 Reagendamentos",
        f"Total de reagendamentos no período: {reag_total}",
        "\n✅ Conclusão e Próximos Passos",
        conclusao_txt,
    ]
    txt_text = "\n".join(txt_lines)

    def img_tag(b64, title):
        if not b64:
            return ""
        return f'<h4 style="margin:12px 0 4px 0;">{title}</h4><img style="max-width:600px;display:block;margin-bottom:16px;" src="data:image/png;base64,{b64}" />'

    html_parts = [
        "<p>Boa tarde,</p>",
        f"<p>Para garantirmos um serviço cada vez mais alinhado com as necessidades de nossos clientes, "
        f"gostaria de compartilhar com vocês o <b>Diário das Operações de {label}</b>, "
        f"referente ao cliente <b>{embarcador}</b>.</p>",
        "<p>O objetivo de dividir essas informações é proporcionar visibilidade das operações, "
        "de modo a aprimorar nossos processos e identificar oportunidades de melhoria.</p>",
        '<p><i>(FONTE: QLIK + OPENTECH)</i></p>',
        "<h3>📈 Dados Mensais – Análise Operacional</h3>",
        "<ul>",
        f"<li><b>Total de operações no período:</b> {total_ops}</li>",
        f"<li><b>Porto mais movimentado:</b> {porto_top}</li>",
        f"<li><b>Porto com menor movimentação:</b> {porto_low}</li>",
        "</ul>",
        img_tag(graf_ops_mes_b64, "Operações por mês / Evolução de volume"),
        "<h3>⏱️ Atrasos</h3>",
        f"<p><b>Coletas (total):</b> {atrasos_coleta}</p>",
        img_tag(graf_atraso_col_b64, "Atrasos na coleta por motivo"),
        f"<p><b>Entregas (total):</b> {atrasos_entrega}</p>",
        img_tag(graf_atraso_ent_b64, "Atrasos na entrega por motivo"),
        "<h3>🔄 Reagendamentos</h3>",
        f"<p><b>Total de reagendamentos no período:</b> {reag_total}</p>",
        img_tag(graf_reag_b64, "Reagendamentos por motivo"),
        "<h3>✅ Conclusão e Próximos Passos</h3>",
        f"<p>{conclusao_txt}</p>",
    ]
    html_full = '<div style="font-family:Segoe UI,Roboto,Arial,sans-serif;font-size:14px;color:#1a1a1a;line-height:1.5;">' + "".join(html_parts) + "</div>"
    return txt_text, html_full


def build_eml(subject: str, body_html: str, body_txt: str,
              from_addr="ops@empresa.com", to_addr="cliente@empresa.com") -> bytes:
    import uuid
    boundary = "====BOUNDARY_" + str(uuid.uuid4())
    eml_lines = [
        f"From: {from_addr}",
        f"To: {to_addr}",
        f"Subject: {subject}",
        "MIME-Version: 1.0",
        f'Content-Type: multipart/alternative; boundary="{boundary}"',
        "",
        f"--{boundary}",
        "Content-Type: text/plain; charset=UTF-8",
        "Content-Transfer-Encoding: 8bit",
        "",
        body_txt,
        "",
        f"--{boundary}",
        "Content-Type: text/html; charset=UTF-8",
        "Content-Transfer-Encoding: 8bit",
        "",
        body_html,
        "",
        f"--{boundary}--",
        "",
    ]
    return "\r\n".join(eml_lines).encode("utf-8", errors="replace")


# =============================================================================
# DB helpers
# =============================================================================

def save_upload_row(client: str, ym: str, kind: str, blob: bytes):
    now_iso = datetime.utcnow().isoformat()
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM uploads WHERE client=:c AND ym=:y AND kind=:k"),
                     {"c": client, "y": ym, "k": kind})
        conn.execute(text("INSERT INTO uploads (client,ym,kind,data,created_at) VALUES (:c,:y,:k,:d,:t)"),
                     {"c": client, "y": ym, "k": kind, "d": blob, "t": now_iso})


def get_latest_blob(client: str, ym: str, kind: str) -> Optional[bytes]:
    with engine.begin() as conn:
        row = conn.execute(
            text("SELECT data FROM uploads WHERE client=:c AND ym=:y AND kind=:k ORDER BY id DESC LIMIT 1"),
            {"c": client, "y": ym, "k": kind},
        ).fetchone()
    return row[0] if row else None


# =============================================================================
# API ROUTES
# =============================================================================

@app.post("/api/upload")
async def upload(client: str = Form(...),
                 booking: UploadFile = File(...),
                 multimodal: UploadFile = File(...),
                 transportes: UploadFile = File(...)):
    b_booking = await booking.read()
    b_multi = await multimodal.read()
    b_transp = await transportes.read()

    booking_sheets = parse_excel_bytes(b_booking)
    if not booking_sheets:
        raise HTTPException(status_code=400, detail="Arquivo booking vazio/inválido")

    df_all = pd.concat(booking_sheets.values(), ignore_index=True)

    col_dt  = ensure_col(df_all, CANDS_BOOKING_DT)
    col_emb = ensure_col(df_all, CANDS_BOOKING_EMB)
    col_stat= ensure_col(df_all, CANDS_BOOKING_STAT)

    if col_dt is None:
        raise HTTPException(status_code=400, detail="Coluna de data não encontrada no Booking.")
    if col_emb is None:
        raise HTTPException(status_code=400, detail="Coluna de embarcador/cliente não encontrada no Booking.")

    df_all["__ym"] = df_all[col_dt].apply(extract_period_ym)
    df_active = df_all[df_all[col_stat].astype(str).str.strip().str.lower() == "ativo"] if col_stat else df_all

    embarcadores_list = sorted(df_active[col_emb].astype(str).str.strip().dropna().unique().tolist())
    periods_list = sorted(df_all["__ym"].dropna().unique().tolist())

    now = datetime.utcnow().isoformat()
    with engine.begin() as conn:
        for ym in periods_list:
            for kind, blob in [("booking", b_booking), ("multi", b_multi), ("transp", b_transp)]:
                conn.execute(text("DELETE FROM uploads WHERE client=:c AND ym=:y AND kind=:k"),
                             {"c": client, "y": ym, "k": kind})
                conn.execute(text("INSERT INTO uploads (client,ym,kind,data,created_at) VALUES (:c,:y,:k,:d,:t)"),
                             {"c": client, "y": ym, "k": kind, "d": blob, "t": now})

    return JSONResponse({"status": "ok", "periods": periods_list, "embarcadores": embarcadores_list})


@app.get("/api/summary")
def api_summary(client: str, ym: str = Query(...), embarcador: str = Query(...)):
    ym_list = [y.strip() for y in ym.split(",") if y.strip()]
    if not ym_list:
        raise HTTPException(status_code=400, detail="Nenhum período informado")

    booking_frames, multi_frames, transp_frames = [], [], []
    for y in ym_list:
        b_blob = get_latest_blob(client, y, "booking")
        m_blob = get_latest_blob(client, y, "multi")
        t_blob = get_latest_blob(client, y, "transp")
        if not b_blob or not m_blob or not t_blob:
            raise HTTPException(status_code=400, detail=f"Faltam planilhas p/ {y}.")
        booking_frames.append(load_booking_df(b_blob, [y], embarcador))
        multi_frames.append(load_multi_df(m_blob, [y], embarcador))
        transp_frames.append(load_transp_df(t_blob, [y], embarcador))

    booking_concat = pd.concat(booking_frames, ignore_index=True) if booking_frames else pd.DataFrame()
    multi_concat   = pd.concat(multi_frames,   ignore_index=True) if multi_frames   else pd.DataFrame()
    transp_concat  = pd.concat(transp_frames,  ignore_index=True) if transp_frames  else pd.DataFrame()

    kpis = compute_kpis(booking_concat, multi_concat, transp_concat)
    debug_info = {
        "booking_len": len(booking_concat),
        "booking_sum_qtde": int(booking_concat["qtde"].sum()) if len(booking_concat) else 0,
        "transp_len": len(transp_concat),
        "transp_coleta_total": int((transp_concat["tipo_norm"] == "coleta").sum()) if len(transp_concat) else 0,
        "transp_entrega_total": int((transp_concat["tipo_norm"] == "entrega").sum()) if len(transp_concat) else 0,
        "multi_len": len(multi_concat),
        "multi_reag_total": int(multi_concat["flag"].sum()) if len(multi_concat) else 0,
    }
    return JSONResponse({"kpis": kpis, "debug": debug_info})


@app.post("/api/generate-email")
async def api_generate_email(payload: dict):
    client = payload.get("client")
    yms = payload.get("yms", [])
    embarcador = payload.get("embarcador")
    if not client or not yms or not embarcador:
        raise HTTPException(status_code=400, detail="Campos obrigatórios ausentes.")

    booking_frames, multi_frames, transp_frames = [], [], []
    for y in yms:
        booking_frames.append(load_booking_df(get_latest_blob(client, y, "booking"), [y], embarcador))
        multi_frames.append(load_multi_df(get_latest_blob(client, y, "multi"), [y], embarcador))
        transp_frames.append(load_transp_df(get_latest_blob(client, y, "transp"), [y], embarcador))

    booking_concat = pd.concat(booking_frames, ignore_index=True) if booking_frames else pd.DataFrame()
    multi_concat   = pd.concat(multi_frames,   ignore_index=True) if multi_frames   else pd.DataFrame()
    transp_concat  = pd.concat(transp_frames,  ignore_index=True) if transp_frames  else pd.DataFrame()

    kpis = compute_kpis(booking_concat, multi_concat, transp_concat)
    txt, html = build_email_v2(kpis, yms, embarcador, booking_concat, transp_concat, multi_concat)

    return JSONResponse({"status": "ok", "email": txt, "email_html": html, "conclusao": default_conclusao(kpis)})


@app.post("/api/generate-eml-by")
async def api_generate_eml_by(payload: dict):
    client = payload.get("client")
    yms = payload.get("yms", [])
    embarcador = payload.get("embarcador")
    if not client or not yms or not embarcador:
        raise HTTPException(status_code=400, detail="Campos obrigatórios ausentes.")

    booking_frames, multi_frames, transp_frames = [], [], []
    for y in yms:
        booking_frames.append(load_booking_df(get_latest_blob(client, y, "booking"), [y], embarcador))
        multi_frames.append(load_multi_df(get_latest_blob(client, y, "multi"), [y], embarcador))
        transp_frames.append(load_transp_df(get_latest_blob(client, y, "transp"), [y], embarcador))

    booking_concat = pd.concat(booking_frames, ignore_index=True) if booking_frames else pd.DataFrame()
    multi_concat   = pd.concat(multi_frames,   ignore_index=True) if multi_frames   else pd.DataFrame()
    transp_concat  = pd.concat(transp_frames,  ignore_index=True) if transp_frames  else pd.DataFrame()

    kpis = compute_kpis(booking_concat, multi_concat, transp_concat)
    txt, html = build_email_v2(kpis, yms, embarcador, booking_concat, transp_concat, multi_concat)

    subject = f"Diário Operacional – {format_periodos_label(yms)} – {embarcador}"
    raw_eml = build_eml(subject, html, txt)
    b64 = base64.b64encode(raw_eml).decode("ascii")
    return JSONResponse({"status": "ok", "filename": "diario_operacional.eml", "file_b64": b64})


@app.get("/api/health")
def health():
    return {"ok": True}