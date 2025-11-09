# -*- coding: utf-8 -*-
import io
import os
import base64
import math
import hashlib
from datetime import datetime, date
from typing import List, Optional, Dict, Tuple
from collections import defaultdict
from functools import lru_cache

import pandas as pd
import numpy as np

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

from dotenv import load_dotenv
import google.generativeai as genai

# Cache simples em memória (TTL 30 minutos)
from cachetools import TTLCache
kpi_cache = TTLCache(maxsize=200, ttl=900)  # 15 min para KPIs agregados
cache = TTLCache(maxsize=100, ttl=1800)  # 30 minutos



# =============================================================================
# .ENV + CONFIG
# =============================================================================
load_dotenv()

from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
import socket

def _prepare_supabase_url(url: str) -> str:
    """
    Normaliza a URL para o Supabase:
    - Garante sslmode=require
    - Se DB_USE_POOLER=true (default), usa porta 6543; senão mantém/usa 5432
    - Se DB_FORCE_IPV4=true (default), resolve host IPv4 e injeta ?hostaddr=A.B.C.D
    """
    from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
    import socket

    DB_USE_POOLER = os.getenv("DB_USE_POOLER", "true").lower() in ("1","true","yes")
    DB_FORCE_IPV4 = os.getenv("DB_FORCE_IPV4", "true").lower() in ("1","true","yes")

    try:
        u = urlparse(url)
        if not u.scheme.startswith("postgres"):
            return url
        if not u.hostname:
            return url

        q = dict(parse_qsl(u.query or ""))
        if "sslmode" not in q:
            q["sslmode"] = "require"

        port = u.port
        if "supabase.co" in u.hostname:
            if DB_USE_POOLER:
                port = 6543
            else:
                port = 5432 if (u.port is None or u.port == 6543) else u.port

        if DB_FORCE_IPV4:
            try:
                infos = socket.getaddrinfo(u.hostname, port or 5432, family=socket.AF_INET, type=socket.SOCK_STREAM)
                if infos:
                    ipv4 = infos[0][4][0]
                    q["hostaddr"] = ipv4
            except Exception:
                pass

        newu = u._replace(netloc=f"{u.username}:{u.password}@{u.hostname}:{port}", query=urlencode(q))
        return urlunparse(newu)
    except Exception:
        return url
        if u.hostname and "supabase.co" in u.hostname:
            # se porta ausente ou 5432, troca para 6543 (pooler)
            port = u.port or 5432
            if port == 5432:
                port = 6543

            userinfo = ""
            if u.username and u.password:
                userinfo = f"{u.username}:{u.password}@"
            netloc = f"{userinfo}{u.hostname}:{port}"

            # query params
            q = dict(parse_qsl(u.query))
            q.setdefault("sslmode", "require")
            q.setdefault("connect_timeout", "10")

            # tentar resolver A (IPv4) e inserir hostaddr
            try:
                infos = socket.getaddrinfo(u.hostname, port, socket.AF_INET, socket.SOCK_STREAM)
                if infos:
                    ipv4 = infos[0][4][0]
                    q.setdefault("hostaddr", ipv4)
            except Exception:
                pass

            return urlunparse((u.scheme, netloc, u.path, u.params, urlencode(q), u.fragment))
    except Exception:
        pass
    return url

SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL", "postgresql://user:pass@host:5432/dbname")
SUPABASE_DB_URL = _prepare_supabase_url(SUPABASE_DB_URL)


# Debug simples (sem vazar senha):
try:
    _u = urlparse(SUPABASE_DB_URL)
    from urllib.parse import parse_qsl
    _q = dict(parse_qsl(_u.query))
    _host = _u.hostname
    _port = _u.port
    _has_hostaddr = 'hostaddr' in _q
    print(f"[DB] Using host={_host} port={_port} sslmode={_q.get('sslmode')} hostaddr={'yes' if _has_hostaddr else 'no'}")
except Exception:
    pass

engine: Engine = create_engine(
    SUPABASE_DB_URL,
    future=True,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
)
app = FastAPI()

import time

@app.on_event("startup")
def _startup_db_init():
    if not os.getenv("RUN_DB_INIT_AT_IMPORT", "").lower() in ("1","true","yes"):
        delay = 1.0
        for attempt in range(1, 8):
            try:
                _ensure_schema(engine)
                print(f"[DB] Schema ensured on startup (attempt {attempt}).")
                break
            except Exception as e:
                print(f"[DB] Startup init attempt {attempt} failed: {e}")
                time.sleep(delay)
                delay = min(delay * 2, 15)


@app.get("/api/health")
def health():
    db_ok = False
    try:
        from sqlalchemy import text
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        db_ok = True
    except Exception:
        db_ok = False
    return {"ok": True, "db_ok": bool(db_ok), "cache_size": len(cache)}

origins_env = os.getenv("FRONTEND_ORIGIN", "http://localhost:5173,http://127.0.0.1:5173")
origin_list = [o.strip() for o in origins_env.split(",") if o.strip()]
origin_regex = os.getenv("FRONTEND_ORIGIN_REGEX", "")

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


# --- Gemini (AI) ---
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
gemini_model = None
try:
    import google.generativeai as genai  # type: ignore
    if GEMINI_API_KEY:
        genai.configure(api_key=GEMINI_API_KEY)
        GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-pro")
        gemini_model = genai.GenerativeModel(GEMINI_MODEL)
except Exception:
    # Mantém gemini_model=None -> o código cai no fallback sem IA
    gemini_model = None

# =============================================================================
# DB SCHEMA

def _ensure_schema(engine):
    from sqlalchemy import text
    with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS uploads (
                    id SERIAL PRIMARY KEY,
                    client TEXT NOT NULL,
                    ym TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    data BYTEA NOT NULL,
                    hash TEXT,
                    created_at TEXT NOT NULL
                )
            """))
            conn.execute(text("ALTER TABLE uploads ADD COLUMN IF NOT EXISTS hash TEXT"))
            conn.execute(text(
                "CREATE INDEX IF NOT EXISTS idx_uploads_client_ym_kind ON uploads (client, ym, kind)"
            ))
            conn.execute(text(
                "CREATE UNIQUE INDEX IF NOT EXISTS ux_uploads_client_ym_kind_hash ON uploads (client, ym, kind, hash)"
            ))

# Optional: allow running at import (disabled by default)
_RUN_DB_INIT_IMPORT = os.getenv("RUN_DB_INIT_AT_IMPORT", "").lower() in ("1","true","yes")
if _RUN_DB_INIT_IMPORT:
    try:
        _ensure_schema(engine)
    except Exception as e:
        print(f"[DB] Import-time init failed: {e}")

# =============================================================================
# (schema init moved to _ensure_schema)
# ======================================================================================================================================================
# COLUMN CANDIDATES
# =============================================================================
CANDS_BOOKING_DT   = ["DATA_BOOKING", "data_booking", "DATA", "data", "DATA_BOOKING "]
CANDS_BOOKING_EMB  = ["NOME_FANTASIA","Cliente","cliente","Embarcador","embarcador",
                      "NOME_FANTASIA ","cliente embarcador","cliente_embarcador",
                      "nome embarcador","nome_embarcador","shipper","remetente"]
CANDS_BOOKING_QTD  = ["QTDE_CONTAINER","QTDE_CONT","QTD_CONTAINER","QUANTIDADE_BOX_EMBARCADOS"]
CANDS_BOOKING_ID   = ["BOOKING","Booking","NUM_BOOKING","NUM_BOOKING ","BOOKING_ID","ID_BOOKING"]
CANDS_BOOKING_PORT_ORIG = ["SIGLA_PORTO_ORIGEM","Porto da Operação","SIGLA_PORTO_ORIGEM ",
                           "SIGLA_PORTO_ORIGEM_x","SIGLA_PORTO_ORIGEM_y","Porto de origem"]
CANDS_BOOKING_PORT_DEST = ["SIGLA_PORTO_DESTINO","SIGLA_PORTO_DESTINO ","Porto de destino"]
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
CANDS_TRANSP_PORTO_ORIG = ["Porto de origem","Porto origem","SIGLA_PORTO_ORIGEM"]

# =============================================================================
# HELPERS
# =============================================================================
def normalize_str(v: str) -> str:
    import unicodedata
    if v is None:
        return ""
    s = str(v).replace("\u00a0", " ")
    s = unicodedata.normalize("NFKD", s).encode("ASCII", "ignore").decode("ASCII")
    s = s.lower().strip()
    for ch in [",", ".", ";", ":", "/", "\\", "|", "_", "(", ")", "[", "]", "{", "}", "'", '"', "-"]:
        s = s.replace(ch, " ")
    s = " ".join(s.split())
    return s

def parse_excel_bytes(xlsx_bytes: bytes) -> Dict[str, pd.DataFrame]:
    """Parser otimizado de Excel"""
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
    norm_map = {c: normalize_str(c) for c in df.columns}
    for wanted in candidates:
        wn = normalize_str(wanted)
        for col, normed in norm_map.items():
            if normed == wn:
                return col
    for col in df.columns:
        cn = normalize_str(col)
        if any(normalize_str(w) in cn for w in candidates):
            return col
    return None

def ensure_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    col = first_existing_col(df, candidates)
    if col and col in df.columns:
        return col
    print("[WARN] Nenhuma coluna encontrada p/ candidatos:", candidates)
    print("       Colunas disponíveis:", list(df.columns)[:20], "...")
    return None

def extract_period_ym(dt_raw) -> Optional[str]:
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

def normalize_justificativa(value) -> str:
    """
    Normaliza justificativas vazias, NaN, '-', blank para 'Sem justificativa'
    """
    if pd.isna(value):
        return "Sem justificativa"
    s = str(value).strip()
    if s in ["", "-", "nan", "NaN", "None", "null"]:
        return "Sem justificativa"
    if len(s) <= 1:
        return "Sem justificativa"
    return s

STOPWORDS_CORP = {
    "sa","s.a","s.a.","s","a","s/a","ltda","me","epp","industria","industria de","ind",
    "comercio","comercio de","comercio e","com","grupo","holdings","brasil","do","da","de","the",
    "company","co","co.","corp","inc"
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

def format_ym_label(ym: str) -> str:
    meses = ["JAN","FEV","MAR","ABR","MAI","JUN","JUL","AGO","SET","OUT","NOV","DEZ"]
    try:
        yyyy, mm = ym.split("-")
        idx = int(mm) - 1
        return f"{meses[idx]}/{yyyy[2:]}"
    except:
        return ym

def format_periodos_label(yms: List[str]) -> str:
    meses_pt = ["JANEIRO","FEVEREIRO","MARÇO","ABRIL","MAIO","JUNHO","JULHO","AGOSTO","SETEMBRO","OUTUBRO","NOVEMBRO","DEZEMBRO"]
    out_parts = []
    for ym in sorted(yms):
        yyyy, mm = ym.split("-")
        m_idx = int(mm)-1
        nome_mes = meses_pt[m_idx] if 0 <= m_idx < 12 else mm
        out_parts.append(f"{nome_mes}/{yyyy[-2:]}")
    return ", ".join(out_parts)

def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

# =============================================================================
# LOADERS (COM CACHE)
# =============================================================================
@lru_cache(maxsize=32)
def _load_booking_cached(hash_key: str, xlsx_bytes: bytes,
                         selected_ym_tuple: tuple, selected_emb_tuple: tuple) -> pd.DataFrame:
    """Versão cacheável do load_booking_df"""
    return load_booking_df(xlsx_bytes, list(selected_ym_tuple), list(selected_emb_tuple))

def load_booking_df(xlsx_bytes: bytes,
                    selected_ym_list: Optional[List[str]] = None,
                    selected_embarcadores: Optional[List[str]] = None) -> pd.DataFrame:
    sheets = parse_excel_bytes(xlsx_bytes)
    df_all = pd.concat(sheets.values(), ignore_index=True)

@lru_cache(maxsize=64)
def _load_multi_cached(hash_key: str,
                       xlsx_bytes: bytes,
                       selected_ym_tuple: tuple,
                       selected_emb_tuple: tuple) -> pd.DataFrame:
    # Repassa como listas para a função real
    return load_multi_df(xlsx_bytes, list(selected_ym_tuple), list(selected_emb_tuple))

@lru_cache(maxsize=64)
def _load_transp_cached(hash_key: str,
                        xlsx_bytes: bytes,
                        selected_ym_tuple: tuple,
                        selected_emb_tuple: tuple) -> pd.DataFrame:
    return load_transp_df(xlsx_bytes, list(selected_ym_tuple), list(selected_emb_tuple))


    col_status    = ensure_col(df_all, CANDS_BOOKING_STAT)
    col_dt        = ensure_col(df_all, CANDS_BOOKING_DT)
    col_emb       = ensure_col(df_all, CANDS_BOOKING_EMB)
    col_qtd       = ensure_col(df_all, CANDS_BOOKING_QTD)
    col_booking_id= ensure_col(df_all, CANDS_BOOKING_ID)
    col_porto_orig= ensure_col(df_all, CANDS_BOOKING_PORT_ORIG)
    col_porto_dest= ensure_col(df_all, CANDS_BOOKING_PORT_DEST)

    if not col_dt or not col_emb or not col_qtd:
        return pd.DataFrame(columns=["ym","embarcador","booking_id","porto_origem","porto_destino","qtde"])

    df_all["__ym"] = df_all[col_dt].apply(extract_period_ym)

    if col_status:
        df_all = df_all[df_all[col_status].astype(str).str.strip().str.lower() == "ativo"]

    if selected_embarcadores:
        mask = pd.Series([False]*len(df_all), index=df_all.index)
        for emb in selected_embarcadores:
            mask |= df_all[col_emb].apply(lambda v: client_match(emb, str(v)))
        df_all = df_all[mask]

    if selected_ym_list:
        df_all = df_all[df_all["__ym"].isin(selected_ym_list)]

    df_all = df_all[~df_all["__ym"].isna()]
    df_all["__qtde"] = df_all[col_qtd].apply(safe_int)

    if not col_booking_id:
        col_booking_id = "__generated_booking_id"
        df_all[col_booking_id] = (
            df_all["__ym"].astype(str) + "|" + df_all["__qtde"].astype(str) + "|row" + df_all.index.astype(str)
        )

    if not col_porto_orig:
        col_porto_orig = "__porto_orig_dummy"; df_all[col_porto_orig] = ""
    if not col_porto_dest:
        col_porto_dest = "__porto_dest_dummy"; df_all[col_porto_dest] = ""

    records = []
    embarcadores_str = ",".join(selected_embarcadores) if selected_embarcadores else ""
    for (ym_val, bid), sub in df_all.groupby(["__ym", col_booking_id], dropna=False):
        total_qtde = sub["__qtde"].sum()
        best_row = sub.sort_values("__qtde", ascending=False).iloc[0]
        records.append({
            "ym": ym_val,
            "booking_id": bid,
            "porto_origem": str(best_row[col_porto_orig]).strip(),
            "porto_destino": str(best_row[col_porto_dest]).strip(),
            "qtde": int(total_qtde),
            "embarcador": embarcadores_str
        })
    return pd.DataFrame(records, columns=["ym","booking_id","porto_origem","porto_destino","qtde","embarcador"]).reset_index(drop=True)

def load_multi_df(xlsx_bytes: bytes,
                  selected_ym_list: Optional[List[str]] = None,
                  selected_embarcadores: Optional[List[str]] = None) -> pd.DataFrame:
    sheets = parse_excel_bytes(xlsx_bytes)
    if not sheets:
        return pd.DataFrame(columns=["__ym", "porto_op", "tipo_operacao", "motivo_reagenda", "flag"])

    df_all = pd.concat(sheets.values(), ignore_index=True).replace("-", "").fillna("")

    col_cliente   = ensure_col(df_all, CANDS_MULTI_CLIENTE)
    col_causador  = ensure_col(df_all, CANDS_MULTI_CAUSADOR)
    col_area_resp = ensure_col(df_all, CANDS_MULTI_AREA)
    col_just      = ensure_col(df_all, CANDS_MULTI_JUST)
    col_agendamento = None
    for term in ["agendamento","data agendamento","ultima alteracao"]:
        col_agendamento = col_agendamento or first_existing_col(df_all, [term])
    col_agendamento = col_agendamento or ensure_col(df_all, CANDS_MULTI_DT)
    col_porto   = ensure_col(df_all, CANDS_MULTI_PORTO)
    col_tipoop  = ensure_col(df_all, CANDS_MULTI_TIPO_OP)

    if col_agendamento:
        df_all["__ym"] = df_all[col_agendamento].apply(extract_period_ym)
    else:
        df_all["__ym"] = None

    if selected_ym_list:
        df_all = df_all[df_all["__ym"].isin(selected_ym_list)]

    if selected_embarcadores and col_cliente:
        mask = pd.Series([False]*len(df_all), index=df_all.index)
        for emb in selected_embarcadores:
            mask |= df_all[col_cliente].astype(str).apply(lambda v: client_match(emb, v))
        df_all = df_all[mask]

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

    # Aplicar normalização de justificativa
    df_valid["motivo_reagenda"] = just_norm_series.loc[df_valid.index].apply(normalize_justificativa)
    df_valid["porto_op"] = df_valid[col_porto].astype(str).str.strip() if col_porto else ""
    df_valid["tipo_operacao"] = df_valid[col_tipoop].astype(str).str.strip() if col_tipoop else ""
    df_valid["flag"] = 1

    return df_valid[["__ym", "porto_op", "tipo_operacao", "motivo_reagenda", "flag"]].reset_index(drop=True)

def load_transp_df(xlsx_bytes: bytes,
                   selected_ym_list: Optional[List[str]] = None,
                   selected_embarcadores: Optional[List[str]] = None) -> pd.DataFrame:
    sheets = parse_excel_bytes(xlsx_bytes)
    if not sheets:
        return pd.DataFrame(columns=["tipo_norm","justificativa_atraso","__ym","porto_origem"])

    df_all = pd.concat(sheets.values(), ignore_index=True)

    col_embarc        = ensure_col(df_all, CANDS_TRANSP_EMB)
    col_situacao_prog = ensure_col(df_all, CANDS_TRANSP_SIT_PROG)
    col_situacao_prazo= ensure_col(df_all, CANDS_TRANSP_SIT_PRAZO)
    col_tipo_prog     = ensure_col(df_all, CANDS_TRANSP_TIPO)
    col_dt_ref        = ensure_col(df_all, CANDS_TRANSP_DT_REF)
    col_just_transp   = ensure_col(df_all, CANDS_TRANSP_JUST)
    col_porto_orig    = ensure_col(df_all, CANDS_TRANSP_PORTO_ORIG)

    if col_dt_ref:
        df_all["__ym"] = df_all[col_dt_ref].apply(extract_period_ym)
    else:
        df_all["__ym"] = None

    if selected_ym_list:
        df_all = df_all[df_all["__ym"].isin(selected_ym_list)]

    if selected_embarcadores and col_embarc:
        mask = pd.Series([False]*len(df_all), index=df_all.index)
        for emb in selected_embarcadores:
            mask |= df_all[col_embarc].astype(str).apply(lambda v: client_match(emb, v))
        df_all = df_all[mask]

    if col_situacao_prog:
        df_all = df_all[~df_all[col_situacao_prog].astype(str).str.lower().str.contains("cancelad", na=False)]
    if col_situacao_prazo:
        df_all = df_all[df_all[col_situacao_prazo].astype(str).str.strip().str.lower() == "atrasado"]

    if not col_tipo_prog:
        return pd.DataFrame(columns=["tipo_norm","justificativa_atraso","__ym","porto_origem"])

    df_all["tipo_norm"] = df_all[col_tipo_prog].astype(str).str.strip().str.lower()

    # Aplicar normalização de justificativa
    if col_just_transp:
        df_all["justificativa_atraso"] = df_all[col_just_transp].apply(normalize_justificativa)
    else:
        df_all["justificativa_atraso"] = "Sem justificativa"

    df_all["porto_origem"] = df_all[col_porto_orig].astype(str).str.strip() if col_porto_orig else ""
    df_all = df_all.drop_duplicates()

    return df_all[["tipo_norm","justificativa_atraso","__ym","porto_origem"]].reset_index(drop=True)

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
# GRÁFICOS (DPI reduzido para performance)
# =============================================================================
def _save_fig_to_b64(fig) -> str:
    buf = io.BytesIO()
    fig.savefig(buf, format='png', bbox_inches='tight', dpi=100)  # Reduzido de 140 para 100
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode('ascii')

def safe_format_value(val) -> str:
    if val is None or (isinstance(val, float) and (math.isnan(val) or math.isinf(val))):
        return "Sem evidência"
    if val == "" or str(val).strip() == "":
        return "Sem evidência"
    return str(val)

def chart_movimentacao_por_porto(booking_df: pd.DataFrame, yms: List[str]) -> str:
    if booking_df.empty or not yms:
        return ""
    pivot = booking_df.groupby(["porto_origem", "ym"])["qtde"].sum().unstack(fill_value=0)
    pivot = pivot.reindex(columns=sorted(yms), fill_value=0)
    pivot["__total"] = pivot.sum(axis=1)
    pivot = pivot.sort_values("__total", ascending=False).drop("__total", axis=1)
    if len(pivot) == 0:
        return ""
    fig, ax = plt.subplots(figsize=(8, 5), dpi=100)
    x = np.arange(len(pivot.index))
    width = 0.8 / max(len(pivot.columns), 1)
    colors = ['#1f77b4','#ff7f0e','#2ca02c','#d62728','#9467bd','#8c564b']
    for i, col in enumerate(pivot.columns):
        vals = pivot[col].values
        offs = x + i * width
        ax.bar(offs, vals, width=width, label=format_ym_label(col), color=colors[i % len(colors)])
        for xx, val in zip(offs, vals):
            if val > 0:
                ax.text(xx, val + max(vals) * 0.02, str(int(val)), ha="center", va="bottom", fontsize=7, fontweight='bold')
    ax.set_xticks(x + width * (len(pivot.columns) - 1) / 2)
    ax.set_xticklabels([_wrap_label(p, 15) for p in pivot.index], rotation=0, fontsize=9)
    ax.set_ylabel("Quantidade de contêineres", fontsize=10)
    ax.set_title("Movimentação Mensal por Porto", fontsize=11, fontweight='bold', pad=10)
    ax.legend(fontsize=8, ncol=min(len(pivot.columns), 4), loc='upper right')
    ax.yaxis.set_major_locator(MaxNLocator(integer=True))
    ax.grid(axis='y', alpha=0.3, linestyle='--')
    fig.tight_layout()
    return _save_fig_to_b64(fig)

def chart_origem_destino(booking_df: pd.DataFrame) -> str:
    if booking_df.empty:
        return ""
    pivot = booking_df.groupby(["porto_origem", "porto_destino"])["qtde"].sum().unstack(fill_value=0)
    if len(pivot) == 0:
        return ""
    fig, ax = plt.subplots(figsize=(10, 6), dpi=120)
    data = []
    row_labels = []
    col_labels = list(pivot.columns)
    for origem in pivot.index:
        row = []
        for destino in pivot.columns:
            val = pivot.loc[origem, destino]
            row.append(int(val) if val > 0 else 0)
        data.append(row)
        row_labels.append(_wrap_label(str(origem), 12))
    table = ax.table(cellText=data, rowLabels=row_labels, colLabels=col_labels,
                     cellLoc='center', loc='center', bbox=[0, 0, 1, 1])
    table.auto_set_font_size(False)
    table.set_fontsize(9)
    table.scale(1, 2)
    for i in range(len(data)):
        for j in range(len(data[i])):
            if data[i][j] > 0:
                table[(i+1, j)].set_facecolor('#e6f2ff')
    ax.axis('off')
    ax.set_title("Origem x Destino", fontsize=12, fontweight='bold', pad=20)
    fig.tight_layout()
    return _save_fig_to_b64(fig)

def chart_atrasos_por_motivo_e_porto(transp_df: pd.DataFrame, tipo: str) -> str:
    if transp_df.empty:
        return ""
    df_tipo = transp_df[transp_df["tipo_norm"] == tipo]
    if df_tipo.empty:
        return ""
    grouped = df_tipo.groupby(["justificativa_atraso", "porto_origem"]).size().reset_index(name="count")
    top_motivos = grouped.groupby("justificativa_atraso")["count"].sum().nlargest(8).index
    grouped = grouped[grouped["justificativa_atraso"].isin(top_motivos)]
    pivot = grouped.pivot_table(index="justificativa_atraso", columns="porto_origem", values="count", fill_value=0)
    pivot["__total"] = pivot.sum(axis=1)
    pivot = pivot.sort_values("__total", ascending=True).drop("__total", axis=1)
    if len(pivot) == 0:
        return ""
    fig, ax = plt.subplots(figsize=(8, 4.5), dpi=100)
    colors = ['#1f77b4','#ff7f0e','#2ca02c','#d62728','#9467bd']
    x = np.arange(len(pivot))
    width = 0.8 / len(pivot.columns) if len(pivot.columns) > 0 else 0.8
    for i, (col_name, col_data) in enumerate(pivot.items()):
        offset = x + (i - len(pivot.columns)/2 + 0.5) * width
        ax.barh(offset, col_data.values, width * 0.95, label=safe_format_value(col_name), color=colors[i % len(colors)])
        for pos, val in zip(offset, col_data.values):
            if val > 0:
                ax.text(val + max(col_data.values) * 0.02, pos, str(int(val)), ha="left", va="center", fontsize=6)
    ax.set_yticks(x)
    ax.set_yticklabels([_wrap_label(str(lbl), 30) for lbl in pivot.index], fontsize=8)
    ax.set_xlabel("Ocorrências", fontsize=9)
    ax.set_ylabel("")
    ax.set_title(f"Atrasos em {tipo.capitalize()} - Total: {int(pivot.sum().sum())} ocorrências", fontsize=11, fontweight='bold')
    ax.legend(title="Porto", fontsize=7, title_fontsize=8, loc='lower right')
    ax.grid(axis='x', alpha=0.3, linestyle='--')
    fig.tight_layout()
    return _save_fig_to_b64(fig)

def chart_reagendamentos_por_causa_e_porto(multi_df: pd.DataFrame) -> str:
    if multi_df.empty:
        return ""
    grouped = multi_df.groupby(["motivo_reagenda", "porto_op"])["flag"].sum().reset_index()
    top_motivos = grouped.groupby("motivo_reagenda")["flag"].sum().nlargest(8).index
    grouped = grouped[grouped["motivo_reagenda"].isin(top_motivos)]
    pivot = grouped.pivot_table(index="motivo_reagenda", columns="porto_op", values="flag", fill_value=0)
    pivot["__total"] = pivot.sum(axis=1)
    pivot = pivot.sort_values("__total", ascending=True).drop("__total", axis=1)
    if len(pivot) == 0:
        return ""
    fig, ax = plt.subplots(figsize=(8, 4.5), dpi=100)
    colors = ['#1f77b4','#ff7f0e','#2ca02c','#d62728','#9467bd']
    x = np.arange(len(pivot))
    width = 0.8 / len(pivot.columns) if len(pivot.columns) > 0 else 0.8
    for i, (col_name, col_data) in enumerate(pivot.items()):
        offset = x + (i - len(pivot.columns)/2 + 0.5) * width
        ax.barh(offset, col_data.values, width * 0.95, label=safe_format_value(col_name), color=colors[i % len(colors)])
        for pos, val in zip(offset, col_data.values):
            if val > 0:
                ax.text(val + max(col_data.values) * 0.02, pos, str(int(val)), ha="left", va="center", fontsize=6)
    ax.set_yticks(x)
    ax.set_yticklabels([_wrap_label(str(lbl), 30) for lbl in pivot.index], fontsize=8)
    ax.set_xlabel("Quantidade de reagendamentos", fontsize=9)
    ax.set_ylabel("")
    ax.set_title(f"Reagendamentos - Total: {int(pivot.sum().sum())} ocorrências", fontsize=11, fontweight='bold')
    ax.legend(title="Porto", fontsize=7, title_fontsize=8, loc='lower right')
    ax.grid(axis='x', alpha=0.3, linestyle='--')
    fig.tight_layout()
    return _save_fig_to_b64(fig)

def generate_variacao_table(booking_df: pd.DataFrame, yms: List[str]) -> str:
    if booking_df.empty or len(yms) < 2:
        return ""
    yms_sorted = sorted(yms)
    pivot = booking_df.groupby(["porto_origem", "ym"])["qtde"].sum().unstack(fill_value=0)
    pivot = pivot.reindex(columns=yms_sorted, fill_value=0)
    html_rows = []
    for porto in pivot.index:
        row_data = {"Porto": porto}
        for i in range(len(yms_sorted)):
            row_data[format_ym_label(yms_sorted[i])] = int(pivot.loc[porto, yms_sorted[i]])
            if i > 0:
                curr = pivot.loc[porto, yms_sorted[i]]
                prev = pivot.loc[porto, yms_sorted[i-1]]
                if prev > 0:
                    variacao = ((curr - prev) / prev) * 100
                    row_data[f"Var_%_{i}"] = variacao
                else:
                    row_data[f"Var_%_{i}"] = 0 if curr == 0 else 100
        html_rows.append(row_data)
    html = '<table border="1" cellpadding="5" cellspacing="0" style="border-collapse:collapse;font-size:11px;">'
    html += '<thead><tr style="background-color:#1f77b4;color:white;"><th>Porto</th>'
    for i in range(len(yms_sorted)):
        html += f'<th>{format_ym_label(yms_sorted[i])}</th>'
        if i > 0:
            html += '<th>Variação %</th>'
    html += '</tr></thead><tbody>'
    for row in html_rows:
        html += '<tr>'
        html += f'<td><b>{row["Porto"]}</b></td>'
        for i in range(len(yms_sorted)):
            html += f'<td style="text-align:center;">{row[format_ym_label(yms_sorted[i])]}</td>'
            if i > 0:
                var = row[f"Var_%_{i}"]
                color = "#2ca02c" if var > 0 else "#d62728" if var < 0 else "#666"
                html += f'<td style="text-align:center;color:{color};font-weight:bold;">{var:+.1f}%</td>'
        html += '</tr>'
    html += '</tbody></table>'
    return html

# =============================================================================
# NOVAS FUNÇÕES - DETALHAMENTO POR PORTO
# =============================================================================
def generate_detalhamento_por_porto_html(df: pd.DataFrame, tipo: str, col_justificativa: str, col_porto: str) -> str:
    """
    Gera HTML com detalhamento de atrasos/reagendas por porto
    Exemplo:
    MAO (44 atrasos)
      • Falta de documento do cliente: 10
      • Problemas com o DEPOT: 3
      • Sem justificativa: 29
    """
    if df.empty:
        return "<p><i>Nenhum registro encontrado.</i></p>"

    html_parts = ["<div style='margin-top:20px;padding:15px;background:#f8f9fa;border-radius:8px;'>"]
    html_parts.append(f"<h4 style='color:#1976d2;margin-top:0;'>📍 Detalhamento por Porto</h4>")

    # Agrupar por porto
    grouped_porto = df.groupby(col_porto)

    for porto, grupo in grouped_porto:
        if not porto or str(porto).strip() == "":
            porto = "Porto não identificado"

        total_porto = len(grupo)
        html_parts.append(f"<div style='margin-bottom:20px;padding:12px;background:white;border-left:3px solid #1f77b4;'>")
        html_parts.append(f"<h5 style='color:#333;margin-top:0;'><b>{porto}</b> ({total_porto} {tipo})</h5>")
        html_parts.append("<ul style='margin:8px 0;padding-left:20px;'>")

        # Contar justificativas
        contagem = grupo[col_justificativa].value_counts()
        for justif, count in contagem.items():
            html_parts.append(f"<li style='margin:4px 0;'><b>{justif}</b>: {int(count)}</li>")

        html_parts.append("</ul></div>")

    html_parts.append("</div>")
    return "".join(html_parts)

def generate_tendencias_movimentacao_html(booking_df: pd.DataFrame, yms: List[str]) -> str:
    """
    Gera seção de Tendências de Movimentação com análise de crescimento
    """
    if booking_df.empty or len(yms) < 2:
        return "<p><i>Dados insuficientes para análise de tendências (mínimo 2 períodos).</i></p>"

    yms_sorted = sorted(yms)
    volumes = []
    for ym in yms_sorted:
        vol = booking_df[booking_df['ym'] == ym]['qtde'].sum()
        volumes.append(int(vol))

    # Calcular variação total
    variacao_total = ((volumes[-1] - volumes[0]) / volumes[0] * 100) if volumes[0] > 0 else 0
    tendencia_texto = "crescimento" if variacao_total > 0 else "queda" if variacao_total < 0 else "estabilidade"
    cor_tendencia = "#2ca02c" if variacao_total > 0 else "#d62728" if variacao_total < 0 else "#666"

    html_parts = ["<div style='background:#e3f2fd;border-left:4px solid #1976d2;padding:20px;margin:32px 0;border-radius:8px;'>"]
    html_parts.append("<h3 style='color:#1976d2;margin-top:0;'>📈 Tendências de Movimentação</h3>")

    # Resumo executivo
    html_parts.append(f"<p style='font-size:15px;'><b>Análise do período:</b> {format_periodos_label(yms)}</p>")
    html_parts.append(f"<p style='font-size:15px;'>O volume de operações apresentou <b style='color:{cor_tendencia};'>{tendencia_texto} de {abs(variacao_total):.1f}%</b> no período analisado.</p>")

    # Detalhamento mensal
    html_parts.append("<table style='width:100%;border-collapse:collapse;margin-top:15px;'>")
    html_parts.append("<tr style='background:#1976d2;color:white;'><th style='padding:10px;'>Período</th><th>Volume (TEUs)</th><th>Variação</th></tr>")

    for i, ym in enumerate(yms_sorted):
        label_mes = format_ym_label(ym)
        vol = volumes[i]

        if i == 0:
            var_texto = "-"
        else:
            var = ((volumes[i] - volumes[i-1]) / volumes[i-1] * 100) if volumes[i-1] > 0 else 100
            cor = "#2ca02c" if var > 0 else "#d62728" if var < 0 else "#666"
            var_texto = f"<span style='color:{cor};font-weight:bold;'>{var:+.1f}%</span>"

        bg = "#fff" if i % 2 == 0 else "#f8f9fa"
        html_parts.append(f"<tr style='background:{bg};'><td style='padding:8px;'><b>{label_mes}</b></td><td style='text-align:center;'>{vol}</td><td style='text-align:center;'>{var_texto}</td></tr>")

    html_parts.append("</table>")

    # Análise por porto
    if not booking_df.empty:
        html_parts.append("<h4 style='margin-top:20px;'>🎯 Portos em Destaque</h4>")

        # Porto com maior crescimento
        porto_crescimento = {}
        for porto in booking_df['porto_origem'].unique():
            df_porto = booking_df[booking_df['porto_origem'] == porto]
            vol_inicio = df_porto[df_porto['ym'] == yms_sorted[0]]['qtde'].sum()
            vol_fim = df_porto[df_porto['ym'] == yms_sorted[-1]]['qtde'].sum()
            if vol_inicio > 0:
                crescimento = ((vol_fim - vol_inicio) / vol_inicio * 100)
                porto_crescimento[porto] = crescimento

        if porto_crescimento:
            top_crescimento = max(porto_crescimento.items(), key=lambda x: x[1])
            top_queda = min(porto_crescimento.items(), key=lambda x: x[1])

            html_parts.append(f"<ul style='margin-top:10px;'>")
            html_parts.append(f"<li>🔹 <b>Maior crescimento:</b> {top_crescimento[0]} ({top_crescimento[1]:+.1f}%)</li>")
            html_parts.append(f"<li>🔻 <b>Maior queda:</b> {top_queda[0]} ({top_queda[1]:+.1f}%)</li>")
            html_parts.append("</ul>")

    html_parts.append("</div>")
    return "".join(html_parts)

def generate_alinhamento_operacional_html(kpis: Dict, transp_df: pd.DataFrame, multi_df: pd.DataFrame) -> str:
    """
    Gera seção de Considerações e Alinhamento Operacional
    """
    html_parts = ["<div style='background:#fff3e0;border-left:4px solid #ef6c00;padding:20px;margin:32px 0;border-radius:8px;'>"]
    html_parts.append("<h3 style='color:#ef6c00;margin-top:0;'>🎯 Considerações e Alinhamento Operacional</h3>")

    total_ops = kpis.get('total_ops', 0)
    atrasos_coleta = kpis.get('atrasos_coleta', 0)
    atrasos_entrega = kpis.get('atrasos_entrega', 0)
    reagendamentos = kpis.get('reagendamentos', 0)

    # Calcular taxas
    if total_ops > 0:
        taxa_atraso_coleta = (atrasos_coleta / total_ops) * 100
        taxa_atraso_entrega = (atrasos_entrega / total_ops) * 100
    else:
        taxa_atraso_coleta = 0
        taxa_atraso_entrega = 0

    if not multi_df.empty:
        total_operacoes_multi = len(multi_df)
        taxa_reagendamento = (reagendamentos / total_operacoes_multi) * 100 if total_operacoes_multi > 0 else 0
    else:
        taxa_reagendamento = 0

    # Status de Coletas
    html_parts.append("<h4 style='color:#333;'>📦 Status de Coletas</h4>")
    html_parts.append(f"<p style='font-size:14px;margin:8px 0;'>")
    html_parts.append(f"• Total de atrasos: <b style='color:#d32f2f;'>{atrasos_coleta}</b><br>")
    html_parts.append(f"• Taxa de atraso: <b style='color:#d32f2f;'>{taxa_atraso_coleta:.2f}%</b><br>")

    if not transp_df.empty:
        df_coleta = transp_df[transp_df['tipo_norm'] == 'coleta']
        if not df_coleta.empty:
            top_causa_coleta = df_coleta['justificativa_atraso'].value_counts().iloc[0]
            top_causa_nome = df_coleta['justificativa_atraso'].value_counts().index[0]
            html_parts.append(f"• Principal causa: <b>{top_causa_nome}</b> ({int(top_causa_coleta)} ocorrências)<br>")

    html_parts.append("</p>")

    # Status de Entregas
    html_parts.append("<h4 style='color:#333;'>🚚 Status de Entregas</h4>")
    html_parts.append(f"<p style='font-size:14px;margin:8px 0;'>")
    html_parts.append(f"• Total de atrasos: <b style='color:#d32f2f;'>{atrasos_entrega}</b><br>")
    html_parts.append(f"• Taxa de atraso: <b style='color:#d32f2f;'>{taxa_atraso_entrega:.2f}%</b><br>")

    if not transp_df.empty:
        df_entrega = transp_df[transp_df['tipo_norm'] == 'entrega']
        if not df_entrega.empty:
            top_causa_entrega = df_entrega['justificativa_atraso'].value_counts().iloc[0]
            top_causa_nome_ent = df_entrega['justificativa_atraso'].value_counts().index[0]
            html_parts.append(f"• Principal causa: <b>{top_causa_nome_ent}</b> ({int(top_causa_entrega)} ocorrências)<br>")

    html_parts.append("</p>")

    # Reagendamentos
    html_parts.append("<h4 style='color:#333;'>🔄 Reagendamentos</h4>")
    html_parts.append(f"<p style='font-size:14px;margin:8px 0;'>")
    html_parts.append(f"• Total de reagendamentos: <b style='color:#f57c00;'>{reagendamentos}</b><br>")
    html_parts.append(f"• Taxa de reagendamento: <b style='color:#f57c00;'>{taxa_reagendamento:.2f}%</b><br>")

    if not multi_df.empty:
        top_causa_reag = multi_df['motivo_reagenda'].value_counts().iloc[0]
        top_causa_nome_reag = multi_df['motivo_reagenda'].value_counts().index[0]
        html_parts.append(f"• Principal causa: <b>{top_causa_nome_reag}</b> ({int(top_causa_reag)} ocorrências)<br>")

    html_parts.append("</p>")

    # Recomendações Práticas
    html_parts.append("<h4 style='color:#333;margin-top:20px;'>💡 Ações Recomendadas</h4>")
    html_parts.append("<ul style='font-size:14px;line-height:1.8;'>")

    if taxa_atraso_coleta > 5:
        html_parts.append("<li>🔸 <b>Coletas:</b> Implementar checklist de documentação prévia e agendar follow-up 24h antes</li>")

    if taxa_atraso_entrega > 5:
        html_parts.append("<li>🔸 <b>Entregas:</b> Revisar janelas de entrega com clientes recorrentes e verificar disponibilidade de depots</li>")

    if taxa_reagendamento > 10:
        html_parts.append("<li>🔸 <b>Reagendamentos:</b> Intensificar comunicação entre transportador e terminal, estabelecer SLA de resposta</li>")

    html_parts.append("<li>🔸 <b>Monitoramento:</b> Acompanhamento semanal de indicadores e reunião mensal de performance</li>")
    html_parts.append("</ul>")

    html_parts.append("</div>")
    return "".join(html_parts)

# =============================================================================
# EMAIL (VERSÃO MELHORADA)
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

def generate_default_analise_geral(kpis: Dict, yms: List[str]) -> str:
    periodo = format_periodos_label(yms)
    total_ops = kpis.get('total_ops', 0)
    porto_top = kpis.get('porto_top', 'N/D')
    return (
        f"Durante o período de {periodo}, registramos um total de {total_ops} operações, "
        f"com o porto de {porto_top} apresentando o maior volume de movimentação. "
        f"A operação manteve-se dentro dos parâmetros esperados, com alguns pontos de atenção "
        f"identificados nos processos de coleta e entrega que serão detalhados a seguir."
    )

def generate_default_pontos_criticos(kpis: Dict, transp_df: pd.DataFrame, multi_df: pd.DataFrame) -> str:
    pontos = []
    atrasos_coleta = kpis.get('atrasos_coleta', 0)
    atrasos_entrega = kpis.get('atrasos_entrega', 0)
    reag = kpis.get('reagendamentos', 0)
    if atrasos_coleta > 0:
        pontos.append(f"• Atrasos de Coleta: {atrasos_coleta} ocorrências identificadas, impactando o início das operações")
    if atrasos_entrega > 0:
        pontos.append(f"• Atrasos de Entrega: {atrasos_entrega} casos registrados, afetando prazos acordados")
    if reag > 0:
        pontos.append(f"• Reagendamentos: {reag} reagendamentos atribuídos ao Mercosul, indicando necessidade de revisão de janelas")
    if not pontos:
        pontos.append("• Nenhum ponto crítico significativo identificado no período")
    return "\n".join(pontos)

def generate_default_recomendacoes(kpis: Dict) -> str:
    recs = []
    if kpis.get('atrasos_coleta', 0) > 0:
        recs.append("• Implementar checklist pré-coleta para validação de documentação antecipada")
    if kpis.get('atrasos_entrega', 0) > 0:
        recs.append("• Revisar janelas de entrega com clientes recorrentes para otimizar agendamentos")
    if kpis.get('reagendamentos', 0) > 0:
        recs.append("• Intensificar comunicação entre transportador e terminal para reduzir reagendamentos")
    recs.append("• Manter monitoramento contínuo dos indicadores operacionais")
    return "\n".join(recs)

def generate_ai_email_analysis(kpis: Dict[str, object], yms: List[str],
                               booking_df: pd.DataFrame, transp_df: pd.DataFrame,
                               multi_df: pd.DataFrame, embarcadores: List[str]) -> Dict[str, str]:
    if not gemini_model:
        return {
            'analise_geral': generate_default_analise_geral(kpis, yms),
            'pontos_criticos': generate_default_pontos_criticos(kpis, transp_df, multi_df),
            'recomendacoes': generate_default_recomendacoes(kpis),
            'conclusao': default_conclusao(kpis)
        }
    try:
        periodo_label = format_periodos_label(yms)
        emb_label = ", ".join(embarcadores)

        tendencia_volume = ""
        serie_volumes = ""
        if not booking_df.empty and len(yms) >= 2:
            yms_sorted = sorted(yms)
            volumes = []
            for ym in yms_sorted:
                vol = booking_df[booking_df['ym'] == ym]['qtde'].sum()
                volumes.append(int(vol))
            if len(volumes) >= 2:
                variacao = ((volumes[-1] - volumes[0]) / volumes[0] * 100) if volumes[0] > 0 else 0
                tendencia_volume = f"Variação: {variacao:+.1f}% ({volumes[0]} → {volumes[-1]} TEUs)"
                serie_volumes = " → ".join([f"{format_ym_label(yms_sorted[i])}: {volumes[i]}" for i in range(len(volumes))])

        top_atrasos_coleta = ""
        if not transp_df.empty:
            df_coleta = transp_df[transp_df['tipo_norm'] == 'coleta']
            if not df_coleta.empty:
                top_5 = df_coleta['justificativa_atraso'].value_counts().head(5)
                top_atrasos_coleta = "\n".join([f"  • {motivo}: {count} ocorrências" for motivo, count in top_5.items()])

        top_atrasos_entrega = ""
        if not transp_df.empty:
            df_entrega = transp_df[transp_df['tipo_norm'] == 'entrega']
            if not df_entrega.empty:
                top_5 = df_entrega['justificativa_atraso'].value_counts().head(5)
                top_atrasos_entrega = "\n".join([f"  • {motivo}: {count} ocorrências" for motivo, count in top_5.items()])

        top_reagendamentos = ""
        if not multi_df.empty:
            top_5_reag = multi_df['motivo_reagenda'].value_counts().head(5)
            top_reagendamentos = "\n".join([f"  • {motivo}: {int(count)} ocorrências" for motivo, count in top_5_reag.items()])

        portos_volume = ""
        if not booking_df.empty:
            porto_stats = booking_df.groupby('porto_origem')['qtde'].sum().sort_values(ascending=False)
            portos_volume = "\n".join([f"  • {porto}: {int(qtde)} TEUs" for porto, qtde in porto_stats.head(5).items()])

        prompt = f"""
Você é um analista sênior de operações logísticas com 15 anos de experiência. Analise os dados operacionais abaixo e gere um relatório executivo profissional em português.

=== CONTEXTO ===
Período: {periodo_label}
Cliente(s): {emb_label}

=== INDICADORES PRINCIPAIS ===
• Total de operações: {kpis.get('total_ops', 0)} TEUs
• Porto mais movimentado: {kpis.get('porto_top', 'N/D')}
• Porto com menor movimentação: {kpis.get('porto_low', 'N/D')}
• Atrasos de coleta: {kpis.get('atrasos_coleta', 0)}
• Atrasos de entrega: {kpis.get('atrasos_entrega', 0)}
• Reagendamentos (Mercosul): {kpis.get('reagendamentos', 0)}

=== TENDÊNCIA DE VOLUME ===
{tendencia_volume if tendencia_volume else "Dados insuficientes para análise de tendência"}
{serie_volumes if serie_volumes else ""}

=== DISTRIBUIÇÃO POR PORTO ===
{portos_volume if portos_volume else "Sem dados de distribuição"}

=== PRINCIPAIS MOTIVOS DE ATRASO - COLETA ===
{top_atrasos_coleta if top_atrasos_coleta else "Nenhum atraso de coleta registrado"}

=== PRINCIPAIS MOTIVOS DE ATRASO - ENTREGA ===
{top_atrasos_entrega if top_atrasos_entrega else "Nenhum atraso de entrega registrado"}

=== PRINCIPAIS MOTIVOS DE REAGENDAMENTO ===
{top_reagendamentos if top_reagendamentos else "Nenhum reagendamento registrado"}

=== TAREFA ===
Gere um relatório em 4 seções no formato abaixo. Seja específico, use os dados fornecidos e forneça insights acionáveis:

**SEÇÃO 1 - ANÁLISE GERAL DO PERÍODO**
[2-3 parágrafos analisando o desempenho operacional geral, tendências de volume, distribuição entre portos e comparação com períodos anteriores quando aplicável. Seja objetivo e baseado em dados.]

**SEÇÃO 2 - PONTOS CRÍTICOS IDENTIFICADOS**
[Liste 3-5 pontos críticos usando bullet points. Cada ponto deve incluir:
• Problema identificado
• Impacto quantificado
• Padrão observado
Use os dados de atrasos e reagendamentos fornecidos.]

**SEÇÃO 3 - RECOMENDAÇÕES E AÇÕES**
[Liste 3-5 recomendações práticas usando bullet points. Cada recomendação deve:
• Ser específica e acionável
• Estar ligada a um ponto crítico identificado
• Ter potencial de melhoria mensurável
• Ser realista de implementar]

**SEÇÃO 4 - CONCLUSÃO EXECUTIVA**
[2-3 frases resumindo o panorama geral e próximos passos prioritários.]

IMPORTANTE:
- Use linguagem profissional mas acessível
- Seja específico com números e porcentagens dos dados fornecidos
- NÃO invente dados - use apenas o que foi fornecido
- Foque em insights acionáveis
- Mantenha tom construtivo e orientado a soluções
"""
        response = gemini_model.generate_content(prompt)
        ai_text = response.text.strip()

        sections = {
            'analise_geral': '',
            'pontos_criticos': '',
            'recomendacoes': '',
            'conclusao': ''
        }

        current_section = None
        lines = ai_text.split('\n')
        for line in lines:
            line_lower = line.lower().strip()
            if 'seção 1' in line_lower or 'análise geral' in line_lower or 'analise geral' in line_lower:
                current_section = 'analise_geral'; continue
            elif 'seção 2' in line_lower or 'pontos críticos' in line_lower or 'pontos criticos' in line_lower:
                current_section = 'pontos_criticos'; continue
            elif 'seção 3' in line_lower or 'recomendações' in line_lower or 'recomendacoes' in line_lower:
                current_section = 'recomendacoes'; continue
            elif 'seção 4' in line_lower or 'conclusão' in line_lower or 'conclusao' in line_lower:
                current_section = 'conclusao'; continue
            if current_section and line.strip():
                clean_line = line.replace('**', '').strip()
                if clean_line and not clean_line.startswith('SEÇÃO'):
                    sections[current_section] += clean_line + '\n'

        for key in sections:
            sections[key] = sections[key].strip()

        if len(sections['analise_geral']) < 100:
            raise Exception("Resposta da IA muito curta")

        return sections

    except Exception as e:
        print(f"[WARN] Erro ao gerar análise com IA: {e}")
        return {
            'analise_geral': generate_default_analise_geral(kpis, yms),
            'pontos_criticos': generate_default_pontos_criticos(kpis, transp_df, multi_df),
            'recomendacoes': generate_default_recomendacoes(kpis),
            'conclusao': default_conclusao(kpis)
        }

def build_email_v2(kpis: Dict[str, object], yms: List[str], embarcadores: List[str],
                   booking_df: pd.DataFrame, transp_df: pd.DataFrame, multi_df: pd.DataFrame):
    label = format_periodos_label(yms)
    emb_label = ", ".join(embarcadores)

    ai_analysis = generate_ai_email_analysis(kpis, yms, booking_df, transp_df, multi_df, embarcadores)

    graf_movimentacao_b64 = chart_movimentacao_por_porto(booking_df, yms)
    graf_origem_dest_b64  = chart_origem_destino(booking_df)
    graf_atraso_col_b64   = chart_atrasos_por_motivo_e_porto(transp_df, "coleta")
    graf_atraso_ent_b64   = chart_atrasos_por_motivo_e_porto(transp_df, "entrega")
    graf_reag_b64         = chart_reagendamentos_por_causa_e_porto(multi_df)

    tabela_variacao_html  = generate_variacao_table(booking_df, yms)

    # NOVAS SEÇÕES
    tendencias_html = generate_tendencias_movimentacao_html(booking_df, yms)
    alinhamento_html = generate_alinhamento_operacional_html(kpis, transp_df, multi_df)

    # Detalhamento por porto
    df_coleta = transp_df[transp_df['tipo_norm'] == 'coleta'] if not transp_df.empty else pd.DataFrame()
    df_entrega = transp_df[transp_df['tipo_norm'] == 'entrega'] if not transp_df.empty else pd.DataFrame()

    detalhamento_coleta_html = generate_detalhamento_por_porto_html(
        df_coleta, "atrasos", "justificativa_atraso", "porto_origem"
    ) if not df_coleta.empty else ""

    detalhamento_entrega_html = generate_detalhamento_por_porto_html(
        df_entrega, "atrasos", "justificativa_atraso", "porto_origem"
    ) if not df_entrega.empty else ""

    detalhamento_reag_html = generate_detalhamento_por_porto_html(
        multi_df, "reagendamentos", "motivo_reagenda", "porto_op"
    ) if not multi_df.empty else ""

    total_ops = kpis["total_ops"]
    porto_top = safe_format_value(kpis["porto_top"])
    porto_low = safe_format_value(kpis["porto_low"])
    atrasos_coleta = kpis["atrasos_coleta"]
    atrasos_entrega = kpis["atrasos_entrega"]
    reag_total = kpis["reagendamentos"]

    txt_lines = [
        "Boa tarde,\n",
        f"Segue o Diário das Operações de {label}, referente ao(s) cliente(s): {emb_label}.\n",
        "(FONTE: QLIK + OPENTECH)\n",
        "=" * 60,
        "\n📊 INDICADORES DO PERÍODO",
        f"Total de operações: {total_ops} TEUs",
        f"Porto mais movimentado: {porto_top}",
        f"Porto com menor movimentação: {porto_low}",
        f"Atrasos de coleta: {atrasos_coleta}",
        f"Atrasos de entrega: {atrasos_entrega}",
        f"Reagendamentos (Mercosul): {reag_total}",
        "\n" + "=" * 60,
        "\n📈 ANÁLISE GERAL DO PERÍODO",
        ai_analysis['analise_geral'],
        "\n" + "=" * 60,
        "\n⚠️ PONTOS CRÍTICOS IDENTIFICADOS",
        ai_analysis['pontos_criticos'],
        "\n" + "=" * 60,
        "\n💡 RECOMENDAÇÕES E AÇÕES",
        ai_analysis['recomendacoes'],
        "\n" + "=" * 60,
        "\n✅ CONCLUSÃO EXECUTIVA",
        ai_analysis['conclusao'],
        "\n" + "=" * 60,
    ]
    txt_text = "\n".join(txt_lines)

    def img_tag(b64, title):
        if not b64:
            return ""
        return (
            f'<h4 style="margin:20px 0 12px 0;color:#1f77b4;border-left:4px solid #1f77b4;'
            f'padding-left:12px;">{title}</h4>'
            f'<img style="max-width:100%;display:block;margin-bottom:24px;border-radius:8px;'
            f'box-shadow:0 2px 8px rgba(0,0,0,0.1);" src="data:image/png;base64,{b64}" />'
        )

    html_parts = [
        "<p style='font-size:15px;'>Boa tarde,</p>",
        f"<p style='font-size:15px;'>Para garantirmos um serviço cada vez mais alinhado com as necessidades de nossos clientes, "
        f"compartilho o <b>Diário das Operações de {label}</b>, referente ao(s) cliente(s): <b>{emb_label}</b>.</p>",
        "<p style='font-size:14px;color:#666;'><i>(FONTE: QLIK + OPENTECH)</i></p>",

        "<div style='background:#f8f9fa;border-left:4px solid #1f77b4;padding:20px;margin:24px 0;border-radius:8px;'>",
        "<h3 style='color:#1f77b4;margin-top:0;'>📊 Indicadores do Período</h3>",
        "<table style='width:100%;border-collapse:collapse;'>",
        "<tr><td style='padding:8px;'><b>Total de operações:</b></td><td style='padding:8px;'>" + str(total_ops) + " TEUs</td></tr>",
        "<tr style='background:#fff;'><td style='padding:8px;'><b>Porto mais movimentado:</b></td><td style='padding:8px;'>" + porto_top + "</td></tr>",
        "<tr><td style='padding:8px;'><b>Porto com menor movimentação:</b></td><td style='padding:8px;'>" + porto_low + "</td></tr>",
        "<tr style='background:#fff;'><td style='padding:8px;'><b>Atrasos de coleta:</b></td><td style='padding:8px;color:#d32f2f;font-weight:bold;'>" + str(atrasos_coleta) + "</td></tr>",
        "<tr><td style='padding:8px;'><b>Atrasos de entrega:</b></td><td style='padding:8px;color:#d32f2f;font-weight:bold;'>" + str(atrasos_entrega) + "</td></tr>",
        "<tr style='background:#fff;'><td style='padding:8px;'><b>Reagendamentos:</b></td><td style='padding:8px;color:#f57c00;font-weight:bold;'>" + str(reag_total) + "</td></tr>",
        "</table></div>",

        # NOVA SEÇÃO: Tendências de Movimentação
        tendencias_html,

        img_tag(graf_movimentacao_b64, "📈 Movimentação Mensal - Comparativo por Porto"),
        "<h4 style='margin:20px 0 12px 0;color:#1f77b4;border-left:4px solid #1f77b4;padding-left:12px;'>📊 Tabela de Variação Mensal (%)</h4>",
        "<div style='overflow-x:auto;margin-bottom:24px;'>", tabela_variacao_html, "</div>",
        img_tag(graf_origem_dest_b64, "🗺️ Matriz Origem × Destino"),

        "<div style='background:#e3f2fd;border-left:4px solid #1976d2;padding:20px;margin:32px 0;border-radius:8px;'>",
        "<h3 style='color:#1976d2;margin-top:0;'>📈 Análise Geral do Período</h3>",
        f"<div style='font-size:15px;line-height:1.8;white-space:pre-wrap;'>{ai_analysis['analise_geral']}</div>",
        "</div>",

        "<h3 style='color:#d32f2f;margin-top:40px;'>⏱️ Atrasos</h3>",
        f"<p style='font-size:15px;'><b>Coletas (total):</b> <span style='color:#d32f2f;font-weight:bold;'>{atrasos_coleta}</span></p>",
        img_tag(graf_atraso_col_b64, "Atrasos em Coleta por Motivo e Porto"),
        detalhamento_coleta_html,  # NOVO: Lista por porto

        f"<p style='font-size:15px;margin-top:24px;'><b>Entregas (total):</b> <span style='color:#d32f2f;font-weight:bold;'>{atrasos_entrega}</span></p>",
        img_tag(graf_atraso_ent_b64, "Atrasos na Entrega por Motivo e Porto"),
        detalhamento_entrega_html,  # NOVO: Lista por porto

        "<h3 style='color:#f57c00;margin-top:40px;'>🔄 Reagendamentos</h3>",
        f"<p style='font-size:15px;'><b>Total no período:</b> <span style='color:#f57c00;font-weight:bold;'>{reag_total}</span></p>",
        img_tag(graf_reag_b64, "Reagendamentos por Causa e Porto"),
        detalhamento_reag_html,  # NOVO: Lista por porto

        # NOVA SEÇÃO: Considerações e Alinhamento Operacional
        alinhamento_html,

        "<div style='background:#ffebee;border-left:4px solid #c62828;padding:20px;margin:32px 0;border-radius:8px;'>",
        "<h3 style='color:#c62828;margin-top:0;'>⚠️ Pontos Críticos Identificados</h3>",
        f"<div style='font-size:14px;line-height:1.8;white-space:pre-wrap;'>{ai_analysis['pontos_criticos']}</div>",
        "</div>",

        "<div style='background:#e8f5e9;border-left:4px solid #2e7d32;padding:20px;margin:32px 0;border-radius:8px;'>",
        "<h3 style='color:#2e7d32;margin-top:0;'>💡 Recomendações e Ações</h3>",
        f"<div style='font-size:14px;line-height:1.8;white-space:pre-wrap;'>{ai_analysis['recomendacoes']}</div>",
        "</div>",

        "<div style='background:#fff3e0;border-left:4px solid #ef6c00;padding:20px;margin:32px 0;border-radius:8px;'>",
        "<h3 style='color:#ef6c00;margin-top:0;'>✅ Conclusão Executiva</h3>",
        f"<p style='font-size:15px;line-height:1.8;'>{ai_analysis['conclusao']}</p>",
        "</div>",

        "<p style='font-size:14px;color:#666;margin-top:40px;padding-top:20px;border-top:2px solid #e0e0e0;'>",
        "<i>Análise gerada automaticamente com inteligência artificial (Google Gemini) baseada em dados reais das operações.</i>",
        "</p>",
    ]
    html_full = '<div style="font-family:Segoe UI,Roboto,Arial,sans-serif;font-size:14px;color:#1a1a1a;line-height:1.6;max-width:900px;">' + "".join(html_parts) + "</div>"
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
# DB HELPERS
# =============================================================================
def get_latest_blob(client: str, ym: str, kind: str) -> Optional[bytes]:
    cache_key = f"{client}_{ym}_{kind}"
    if cache_key in cache:
        return cache[cache_key]

    with engine.begin() as conn:
        row = conn.execute(
            text("SELECT data FROM uploads WHERE client=:c AND ym=:y AND kind=:k ORDER BY id DESC LIMIT 1"),
            {"c": client, "y": ym, "k": kind},
        ).fetchone()

    if row:
        cache[cache_key] = row[0]
        return row[0]
    return None

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

    h_booking = sha256_bytes(b_booking)
    h_multi   = sha256_bytes(b_multi)
    h_transp  = sha256_bytes(b_transp)

    inserted = []
    skipped = []

    now = datetime.utcnow().isoformat()
    with engine.begin() as conn:
        for ym in periods_list:
            for kind, blob, h in [
                ("booking", b_booking, h_booking),
                ("multi",   b_multi,   h_multi),
                ("transp",  b_transp,  h_transp),
            ]:
                exists = conn.execute(
                    text("SELECT 1 FROM uploads WHERE client=:c AND ym=:y AND kind=:k AND hash=:h LIMIT 1"),
                    {"c": client, "y": ym, "k": kind, "h": h}
                ).fetchone()
                if exists:
                    skipped.append({"ym": ym, "kind": kind, "reason": "hash_igual"})
                    continue
                conn.execute(text("DELETE FROM uploads WHERE client=:c AND ym=:y AND kind=:k"),
                             {"c": client, "y": ym, "k": kind})
                conn.execute(text(
                    "INSERT INTO uploads (client,ym,kind,data,hash,created_at) "
                    "VALUES (:c,:y,:k,:d,:h,:t)"
                ), {"c": client, "y": ym, "k": kind, "d": blob, "h": h, "t": now})
                inserted.append({"ym": ym, "kind": kind})

                # Limpar cache
                cache_key = f"{client}_{ym}_{kind}"
                if cache_key in cache:
                    del cache[cache_key]

    return JSONResponse({
        "status": "ok",
        "periods": periods_list,
        "embarcadores": embarcadores_list,
        "inserted": inserted,
        "skipped": skipped
    })

@app.get("/api/summary")
def api_summary(client: str, ym: str = Query(...), embarcador: str = Query(...)):
    ym_list = [y.strip() for y in ym.split(",") if y.strip()]
    emb_list = [e.strip() for e in embarcador.split(",") if e.strip()]
    if not ym_list:
        raise HTTPException(status_code=400, detail="Nenhum período informado")
    if not emb_list:
        raise HTTPException(status_code=400, detail="Nenhum embarcador informado")

    # Cache de KPIs agregados por combinação (client, ym_list, emb_list)
    _ckey = f"{client}|{','.join(sorted(ym_list))}|{','.join(sorted(emb_list))}"
    cached = kpi_cache.get(_ckey)
    if cached:
        return JSONResponse(cached)

    booking_frames, multi_frames, transp_frames = [], [], []
    for y in ym_list:
        b_blob = get_latest_blob(client, y, "booking")
        m_blob = get_latest_blob(client, y, "multi")
        t_blob = get_latest_blob(client, y, "transp")
        if not b_blob or not m_blob or not t_blob:
            raise HTTPException(status_code=400, detail=f"Faltam planilhas p/ {y}.")
        # Use loaders com cache LRU baseados no hash do arquivo para evitar re-parsing
        h_b = sha256_bytes(b_blob)
        h_m = sha256_bytes(m_blob)
        h_t = sha256_bytes(t_blob)
        booking_frames.append(_load_booking_cached(h_b, b_blob, (y,), tuple(emb_list)))
        multi_frames.append(_load_multi_cached(h_m, m_blob, (y,), tuple(emb_list)))
        transp_frames.append(_load_transp_cached(h_t, t_blob, (y,), tuple(emb_list)))

    booking_concat = pd.concat(booking_frames, ignore_index=True) if booking_frames else pd.DataFrame()
    multi_concat   = pd.concat(multi_frames,   ignore_index=True) if multi_frames   else pd.DataFrame()
    transp_concat  = pd.concat(transp_frames,  ignore_index=True) if transp_frames  else pd.DataFrame()

    kpis = compute_kpis(booking_concat, multi_concat, transp_concat)
    debug_info = {
        "booking_len": len(booking_concat),
        "booking_sum_qtde": int(booking_concat["qtde"].sum()) if len(booking_concat) else 0,
        "transp_len": len(transp_concat),
        "multi_len": len(multi_concat),
    }
    kpi_cache[_ckey] = {"kpis": kpis, "debug": debug_info}
    return JSONResponse({"kpis": kpis, "debug": debug_info})

@app.post("/api/generate-email")
async def api_generate_email(payload: dict):
    client = payload.get("client")
    yms = payload.get("yms", [])
    embarcadores = payload.get("embarcadores", [])
    if not client or not yms or not embarcadores:
        raise HTTPException(status_code=400, detail="Campos obrigatórios ausentes.")

    booking_frames, multi_frames, transp_frames = [], [], []
    for y in yms:
        b_blob = get_latest_blob(client, y, "booking")
        m_blob = get_latest_blob(client, y, "multi")
        t_blob = get_latest_blob(client, y, "transp")
        if not b_blob or not m_blob or not t_blob:
            raise HTTPException(status_code=400, detail=f"Faltam planilhas p/ {y}.")
        h_b = sha256_bytes(b_blob); h_m = sha256_bytes(m_blob); h_t = sha256_bytes(t_blob)
        booking_frames.append(_load_booking_cached(h_b, b_blob, (y,), tuple(embarcadores)))
        multi_frames.append(_load_multi_cached(h_m, m_blob, (y,), tuple(embarcadores)))
        transp_frames.append(_load_transp_cached(h_t, t_blob, (y,), tuple(embarcadores)))

    booking_concat = pd.concat(booking_frames, ignore_index=True) if booking_frames else pd.DataFrame()
    multi_concat   = pd.concat(multi_frames,   ignore_index=True) if multi_frames   else pd.DataFrame()
    transp_concat  = pd.concat(transp_frames,  ignore_index=True) if transp_frames  else pd.DataFrame()

    kpis = compute_kpis(booking_concat, multi_concat, transp_concat)
    txt, html = build_email_v2(kpis, yms, embarcadores, booking_concat, transp_concat, multi_concat)

    return JSONResponse({"status": "ok", "email": txt, "email_html": html})

@app.post("/api/generate-eml-by")
async def api_generate_eml_by(payload: dict):
    client = payload.get("client")
    yms = payload.get("yms", [])
    embarcadores = payload.get("embarcadores", [])
    if not client or not yms or not embarcadores:
        raise HTTPException(status_code=400, detail="Campos obrigatórios ausentes.")

    booking_frames, multi_frames, transp_frames = [], [], []
    for y in yms:
        booking_frames.append(load_booking_df(get_latest_blob(client, y, "booking"), [y], embarcadores))
        multi_frames.append(load_multi_df(get_latest_blob(client, y, "multi"), [y], embarcadores))
        transp_frames.append(load_transp_df(get_latest_blob(client, y, "transp"), [y], embarcadores))

    booking_concat = pd.concat(booking_frames, ignore_index=True) if booking_frames else pd.DataFrame()
    multi_concat   = pd.concat(multi_frames,   ignore_index=True) if multi_frames   else pd.DataFrame()
    transp_concat  = pd.concat(transp_frames,  ignore_index=True) if transp_frames  else pd.DataFrame()

    kpis = compute_kpis(booking_concat, multi_concat, transp_concat)
    txt, html = build_email_v2(kpis, yms, embarcadores, booking_concat, transp_concat, multi_concat)

    emb_label = ", ".join(embarcadores)
    subject = f"Diário Operacional – {format_periodos_label(yms)} – {emb_label}"
    raw_eml = build_eml(subject, html, txt)
    b64 = base64.b64encode(raw_eml).decode("ascii")
    return JSONResponse({"status": "ok", "filename": "diario_operacional.eml", "file_b64": b64})

@app.get("/api/available-data")
def api_available_data(client: str = Query(..., description="Identificador do bucket/cliente")):
    """
    Retorna períodos e embarcadores disponíveis no banco para o cliente.
    Útil para auto-carregar dados ao abrir a aplicação sem precisar refazer upload.
    """
    if not client:
        raise HTTPException(status_code=400, detail="Informe ?client=...")

    try:
        with engine.begin() as conn:
            # Buscar períodos únicos onde temos dados completos (booking, multi, transp)
            periods_query = text("""
                SELECT DISTINCT ym
                FROM uploads
                WHERE client = :c
                  AND ym IN (
                      SELECT ym FROM uploads WHERE client = :c AND kind = 'booking'
                      INTERSECT
                      SELECT ym FROM uploads WHERE client = :c AND kind = 'multi'
                      INTERSECT
                      SELECT ym FROM uploads WHERE client = :c AND kind = 'transp'
                  )
                ORDER BY ym DESC
            """)
            periods_result = conn.execute(periods_query, {"c": client}).fetchall()
            periods = [row[0] for row in periods_result]

            # Se não há períodos, retorna vazio
            if not periods:
                return JSONResponse({
                    "status": "ok",
                    "has_data": False,
                    "periods": [],
                    "embarcadores": []
                })

            # Carregar embarcadores do período mais recente
            latest_period = periods[0]
            booking_blob = get_latest_blob(client, latest_period, "booking")

            if booking_blob:
                # Parsear booking para extrair embarcadores
                booking_sheets = parse_excel_bytes(booking_blob)
                if booking_sheets:
                    df_all = pd.concat(booking_sheets.values(), ignore_index=True)
                    col_emb = ensure_col(df_all, CANDS_BOOKING_EMB)
                    col_stat = ensure_col(df_all, CANDS_BOOKING_STAT)

                    if col_emb:
                        df_active = df_all[df_all[col_stat].astype(str).str.strip().str.lower() == "ativo"] if col_stat else df_all
                        embarcadores = sorted(df_active[col_emb].astype(str).str.strip().dropna().unique().tolist())
                    else:
                        embarcadores = []
                else:
                    embarcadores = []
            else:
                embarcadores = []

            return JSONResponse({
                "status": "ok",
                "has_data": True,
                "periods": periods,
                "embarcadores": embarcadores
            })

    except Exception as e:
        print(f"[ERROR] Falha ao buscar dados disponíveis: {str(e)}")
        return JSONResponse({
            "status": "error",
            "has_data": False,
            "periods": [],
            "embarcadores": [],
            "error": str(e)
        })

@app.delete("/api/flush")
def api_flush(client: str = Query(..., description="Identificador do bucket/cliente"),
              ym: Optional[str] = Query(None, description="Opcional: período YYYY-MM para limpar apenas esse mês")):
    if not client:
        raise HTTPException(status_code=400, detail="Informe ?client=...")

    with engine.begin() as conn:
        if ym:
            res = conn.execute(text("DELETE FROM uploads WHERE client=:c AND ym=:y"),
                               {"c": client, "y": ym})
            deleted = res.rowcount or 0
            detail = {"client": client, "ym": ym}
        else:
            res = conn.execute(text("DELETE FROM uploads WHERE client=:c"), {"c": client})
            deleted = res.rowcount or 0
            detail = {"client": client, "ym": None}

    # Limpar cache
    cache.clear()

    return JSONResponse({"status": "ok", "deleted": int(deleted), "detail": detail})

@app.get("/api/health")
def health():
    db_ok = False
    try:
        from sqlalchemy import text
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        db_ok = True
    except Exception:
        db_ok = False
    return {"ok": True, "db_ok": bool(db_ok), "cache_size": len(cache)}

@app.get("/api/clear-cache")
def clear_cache():
    """Endpoint para limpar cache manualmente"""
    cache.clear()
    return {"status": "ok", "message": "Cache limpo com sucesso"}