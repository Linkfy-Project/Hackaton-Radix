"""
RADAR RJ — Mapa Inteligente de Perfis de Carga e Geração Distribuída (MMGD)

✅ Ajustes aplicados (alinhado ao que você pediu):
1) DASHBOARD:
   - Remove TODAS as coisas de MMGD “BDGD” (real) do dashboard:
     * remove scatter BDGD vs Visão
     * remove ranking de discrepâncias (Δ)
     * remove colunas BDGD_MMGD_KW / MMGD_DELTA_KW / MMGD_RATIO_CV_VS_BDGD da tabela principal
   - Mantém SOMENTE MMGD da Visão Computacional no dashboard (área, qtd, conf, kW estimado, risco).
   - Adiciona análises “úteis” dos popups nas tabelas principais:
     * Perfil predominante (CLASSE_PRED, PERC_PRED, CARGA_TOTAL_KW)
     * MMGD visão (area_total_m2, qtd_paineis, conf_media, CV_MMGD_EST_KW)
     * Risco (RISCO_MMGD_RATIO, RISCO_MMGD_NIVEL) e capacidade (SUB_CAP_KW)

2) MAPA:
   - Overlay do “Mapa Inteligente” usa RISCO_MMGD_NIVEL (não densidade).
   - Mantém popup completo (com BDGD perfis de carga, que você NÃO pediu pra remover do popup).
   - Filtro por subestação funciona (passa filtro_subs corretamente).

3) SOLAR (Visão Computacional):
   - Adiciona parâmetros configuráveis na UI:
     * MAX_WORKERS_DOWNLOAD
     * MAX_TILES
     * ZOOM (quando aplicável)
     * (além de conf e COD_IDs)
   - Tenta repassar esses parâmetros para o pipeline de forma segura (se a função aceitar).

Obs:
- O popup ainda mostra BDGD_MMGD_KW se existir no GeoJSON, mas isso NÃO aparece no dashboard.
- Se você quiser remover BDGD do popup também, me fala e eu tiro em 2 blocos.
"""

from __future__ import annotations

import base64
import io
import inspect
import math
import os
import runpy
import time
import traceback
import zipfile
from pathlib import Path
from typing import List, Optional, Tuple, Dict

import numpy as np
import pandas as pd
import geopandas as gpd
import streamlit as st
import folium
from streamlit_folium import st_folium
import geobr
from folium.plugins import Fullscreen, MousePosition, MeasureControl, AntPath
from folium.features import DivIcon

# Plotly é opcional
try:
    import plotly.express as px
except Exception:
    px = None

# -------------------------
# IMPORTS DO PIPELINE (se existirem no projeto)
# -------------------------
try:
    import extrator  # ETL geográfico
except Exception:
    extrator = None

consumo_etl = None
consumo_import_error = None
try:
    import extrair_estatisticas_consumo as consumo_etl
except Exception:
    consumo_etl = None
    consumo_import_error = traceback.format_exc()

try:
    from Solar import solar_panels_rj_2stage as solar_pipeline
except Exception:
    solar_pipeline = None


# -------------------------
# CONFIG / PATHS
# -------------------------
PASTA_BRUTOS = "Dados Brutos"
PASTA_PROC = "Dados Processados"

ARQUIVO_UNIFICADO = os.path.join(PASTA_PROC, "dados_finais_rj.geojson")
ARQUIVO_PERFIS = os.path.join(PASTA_PROC, "perfis_consumo.csv")
ARQUIVO_SOLAR_DETECCOES = os.path.join(PASTA_PROC, "solar_paineis_deteccoes.geojson")
ARQUIVO_SOLAR_RESUMO = os.path.join(PASTA_PROC, "solar_resumo_por_area.csv")

INSTRUCOES_BASE_COMPLETA = os.path.join(PASTA_PROC, "README_BASE_COMPLETA.txt")

# ENEL em azul (como você pediu)
CORES_DISTRIBUIDORA = {"LIGHT": "#ff4fa3", "ENEL": "#1e88e5"}  # rosa e azul

ASSETS_DIR = Path(__file__).parent / "assets"
ICONS_DIR = ASSETS_DIR / "icons"

st.set_page_config(page_title="RADAR RJ — Perfis & MMGD", layout="wide")

os.makedirs(PASTA_BRUTOS, exist_ok=True)
os.makedirs(PASTA_PROC, exist_ok=True)


# -------------------------
# HELPERS
# -------------------------
def run_etl_module(module, module_name: str) -> None:
    """
    Roda um módulo ETL de forma robusta:
      - chama main()/run()/pipeline()/executar()/etl()/processar() se existir
      - senão roda como script via runpy
    """
    if module is None:
        raise RuntimeError(f"Módulo {module_name} não importado.")

    for fn_name in ["main", "run", "pipeline", "executar", "etl", "processar"]:
        fn = getattr(module, fn_name, None)
        if callable(fn):
            fn()
            return

    # fallback: tenta achar uma única função sem args
    funcs = []
    for name, obj in inspect.getmembers(module, inspect.isfunction):
        if obj.__module__ == module.__name__:
            try:
                sig = inspect.signature(obj)
                if len(sig.parameters) == 0:
                    funcs.append((name, obj))
            except Exception:
                pass

    if len(funcs) == 1:
        funcs[0][1]()
        return

    runpy.run_module(module.__name__, run_name="__main__")


def _human_bytes(n: int) -> str:
    for unit in ["B", "KB", "MB", "GB"]:
        if n < 1024:
            return f"{n:.0f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


def _save_uploaded_file(uploaded, dst_path: str) -> str:
    os.makedirs(os.path.dirname(dst_path) or ".", exist_ok=True)
    with open(dst_path, "wb") as f:
        f.write(uploaded.getbuffer())
    return dst_path


def _extract_zip_to(zip_bytes: bytes, dst_dir: str) -> List[str]:
    os.makedirs(dst_dir, exist_ok=True)
    extracted = []
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        for member in z.infolist():
            if ".." in member.filename.replace("\\", "/"):
                continue
            z.extract(member, dst_dir)
            extracted.append(os.path.join(dst_dir, member.filename))
    return extracted


def ui_section(title: str, subtitle: str = ""):
    st.markdown(
        f"""
        <div style="
            border:1px solid rgba(0,0,0,.10);
            border-radius:14px;
            padding:14px 16px;
            margin:10px 0 18px 0;
            background: rgba(250,250,250,.85);
        ">
          <div style="font-size:16px; font-weight:700; margin-bottom:4px;">{title}</div>
          <div style="font-size:12px; color:rgba(0,0,0,.65);">{subtitle}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


# ✅ helpers que SEMPRE retornam Series numérica (nunca int escalar)
def _num_series(df: pd.DataFrame, col: str) -> pd.Series:
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return pd.Series(dtype="float64")
    if col not in df.columns:
        return pd.Series(np.zeros(len(df), dtype="float64"), index=df.index)
    return pd.to_numeric(df[col], errors="coerce").fillna(0.0).astype("float64")


def _num_sum(df: pd.DataFrame, col: str) -> float:
    s = _num_series(df, col)
    return float(s.sum()) if len(s) else 0.0


def _try_icon_file(stem: str) -> Optional[Path]:
    p_png = ICONS_DIR / f"{stem}.png"
    if p_png.exists():
        return p_png
    p_raw = ICONS_DIR / stem
    if p_raw.exists():
        return p_raw
    return None


def get_image_base64(path: Path) -> str:
    try:
        data = path.read_bytes()
        return base64.b64encode(data).decode("utf-8")
    except Exception:
        return ""


def potencia_to_icon_size(p_mva: float, min_px=18, max_px=44) -> int:
    try:
        p = float(p_mva or 0)
    except Exception:
        p = 0.0
    if p <= 0:
        return min_px
    s = min_px + (math.log10(p + 1) / math.log10(200 + 1)) * (max_px - min_px)
    return int(max(min_px, min(max_px, s)))


def normalize_classificacao_for_icon(classificacao_raw: str) -> str:
    s = str(classificacao_raw or "").strip()
    su = s.upper()
    if "PLENA" in su:
        return "raio"
    if "SAT" in su or "SATÉLITE" in su or "SATELITE" in su:
        return "satelite"
    if "TRANSFORMADORA" in su and "PURA" in su:
        return "transformador"
    if "TRANSPORTE" in su or "MANOBRA" in su:
        return "torre"
    return "raio"


def make_png_marker_icon(classificacao: str, potencia_mva: float, distribuidora: str | None = None) -> DivIcon:
    stem = normalize_classificacao_for_icon(classificacao)
    icon_path = _try_icon_file(stem)

    size = potencia_to_icon_size(potencia_mva)
    dist = (str(distribuidora or "").upper()).strip()
    badge = CORES_DISTRIBUIDORA.get(dist, "#999999")

    if icon_path is None:
        return DivIcon(html=f"""
        <div style="
            width:{size}px;height:{size}px;display:flex;align-items:center;justify-content:center;
            border-radius:999px;background:{badge};color:#fff;font-weight:700;
            box-shadow:0 1px 3px rgba(0,0,0,.35);
        ">●</div>
        """)

    b64 = get_image_base64(icon_path)
    if not b64:
        return DivIcon(html=f"""
        <div style="
            width:{size}px;height:{size}px;display:flex;align-items:center;justify-content:center;
            border-radius:999px;background:{badge};color:#fff;font-weight:700;
            box-shadow:0 1px 3px rgba(0,0,0,.35);
        ">●</div>
        """)

    html = f"""
    <div style="
        width:{size}px; height:{size}px; position: relative;
        display:flex; align-items:center; justify-content:center;
    ">
      <div style="
        position:absolute; inset:0;
        border-radius: 999px;
        background: {badge};
        opacity: 0.85;
        filter: drop-shadow(0 1px 2px rgba(0,0,0,.35));
      "></div>
      <img src="data:image/png;base64,{b64}"
           style="width:{int(size*0.86)}px; height:{int(size*0.86)}px; object-fit:contain;
                  position: relative; z-index: 2; " />
    </div>
    """
    return DivIcon(html=html)


def add_map_legend(m: folium.Map):
    legend_html = f"""
    <div style="
        position: fixed; bottom: 25px; left: 25px; z-index: 9999;
        background: rgba(20,20,20,0.88); color: #fff;
        padding: 12px 14px; border-radius: 10px; font-size: 12px; width: 320px;
        box-shadow: 0 6px 18px rgba(0,0,0,0.35);
    ">
      <div style="font-weight:800; margin-bottom:8px;">Legenda</div>

      <div style="font-weight:700; margin:6px 0 6px;">Distribuidora</div>
      <div style="display:flex; gap:10px; align-items:center; flex-wrap:wrap;">
        <div><span style="display:inline-block;width:12px;height:12px;background:{CORES_DISTRIBUIDORA.get('LIGHT','#ff4fa3')};margin-right:6px;border-radius:3px;"></span>LIGHT</div>
        <div><span style="display:inline-block;width:12px;height:12px;background:{CORES_DISTRIBUIDORA.get('ENEL','#1e88e5')};margin-right:6px;border-radius:3px;"></span>ENEL</div>
      </div>

      <hr style="border:none;border-top:1px solid rgba(255,255,255,0.18); margin:10px 0;">

     <div style="font-weight:700; margin-bottom:6px;">Risco MMGD (Visão/SE)</div>
     <div><span style="display:inline-block;width:12px;height:12px;background:#BDBDBD;margin-right:8px;border-radius:2px;"></span>Sem dados / Sem detecção</div>
     <div><span style="display:inline-block;width:12px;height:12px;background:#2ecc71;margin-right:8px;border-radius:2px;"></span>Baixo (&lt; 10%)</div>
     <div><span style="display:inline-block;width:12px;height:12px;background:#f39c12;margin-right:8px;border-radius:2px;"></span>Médio (10–25%)</div>
     <div><span style="display:inline-block;width:12px;height:12px;background:#e74c3c;margin-right:8px;border-radius:2px;"></span>Alto (&gt; 25%)</div>

      <hr style="border:none;border-top:1px solid rgba(255,255,255,0.18); margin:10px 0;">

      <div style="font-weight:700; margin-bottom:6px;">Ícones</div>
      <div>⚡ Distribuição Plena</div>
      <div>📡 Distribuição Satélite</div>
      <div>🔄 Transformadora Pura</div>
      <div>🏗️ Transporte/Manobra</div>
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend_html))


# -------------------------
# LOADERS
# -------------------------
@st.cache_data(show_spinner=False)
def load_rj_shape() -> gpd.GeoDataFrame:
    rj_shape = geobr.read_state(code_state="RJ", year=2020)
    return rj_shape.to_crs("EPSG:4326")


@st.cache_data(show_spinner=False)
def load_unificado(path: str) -> Optional[gpd.GeoDataFrame]:
    if not os.path.exists(path):
        return None
    gdf = gpd.read_file(path)
    if gdf.crs is None:
        gdf = gdf.set_crs("EPSG:4326", allow_override=True)
    else:
        gdf = gdf.to_crs("EPSG:4326")
    return gdf


@st.cache_data(show_spinner=False)
def load_perfis(path: str) -> Optional[pd.DataFrame]:
    if not os.path.exists(path):
        return None
    try:
        return pd.read_csv(path, sep=";")
    except Exception:
        return pd.read_csv(path)


@st.cache_data(show_spinner=False)
def load_solar_resumo(path: str) -> Optional[pd.DataFrame]:
    if not os.path.exists(path):
        return None
    return pd.read_csv(path)


# -------------------------
# ENRIQUECIMENTO / FEATURES
# -------------------------
def geodesic_area_km2(gdf: gpd.GeoDataFrame, geom_col="geometry") -> pd.Series:
    try:
        from pyproj import Geod
        geod = Geod(ellps="WGS84")

        def _area_one(geom) -> float:
            if geom is None or geom.is_empty:
                return 0.0
            if geom.geom_type == "Polygon":
                lon, lat = geom.exterior.coords.xy
                a, _ = geod.polygon_area_perimeter(lon, lat)
                area = abs(a)
                for interior in geom.interiors:
                    lon, lat = interior.coords.xy
                    a, _ = geod.polygon_area_perimeter(lon, lat)
                    area -= abs(a)
                return area / 1e6
            if geom.geom_type == "MultiPolygon":
                return sum(_area_one(p) for p in geom.geoms)
            return 0.0

        return gdf[geom_col].apply(_area_one)

    except Exception:
        return gdf.to_crs(3857).area / 1e6


def classify_mmgd(df: pd.DataFrame, col_density: str = "PAINEL_DENS_M2_KM2") -> pd.Series:
    s = pd.to_numeric(df.get(col_density, 0), errors="coerce").fillna(0.0)
    nonzero = s[s > 0]
    if len(nonzero) < 10:
        bins = [0, 50, 200, float("inf")]
    else:
        q1 = float(nonzero.quantile(0.33))
        q2 = float(nonzero.quantile(0.66))
        bins = [0, q1, q2, float("inf")]
    labels = ["Baixa", "Média", "Alta"]
    out = pd.cut(s, bins=bins, labels=labels, include_lowest=True).astype("object")
    out = out.fillna("Sem detecção")
    out[s == 0] = "Sem detecção"
    return out


def detect_mmgd_real_column(gdf: gpd.GeoDataFrame) -> Optional[str]:
    candidates = [
        "MMGD_KW", "MMGD_KWP", "GD_KW", "GD_KWP",
        "POT_GD_KW", "POT_GD", "POT_MMGD", "POT_MMGD_KW",
        "POTENCIA_GD_KW", "POTENCIA_MMGD_KW",
        "MMGD_TOTAL_KW", "GD_TOTAL_KW", "KW_GD", "KW_MMGD",
    ]
    cols = set(gdf.columns)
    for c in candidates:
        if c in cols:
            return c
    for c in gdf.columns:
        cu = str(c).upper()
        if ("MMGD" in cu or "GERACAO" in cu or "GD" in cu) and ("KW" in cu or "KWP" in cu or "POT" in cu):
            return c
    return None


def merge_enrichment(
    gdf: gpd.GeoDataFrame,
    df_perfis: Optional[pd.DataFrame],
    df_solar: Optional[pd.DataFrame],
) -> Tuple[gpd.GeoDataFrame, pd.DataFrame]:
    gdf = gdf.copy()

    if "AREA_KM2" not in gdf.columns:
        gdf["AREA_KM2"] = geodesic_area_km2(gdf)

    # PERFIS (BDGD) -> usados para responder “perfil predominante”
    if df_perfis is not None and not df_perfis.empty:
        dfp = df_perfis.copy()
        dfp["COD_ID"] = dfp["COD_ID"].astype(str)
        dfp["SOMA_CAR_INST"] = pd.to_numeric(dfp.get("SOMA_CAR_INST", 0), errors="coerce").fillna(0.0)

        pivot = (
            dfp.pivot_table(index="COD_ID", columns="CLASSE", values="SOMA_CAR_INST", aggfunc="sum", fill_value=0.0)
            .reset_index()
        )
        classe_cols = [c for c in pivot.columns if c != "COD_ID"]
        if classe_cols:
            pivot["CARGA_TOTAL_KW"] = pivot[classe_cols].sum(axis=1)
            pivot["CLASSE_PRED"] = pivot[classe_cols].idxmax(axis=1)
            pivot["PERC_PRED"] = 0.0
            mask = pivot["CARGA_TOTAL_KW"] > 0
            if mask.any():
                vals = pivot.loc[mask, classe_cols].to_numpy(dtype=float)
                winner_idx = np.argmax(vals, axis=1)
                winner_val = np.take_along_axis(vals, winner_idx.reshape(-1, 1), axis=1).ravel()
                denom = pivot.loc[mask, "CARGA_TOTAL_KW"].to_numpy(dtype=float)
                perc = np.where(denom > 0, (winner_val / denom) * 100.0, 0.0)
                pivot.loc[mask, "PERC_PRED"] = perc

        gdf["COD_ID"] = gdf["COD_ID"].astype(str)
        keep_cols = [c for c in ["COD_ID", "CLASSE_PRED", "PERC_PRED", "CARGA_TOTAL_KW"] if c in pivot.columns]
        gdf = gdf.merge(pivot[keep_cols], on="COD_ID", how="left")

    # SOLAR/MMGD (visão)
    if df_solar is not None and not df_solar.empty:
        dfs = df_solar.copy()
        possible_id_cols = ["cod_id", "COD_ID", "codid", "CODID", "Cod_Id", "codId"]
        id_col = next((c for c in possible_id_cols if c in dfs.columns), None)
        if id_col is not None:
            dfs[id_col] = dfs[id_col].astype(str)
            for c in ["area_total_m2", "qtd_paineis", "conf_media"]:
                if c not in dfs.columns:
                    dfs[c] = 0.0

            gdf["COD_ID"] = gdf["COD_ID"].astype(str)
            gdf = gdf.merge(
                dfs[[id_col, "area_total_m2", "qtd_paineis", "conf_media"]],
                left_on="COD_ID",
                right_on=id_col,
                how="left",
            )
            if id_col in gdf.columns and id_col != "COD_ID":
                gdf = gdf.drop(columns=[id_col])

            gdf["area_total_m2"] = pd.to_numeric(gdf.get("area_total_m2"), errors="coerce").fillna(0.0)
            gdf["qtd_paineis"] = pd.to_numeric(gdf.get("qtd_paineis"), errors="coerce").fillna(0.0)
            gdf["conf_media"] = pd.to_numeric(gdf.get("conf_media"), errors="coerce").fillna(0.0)

            gdf["PAINEL_DENS_M2_KM2"] = 0.0
            mask2 = pd.to_numeric(gdf["AREA_KM2"], errors="coerce").fillna(0) > 0
            gdf.loc[mask2, "PAINEL_DENS_M2_KM2"] = gdf.loc[mask2, "area_total_m2"] / gdf.loc[mask2, "AREA_KM2"]
            gdf["MMGD_CATEG"] = classify_mmgd(gdf)

    # (mantemos estes campos porque o popup pode usar; MAS dashboard não usa BDGD MMGD)
    mmgd_col = detect_mmgd_real_column(gdf)
    if mmgd_col is not None:
        gdf["BDGD_MMGD_KW"] = pd.to_numeric(gdf[mmgd_col], errors="coerce").fillna(0.0)
        gdf["BDGD_MMGD_FONTE"] = mmgd_col
    else:
        gdf["BDGD_MMGD_KW"] = pd.NA
        gdf["BDGD_MMGD_FONTE"] = pd.NA

    # Visão: kW estimado (default; recalculado no mapa/dashboard usando session_state)
    if "area_total_m2" in gdf.columns:
        gdf["CV_MMGD_EST_KW"] = pd.to_numeric(gdf["area_total_m2"], errors="coerce").fillna(0.0) * 0.20
    else:
        gdf["CV_MMGD_EST_KW"] = 0.0

    # Capacidade estimada da subestação (kW) a partir da potência em MVA (assumindo fp ~ 0.90)
    if "POTENCIA_CALCULADA" in gdf.columns:
        gdf["SUB_CAP_KW"] = pd.to_numeric(gdf["POTENCIA_CALCULADA"], errors="coerce").fillna(0.0) * 1000.0 * 0.90
    else:
        gdf["SUB_CAP_KW"] = 0.0

    # Risco: MMGD (Visão) / Capacidade da Subestação
    sub_cap = pd.to_numeric(gdf["SUB_CAP_KW"], errors="coerce").astype("float64").replace(0, np.nan)
    cv_est = pd.to_numeric(gdf["CV_MMGD_EST_KW"], errors="coerce").astype("float64").fillna(0.0)
    ratio = cv_est / sub_cap
    gdf["RISCO_MMGD_RATIO"] = np.nan_to_num(ratio, nan=0.0, posinf=0.0, neginf=0.0)

    gdf["RISCO_MMGD_NIVEL"] = pd.cut(
        gdf["RISCO_MMGD_RATIO"],
        bins=[-1, 0.10, 0.25, 1e9],
        labels=["Baixo", "Médio", "Alto"],
    ).astype(str)

    cols = [
        "COD_ID", "NOM", "DISTRIBUIDORA", "CLASSIFICACAO", "POTENCIA_CALCULADA",
        "SUB_CAP_KW",
        "CLASSE_PRED", "PERC_PRED", "CARGA_TOTAL_KW",
        "area_total_m2", "qtd_paineis", "conf_media", "PAINEL_DENS_M2_KM2", "MMGD_CATEG",
        "AREA_KM2",
        "CV_MMGD_EST_KW",
        "RISCO_MMGD_RATIO", "RISCO_MMGD_NIVEL",
        # mantidos no df_resumo (não exibimos no dashboard por padrão)
        "BDGD_MMGD_KW", "BDGD_MMGD_FONTE",
    ]
    cols = [c for c in cols if c in gdf.columns]
    df_resumo = pd.DataFrame(gdf[cols]).copy() if cols else pd.DataFrame()
    return gdf, df_resumo


# -------------------------
# POPUP COMPLETO (com donut + sazonalidade)
# -------------------------
def _donut_svg(data_map: dict, total: float, size_px: int = 140) -> str:
    if total <= 0:
        return ""

    limiar = 0.05 * total
    final_data = {"Outros": 0.0}
    for k, v in data_map.items():
        if float(v) < limiar:
            final_data["Outros"] += float(v)
        else:
            final_data[k] = float(v)
    if final_data.get("Outros", 0) == 0:
        final_data.pop("Outros", None)

    labels = list(final_data.keys())
    values = [final_data[k] for k in labels]
    colors = ['#e6194b', '#3cb44b', '#ffe119', '#4363d8', '#f58231', '#911eb4', '#46f0f0', '#f032e6', '#bcf60c']

    svg = f'<svg width="{size_px}" height="{size_px}" viewBox="-1 -1 2 2" style="transform: rotate(-90deg); display:block; margin: 10px auto; filter: drop-shadow(0 2px 3px rgba(0,0,0,0.2));">'
    svg += '<style>path { transition: opacity 0.2s; cursor: pointer; } path:hover { opacity: 0.7; }</style>'
    cumulative = 0.0

    for i, (lab, val) in enumerate(zip(labels, values)):
        pct = (val / total) if total else 0
        start_x = math.cos(2 * math.pi * cumulative)
        start_y = math.sin(2 * math.pi * cumulative)
        cumulative += pct
        end_x = math.cos(2 * math.pi * cumulative)
        end_y = math.sin(2 * math.pi * cumulative)

        large_arc = 1 if pct > 0.5 else 0
        path_data = f"M {start_x} {start_y} A 1 1 0 {large_arc} 1 {end_x} {end_y} L 0 0"
        color = colors[i % len(colors)]

        svg += f'<path d="{path_data}" fill="{color}" stroke="white" stroke-width="0.01">'
        svg += f'<title>{lab}: {val:.1f} kW ({pct:.1%})</title>'
        svg += '</path>'

    svg += '<circle cx="0" cy="0" r="0.4" fill="white" /></svg>'

    legend = '<div style="display:grid; grid-template-columns: 1fr 1fr; gap: 5px; text-align:left; margin-top: 10px;">'
    for i, (lab, val) in enumerate(zip(labels, values)):
        pct = (val / total) * 100 if total else 0
        color = colors[i % len(colors)]
        legend += '<div style="font-size:10px; display:flex; align-items:center;">'
        legend += f'<span style="width:8px;height:8px;background:{color};display:inline-block;margin-right:5px;border-radius:2px;"></span>'
        legend += f'<span>{lab}: <b>{pct:.1f}%</b></span>'
        legend += '</div>'
    legend += "</div>"

    box = '<div style="margin-top: 15px; padding: 10px; border: 1px solid #eee; border-radius: 8px; background: #fdfdfd; text-align: center;">'
    box += '<div style="font-size: 12px; font-weight: bold; margin-bottom: 8px; color: #333;">Distribuição de Carga</div>'
    box += svg + legend + "</div>"
    return box


def _sazonalidade_html(sub_perfis: pd.DataFrame) -> str:
    meses_abrev = ['Jan','Fev','Mar','Abr','Mai','Jun','Jul','Ago','Set','Out','Nov','Dez']
    consumo_mensal = [0.0] * 12

    for _, p in sub_perfis.iterrows():
        for i in range(1, 13):
            col = f"ENE_{i:02d}"
            if col in sub_perfis.columns:
                try:
                    consumo_mensal[i-1] += float(p.get(col, 0) or 0)
                except Exception:
                    pass

    if sum(consumo_mensal) <= 0:
        return ""

    html = "<div style='margin-top: 10px;'><b>📅 Sazonalidade (kWh):</b></div>"
    html += "<div style='display: grid; grid-template-columns: repeat(4, 1fr); gap: 2px; font-size: 9px; margin-top: 3px;'>"
    for i, m in enumerate(meses_abrev):
        html += f"<div style='background:#f0f0f0; padding: 2px; text-align:center;'>{m}<br><b>{consumo_mensal[i]/1000:.1f}k</b></div>"
    html += "</div>"

    media = sum(consumo_mensal) / 12
    html += f"<div style='margin-top: 8px; font-size: 11px; font-weight: bold; color: #1565c0;'>Média(12 meses): {media/1000:.1f}k kWh</div>"
    return html


def gerar_html_popup_completo(row, df_perfis=None) -> str:
    cod_id = str(row.get("COD_ID", ""))
    dist = row.get("DISTRIBUIDORA", "N/A")
    classificacao = row.get("CLASSIFICACAO", "Não Classificada")
    titulo = (row.get("NOM") or cod_id) if cod_id else (row.get("NOM") or "Subestação")

    stats_html = f'<div style="font-family: sans-serif; min-width: 300px; max-width: 390px;">'
    stats_html += f'<h4 style="margin: 0 0 10px 0; color: #333; border-bottom: 1px solid #ccc; padding-bottom: 5px;">{titulo}</h4>'
    stats_html += f"<b>Distribuidora:</b> {dist}<br>"
    stats_html += f"<b>Classificação:</b> {classificacao}<br>"

    pot = row.get("POTENCIA_CALCULADA", None)
    sub_mae = row.get("SUB_MAE", None)
    if pot is not None and pd.notna(pot):
        stats_html += f"<b>Potência:</b> {float(pot):.2f} MVA<br>"
    if sub_mae and pd.notna(sub_mae) and str(sub_mae) not in ("0", "None"):
        stats_html += f"<b>Alimentada por (ID):</b> {sub_mae}<br>"

    # perfil predominante
    classe_pred = row.get("CLASSE_PRED")
    perc_pred = row.get("PERC_PRED")
    if classe_pred and pd.notna(classe_pred):
        stats_html += "<hr style='margin: 10px 0; border-top: 2px solid #333;'>"
        stats_html += "<b>🧾 Perfil predominante de consumo:</b><br>"
        nome = str(classe_pred).replace("_", " ").title()
        if perc_pred is not None and pd.notna(perc_pred):
            stats_html += f"<span style='font-size: 13px;'><b>{nome}</b> ({float(perc_pred):.1f}%)</span><br>"
        else:
            stats_html += f"<span style='font-size: 13px;'><b>{nome}</b></span><br>"

    # MMGD visão + (opcional) BDGD real (mantido no popup)
    stats_html += "<hr style='margin: 10px 0; border-top: 2px solid #333;'>"
    stats_html += "<b>☀️ Geração Distribuída (MMGD):</b><br>"

    cat = row.get("MMGD_CATEG", "Sem detecção")
    area_total = float(row.get("area_total_m2", 0) or 0)
    qtd = int(float(row.get("qtd_paineis", 0) or 0))
    dens = float(row.get("PAINEL_DENS_M2_KM2", 0) or 0)
    cv_kw = row.get("CV_MMGD_EST_KW", None)

    stats_html += f"<div style='margin-top:4px;'><b>Visão (painéis):</b> <span style='font-weight:700;'>{cat}</span></div>"
    stats_html += f"Área total detectada: <b>{area_total:.1f}</b> m²<br>"
    stats_html += f"Qtd. detecções: <b>{qtd}</b><br>"
    stats_html += f"Densidade: <b>{dens:.1f}</b> m²/km²<br>"
    if cv_kw is not None and pd.notna(cv_kw):
        stats_html += f"Potência estimada (visão): <b>{float(cv_kw):.0f}</b> kW<br>"
        sub_cap = row.get("SUB_CAP_KW", None)
        if sub_cap is not None and pd.notna(sub_cap) and float(sub_cap) > 0:
            r = float(row.get("RISCO_MMGD_RATIO", 0) or 0) * 100.0
            nivel = row.get("RISCO_MMGD_NIVEL", "")
            cor = "#2ecc71" if r < 10 else ("#f39c12" if r < 25 else "#e74c3c")
            stats_html += f"Capacidade estimada (SE): <b>{float(sub_cap):.0f}</b> kW<br>"
            stats_html += f"Risco MMGD (visão/SE): <b style='color:{cor};'>{r:.1f}% ({nivel})</b><br>"

    bdgd_kw = row.get("BDGD_MMGD_KW", None)
    bdgd_src = row.get("BDGD_MMGD_FONTE", None)
    if bdgd_kw is not None and pd.notna(bdgd_kw):
        stats_html += "<div style='margin-top:8px;'><b>BDGD (real):</b><br>"
        stats_html += f"Potência instalada: <b>{float(bdgd_kw):.0f}</b> kW<br>"
        if bdgd_src and pd.notna(bdgd_src):
            stats_html += f"<span style='font-size:10px;color:#666;'>Fonte: {bdgd_src}</span><br>"
        stats_html += "</div>"
    else:
        stats_html += "<div style='margin-top:8px; font-size:11px; color:#666;'>BDGD (real): não disponível no GeoJSON.</div>"

    # Detalhe BDGD por classe (donut + sazonalidade)
    if df_perfis is not None and cod_id:
        sub_perfis = df_perfis[df_perfis["COD_ID"].astype(str) == cod_id]
        if not sub_perfis.empty:
            stats_html += "<hr style='margin: 10px 0; border-top: 2px solid #333;'>"
            stats_html += "<b>📊 Perfil de Consumo (BDGD):</b><br>"
            stats_html += "<table style='width:100%; font-size: 11px; border-collapse: collapse; margin-top: 5px;'>"
            stats_html += "<tr style='background: #eee;'><th>Classe</th><th>Qtd</th><th>Carga Instalada (kW)</th></tr>"

            total_carga = 0.0
            data_map = {}
            for _, p in sub_perfis.iterrows():
                classe_nome = str(p.get("CLASSE", "")).replace("_", " ").title()
                carga = float(p.get("SOMA_CAR_INST", 0) or 0)
                qtd_cli = int(float(p.get("QTD_CLIENTES", 0) or 0))
                stats_html += f"<tr><td>{classe_nome}</td><td align='right'>{qtd_cli}</td><td align='right'>{carga:.1f}</td></tr>"
                total_carga += carga
                data_map[classe_nome] = data_map.get(classe_nome, 0.0) + carga

            stats_html += "</table>"
            stats_html += f"<div style='margin-top: 5px; font-weight: bold; color: #d32f2f;'>Carga Instalada Total: {total_carga:.1f} kW</div>"
            stats_html += _donut_svg(data_map, total_carga)
            stats_html += _sazonalidade_html(sub_perfis)

    stats_html += "</div>"
    return stats_html


# -------------------------
# MAPA
# -------------------------
def construir_mapa_completo(
    rj_shape: gpd.GeoDataFrame,
    gdf: gpd.GeoDataFrame,
    df_perfis: Optional[pd.DataFrame],
    filtro_dist: List[str],
    filtro_class: List[str],
    filtro_mmgd: List[str],
    filtro_subs: Optional[List[str]],
    filtro_min_mva: float,
    usar_popup_completo: bool = True,
    mostrar_hierarquia: bool = True,
) -> folium.Map:
    m = folium.Map(location=[-22.9, -43.2], zoom_start=9, control_scale=True, tiles=None, prefer_canvas=True)

    folium.TileLayer("CartoDB positron", name="CartoDB (claro)").add_to(m)
    folium.TileLayer("OpenStreetMap", name="OpenStreetMap").add_to(m)
    folium.TileLayer(
        tiles="https://mt1.google.com/vt/lyrs=s&x={x}&y={y}&z={z}",
        attr="Google",
        name="Google Satélite",
        overlay=False,
        control=True,
        max_zoom=21,
    ).add_to(m)

    Fullscreen().add_to(m)
    MousePosition().add_to(m)
    MeasureControl(position="topleft").add_to(m)

    folium.GeoJson(
        rj_shape,
        name="RJ",
        style_function=lambda x: {"fillOpacity": 0.03, "weight": 2, "color": "#111"},
    ).add_to(m)

    if gdf is None or gdf.empty:
        folium.LayerControl().add_to(m)
        return m

    gf = gdf.copy()

    for col in ["DISTRIBUIDORA", "CLASSIFICACAO", "MMGD_CATEG"]:
        if col in gf.columns:
            gf[col] = gf[col].replace({0: None, "0": None}).fillna("DESCONHECIDO")

    if filtro_dist and "DISTRIBUIDORA" in gf.columns:
        gf = gf[gf["DISTRIBUIDORA"].isin(filtro_dist)]
    if filtro_class and "CLASSIFICACAO" in gf.columns:
        gf = gf[gf["CLASSIFICACAO"].isin(filtro_class)]
    if filtro_mmgd and "MMGD_CATEG" in gf.columns:
        gf = gf[gf["MMGD_CATEG"].astype(str).isin(filtro_mmgd)]

    # ✅ filtro por subestação (nome ou COD_ID)
    if filtro_subs:
        subs_norm = [str(x) for x in filtro_subs]
        if "NOM" in gf.columns and "COD_ID" in gf.columns:
            nom = gf["NOM"].fillna("").astype(str)
            cod = gf["COD_ID"].fillna("").astype(str)
            label = (cod + " — " + nom).str.strip()
            gf = gf[label.isin(subs_norm) | cod.isin(subs_norm) | nom.isin(subs_norm)]
        elif "COD_ID" in gf.columns:
            gf = gf[gf["COD_ID"].astype(str).isin(subs_norm)]
        elif "NOM" in gf.columns:
            gf = gf[gf["NOM"].astype(str).isin(subs_norm)]

    if filtro_min_mva and "POTENCIA_CALCULADA" in gf.columns:
        gf = gf[pd.to_numeric(gf["POTENCIA_CALCULADA"], errors="coerce").fillna(0) >= float(filtro_min_mva)]

    risk_colors = {
        "Sem dados": "#BDBDBD",
        "Baixo": "#2ecc71",
        "Médio": "#f39c12",
        "Alto":  "#e74c3c",
        "Sem detecção": "#BDBDBD",
    }

    classificacoes = sorted(gf["CLASSIFICACAO"].dropna().unique().tolist()) if "CLASSIFICACAO" in gf.columns else ["(sem classificação)"]
    if "CLASSIFICACAO" not in gf.columns:
        gf["CLASSIFICACAO"] = "(sem classificação)"

    groups = {c: folium.FeatureGroup(name=f"⚡ {c}", show=True) for c in classificacoes}
    group_mmgd = folium.FeatureGroup(name="☀️ Risco MMGD (Visão/SE)", show=True)

    def style_mmgd(feat):
        props = feat.get("properties", {})
        sub_cap = props.get("SUB_CAP_KW", None)
        try:
            sub_cap_ok = float(sub_cap) > 0
        except Exception:
            sub_cap_ok = False

        area_m2 = props.get("area_total_m2", None)
        try:
            has_detection = float(area_m2) > 0
        except Exception:
            has_detection = False

        if not sub_cap_ok:
            nivel = "Sem dados"
        else:
            nivel = str(props.get("RISCO_MMGD_NIVEL", "Baixo"))
            if not has_detection:
                nivel = "Sem detecção"

        color = risk_colors.get(nivel, risk_colors["Sem dados"])
        return {"fillColor": color, "color": color, "weight": 1, "fillOpacity": 0.30}

    tooltip_fields = [c for c in ["COD_ID", "NOM", "DISTRIBUIDORA"] if c in gf.columns]
    tooltip_aliases = ["COD_ID", "Nome", "Dist"][: len(tooltip_fields)]

    folium.GeoJson(
        gf,
        name="MMGD Overlay",
        style_function=style_mmgd,
        tooltip=folium.GeoJsonTooltip(fields=tooltip_fields, aliases=tooltip_aliases),
    ).add_to(group_mmgd)

    def style_class(feat):
        props = feat.get("properties", {})
        dist = props.get("DISTRIBUIDORA", "N/A")
        base_color = CORES_DISTRIBUIDORA.get(dist, "gray")
        return {"fillColor": base_color, "color": base_color, "weight": 1, "fillOpacity": 0.10}

    popup_fn = gerar_html_popup_completo if usar_popup_completo else gerar_html_popup_completo

    for c in classificacoes:
        sub = gf[gf["CLASSIFICACAO"] == c].copy()

        folium.GeoJson(
            sub,
            name=f"Polígonos — {c}",
            style_function=style_class,
            tooltip=folium.GeoJsonTooltip(fields=tooltip_fields, aliases=tooltip_aliases),
        ).add_to(groups[c])

        for _, row in sub.iterrows():
            try:
                popup_html = popup_fn(row, df_perfis=df_perfis)
                pot = float(pd.to_numeric(row.get("POTENCIA_CALCULADA", 0), errors="coerce") or 0)
                icon = make_png_marker_icon(str(row.get("CLASSIFICACAO", "default")), pot, str(row.get("DISTRIBUIDORA", "")))
                cent = row.geometry.centroid
                folium.Marker(
                    location=[cent.y, cent.x],
                    icon=icon,
                    popup=folium.Popup(popup_html, max_width=520),
                ).add_to(groups[c])
            except Exception:
                continue

        groups[c].add_to(m)

    group_mmgd.add_to(m)

    if mostrar_hierarquia and "SUB_MAE" in gf.columns and "COD_ID" in gf.columns:
        try:
            gtemp = gf.copy()
            gtemp["COD_ID"] = gtemp["COD_ID"].astype(str)
            centers = {str(r["COD_ID"]): (r.geometry.centroid.y, r.geometry.centroid.x) for _, r in gtemp.iterrows()}

            for _, r in gtemp.iterrows():
                cod = str(r["COD_ID"])
                mae = r.get("SUB_MAE", None)
                if mae is None or pd.isna(mae):
                    continue
                mae = str(mae)
                if mae in centers and cod in centers and mae != cod:
                    AntPath(
                        locations=[centers[mae], centers[cod]],
                        color="#00E5FF",
                        weight=2,
                        opacity=0.75,
                        delay=800,
                    ).add_to(m)
        except Exception:
            pass

    add_map_legend(m)
    folium.LayerControl(collapsed=False).add_to(m)
    return m


# -------------------------
# EXPORT / BASE COMPLETA
# -------------------------
def escrever_instrucoes_base_completa() -> str:
    txt = """BASE COMPLETA — RADAR RJ (Perfis de Carga + MMGD)

📦 Arquivos principais (em Dados Processados/):
1) dados_finais_rj.geojson
   - Polígonos por subestação/área atendida (COD_ID)
   - Campos relevantes: COD_ID, NOM, DISTRIBUIDORA, CLASSIFICACAO, SUB_MAE, POTENCIA_CALCULADA

2) perfis_consumo.csv  (separador ';')
   - Estatísticas BDGD por COD_ID e CLASSE
   - Campos: COD_ID, CLASSE, QTD_CLIENTES, SOMA_CAR_INST, ENE_01..ENE_12 (se existirem)

3) solar_resumo_por_area.csv
   - Resumo YOLO-SEG de painéis por COD_ID
   - Campos: distribuidora, cod_id, area_total_m2, qtd_paineis, conf_media, area_media_m2

4) solar_paineis_deteccoes.geojson (opcional / grande)
   - Geometrias (polígonos) de cada detecção com area_m2 e conf

🧠 Como responder as perguntas do hackathon:
A) Perfil predominante de consumo:
   - Somar SOMA_CAR_INST por CLASSE dentro do COD_ID
   - Classe predominante = maior SOMA_CAR_INST
   - Percentual = (SOMA_CAR_INST da classe / soma total) * 100

B) Presença relevante de MMGD (Visão Computacional):
   - usar area_total_m2 (área de painéis detectados) do solar_resumo_por_area.csv
   - densidade opcional: area_total_m2 / AREA_KM2 (m²/km²)
   - risco opcional: (kW estimado visão / capacidade SE estimada)
"""
    os.makedirs(PASTA_PROC, exist_ok=True)
    with open(INSTRUCOES_BASE_COMPLETA, "w", encoding="utf-8") as f:
        f.write(txt)
    return txt


# -------------------------
# SOLAR: helper para repassar kwargs só se a função aceitar
# -------------------------
def _call_with_accepted_kwargs(fn, **kwargs):
    try:
        sig = inspect.signature(fn)
        accepted = set(sig.parameters.keys())
        safe = {k: v for k, v in kwargs.items() if k in accepted}
        return fn(**safe)
    except Exception:
        # fallback: tenta chamar só com o mínimo
        return fn()


# -------------------------
# APP UI
# -------------------------
st.title("RADAR RJ — Perfis de Consumo & MMGD (Visão Computacional + BDGD Perfis)")

with st.expander("📌 Objetivo", expanded=True):
    st.markdown(
        """
- **Perfil predominante de consumo**: calculado pela BDGD (carga instalada por classe).
- **Presença/risco de MMGD**: estimada por detecção de painéis via visão computacional (YOLO-SEG).
- **Dashboard**: **somente visão computacional** (MMGD) + perfis (BDGD) para a pergunta 1 do desafio.
"""
    )

tabs = st.tabs(["1) Upload & Pipeline", "2) Mapa Interativo", "3) Dashboard", "4) Solar (MMGD)", "5) Export / Base Completa"])

# TAB 1
with tabs[0]:
    st.subheader("Upload / Execução de pipeline")

    ui_section("📦 Dados brutos", "Envie ZIP com a pasta `Dados Brutos/` (opcional).")
    zip_raw = st.file_uploader("ZIP com Dados Brutos", type=["zip"], key="zip_raw")
    if zip_raw is not None:
        extracted = _extract_zip_to(zip_raw.getvalue(), PASTA_BRUTOS)
        st.success(f"ZIP extraído em `{PASTA_BRUTOS}/` ({len(extracted)} itens).")

    st.divider()
    ui_section("✅ Dados processados", "Envie os outputs prontos (recomendado para demo rápida).")

    colA, colB = st.columns(2)
    with colA:
        up_geo = st.file_uploader("GeoJSON `dados_finais_rj.geojson`", type=["geojson", "json"], key="up_geo")
        if up_geo is not None:
            _save_uploaded_file(up_geo, ARQUIVO_UNIFICADO)
            st.success(f"Salvo em `{ARQUIVO_UNIFICADO}`")

        up_perf = st.file_uploader("CSV `perfis_consumo.csv`", type=["csv"], key="up_perf")
        if up_perf is not None:
            _save_uploaded_file(up_perf, ARQUIVO_PERFIS)
            st.success(f"Salvo em `{ARQUIVO_PERFIS}`")

    with colB:
        up_solar = st.file_uploader("CSV `solar_resumo_por_area.csv` (opcional)", type=["csv"], key="up_solar")
        if up_solar is not None:
            _save_uploaded_file(up_solar, ARQUIVO_SOLAR_RESUMO)
            st.success(f"Salvo em `{ARQUIVO_SOLAR_RESUMO}`")

        up_sol_det = st.file_uploader("GeoJSON `solar_paineis_deteccoes.geojson` (opcional)", type=["geojson", "json"], key="up_sol_det")
        if up_sol_det is not None:
            _save_uploaded_file(up_sol_det, ARQUIVO_SOLAR_DETECCOES)
            st.success(f"Salvo em `{ARQUIVO_SOLAR_DETECCOES}`")

    st.divider()
    st.subheader("Rodar ETLs (se quiser)")

    st.write("extrator importado?", extrator is not None)
    st.write("extrair_estatisticas_consumo importado?", consumo_etl is not None)
    if consumo_etl is None and consumo_import_error:
        st.error("Falha ao importar extrair_estatisticas_consumo.py:")
        st.code(consumo_import_error)

    colX, colY = st.columns(2)
    with colX:
        if st.button("▶️ Rodar ETL geográfico (extrator.py)", use_container_width=True, disabled=(extrator is None)):
            with st.spinner("Rodando ETL geográfico..."):
                t0 = time.time()
                try:
                    run_etl_module(extrator, "extrator")
                    st.success(f"ETL geográfico finalizado em {time.time()-t0:.1f}s. Saída: {ARQUIVO_UNIFICADO}")
                    st.cache_data.clear()
                except Exception as e:
                    st.error(f"Erro no ETL geográfico: {e}")
    with colY:
        if st.button("▶️ Rodar ETL perfis (BDGD)", use_container_width=True, disabled=(consumo_etl is None)):
            with st.spinner("Rodando ETL de perfis..."):
                t0 = time.time()
                try:
                    run_etl_module(consumo_etl, "extrair_estatisticas_consumo")
                    st.success(f"Perfis gerados em {time.time()-t0:.1f}s. Saída: {ARQUIVO_PERFIS}")
                    st.cache_data.clear()
                except Exception as e:
                    st.error(f"Erro no ETL de perfis: {e}")


# Carrega dados
rj_shape = load_rj_shape()
gdf_unificado = load_unificado(ARQUIVO_UNIFICADO)
df_perfis = load_perfis(ARQUIVO_PERFIS)
df_solar_resumo = load_solar_resumo(ARQUIVO_SOLAR_RESUMO)

df_resumo = None
if gdf_unificado is not None and not gdf_unificado.empty:
    gdf_enriq, df_resumo = merge_enrichment(gdf_unificado, df_perfis, df_solar_resumo)
else:
    gdf_enriq = gdf_unificado


# TAB 2 — MAPA
with tabs[1]:
    st.subheader("Mapa interativo")

    if gdf_enriq is None or gdf_enriq.empty:
        st.warning("Ainda não há `dados_finais_rj.geojson`. Faça upload ou rode o ETL na aba 1.")
    else:
        with st.sidebar:
            st.header("Filtros do Mapa")

            dists = sorted(gdf_enriq.get("DISTRIBUIDORA", pd.Series(dtype=str)).dropna().unique().tolist())
            filtro_dist = st.multiselect("Distribuidora", options=dists, default=dists)

            classes = sorted(gdf_enriq.get("CLASSIFICACAO", pd.Series(dtype=str)).dropna().unique().tolist())
            filtro_class = st.multiselect("Classificação", options=classes, default=classes)

            mmgds = ["Sem detecção", "Baixa", "Média", "Alta"] if "MMGD_CATEG" in gdf_enriq.columns else []
            filtro_mmgd = st.multiselect("MMGD (nível)", options=mmgds, default=mmgds) if mmgds else []

            subs_opts = (
                gdf_enriq.get("COD_ID", pd.Series(dtype=str)).astype(str).fillna("") +
                " — " +
                gdf_enriq.get("NOM", pd.Series(dtype=str)).fillna("").astype(str)
            ).str.strip()
            subs_opts = sorted([s for s in subs_opts.unique().tolist() if s and s != "—"])
            filtro_subs = st.multiselect("Subestação (COD_ID — Nome)", options=subs_opts, default=[])

            filtro_min_mva = st.slider("Potência mínima (MVA)", min_value=0.0, max_value=200.0, value=0.0, step=1.0)

            st.divider()
            usar_popup_completo = st.toggle("Popup completo", value=True)
            mostrar_hierarquia = st.toggle("Mostrar hierarquia (SUB_MAE → filha)", value=True)

        # Alinhamento com dashboard: recalcula CV_MMGD_EST_KW com kw_per_m2 da sessão
        kw = float(st.session_state.get("kw_per_m2", 0.20))
        gdf_map = gdf_enriq.copy()
        if "area_total_m2" in gdf_map.columns:
            gdf_map["CV_MMGD_EST_KW"] = pd.to_numeric(gdf_map["area_total_m2"], errors="coerce").fillna(0.0) * kw

        # Recalcular risco com o CV_MMGD_EST_KW atualizado
        if "SUB_CAP_KW" in gdf_map.columns and "CV_MMGD_EST_KW" in gdf_map.columns:
            sub_cap = pd.to_numeric(gdf_map["SUB_CAP_KW"], errors="coerce").astype("float64").replace(0, np.nan)
            cv_est = pd.to_numeric(gdf_map["CV_MMGD_EST_KW"], errors="coerce").astype("float64").fillna(0.0)
            ratio = cv_est / sub_cap
            gdf_map["RISCO_MMGD_RATIO"] = np.nan_to_num(ratio, nan=0.0, posinf=0.0, neginf=0.0)

            gdf_map["RISCO_MMGD_NIVEL"] = pd.cut(
                gdf_map["RISCO_MMGD_RATIO"],
                bins=[-1, 0.10, 0.25, 1e9],
                labels=["Baixo", "Médio", "Alto"],
            ).astype(str)

        m = construir_mapa_completo(
            rj_shape=rj_shape,
            gdf=gdf_map,
            df_perfis=df_perfis,
            filtro_dist=filtro_dist,
            filtro_class=filtro_class,
            filtro_mmgd=filtro_mmgd,
            filtro_subs=filtro_subs if filtro_subs else None,
            filtro_min_mva=filtro_min_mva,
            usar_popup_completo=usar_popup_completo,
            mostrar_hierarquia=mostrar_hierarquia,
        )

        st_folium(m, height=1100, use_container_width=True)


# TAB 3 — DASHBOARD (SEM BDGD MMGD)
with tabs[2]:
    st.subheader("Dashboard (alinhado ao desafio)")

    if df_resumo is None or df_resumo.empty:
        st.warning("Sem dados suficientes. Gere/importe `dados_finais_rj.geojson` e `perfis_consumo.csv`.")
    else:
        colf1, colf2, colf3 = st.columns(3)
        with colf1:
            dists = sorted(df_resumo.get("DISTRIBUIDORA", pd.Series(dtype=str)).dropna().unique().tolist())
            fd = st.multiselect("Distribuidora", dists, default=dists)
        with colf2:
            mm = ["Sem detecção", "Baixa", "Média", "Alta"] if "MMGD_CATEG" in df_resumo.columns else []
            fm = st.multiselect("MMGD (visão)", mm, default=mm) if mm else []
        with colf3:
            min_mva = st.number_input("Potência mínima (MVA)", min_value=0.0, value=0.0, step=1.0)

        st.markdown("#### 🎛️ Filtro por Subestações")
        q_sub = st.text_input("Buscar (COD_ID ou nome contém...)", value="", placeholder="Ex: 10385925 ou SANTA CECILIA")

        dff = df_resumo.copy()
        if fd and "DISTRIBUIDORA" in dff.columns:
            dff = dff[dff["DISTRIBUIDORA"].isin(fd)]
        if fm and "MMGD_CATEG" in dff.columns:
            dff = dff[dff["MMGD_CATEG"].astype(str).isin(fm)]
        if "POTENCIA_CALCULADA" in dff.columns:
            dff = dff[pd.to_numeric(dff["POTENCIA_CALCULADA"], errors="coerce").fillna(0) >= float(min_mva)]

        options_sub = []
        if "COD_ID" in dff.columns:
            tmp_opts = dff.copy()
            tmp_opts["COD_ID"] = tmp_opts["COD_ID"].astype(str)
            tmp_opts["NOM"] = tmp_opts.get("NOM", "").astype(str)
            tmp_opts["LABEL"] = tmp_opts["COD_ID"] + " — " + tmp_opts["NOM"]
            options_sub = sorted(tmp_opts["LABEL"].unique().tolist())

        if q_sub.strip():
            q = q_sub.strip().lower()
            options_sub = [o for o in options_sub if q in o.lower()]

        sel_sub = st.multiselect("Selecionar subestações (opcional)", options=options_sub, default=[])
        if sel_sub and "COD_ID" in dff.columns:
            sel_ids = [s.split(" — ")[0].strip() for s in sel_sub]
            dff = dff[dff["COD_ID"].astype(str).isin(sel_ids)]

        # KPIs
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Subestações", f"{len(dff):,}".replace(",", "."))
        k2.metric("Carga instalada total (kW)", f"{_num_sum(dff, 'CARGA_TOTAL_KW'):,.0f}".replace(",", "."))
        k3.metric("Área total painéis (m²)", f"{_num_sum(dff, 'area_total_m2'):,.0f}".replace(",", "."))
        k4.metric("Detecções (qtd.)", f"{_num_sum(dff, 'qtd_paineis'):,.0f}".replace(",", "."))

        st.divider()

        kw_per_m2 = st.slider(
            "Fator de conversão (kW por m² de painel detectado)",
            min_value=0.10, max_value=0.30, value=float(st.session_state.get("kw_per_m2", 0.20)), step=0.01
        )
        st.session_state["kw_per_m2"] = float(kw_per_m2)

        dff = dff.copy()
        if "area_total_m2" in dff.columns:
            dff["CV_MMGD_EST_KW"] = pd.to_numeric(dff["area_total_m2"], errors="coerce").fillna(0.0) * float(kw_per_m2)

        # recalcula risco alinhado ao fator
        if "SUB_CAP_KW" in dff.columns and "CV_MMGD_EST_KW" in dff.columns:
            sub_cap = pd.to_numeric(dff["SUB_CAP_KW"], errors="coerce").astype("float64").replace(0, np.nan)
            cv_est = pd.to_numeric(dff["CV_MMGD_EST_KW"], errors="coerce").astype("float64").fillna(0.0)
            ratio = cv_est / sub_cap
            dff["RISCO_MMGD_RATIO"] = np.nan_to_num(ratio, nan=0.0, posinf=0.0, neginf=0.0)
            dff["RISCO_MMGD_NIVEL"] = pd.cut(
                dff["RISCO_MMGD_RATIO"],
                bins=[-1, 0.10, 0.25, 1e9],
                labels=["Baixo", "Médio", "Alto"],
            ).astype(str)

        cA, cB, cC = st.columns(3)
        cA.metric("MMGD (visão) estimado total (kW)", f"{_num_sum(dff, 'CV_MMGD_EST_KW'):,.0f}".replace(",", "."))
        cB.metric("Risco médio (visão/SE)", f"{(pd.to_numeric(dff.get('RISCO_MMGD_RATIO', 0), errors='coerce').fillna(0).mean() * 100):.1f}%")
        cC.metric("Confiança média (visão)", f"{pd.to_numeric(dff.get('conf_media', 0), errors='coerce').fillna(0).mean():.2f}")

        st.divider()
        st.markdown("### 📊 Gráficos (Visão Computacional + Perfis)")

        if px is None:
            st.info("Plotly não está disponível (instale `plotly`).")
        else:
            g1, g2 = st.columns(2)

            # (A) Histograma do risco (Visão/SE)
            with g1:
                if "RISCO_MMGD_RATIO" in dff.columns:
                    tmp = dff.copy()
                    tmp["RISCO_%"] = pd.to_numeric(tmp["RISCO_MMGD_RATIO"], errors="coerce").fillna(0.0) * 100.0
                    st.plotly_chart(
                        px.histogram(
                            tmp,
                            x="RISCO_%",
                            nbins=30,
                            title="Distribuição do Risco MMGD (Visão/SE) — %"
                        ),
                        use_container_width=True,
                    )
                else:
                    st.info("Sem RISCO_MMGD_RATIO para plotar.")

            # (B) Barras: top 15 maior risco
            with g2:
                if "RISCO_MMGD_RATIO" in dff.columns:
                    top = dff.copy()
                    top["RISCO_%"] = pd.to_numeric(top["RISCO_MMGD_RATIO"], errors="coerce").fillna(0.0) * 100.0
                    top = top.sort_values("RISCO_%", ascending=False).head(15)
                    st.plotly_chart(
                        px.bar(
                            top,
                            x="NOM" if "NOM" in top.columns else "COD_ID",
                            y="RISCO_%",
                            title="Top 15 Subestações — Maior Risco (Visão/SE)",
                            hover_data=[c for c in ["COD_ID", "DISTRIBUIDORA", "CV_MMGD_EST_KW", "SUB_CAP_KW", "conf_media"] if c in top.columns],
                        ),
                        use_container_width=True,
                    )
                else:
                    st.info("Sem RISCO_MMGD_RATIO para plotar.")

            g3, g4 = st.columns(2)

            # (C) Scatter: potência estimada visão vs capacidade da SE
            with g3:
                if "CV_MMGD_EST_KW" in dff.columns and "SUB_CAP_KW" in dff.columns:
                    tmp = dff.copy()
                    tmp["CV_MMGD_EST_KW"] = pd.to_numeric(tmp["CV_MMGD_EST_KW"], errors="coerce").fillna(0.0)
                    tmp["SUB_CAP_KW"] = pd.to_numeric(tmp["SUB_CAP_KW"], errors="coerce").fillna(0.0)
                    tmp = tmp[(tmp["SUB_CAP_KW"] > 0) & (tmp["CV_MMGD_EST_KW"] > 0)]
                    if tmp.empty:
                        st.info("Sem pontos válidos (precisa SUB_CAP_KW>0 e CV_MMGD_EST_KW>0).")
                    else:
                        st.plotly_chart(
                            px.scatter(
                                tmp,
                                x="SUB_CAP_KW",
                                y="CV_MMGD_EST_KW",
                                title="MMGD estimado (Visão) vs Capacidade estimada da SE",
                                hover_data=[c for c in ["COD_ID", "NOM", "DISTRIBUIDORA", "conf_media", "area_total_m2"] if c in tmp.columns],
                            ),
                            use_container_width=True,
                        )
                else:
                    st.info("Sem CV_MMGD_EST_KW ou SUB_CAP_KW para plotar.")

            # (D) Barras: perfil predominante (contagem por classe)
            with g4:
                if "CLASSE_PRED" in dff.columns:
                    tmp = dff.copy()
                    tmp["CLASSE_PRED"] = tmp["CLASSE_PRED"].fillna("Sem perfil")
                    counts = tmp["CLASSE_PRED"].value_counts().reset_index()
                    counts.columns = ["CLASSE_PRED", "QTD_SES"]
                    st.plotly_chart(
                        px.bar(
                            counts,
                            x="CLASSE_PRED",
                            y="QTD_SES",
                            title="Distribuição do Perfil Predominante (BDGD) — contagem de SEs",
                        ),
                        use_container_width=True,
                    )
                else:
                    st.info("Sem CLASSE_PRED para plotar.")


        st.divider()

        # ✅ Tabela principal (sem BDGD MMGD)
        st.markdown("#### 📌 Tabela principal (perfil + MMGD visão + risco)")
        display_cols = {
            "COD_ID": "ID Subestação",
            "NOM": "Subestação",
            "DISTRIBUIDORA": "Distribuidora",
            "CLASSIFICACAO": "Classificação",
            "POTENCIA_CALCULADA": "Potência SE (MVA)",
            "SUB_CAP_KW": "Capacidade SE (kW, est.)",
            "CLASSE_PRED": "Perfil predominante",
            "PERC_PRED": "% perfil predominante",
            "CARGA_TOTAL_KW": "Carga instalada total (kW)",
            "MMGD_CATEG": "MMGD (visão) nível",
            "area_total_m2": "Área painéis (m²)",
            "qtd_paineis": "Qtd detecções",
            "conf_media": "Confiança média",
            "CV_MMGD_EST_KW": "MMGD (visão) kW est.",
            "RISCO_MMGD_RATIO": "Risco MMGD (visão/SE)",
            "RISCO_MMGD_NIVEL": "Nível de risco",
            "PAINEL_DENS_M2_KM2": "Densidade (m²/km²)",
        }
        cols = [c for c in display_cols.keys() if c in dff.columns]
        dff_disp = dff[cols].copy()

        # formata risco
        if "RISCO_MMGD_RATIO" in dff_disp.columns:
            dff_disp["RISCO_MMGD_RATIO"] = pd.to_numeric(dff_disp["RISCO_MMGD_RATIO"], errors="coerce").fillna(0.0)

        dff_disp = dff_disp.rename(columns=display_cols)
        st.dataframe(dff_disp, use_container_width=True, height=560)

        st.markdown("#### 🧾 Ranking — maior risco (visão/SE)")
        if "RISCO_MMGD_RATIO" in dff.columns:
            top_risco = dff.sort_values("RISCO_MMGD_RATIO", ascending=False).head(20).copy()
            show = ["COD_ID","NOM","DISTRIBUIDORA","SUB_CAP_KW","CV_MMGD_EST_KW","RISCO_MMGD_RATIO","RISCO_MMGD_NIVEL","conf_media","area_total_m2","qtd_paineis"]
            show = [c for c in show if c in top_risco.columns]
            st.dataframe(top_risco[show], use_container_width=True, height=360)
        else:
            st.info("Sem risco calculado (faltando SUB_CAP_KW ou CV_MMGD_EST_KW).")


# TAB 4 — SOLAR (opcional)
with tabs[3]:
    st.subheader("Solar (MMGD) — execução por recorte (configurável)")

    if solar_pipeline is None:
        st.info("Módulo Solar não importado. Se quiser usar, verifique `Solar/solar_panels_rj_2stage.py`.")
    else:
        st.markdown("Rode apenas por COD_IDs selecionados (mais rápido e controlável).")
        if gdf_unificado is None or gdf_unificado.empty:
            st.warning("Você precisa do `dados_finais_rj.geojson` primeiro (aba 1).")
        else:
            dists = sorted(gdf_unificado.get("DISTRIBUIDORA", pd.Series(dtype=str)).dropna().unique().tolist())

            col1, col2, col3, col4 = st.columns(4)
            with col1:
                dist_sel = st.selectbox("Distribuidora", options=["(todas)"] + dists, index=0)
            with col2:
                conf = st.slider("Confiança (conf)", min_value=0.10, max_value=0.70, value=0.25, step=0.05)
            with col3:
                max_tiles = st.number_input("MAX_TILES (limite)", min_value=100, value=30000, step=1000)
            with col4:
                max_workers = st.number_input("MAX_WORKERS_DOWNLOAD", min_value=1, value=16, step=1)

            colz1, colz2 = st.columns(2)
            with colz1:
                zoom = st.number_input("ZOOM (se aplicável)", min_value=16, max_value=21, value=20, step=1)
            with colz2:
                top_n = st.number_input("Top-N por potência (opcional)", min_value=0, value=0, step=10)

            gf = gdf_unificado.copy()
            if dist_sel != "(todas)" and "DISTRIBUIDORA" in gf.columns:
                gf = gf[gf["DISTRIBUIDORA"] == dist_sel]
            gf["COD_ID"] = gf["COD_ID"].astype(str)

            cod_default = []
            if top_n and top_n > 0 and "POTENCIA_CALCULADA" in gf.columns:
                cod_default = gf.sort_values("POTENCIA_CALCULADA", ascending=False).head(int(top_n))["COD_ID"].tolist()

            cod_ids = st.multiselect("Selecionar COD_IDs", options=sorted(gf["COD_ID"].unique().tolist()), default=cod_default)

            if st.button("▶️ Rodar detecção solar (COD_IDs selecionados)", use_container_width=True, disabled=(len(cod_ids) == 0)):
                with st.spinner("Rodando YOLO-SEG..."):
                    try:
                        # tenta chamar com kwargs aceitos pela função
                        if hasattr(solar_pipeline, "run_for_cod_ids") and callable(solar_pipeline.run_for_cod_ids):
                            _call_with_accepted_kwargs(
                                solar_pipeline.run_for_cod_ids,
                                cod_ids=cod_ids,
                                distribuidora=None if dist_sel == "(todas)" else dist_sel,
                                conf=float(conf),
                                MAX_WORKERS_DOWNLOAD=int(max_workers),
                                max_workers_download=int(max_workers),
                                MAX_TILES=int(max_tiles),
                                max_tiles=int(max_tiles),
                                ZOOM=int(zoom),
                                zoom=int(zoom),
                            )
                            st.success("Solar finalizado. Veja `Dados Processados/solar_resumo_por_area.csv`.")
                        else:
                            st.error("Não encontrei a função solar_pipeline.run_for_cod_ids().")
                        st.cache_data.clear()
                    except Exception as e:
                        st.error(f"Erro no Solar: {e}")

            st.divider()
            if os.path.exists(ARQUIVO_SOLAR_RESUMO):
                st.dataframe(pd.read_csv(ARQUIVO_SOLAR_RESUMO).head(300), use_container_width=True, height=360)
            else:
                st.info("Ainda não existe `solar_resumo_por_area.csv`.")


# TAB 5 — EXPORT
with tabs[4]:
    st.subheader("Export / Base Completa")

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**Downloads rápidos**")
        for p in [ARQUIVO_UNIFICADO, ARQUIVO_PERFIS, ARQUIVO_SOLAR_RESUMO, ARQUIVO_SOLAR_DETECCOES]:
            if os.path.exists(p):
                with open(p, "rb") as f:
                    st.download_button(
                        label=f"⬇️ Baixar {os.path.basename(p)} ({_human_bytes(os.path.getsize(p))})",
                        data=f,
                        file_name=os.path.basename(p),
                        mime="application/octet-stream",
                        use_container_width=True,
                    )
            else:
                st.caption(f"— {os.path.basename(p)} (não encontrado)")

    with col2:
        st.markdown("**Gerar ZIP da base completa**")
        if st.button("📦 Gerar BASE_COMPLETA.zip", use_container_width=True):
            escrever_instrucoes_base_completa()
            buf = io.BytesIO()
            with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as z:
                for p in [ARQUIVO_UNIFICADO, ARQUIVO_PERFIS, ARQUIVO_SOLAR_RESUMO, ARQUIVO_SOLAR_DETECCOES, INSTRUCOES_BASE_COMPLETA]:
                    if os.path.exists(p):
                        z.write(p, arcname=os.path.join("Dados Processados", os.path.basename(p)))
            st.download_button("⬇️ Baixar BASE_COMPLETA.zip", data=buf.getvalue(), file_name="BASE_COMPLETA.zip", mime="application/zip", use_container_width=True)
            st.success("ZIP gerado com os arquivos disponíveis + README_BASE_COMPLETA.txt")

with st.sidebar:
    st.divider()
    st.caption("📁 Ícones esperados em: assets/icons/raio.png, satelite.png, torre.png, transformador.png")
    if ICONS_DIR.exists():
        pngs = sorted([p.name for p in ICONS_DIR.glob("*.png")])
        st.caption("Encontrados: " + (", ".join(pngs) if pngs else "(nenhum .png)"))
    else:
        st.caption("Pasta assets/icons não encontrada.")
