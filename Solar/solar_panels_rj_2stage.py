"""
Solar Panels Pipeline (RJ) — 1 Estágio (SINGLE) — Zoom 20
Detecta painéis dentro das áreas do ETL e calcula área (m²) por máscara.

Fluxo:
1) extrator.py -> gera Dados Processados/dados_finais_rj.geojson
2) este script -> gera Dados Processados/solar_paineis_deteccoes.geojson + CSV resumo
"""

import os
import math
import time
import sqlite3
import hashlib
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Tuple, Optional, Iterable

import numpy as np
import pandas as pd
import geopandas as gpd
from tqdm import tqdm

from shapely.geometry import Polygon, box
from shapely.ops import unary_union
from shapely.prepared import prep

from ultralytics import YOLO


# ---------------- Paths ----------------

ETL_GEOJSON = os.path.join("Dados Processados", "dados_finais_rj.geojson")
MODEL_PATH = os.path.join("Solar", "best.pt")

OUT_DETECTIONS_GEOJSON = os.path.join("Dados Processados", "solar_paineis_deteccoes.geojson")
OUT_SUMMARY_CSV = os.path.join("Dados Processados", "solar_resumo_por_area.csv")

CHECKPOINT_DB = os.path.join("Solar", "solar_checkpoint.db")
TILES_CACHE_DIR = os.path.join("Solar", "tiles_cache")


# ---------------- Tiles Download ----------------

TILE_URL = "https://mt1.google.com/vt/lyrs=s&x={x}&y={y}&z={z}"
TILE_HEADERS = {"User-Agent": "Mozilla/5.0"}

DOWNLOAD_TIMEOUT = 15
DOWNLOAD_RETRIES = 2
MAX_WORKERS_DOWNLOAD = 16

# tile do Google normalmente é 256x256
TILE_PX = 256


# ---------------- SINGLE Stage Params ----------------

Z = 20
CONF = 0.25  # ajuste: 0.25~0.40 costuma ser bom; suba para reduzir falso positivo

TILE_CHUNK = 1200
BATCH = 32

MAX_TILES_PER_DIST = 3_000_000


# ---------------- TEST MODE ----------------
TEST_MODE = True
TEST_DISTRIBUIDORA = "LIGHT"   # "LIGHT" / "ENEL" / None
TEST_COD_IDS = ["10385925"]    # [] para desligar filtro de COD_ID
TEST_BBOX = None              # (minlon, minlat, maxlon, maxlat) ou None
TEST_MAX_TILES = 300000


# ---------------- Tile math utils ----------------

def latlon_to_tile(lat: float, lon: float, zoom: int) -> Tuple[int, int]:
    lat_rad = math.radians(lat)
    n = 2.0 ** zoom
    xtile = int((lon + 180.0) / 360.0 * n)
    ytile = int((1.0 - math.log(math.tan(lat_rad) + (1 / math.cos(lat_rad))) / math.pi) / 2.0 * n)
    return xtile, ytile


def tile_to_latlon(x: int, y: int, zoom: int) -> Tuple[float, float]:
    n = 2.0 ** zoom
    lon_deg = x / n * 360.0 - 180.0
    lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * y / n)))
    return math.degrees(lat_rad), lon_deg


def tile_bounds_wgs84(x: int, y: int, z: int) -> Tuple[float, float, float, float]:
    lat1, lon1 = tile_to_latlon(x, y, z)
    lat2, lon2 = tile_to_latlon(x + 1, y + 1, z)
    minlon, maxlon = lon1, lon2
    maxlat, minlat = lat1, lat2
    return (minlon, minlat, maxlon, maxlat)


def tile_polygon_wgs84(t: Tuple[int, int, int]) -> Polygon:
    x, y, z = t
    minlon, minlat, maxlon, maxlat = tile_bounds_wgs84(x, y, z)
    return box(minlon, minlat, maxlon, maxlat)


def tiles_covering_geometry_bbox(geom, z: int) -> List[Tuple[int, int, int]]:
    minx, miny, maxx, maxy = geom.bounds
    x_min, y_max = latlon_to_tile(miny, minx, z)
    x_max, y_min = latlon_to_tile(maxy, maxx, z)

    x0, x1 = sorted([x_min, x_max])
    y0, y1 = sorted([y_min, y_max])

    tiles = []
    for x in range(x0, x1 + 1):
        for y in range(y0, y1 + 1):
            tiles.append((x, y, z))
    return tiles


def filter_tiles_intersecting_geom(tiles: List[Tuple[int, int, int]], geom, desc: str) -> List[Tuple[int, int, int]]:
    if not tiles:
        return []
    prepared = prep(geom)
    minx, miny, maxx, maxy = geom.bounds

    out = []
    for t in tqdm(tiles, desc=desc, unit="tile"):
        poly = tile_polygon_wgs84(t)
        bx0, by0, bx1, by1 = poly.bounds
        if bx1 < minx or bx0 > maxx or by1 < miny or by0 > maxy:
            continue
        if prepared.intersects(poly):
            out.append(t)
    return out


def chunked(seq: List, size: int) -> Iterable[List]:
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


# ---------------- Cache / Download ----------------

def _tile_cache_path(x: int, y: int, z: int) -> str:
    d = os.path.join(TILES_CACHE_DIR, str(z), str(x))
    os.makedirs(d, exist_ok=True)
    return os.path.join(d, f"{y}.jpg")


def download_single_tile(t: Tuple[int, int, int]) -> Optional[str]:
    x, y, z = t
    path = _tile_cache_path(x, y, z)

    if os.path.exists(path) and os.path.getsize(path) > 1024:
        return path

    url = TILE_URL.format(x=x, y=y, z=z)
    tmp = path + ".part"

    for _ in range(DOWNLOAD_RETRIES + 1):
        try:
            r = requests.get(url, headers=TILE_HEADERS, timeout=DOWNLOAD_TIMEOUT)
            if r.status_code == 200 and r.content and len(r.content) > 1024:
                with open(tmp, "wb") as f:
                    f.write(r.content)
                os.replace(tmp, path)
                return path
        except Exception:
            continue

    try:
        if os.path.exists(tmp):
            os.remove(tmp)
    except Exception:
        pass

    return None


def download_tiles_parallel(tiles: List[Tuple[int, int, int]], max_workers: int, desc: str) -> Dict[Tuple[int, int, int], str]:
    out = {}
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(download_single_tile, t): t for t in tiles}
        for fut in tqdm(as_completed(futures), total=len(futures), desc=desc, unit="tile"):
            t = futures[fut]
            try:
                path = fut.result()
                if path and os.path.exists(path) and os.path.getsize(path) > 1024:
                    out[t] = path
            except Exception:
                pass
    return out


# ---------------- Checkpoint DB ----------------

def db_connect(path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    return sqlite3.connect(path, check_same_thread=False)


def db_init(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS tiles (
            distribuidora TEXT,
            z INTEGER,
            x INTEGER,
            y INTEGER,
            stage TEXT,
            status TEXT,
            has_panel INTEGER,
            updated_at REAL,
            PRIMARY KEY (distribuidora, z, x, y, stage)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS detections (
            id TEXT PRIMARY KEY,
            distribuidora TEXT,
            cod_id TEXT,
            z INTEGER,
            x INTEGER,
            y INTEGER,
            conf REAL,
            area_m2 REAL,
            geometry_wkt TEXT,
            created_at REAL
        )
        """
    )
    conn.commit()


def db_tile_is_done(conn: sqlite3.Connection, dist: str, t: Tuple[int, int, int], stage: str) -> bool:
    x, y, z = t
    cur = conn.cursor()
    cur.execute(
        "SELECT status FROM tiles WHERE distribuidora=? AND z=? AND x=? AND y=? AND stage=?",
        (dist, z, x, y, stage),
    )
    row = cur.fetchone()
    return bool(row and row[0] == "done")


def db_tile_mark(conn: sqlite3.Connection, dist: str, t: Tuple[int, int, int], stage: str, status: str, has_panel: int = 0):
    x, y, z = t
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO tiles (distribuidora, z, x, y, stage, status, has_panel, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(distribuidora, z, x, y, stage)
        DO UPDATE SET status=excluded.status, has_panel=excluded.has_panel, updated_at=excluded.updated_at
        """,
        (dist, z, x, y, stage, status, has_panel, time.time()),
    )
    conn.commit()


def _det_id(dist: str, cod_id: str, t: Tuple[int, int, int], idx: int) -> str:
    s = f"{dist}|{cod_id}|{t[2]}|{t[0]}|{t[1]}|{idx}"
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def db_add_detection(conn: sqlite3.Connection, det: dict):
    cur = conn.cursor()
    cur.execute(
        """
        INSERT OR REPLACE INTO detections
        (id, distribuidora, cod_id, z, x, y, conf, area_m2, geometry_wkt, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            det["id"],
            det["distribuidora"],
            det["cod_id"],
            det["z"],
            det["x"],
            det["y"],
            float(det["conf"]),
            float(det["area_m2"]),
            det["geometry_wkt"],
            time.time(),
        ),
    )
    conn.commit()


# ---------------- Geo + YOLO helpers ----------------

def sanitize_geometries(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    gdf = gdf[gdf.geometry.notna()].copy()
    gdf = gdf[~gdf.geometry.is_empty].copy()

    try:
        gdf["geometry"] = gdf["geometry"].make_valid()
    except Exception:
        pass

    def _fix(g):
        if g is None or g.is_empty:
            return None
        try:
            if not g.is_valid:
                g2 = g.buffer(0)
                return g2 if (g2 is not None and not g2.is_empty) else None
            return g
        except Exception:
            return None

    gdf["geometry"] = gdf["geometry"].apply(_fix)
    gdf = gdf[gdf.geometry.notna()].copy()
    gdf = gdf[~gdf.geometry.is_empty].copy()
    return gdf


def yolo_infer_batch_paths(model: YOLO, items, conf: float, imgsz: int = 640):
    paths = [p for _, p in items]
    results = model.predict(paths, conf=conf, imgsz=imgsz, batch=len(paths), verbose=False)
    return results


def has_any_mask(res) -> bool:
    try:
        return res.masks is not None and res.masks.data is not None and res.masks.data.shape[0] > 0
    except Exception:
        return False


def extract_masks_confs_contours(res):
    """
    Retorna lista de (mask_bool, conf, contour_norm_xy)
    - mask_bool vem de res.masks.data (HxW) no tamanho do imgsz
    - contour_norm_xy vem de res.masks.xyn (Nx2) normalizado (0..1)
    """
    if not has_any_mask(res):
        return []

    masks = res.masks.data.cpu().numpy()  # (n, H, W)
    confs = res.boxes.conf.cpu().numpy() if res.boxes is not None else np.ones((masks.shape[0],), dtype=float)

    contours = None
    try:
        contours = res.masks.xyn  # list of (N,2) arrays
    except Exception:
        contours = None

    out = []
    for i in range(masks.shape[0]):
        m = masks[i] > 0.5
        c = float(confs[i]) if i < len(confs) else 1.0
        cn = None
        if contours is not None and i < len(contours):
            cn = contours[i]
        out.append((m, c, cn))
    return out


def tile_center_latlon(t: Tuple[int, int, int]) -> Tuple[float, float]:
    x, y, z = t
    minlon, minlat, maxlon, maxlat = tile_bounds_wgs84(x, y, z)
    return ((minlat + maxlat) / 2, (minlon + maxlon) / 2)


def meters_per_pixel(lat: float, z: int) -> float:
    # WebMercator m/px para pixels do tile "real" (256 px) naquele zoom
    return 156543.03392 * math.cos(math.radians(lat)) / (2 ** z)


def mask_to_area_m2(mask_bool, lat_center: float, z: int) -> float:
    """
    Corrige o fato do YOLO redimensionar o tile (256) para imgsz (ex.: 640).
    A máscara está no grid (H,W) do imgsz; então o m/px efetivo precisa ser escalado.
    """
    H, W = mask_bool.shape
    if W <= 1 or H <= 1:
        return 0.0

    mpp_tile = meters_per_pixel(lat_center, z)          # m/px no tile "verdadeiro" (256px)
    scale = TILE_PX / float(W)                          # W é imgsz (ex.: 640). Ex.: 256/640=0.4
    mpp_mask = mpp_tile * scale                         # m/px no grid da máscara
    return float(mask_bool.sum()) * (mpp_mask ** 2)


def choose_best_cod_id(candidates: gpd.GeoDataFrame, tile_poly: Polygon) -> str:
    best_cod = str(candidates.iloc[0]["COD_ID"])
    best_area = -1.0
    for _, row in candidates.iterrows():
        try:
            inter = row.geometry.intersection(tile_poly)
            a = inter.area if not inter.is_empty else 0.0
            if a > best_area:
                best_area = a
                best_cod = str(row["COD_ID"])
        except Exception:
            continue
    return best_cod


def contour_to_polygon_wgs84(contour_norm_xy, t: Tuple[int, int, int]) -> Optional[Polygon]:
    """
    Converte o contorno normalizado (0..1) para um polígono em lon/lat dentro do tile.
    """
    if contour_norm_xy is None:
        return None
    try:
        pts = np.asarray(contour_norm_xy)
        if pts.shape[0] < 3:
            return None
        x, y, z = t
        minlon, minlat, maxlon, maxlat = tile_bounds_wgs84(x, y, z)

        coords = []
        for xn, yn in pts:
            lon = minlon + float(xn) * (maxlon - minlon)
            lat = maxlat - float(yn) * (maxlat - minlat)
            coords.append((lon, lat))

        poly = Polygon(coords)
        if poly.is_empty:
            return None
        if not poly.is_valid:
            poly = poly.buffer(0)
        return poly if (poly is not None and not poly.is_empty) else None
    except Exception:
        return None


# ---------------- SINGLE Stage ----------------

def run_single_zoom20(conn, model, dist: str, areas_dist: gpd.GeoDataFrame, union_geom):
    print(f"\n==============================")
    print(f"[{dist}] SINGLE — z{Z}, conf={CONF}")
    print(f"==============================")

    tiles = tiles_covering_geometry_bbox(union_geom, Z)
    tiles = filter_tiles_intersecting_geom(tiles, union_geom, f"[{dist}] Filtrando tiles z{Z} (interseção)")
    print(f"[{dist}] Tiles após filtro: {len(tiles)}")

    if TEST_MODE and len(tiles) > TEST_MAX_TILES:
        tiles = tiles[:TEST_MAX_TILES]
        print(f"[{dist}] [TEST] Limitando tiles para {len(tiles)}")

    if len(tiles) > MAX_TILES_PER_DIST and not TEST_MODE:
        raise RuntimeError(f"[{dist}] Tiles demais: {len(tiles)}.")

    tiles = [t for t in tiles if not db_tile_is_done(conn, dist, t, "single")]
    print(f"[{dist}] Tiles pendentes: {len(tiles)}")

    detections_count = 0

    for chunk_idx, tile_chunk in enumerate(chunked(tiles, TILE_CHUNK), start=1):
        print(f"\n[{dist}] Chunk {chunk_idx} — {len(tile_chunk)} tiles")

        tile_map = download_tiles_parallel(tile_chunk, MAX_WORKERS_DOWNLOAD, desc=f"[{dist}] Download z{Z}")
        items = list(tile_map.items())

        if not items:
            for t in tile_chunk:
                db_tile_mark(conn, dist, t, "single", "done", has_panel=0)
            continue

        batches = list(chunked(items, BATCH))
        for batch in tqdm(batches, desc=f"[{dist}] Infer z{Z}", unit="batch"):
            results = yolo_infer_batch_paths(model, batch, conf=CONF, imgsz=640)

            for (t, _path), res in zip(batch, results):
                x, y, z = t
                tile_poly = tile_polygon_wgs84(t)

                if not has_any_mask(res):
                    db_tile_mark(conn, dist, t, "single", "done", has_panel=0)
                    continue

                candidates = areas_dist[areas_dist.intersects(tile_poly)]
                if candidates.empty:
                    # achou máscara, mas tile não bateu em área (raro, mas pode acontecer na borda)
                    db_tile_mark(conn, dist, t, "single", "done", has_panel=1)
                    continue

                best_cod = choose_best_cod_id(candidates, tile_poly)
                latc, _lonc = tile_center_latlon(t)

                masks = extract_masks_confs_contours(res)
                if not masks:
                    db_tile_mark(conn, dist, t, "single", "done", has_panel=0)
                    continue

                for idx_det, (mask_bool, conf, contour_norm) in enumerate(masks):
                    area_m2 = mask_to_area_m2(mask_bool, latc, z)

                    # geometria: polígono do contorno (melhor pro mapa)
                    poly = contour_to_polygon_wgs84(contour_norm, t)
                    if poly is None:
                        # fallback: tile inteiro (ainda funciona)
                        poly = tile_poly

                    det = {
                        "id": _det_id(dist, best_cod, t, idx_det),
                        "distribuidora": dist,
                        "cod_id": best_cod,
                        "z": z,
                        "x": x,
                        "y": y,
                        "conf": float(conf),
                        "area_m2": float(area_m2),
                        "geometry_wkt": poly.wkt,
                    }
                    db_add_detection(conn, det)
                    detections_count += 1

                db_tile_mark(conn, dist, t, "single", "done", has_panel=1)

    print(f"\n[{dist}] SINGLE concluído. Detections adicionadas: {detections_count}")
    return detections_count


# ---------------- Export ----------------

def export_results(conn: sqlite3.Connection):
    print("\n[Export] Lendo detecções do DB...")
    cur = conn.cursor()
    rows = cur.execute(
        "SELECT distribuidora, cod_id, conf, area_m2, geometry_wkt FROM detections"
    ).fetchall()

    if not rows:
        print("[Export] Nenhuma detecção encontrada.")
        return

    df = pd.DataFrame(rows, columns=["DISTRIBUIDORA", "COD_ID", "conf", "area_m2", "wkt"])
    gdf = gpd.GeoDataFrame(df, geometry=gpd.GeoSeries.from_wkt(df["wkt"]), crs="EPSG:4326").drop(columns=["wkt"])

    os.makedirs(os.path.dirname(OUT_DETECTIONS_GEOJSON), exist_ok=True)

    print(f"[Export] Salvando detecções: {OUT_DETECTIONS_GEOJSON}")
    gdf.to_file(OUT_DETECTIONS_GEOJSON, driver="GeoJSON")

    resumo = (
        gdf.groupby(["DISTRIBUIDORA", "COD_ID"])
        .agg(
            area_total_m2=("area_m2", "sum"),
            qtd_paineis=("area_m2", "count"),
            conf_media=("conf", "mean"),
            area_media_m2=("area_m2", "mean"),
        )
        .reset_index()
        .sort_values(["area_total_m2"], ascending=False)
    )

    print(f"[Export] Salvando resumo: {OUT_SUMMARY_CSV}")
    resumo.to_csv(OUT_SUMMARY_CSV, index=False)

    print(f"[Export] OK. Detections: {len(gdf)} | Linhas resumo: {len(resumo)}")


# ---------------- Main ----------------

def main():
    t0 = time.time()

    os.makedirs(os.path.dirname(CHECKPOINT_DB), exist_ok=True)
    os.makedirs(TILES_CACHE_DIR, exist_ok=True)
    os.makedirs(os.path.dirname(OUT_DETECTIONS_GEOJSON), exist_ok=True)

    print("[Solar] Lendo áreas do ETL...")
    areas = gpd.read_file(ETL_GEOJSON)

    if areas.empty:
        raise RuntimeError("GeoJSON do ETL está vazio.")

    if "DISTRIBUIDORA" not in areas.columns:
        raise RuntimeError("GeoJSON não tem coluna DISTRIBUIDORA. Verifique o extrator.py.")

    if "COD_ID" not in areas.columns:
        raise RuntimeError("GeoJSON não tem coluna COD_ID. Verifique o extrator.py.")

    areas["DISTRIBUIDORA"] = areas["DISTRIBUIDORA"].astype(str).str.upper().str.strip()
    areas["COD_ID"] = areas["COD_ID"].astype(str)

    # Áreas efetivas: por padrão, processar somente Plena/Satélite (áreas consolidadas)
    if "FEATURE_KIND" in areas.columns:
        areas = areas[areas["FEATURE_KIND"] == "AREA_MASTER"].copy()
    elif "CLASSIFICACAO" in areas.columns:
        areas["CLASSIFICACAO"] = areas["CLASSIFICACAO"].astype(str)
        areas = areas[
            areas["CLASSIFICACAO"].str.startswith("1. Distribuição Plena")
            | areas["CLASSIFICACAO"].str.startswith("2. Distribuição Satélite")
        ].copy()

    # Segurança: manter apenas geometrias poligonais para o recorte de tiles
    try:
        areas = areas[areas.geometry.geom_type.isin(["Polygon", "MultiPolygon"])].copy()
    except Exception:
        pass


    # ---------------- TEST FILTERS ----------------
    if TEST_MODE:
        print("\n[TEST] Modo teste ligado.")

        if TEST_DISTRIBUIDORA:
            areas = areas[areas["DISTRIBUIDORA"] == str(TEST_DISTRIBUIDORA).upper()].copy()
            print(f"[TEST] Filtrando por distribuidora: {TEST_DISTRIBUIDORA} -> {len(areas)} áreas")

        if TEST_COD_IDS:
            ids = [str(x) for x in TEST_COD_IDS]
            areas = areas[areas["COD_ID"].isin(ids)].copy()
            print(f"[TEST] Filtrando por COD_IDs: {ids} -> {len(areas)} áreas")

        if TEST_BBOX:
            minlon, minlat, maxlon, maxlat = TEST_BBOX
            bbox_geom = box(minlon, minlat, maxlon, maxlat)
            areas = areas[areas.intersects(bbox_geom)].copy()
            print(f"[TEST] Filtrando por BBOX {TEST_BBOX} -> {len(areas)} áreas")

        if areas.empty:
            raise RuntimeError("[TEST] Filtros deixaram 0 áreas. Ajuste TEST_COD_IDS/TEST_BBOX.")

    print("[Solar] Carregando modelo YOLO...")
    model = YOLO(MODEL_PATH)

    conn = db_connect(CHECKPOINT_DB)
    db_init(conn)

    dist_order = [d for d in ["ENEL", "LIGHT"] if d in set(areas["DISTRIBUIDORA"].unique())]
    others = [d for d in sorted(set(areas["DISTRIBUIDORA"].unique())) if d not in dist_order]
    dist_list = dist_order + others

    for dist in dist_list:
        print(f"\n\n###############################################")
        print(f"### PROCESSANDO DISTRIBUIDORA: {dist}")
        print(f"###############################################")

        areas_dist = areas[areas["DISTRIBUIDORA"] == dist].copy()
        areas_dist = sanitize_geometries(areas_dist)

        if areas_dist.empty:
            print(f"[{dist}] Sem geometria válida após saneamento. Pulando.")
            continue

        print(f"[{dist}] Unificando geometrias (union) para filtrar tiles...")
        try:
            union_geom = unary_union(areas_dist.geometry.values)
        except Exception:
            areas_dist["geometry"] = areas_dist.geometry.buffer(0)
            union_geom = unary_union(areas_dist.geometry.values)

        try:
            if hasattr(union_geom, "is_valid") and not union_geom.is_valid:
                union_geom = union_geom.buffer(0)
        except Exception:
            pass

        run_single_zoom20(conn, model, dist, areas_dist, union_geom)

    export_results(conn)
    conn.close()

    dt = time.time() - t0
    print(f"\n[OK] Pipeline completo. Tempo total: {dt/60:.1f} min")

# ---------------- Streamlit-friendly API ----------------

def run_for_cod_ids(
    cod_ids,
    distribuidora=None,
    conf: float = 0.25,
    max_tiles: int = 5000,
    max_workers_download: int = 12,
    batch: int = 16,
):
    """Executa o pipeline solar **somente** para uma lista de COD_IDs.

    Motivo: o YOLO-SEG é pesado; no Streamlit precisamos rodar por recorte.
    O pipeline continua incremental via CHECKPOINT_DB (não reprocessa tiles já marcados como done).

    Args:
        cod_ids: lista de COD_ID (strings)
        distribuidora: 'LIGHT' / 'ENEL' / None
        conf: threshold de confiança do YOLO
        max_tiles: limite de segurança por execução
        max_workers_download: paralelismo no download de tiles
        batch: batch size do YOLO
    """
    global TEST_MODE, TEST_DISTRIBUIDORA, TEST_COD_IDS, CONF, MAX_TILES_PER_DIST, MAX_WORKERS_DOWNLOAD, BATCH

    TEST_MODE_OLD = TEST_MODE
    TEST_DISTRIBUIDORA_OLD = TEST_DISTRIBUIDORA
    TEST_COD_IDS_OLD = TEST_COD_IDS
    CONF_OLD = CONF
    MAX_TILES_OLD = MAX_TILES_PER_DIST
    MAX_WORKERS_OLD = MAX_WORKERS_DOWNLOAD
    BATCH_OLD = BATCH

    try:
        TEST_MODE = True
        TEST_DISTRIBUIDORA = distribuidora
        TEST_COD_IDS = [str(x) for x in cod_ids]
        CONF = float(conf)
        MAX_TILES_PER_DIST = int(max_tiles)
        MAX_WORKERS_DOWNLOAD = int(max_workers_download)
        BATCH = int(batch)

        main()
    finally:
        TEST_MODE = TEST_MODE_OLD
        TEST_DISTRIBUIDORA = TEST_DISTRIBUIDORA_OLD
        TEST_COD_IDS = TEST_COD_IDS_OLD
        CONF = CONF_OLD
        MAX_TILES_PER_DIST = MAX_TILES_OLD
        MAX_WORKERS_DOWNLOAD = MAX_WORKERS_OLD
        BATCH = BATCH_OLD


if __name__ == "__main__":
    main()
