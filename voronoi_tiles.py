"""
Este script identifica a célula de Voronoi correspondente a uma coordenada (Lat/Lon)
e baixa todos os tiles do Google Maps que cobrem essa área específica.
Inspirado no mosaico_circular.py, utiliza paralelismo e precisão geográfica.
"""

import os
import math
import requests
import geopandas as gpd
import geobr
import pandas as pd
import numpy as np
from PIL import Image
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from shapely.geometry import box, MultiPoint, Point, Polygon
from shapely.ops import voronoi_diagram
from PIL import Image, ImageDraw

# Aumentar o limite de pixels para evitar DecompressionBombError em mosaicos grandes
Image.MAX_IMAGE_PIXELS = None

# --- CONFIGURAÇÕES GLOBAIS ---
# Coordenada de exemplo (pode ser alterada pelo usuário)
LAT_ALVO = -22.9068
LON_ALVO = -43.1729
ZOOM = 20  # Nível de zoom para os tiles
PASTA_TILES = "tiles_cache"
ARQUIVO_SAIDA = "mosaico_voronoi.png"
MAX_WORKERS = 15  # Downloads simultâneos
CAMINHO_GDB = r"LIGHT_382_2021-09-30_M10_20231218-2133.gdb"
NOME_CAMADA_SUB = 'SUB'

def latlon_to_tile(lat: float, lon: float, zoom: int) -> tuple[int, int]:
    """Converte Lat/Lon para índices de tile X, Y."""
    lat_rad = math.radians(lat)
    n = 2.0 ** zoom
    xtile = int((lon + 180.0) / 360.0 * n)
    ytile = int((1.0 - math.log(math.tan(lat_rad) + (1 / math.cos(lat_rad))) / math.pi) / 2.0 * n)
    return xtile, ytile

def tile_to_latlon(x: int, y: int, zoom: int) -> tuple[float, float]:
    """Converte índices de tile X, Y para Lat/Lon (Canto superior esquerdo)."""
    n = 2.0 ** zoom
    lon_deg = x / n * 360.0 - 180.0
    lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * y / n)))
    return math.degrees(lat_rad), lon_deg

def tile_to_latlon_bbox(x: int, y: int, zoom: int) -> box:
    """Retorna o bounding box (shapely) de um tile específico."""
    lat_top, lon_left = tile_to_latlon(x, y, zoom)
    lat_bottom, lon_right = tile_to_latlon(x + 1, y + 1, zoom)
    
    # shapely box(minx, miny, maxx, maxy)
    return box(min(lon_left, lon_right), min(lat_bottom, lat_top), max(lon_left, lon_right), max(lat_bottom, lat_top))

def download_single_tile(x: int, y: int, z: int, path: str) -> str:
    """Baixa uma única tile do Google Maps."""
    if os.path.exists(path):
        return path
    
    url = f"https://mt1.google.com/vt/lyrs=s&x={x}&y={y}&z={z}"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            with open(path, 'wb') as f:
                f.write(response.content)
            return path
    except Exception as e:
        # DEBUG: Log de erro silencioso para não poluir a barra de progresso
        pass
    return None

def obter_voronoi_e_baixar_tiles():
    """
    Lógica principal: Carrega dados, gera Voronoi, encontra a célula e baixa os tiles.
    """
    print(f"DEBUG: Iniciando processamento para Lat={LAT_ALVO}, Lon={LON_ALVO}, Zoom={ZOOM}")

    # 1. Carregar contorno do Rio de Janeiro e Subestações
    print("DEBUG: Carregando contorno do RJ e dados da LIGHT...")
    try:
        rj_shape = geobr.read_municipality(code_muni=3304557, year=2020)
        rj_shape = rj_shape.to_crs("EPSG:4326")
        
        gdf_sub = gpd.read_file(CAMINHO_GDB, layer=NOME_CAMADA_SUB)
        # Projetar para calcular centroide corretamente e voltar para 4326
        gdf_sub_projected = gdf_sub.to_crs("EPSG:31983")
        gdf_sub['geometry'] = gdf_sub_projected.geometry.centroid.to_crs("EPSG:4326")
        gdf_sub = gdf_sub.to_crs("EPSG:4326")
        
        # Filtrar subestações dentro do RJ
        gdf_sub_rj = gpd.clip(gdf_sub, rj_shape)
    except Exception as e:
        print(f"DEBUG ERROR: Falha ao carregar dados geográficos: {e}")
        return

    # 2. Gerar Diagrama de Voronoi
    print("DEBUG: Gerando diagrama de Voronoi...")
    bounds = rj_shape.total_bounds
    pontos_unicos = gdf_sub_rj.geometry.drop_duplicates()
    pontos_uniao = MultiPoint(pontos_unicos.tolist())
    region_box = box(*bounds)
    voronoi_geo = voronoi_diagram(pontos_uniao, envelope=region_box)
    gdf_voronoi = gpd.GeoDataFrame(geometry=list(voronoi_geo.geoms), crs="EPSG:4326")
    
    # Recortar Voronoi com o limite do município
    gdf_voronoi_rj = gpd.clip(gdf_voronoi, rj_shape)

    # 3. Encontrar a célula que contém o ponto alvo
    ponto_alvo = Point(LON_ALVO, LAT_ALVO)
    celula_alvo = None
    
    for _, row in gdf_voronoi_rj.iterrows():
        if row.geometry.contains(ponto_alvo):
            celula_alvo = row.geometry
            break
    
    if celula_alvo is None:
        print("DEBUG: O ponto alvo não está dentro de nenhuma célula de Voronoi no RJ.")
        return

    print("DEBUG: Célula de Voronoi encontrada. Calculando tiles...")

    # 4. Determinar Bounding Box da célula e range de tiles
    v_minx, v_miny, v_maxx, v_maxy = celula_alvo.bounds
    
    # Converter bounds para tiles (considerando que y_min no tile é o lat_max)
    x_start, y_start = latlon_to_tile(v_maxy, v_minx, ZOOM)
    x_end, y_end = latlon_to_tile(v_miny, v_maxx, ZOOM)

    if not os.path.exists(PASTA_TILES):
        os.makedirs(PASTA_TILES)

    # 5. Identificar tiles que intersectam a célula
    tiles_para_baixar = []
    for x in range(x_start, x_end + 1):
        for y in range(y_start, y_end + 1):
            tile_bbox = tile_to_latlon_bbox(x, y, ZOOM)
            if celula_alvo.intersects(tile_bbox):
                tile_path = os.path.join(PASTA_TILES, f"{ZOOM}_{x}_{y}.png")
                tiles_para_baixar.append((x, y, tile_path))

    total_tiles = len(tiles_para_baixar)
    print(f"DEBUG: Total de tiles a processar na célula: {total_tiles}")

    # 6. Download Paralelo (Otimizado: Pula o que já existe)
    tasks = []
    tiles_efetivos = []
    
    for x, y, path in tiles_para_baixar:
        if not os.path.exists(path):
            tiles_efetivos.append((x, y, path))
    
    total_novos = len(tiles_efetivos)
    print(f"DEBUG: {total_novos} tiles novos para baixar (de {total_tiles} totais na área).")

    if total_novos > 0:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for x, y, path in tiles_efetivos:
                tasks.append(executor.submit(download_single_tile, x, y, ZOOM, path))

            for _ in tqdm(as_completed(tasks), total=total_novos, desc="Baixando Tiles Voronoi", unit="tile"):
                pass
    else:
        print("DEBUG: Todos os tiles já estão no cache.")

    print(f"DEBUG: Download concluído. Iniciando montagem do mosaico...")

    # 7. Montagem do Mosaico (Stitching) com Máscara de Precisão
    num_tiles_x = x_end - x_start + 1
    num_tiles_y = y_end - y_start + 1
    largura_px = num_tiles_x * 256
    altura_px = num_tiles_y * 256
    
    mosaico = Image.new("RGB", (largura_px, altura_px))
    mask = Image.new("L", (largura_px, altura_px), 0)
    draw = ImageDraw.Draw(mask)
    
    # Coordenadas geográficas do topo-esquerda do mosaico
    lat_topo_mosaico, lon_esq_mosaico = tile_to_latlon(x_start, y_start, ZOOM)
    # Coordenadas geográficas do fundo-direita do mosaico
    lat_fundo_mosaico, lon_dir_mosaico = tile_to_latlon(x_end + 1, y_end + 1, ZOOM)
    
    for x, y, path in tiles_para_baixar:
        if os.path.exists(path):
            tile_img = Image.open(path)
            px = (x - x_start) * 256
            py = (y - y_start) * 256
            mosaico.paste(tile_img, (px, py))
            
    # Criar polígono da célula em coordenadas de pixel para a máscara
    def geo_to_pixel(lon, lat):
        px = (lon - lon_esq_mosaico) / (lon_dir_mosaico - lon_esq_mosaico) * largura_px
        py = (lat_topo_mosaico - lat) / (lat_topo_mosaico - lat_fundo_mosaico) * altura_px
        return px, py

    if celula_alvo.geom_type == 'Polygon':
        polygons = [celula_alvo]
    else: # MultiPolygon
        polygons = celula_alvo.geoms
        
    for poly in polygons:
        pixel_coords = [geo_to_pixel(lon, lat) for lon, lat in poly.exterior.coords]
        draw.polygon(pixel_coords, fill=255)
        for interior in poly.interiors:
            pixel_coords_int = [geo_to_pixel(lon, lat) for lon, lat in interior.coords]
            draw.polygon(pixel_coords_int, fill=0)

    # Finalização do Mosaico
    resultado = Image.new("RGBA", (largura_px, altura_px), (0, 0, 0, 0))
    resultado.paste(mosaico, (0, 0), mask=mask)
    
    # Crop para remover áreas vazias ao redor
    bbox_mask = mask.getbbox()
    if bbox_mask:
        resultado = resultado.crop(bbox_mask)
        
    resultado.save(ARQUIVO_SAIDA)
    print(f"DEBUG: Mosaico finalizado e salvo em '{ARQUIVO_SAIDA}'.")

if __name__ == "__main__":
    # Parâmetros que o usuário pode mudar facilmente
    LAT_ALVO = -22.9035
    LON_ALVO = -43.1750
    ZOOM = 20
    
    obter_voronoi_e_baixar_tiles()
