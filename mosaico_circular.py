"""
Este script baixa tiles do Google Maps para cobrir uma área circular com PRECISÃO TOTAL.
Utiliza a fórmula de Haversine para garantir que o raio em metros seja exato no terreno.
Otimizado com PARALELISMO (Threads) para downloads ultra-rápidos.
"""

import requests
from PIL import Image, ImageDraw
import math
import os
import numpy as np
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- CONFIGURAÇÕES GLOBAIS ---
LAT_CENTRO = -22.989476   # Parque Ibirapuera
LON_CENTRO = -43.247301
RAIO_METROS = 280      # Raio exato em metros
ZOOM = 20              # Zoom máximo
PASTA_TILES = "tiles_cache"
ARQUIVO_SAIDA = "mosaico_circular_preciso.png"
MAX_WORKERS = 10       # Número de downloads simultâneos

def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calcula a distância em metros entre dois pontos."""
    R = 6371000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))

def latlon_to_tile(lat: float, lon: float, zoom: int) -> tuple[int, int]:
    """Converte Lat/Lon para índices de tile X, Y."""
    lat_rad = math.radians(lat)
    n = 2.0 ** zoom
    xtile = int((lon + 180.0) / 360.0 * n)
    ytile = int((1.0 - math.log(math.tan(lat_rad) + (1 / math.cos(lat_rad))) / math.pi) / 2.0 * n)
    return xtile, ytile

def tile_to_latlon(x: int, y: int, zoom: int) -> tuple[float, float]:
    """Converte índices de tile X, Y para Lat/Lon."""
    n = 2.0 ** zoom
    lon_deg = x / n * 360.0 - 180.0
    lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * y / n)))
    return math.degrees(lat_rad), lon_deg

def download_single_tile(x: int, y: int, z: int, path: str) -> str:
    """
    Função para baixar uma única tile. Thread-safe.
    """
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
        print(f"DEBUG: Erro no download da tile {x},{y}: {e}")
    return None

def criar_mosaico_paralelo():
    print(f"DEBUG: Iniciando mosaico OTIMIZADO. Raio={RAIO_METROS}m, Workers={MAX_WORKERS}")

    # 1. Bounding Box
    delta_lat = RAIO_METROS / 111111.0
    delta_lon = RAIO_METROS / (111111.0 * math.cos(math.radians(LAT_CENTRO)))
    x_min, y_min = latlon_to_tile(LAT_CENTRO + delta_lat, LON_CENTRO - delta_lon, ZOOM)
    x_max, y_max = latlon_to_tile(LAT_CENTRO - delta_lat, LON_CENTRO + delta_lon, ZOOM)

    num_tiles_x = x_max - x_min + 1
    num_tiles_y = y_max - y_min + 1
    total_tiles = num_tiles_x * num_tiles_y

    if not os.path.exists(PASTA_TILES):
        os.makedirs(PASTA_TILES)

    # 2. Download Paralelo
    tasks = []
    tile_map = {} # Para saber onde colar cada tile depois
    
    print(f"DEBUG: Preparando download de {total_tiles} tiles...")
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for i, x in enumerate(range(x_min, x_max + 1)):
            for j, y in enumerate(range(y_min, y_max + 1)):
                tile_path = os.path.join(PASTA_TILES, f"{ZOOM}_{x}_{y}.png")
                # Agendamos o download
                future = executor.submit(download_single_tile, x, y, ZOOM, tile_path)
                tasks.append(future)
                tile_map[future] = (i, j)

        # Barra de progresso acompanhando os futures
        for future in tqdm(as_completed(tasks), total=total_tiles, desc="Baixando Tiles", unit="tile"):
            pass # O tqdm atualiza conforme as threads terminam

    # 3. Montagem do Mosaico (Stitching)
    print("DEBUG: Montando mosaico...")
    largura_px = num_tiles_x * 256
    altura_px = num_tiles_y * 256
    mosaico = Image.new("RGB", (largura_px, altura_px))

    # Re-lemos os arquivos para montar (agora que todos baixaram)
    for future, (i, j) in tile_map.items():
        path = future.result()
        if path and os.path.exists(path):
            tile_img = Image.open(path)
            mosaico.paste(tile_img, (i * 256, j * 256))

    # 4. Máscara de Precisão Haversine
    print("DEBUG: Aplicando máscara de precisão...")
    lat_topo, lon_esquerda = tile_to_latlon(x_min, y_min, ZOOM)
    lat_fundo, lon_direita = tile_to_latlon(x_max + 1, y_max + 1, ZOOM)

    mask = Image.new("L", (largura_px, altura_px), 0)
    draw = ImageDraw.Draw(mask)

    p1 = (LAT_CENTRO, LON_CENTRO)
    dist_h = haversine(LAT_CENTRO, LON_CENTRO, LAT_CENTRO, lon_direita)
    px_m_lon = (largura_px / 2) / dist_h if dist_h > 0 else 1
    dist_v = haversine(LAT_CENTRO, LON_CENTRO, lat_fundo, LON_CENTRO)
    px_m_lat = (altura_px / 2) / dist_v if dist_v > 0 else 1

    raio_x = RAIO_METROS * px_m_lon
    raio_y = RAIO_METROS * px_m_lat
    cx = (LON_CENTRO - lon_esquerda) / (lon_direita - lon_esquerda) * largura_px
    cy = (lat_topo - LAT_CENTRO) / (lat_topo - lat_fundo) * altura_px

    draw.ellipse([cx - raio_x, cy - raio_y, cx + raio_x, cy + raio_y], fill=255)

    # 5. Finalização
    resultado = Image.new("RGBA", (largura_px, altura_px), (0, 0, 0, 0))
    resultado.paste(mosaico, (0, 0), mask=mask)
    
    bbox = mask.getbbox()
    if bbox:
        resultado = resultado.crop(bbox)

    resultado.save(ARQUIVO_SAIDA)
    print(f"DEBUG: Mosaico finalizado em {ARQUIVO_SAIDA}")

if __name__ == "__main__":
    criar_mosaico_paralelo()
