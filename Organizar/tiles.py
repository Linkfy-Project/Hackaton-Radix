import requests
from PIL import Image
import math

def get_google_tile(x, y, z):
    """Baixa um tile específico do servidor do Google"""
    # lyrs=s (Satélite), lyrs=y (Híbrido), lyrs=m (Mapa)
    url = f"https://mt1.google.com/vt/lyrs=s&x={x}&y={y}&z={z}"
    response = requests.get(url, stream=True)
    return Image.open(response.raw)

def latlon_to_tile(lat, lon, zoom):
    """Converte coordenadas para índices de tile X e Y"""
    lat_rad = math.radians(lat)
    n = 2.0 ** zoom
    xtile = int((lon + 180.0) / 360.0 * n)
    ytile = int((1.0 - math.log(math.tan(lat_rad) + (1 / math.cos(lat_rad))) / math.pi) / 2.0 * n)
    return xtile, ytile

# Configurações
lat, lon = -23.005435, -43.342231 # Parque Ibirapuera
zoom = 20 # Zoom máximo
# -23.005435° lat
# -43.342231° lon
# Obtém o tile central
x, y = latlon_to_tile(lat, lon, zoom)

# Baixa e salva
tile_img = get_google_tile(x, y, zoom)
tile_img.save("chunk_direto.png")
print("Chunk baixado com sucesso via Tile Server!")