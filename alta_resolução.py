import requests
from PIL import Image
import math

def latlon_to_tile(lat, lon, zoom):
    lat_rad = math.radians(lat)
    n = 2.0 ** zoom
    xtile = int((lon + 180.0) / 360.0 * n)
    ytile = int((1.0 - math.log(math.tan(lat_rad) + (1 / math.cos(lat_rad))) / math.pi) / 2.0 * n)
    return xtile, ytile

def get_high_res_satellite(lat, lon, zoom, size=3):
    """
    size: número de blocos ao redor do centro (ex: 3 criará uma grade 3x3)
    """
    center_x, center_y = latlon_to_tile(lat, lon, zoom)
    
    # Criar uma imagem em branco para o mosaico (256 pixels por bloco)
    total_size = size * 256
    canvas = Image.new('RGB', (total_size, total_size))

    start_x = center_x - size // 2
    start_y = center_y - size // 2

    print(f"Baixando {size*size} blocos para criar uma imagem de {total_size}x{total_size}px...")

    for i in range(size):
        for j in range(size):
            x, y = start_x + i, start_y + j
            url = f"https://mt1.google.com/vt/lyrs=s&x={x}&y={y}&z={z}"
            
            try:
                response = requests.get(url, stream=True, timeout=10)
                tile = Image.open(response.raw)
                # Cola o bloco na posição correta do canvas
                canvas.paste(tile, (i * 256, j * 256))
            except Exception as e:
                print(f"Erro no bloco {x}, {y}: {e}")

    return canvas

# --- CONFIGURAÇÃO ---
lat = -23.005435  # Latitude
lon = -43.342231  # Longitude
z = 19          # Zoom (19 é muito detalhado, 20-21 é o limite)
grid_size = 5   # Uma grade 5x5 resultará em uma imagem de 1280x1280 pixels

imagem_final = get_high_res_satellite(lat, lon, z, size=grid_size)
imagem_final.save("mapa_alta_resolucao.png")
imagem_final.show()