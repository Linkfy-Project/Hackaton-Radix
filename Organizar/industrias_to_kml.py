"""
Este script utiliza a API Overpass do OpenStreetMap para coletar coordenadas de
estabelecimentos industriais na cidade do Rio de Janeiro.
Os dados coletados são salvos em um arquivo KML exclusivo para indústrias.
"""

import requests
import simplekml
import time

# --- CONFIGURAÇÕES ---
# Bounding Box aproximada do Rio de Janeiro (S, W, N, E)
BBOX_RJ = "-23.08, -43.79, -22.74, -43.10"
ARQUIVO_KML = "industrias_rj.kml"

def get_industrial_data():
    """
    Consulta a API Overpass para obter pontos de indústria.
    """
    print("DEBUG: Consultando API Overpass para indústrias...")
    
    # Query Overpass: busca nodes e centroids de áreas industriais
    overpass_query = f"""
    [out:json][timeout:180];
    (
      node["landuse"="industrial"]({BBOX_RJ});
      way["landuse"="industrial"]({BBOX_RJ});
      node["industrial"]({BBOX_RJ});
      way["industrial"]({BBOX_RJ});
    );
    out center;
    """
    
    url = "https://overpass-api.de/api/interpreter"
    
    try:
        response = requests.post(url, data={'data': overpass_query})
        response.raise_for_status()
        data = response.json()
        return data.get('elements', [])
    except Exception as e:
        print(f"DEBUG ERROR (OSM): {e}")
        return []

def create_industrial_kml(elements: list):
    """
    Cria um arquivo KML a partir dos elementos industriais do OSM.
    """
    print(f"DEBUG: Gerando arquivo KML com {len(elements)} indústrias...")
    kml = simplekml.Kml()
    
    for el in elements:
        # Obter coordenadas (center para ways, lat/lon para nodes)
        lat = el.get('lat') or el.get('center', {}).get('lat')
        lon = el.get('lon') or el.get('center', {}).get('lon')
        
        if lat is None or lon is None:
            continue
            
        tags = el.get('tags', {})
        name = tags.get('name', 'Indústria Sem Nome')
        
        pnt = kml.newpoint(name=name, coords=[(lon, lat)])
        
        # Descrição detalhada
        desc = []
        if 'industrial' in tags: desc.append(f"Tipo: {tags['industrial']}")
        if 'landuse' in tags: desc.append(f"Uso: {tags['landuse']}")
        if 'addr:street' in tags: desc.append(f"Rua: {tags['addr:street']}")
        
        pnt.description = "\n".join(desc)
        
        # Estilo: Ícone vermelho para indústrias
        pnt.style.iconstyle.icon.href = 'http://maps.google.com/mapfiles/kml/paddle/red-circle.png'
            
    kml.save(ARQUIVO_KML)
    print(f"DEBUG: Sucesso! Arquivo salvo em: {ARQUIVO_KML}")

def main():
    start_time = time.time()
    elements = get_industrial_data()
    
    if elements:
        create_industrial_kml(elements)
    else:
        print("DEBUG: Nenhum dado industrial retornado do OSM.")
        
    print(f"DEBUG: Tempo total: {time.time() - start_time:.2f} segundos")

if __name__ == "__main__":
    main()
