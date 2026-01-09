"""
Este script utiliza a API Overpass do OpenStreetMap para coletar coordenadas de
estabelecimentos comerciais e industriais na cidade do Rio de Janeiro.
Os dados coletados são salvos em um arquivo KML.
"""

import requests
import simplekml
import time

# --- CONFIGURAÇÕES ---
# Bounding Box aproximada do Rio de Janeiro (S, W, N, E)
BBOX_RJ = "-23.08, -43.79, -22.74, -43.10"
ARQUIVO_KML = "comercios_industrias_rj.kml"

def get_osm_data():
    """
    Consulta a API Overpass para obter pontos de comércio e indústria.
    """
    print("DEBUG: Consultando API Overpass do OpenStreetMap...")
    
    # Query Overpass: busca apenas nodes para ser mais rápido e evitar timeout
    overpass_query = f"""
    [out:json][timeout:180];
    (
      node["shop"]({BBOX_RJ});
      node["landuse"="industrial"]({BBOX_RJ});
      node["industrial"]({BBOX_RJ});
    );
    out body;
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

def create_kml(elements: list):
    """
    Cria um arquivo KML a partir dos elementos do OSM.
    """
    print(f"DEBUG: Gerando arquivo KML com {len(elements)} pontos...")
    kml = simplekml.Kml()
    
    # Pastas para organizar
    folder_shop = kml.newfolder(name="Comércios (Shops)")
    folder_ind = kml.newfolder(name="Indústrias")
    
    for el in elements:
        # Obter coordenadas (center para ways, lat/lon para nodes)
        lat = el.get('lat') or el.get('center', {}).get('lat')
        lon = el.get('lon') or el.get('center', {}).get('lon')
        
        if lat is None or lon is None:
            continue
            
        tags = el.get('tags', {})
        name = tags.get('name', 'Sem Nome')
        
        # Determinar categoria
        is_shop = 'shop' in tags
        is_industrial = 'industrial' in tags or tags.get('landuse') == 'industrial'
        
        if is_shop:
            pnt = folder_shop.newpoint(name=name, coords=[(lon, lat)])
            pnt.description = f"Tipo: {tags.get('shop')}\nEndereço: {tags.get('addr:street', '')}"
            pnt.style.iconstyle.icon.href = 'http://maps.google.com/mapfiles/kml/paddle/blu-circle.png'
        elif is_industrial:
            pnt = folder_ind.newpoint(name=name, coords=[(lon, lat)])
            pnt.description = f"Tipo: {tags.get('industrial') or tags.get('landuse')}\nEndereço: {tags.get('addr:street', '')}"
            pnt.style.iconstyle.icon.href = 'http://maps.google.com/mapfiles/kml/paddle/red-circle.png'
            
    kml.save(ARQUIVO_KML)
    print(f"DEBUG: Sucesso! Arquivo salvo em: {ARQUIVO_KML}")

def main():
    start_time = time.time()
    elements = get_osm_data()
    
    if elements:
        create_kml(elements)
    else:
        print("DEBUG: Nenhum dado retornado do OSM.")
        
    print(f"DEBUG: Tempo total: {time.time() - start_time:.2f} segundos")

if __name__ == "__main__":
    main()
