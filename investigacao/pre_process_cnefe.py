"""
Este script realiza o pré-processamento dos dados do CNEFE_RJ.csv,
associando cada endereço à área de influência (Voronoi) das subestações da LIGHT.
O resultado é salvo em um arquivo CSV para ser consumido rapidamente pelo Streamlit.
"""

import pandas as pd
import geopandas as gpd
import geobr
from shapely.geometry import box, MultiPoint
from shapely.ops import voronoi_diagram
from tqdm import tqdm
import os
import requests

# --- CONFIGURAÇÕES ---
CAMINHO_GDB = r"LIGHT_382_2021-09-30_M10_20231218-2133.gdb"
CAMINHO_CNEFE = "CNEFE_RJ.csv"
NOME_CAMADA_SUB = 'SUB'
NOME_CAMADA_TR = 'UNTRS'
ARQUIVO_SAIDA = "cnefe_stats_by_sub.csv"
ARQUIVO_PONTOS_AMOSTRA = "cnefe_sample_points.csv" # Para o cluster no mapa
LIMITE_PONTOS = None # None para processar TODOS os pontos

def get_osm_data(rj_bounds):
    """
    Consulta a API Overpass para obter pontos de comércio e indústria no RJ.
    """
    print("DEBUG: Consultando API Overpass do OpenStreetMap para dados complementares...")
    bbox = f"{rj_bounds[1]}, {rj_bounds[0]}, {rj_bounds[3]}, {rj_bounds[2]}"
    
    overpass_query = f"""
    [out:json][timeout:180];
    (
      node["shop"]({bbox});
      way["shop"]({bbox});
      node["landuse"="industrial"]({bbox});
      way["landuse"="industrial"]({bbox});
      node["industrial"]({bbox});
      way["industrial"]({bbox});
    );
    out center;
    """
    
    url = "https://overpass-api.de/api/interpreter"
    try:
        response = requests.post(url, data={'data': overpass_query})
        response.raise_for_status()
        elements = response.json().get('elements', [])
        
        points = []
        for el in elements:
            lat = el.get('lat') or el.get('center', {}).get('lat')
            lon = el.get('lon') or el.get('center', {}).get('lon')
            if lat and lon:
                tags = el.get('tags', {})
                category = 'OSM_SHOP' if 'shop' in tags else 'OSM_INDUSTRIAL'
                points.append({'lat': lat, 'lon': lon, 'category': category})
        
        return pd.DataFrame(points)
    except Exception as e:
        print(f"DEBUG ERROR (OSM): {e}")
        return pd.DataFrame()

def pre_process():
    print("DEBUG: Iniciando pré-processamento...")
    
    # 1. Carregar contorno do RJ e Subestações
    print("DEBUG: Carregando contorno do Rio de Janeiro e dados da LIGHT...")
    rj_shape = geobr.read_municipality(code_muni=3304557, year=2020)
    rj_shape = rj_shape.to_crs("EPSG:4326")
    
    gdf_sub = gpd.read_file(CAMINHO_GDB, layer=NOME_CAMADA_SUB)
    
    # Projetar para calcular centroide corretamente
    gdf_sub_projected = gdf_sub.to_crs("EPSG:31983")
    gdf_sub['geometry'] = gdf_sub_projected.geometry.centroid.to_crs("EPSG:4326")
    gdf_sub = gdf_sub.to_crs("EPSG:4326")
    
    # Filtrar subestações dentro do RJ
    gdf_sub_rj = gpd.clip(gdf_sub, rj_shape)
    
    # 2. Gerar Voronoi
    print("DEBUG: Gerando diagrama de Voronoi...")
    bounds = rj_shape.total_bounds
    pontos_unicos = gdf_sub_rj.geometry.drop_duplicates()
    pontos_uniao = MultiPoint(pontos_unicos.tolist())
    region_box = box(*bounds)
    voronoi_geo = voronoi_diagram(pontos_uniao, envelope=region_box)
    gdf_voronoi = gpd.GeoDataFrame(geometry=list(voronoi_geo.geoms), crs="EPSG:4326")
    voronoi_recortado = gpd.clip(gdf_voronoi, rj_shape)
    
    # Associar Voronoi ao COD_ID da subestação
    voronoi_com_sub = gpd.sjoin(voronoi_recortado, gdf_sub_rj[['COD_ID', 'geometry']], how='left', predicate='contains')
    
    # 3. Obter e Processar dados do OSM
    df_osm = get_osm_data(bounds)
    osm_stats = pd.DataFrame()
    if not df_osm.empty:
        print(f"DEBUG: Processando {len(df_osm)} pontos do OSM...")
        gdf_osm = gpd.GeoDataFrame(
            df_osm,
            geometry=gpd.points_from_xy(df_osm.lon, df_osm.lat),
            crs="EPSG:4326"
        )
        joined_osm = gpd.sjoin(gdf_osm, voronoi_com_sub[['COD_ID', 'geometry']], how='inner', predicate='within')
        osm_stats = joined_osm.groupby(['COD_ID', 'category']).size().unstack(fill_value=0)

    # 4. Carregar CNEFE em chunks com barra de progresso
    print("DEBUG: Lendo TODOS os pontos do CNEFE e associando às áreas...")
    chunk_size = 200000
    total_rows = 8962201 # Valor aproximado
    
    all_stats = []
    sample_points = []
    
    with tqdm(total=total_rows, desc="Processando CNEFE") as pbar:
        for chunk in pd.read_csv(CAMINHO_CNEFE, sep=';', usecols=['LATITUDE', 'LONGITUDE', 'COD_ESPECIE'], chunksize=chunk_size):
            chunk = chunk.dropna(subset=['LATITUDE', 'LONGITUDE'])
            
            gdf_chunk = gpd.GeoDataFrame(
                chunk, 
                geometry=gpd.points_from_xy(chunk.LONGITUDE, chunk.LATITUDE),
                crs="EPSG:4326"
            )
            
            joined = gpd.sjoin(gdf_chunk, voronoi_com_sub[['COD_ID', 'geometry']], how='inner', predicate='within')
            chunk_stats = joined.groupby(['COD_ID', 'COD_ESPECIE']).size().reset_index(name='count')
            all_stats.append(chunk_stats)
            sample_points.append(chunk[['LATITUDE', 'LONGITUDE']].sample(frac=0.01))
            pbar.update(len(chunk))
            
    # 5. Consolidar e Salvar
    print("DEBUG: Consolidando estatísticas...")
    df_final_stats = pd.concat(all_stats).groupby(['COD_ID', 'COD_ESPECIE'])['count'].sum().unstack(fill_value=0)
    
    # Integrar dados do OSM
    if not osm_stats.empty:
        df_final_stats = df_final_stats.merge(osm_stats, on='COD_ID', how='left').fillna(0)
    
    df_final_stats.to_csv(ARQUIVO_SAIDA)
    
    print("DEBUG: Salvando amostra de pontos para o mapa...")
    df_sample_points = pd.concat(sample_points)
    df_sample_points.to_csv(ARQUIVO_PONTOS_AMOSTRA, index=False)
    
    print(f"DEBUG: Sucesso! Arquivos gerados:\n- {ARQUIVO_SAIDA}\n- {ARQUIVO_PONTOS_AMOSTRA}")

if __name__ == "__main__":
    pre_process()
