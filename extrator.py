"""
Este script é o motor de processamento de dados (ETL) do projeto.
Ele extrai dados geográficos de múltiplos GDBs, gera áreas REAIS de influência 
(via Convex Hull dos transformadores), resolve sobreposições territoriais
e integra diversas camadas de estatísticas (CNEFE, OSM, etc.) em um arquivo GeoJSON unificado.

Arquitetura: Modular (Data Providers) para facilitar a adição de novas fontes de dados.
"""

import geopandas as gpd
import geobr
import pandas as pd
from shapely.geometry import box, MultiPoint
from shapely.ops import unary_union
import os
import json
import glob
import fiona
import requests
from tqdm import tqdm
from typing import List, Dict, Optional

# --- CONFIGURAÇÕES GLOBAIS ---
PASTA_DADOS_BRUTOS = "Dados Brutos"
PASTA_SAIDA = "Dados Processados"
ARQUIVO_SAIDA_FINAL = os.path.join(PASTA_SAIDA, "dados_finais_rj.geojson")
ARQUIVO_CONTROLE = os.path.join(PASTA_SAIDA, "controle_processamento.json")

# Registro de Camadas de Dados (Data Providers)
DATA_PROVIDERS_CONFIG = {
    'OSM': True,
    'CNEFE': True
}

# Configurações de Caminhos Específicos
PATHS = {
    'CNEFE': r"Dados Brutos/CNFE IBGE/CNEFE_RJ.csv"
}

# Mapeamento de camadas por distribuidora
MAPA_CAMADAS_GDB = {
    'LIGHT': {
        'SUB': 'SUB', 
        'TR_NOMINAL': 'UNTRS', # Transformadores para potência
        'TR_GEOGRAFICO': 'UNTRD' # Transformadores para área real
    },
    'ENEL': {
        'SUB': 'SUB', 
        'TR_NOMINAL': 'UNTRMT',
        'TR_GEOGRAFICO': 'UNTRMT' # Na ENEL parece ser a mesma camada
    }
}

# --- CLASSES E FUNÇÕES DE SUPORTE ---

class DataManager:
    """Gerencia o estado e o controle de processamento."""
    def __init__(self, controle_path: str):
        self.path = controle_path
        self.data = self._load()

    def _load(self) -> dict:
        if os.path.exists(self.path):
            with open(self.path, 'r') as f:
                return json.load(f)
        return {}

    def save(self):
        if not os.path.exists(os.path.dirname(self.path)):
            os.makedirs(os.path.dirname(self.path))
        with open(self.path, 'w') as f:
            json.dump(self.data, f, indent=4)

    def needs_update(self, file_path: str) -> bool:
        nome = os.path.basename(file_path)
        mtime = os.path.getmtime(file_path)
        last_val = self.data.get(nome, 0)
        if isinstance(last_val, dict):
            last_val = last_val.get('mtime', 0)
        return last_val < mtime

    def update_mtime(self, file_path: str):
        self.data[os.path.basename(file_path)] = {'mtime': os.path.getmtime(file_path)}

# --- DATA PROVIDERS (CAMADAS DE ESTATÍSTICA) ---

def provider_osm(areas_gdf: gpd.GeoDataFrame, bounds: List[float]) -> pd.DataFrame:
    """Obtém dados de comércio e indústria do OpenStreetMap."""
    print("DEBUG: [Provider OSM] Consultando API Overpass...")
    bbox = f"{bounds[1]}, {bounds[0]}, {bounds[3]}, {bounds[2]}"
    query = f"""
    [out:json][timeout:180];
    (node["shop"]({bbox}); way["shop"]({bbox});
     node["landuse"="industrial"]({bbox}); way["landuse"="industrial"]({bbox});
     node["industrial"]({bbox}); way["industrial"]({bbox}););
    out center;
    """
    try:
        response = requests.post("https://overpass-api.de/api/interpreter", data={'data': query})
        response.raise_for_status()
        elements = response.json().get('elements', [])
        points = []
        for el in elements:
            lat = el.get('lat') or el.get('center', {}).get('lat')
            lon = el.get('lon') or el.get('center', {}).get('lon')
            if lat and lon:
                category = 'OSM_SHOP' if 'shop' in el.get('tags', {}) else 'OSM_INDUSTRIAL'
                points.append({'lat': lat, 'lon': lon, 'category': category})
        
        if not points: return pd.DataFrame()
        
        df = pd.DataFrame(points)
        gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.lon, df.lat), crs="EPSG:4326")
        joined = gpd.sjoin(gdf, areas_gdf[['COD_ID', 'geometry']], how='inner', predicate='within')
        return joined.groupby(['COD_ID', 'category']).size().unstack(fill_value=0)
    except Exception as e:
        print(f"DEBUG ERROR: [Provider OSM] {e}")
        return pd.DataFrame()

def provider_cnefe(areas_gdf: gpd.GeoDataFrame) -> pd.DataFrame:
    """Processa os milhões de pontos do CNEFE IBGE."""
    path = PATHS['CNEFE']
    if not os.path.exists(path):
        print(f"DEBUG WARNING: [Provider CNEFE] Arquivo não encontrado em {path}")
        return pd.DataFrame()

    print(f"DEBUG: [Provider CNEFE] Processando Big Data...")
    all_stats = []
    with tqdm(total=9000000, desc="CNEFE") as pbar:
        for chunk in pd.read_csv(path, sep=';', usecols=['LATITUDE', 'LONGITUDE', 'COD_ESPECIE'], chunksize=300000):
            chunk = chunk.dropna(subset=['LATITUDE', 'LONGITUDE'])
            gdf_chunk = gpd.GeoDataFrame(chunk, geometry=gpd.points_from_xy(chunk.LONGITUDE, chunk.LATITUDE), crs="EPSG:4326")
            joined = gpd.sjoin(gdf_chunk, areas_gdf[['COD_ID', 'geometry']], how='inner', predicate='within')
            if not joined.empty:
                all_stats.append(joined.groupby(['COD_ID', 'COD_ESPECIE']).size().reset_index(name='count'))
            pbar.update(len(chunk))
            
    if not all_stats: return pd.DataFrame()
    return pd.concat(all_stats).groupby(['COD_ID', 'COD_ESPECIE'])['count'].sum().unstack(fill_value=0)

# --- CORE PIPELINE ---

def extrair_dados_completos_gdb(caminho_gdb: str) -> Optional[Dict]:
    """Extrai subestações, potências e pontos de transformadores para área real."""
    nome = os.path.basename(caminho_gdb).upper()
    dist = 'ENEL' if 'ENEL' in nome else 'LIGHT'
    
    try:
        camadas = fiona.listlayers(caminho_gdb)
        cfg = MAPA_CAMADAS_GDB[dist]
        
        if cfg['SUB'] not in camadas: return None
        
        # 1. Subestações (Pontos)
        gdf_sub = gpd.read_file(caminho_gdb, layer=cfg['SUB'])
        
        # 2. Potência Nominal
        if cfg['TR_NOMINAL'] in camadas:
            gdf_tr_nom = gpd.read_file(caminho_gdb, layer=cfg['TR_NOMINAL'])
            col = 'SUB' if 'SUB' in gdf_tr_nom.columns else None
            if col:
                pot = gdf_tr_nom.groupby(col)['POT_NOM'].sum().reset_index()
                pot.columns = ['COD_ID', 'POTENCIA_CALCULADA']
                gdf_sub = gdf_sub.merge(pot, on='COD_ID', how='left').fillna({'POTENCIA_CALCULADA': 0})
        
        # 3. Transformadores Geográficos (para Área Real)
        gdf_tr_geo = None
        if cfg['TR_GEOGRAFICO'] in camadas:
            gdf_tr_geo = gpd.read_file(caminho_gdb, layer=cfg['TR_GEOGRAFICO'], columns=['SUB', 'geometry'])
            gdf_tr_geo['SUB'] = gdf_tr_geo['SUB'].astype(str).str.strip()
        
        gdf_sub['DISTRIBUIDORA'] = dist
        gdf_sub['FONTE_GDB'] = os.path.basename(caminho_gdb)
        
        return {'subs': gdf_sub, 'tr_geo': gdf_tr_geo}
    except Exception as e:
        print(f"DEBUG ERROR: Falha em {caminho_gdb}: {e}")
        return None

def run_pipeline():
    manager = DataManager(ARQUIVO_CONTROLE)
    gdbs = glob.glob(os.path.join(PASTA_DADOS_BRUTOS, "**", "*.gdb"), recursive=True)
    
    if not gdbs:
        print("DEBUG: Nenhum GDB encontrado.")
        return

    houve_mudanca = False
    all_subs_data = []
    for gdb in gdbs:
        if manager.needs_update(gdb): houve_mudanca = True
        data = extrair_dados_completos_gdb(gdb)
        if data: all_subs_data.append(data)

    if not houve_mudanca and os.path.exists(ARQUIVO_SAIDA_FINAL):
        print("DEBUG: Tudo atualizado. Nada a fazer.")
        return

    # 1. Gerar Áreas Reais (Convex Hull)
    print("DEBUG: Gerando áreas reais de atendimento (Convex Hull)...")
    poligonos_reais = []
    
    for data in all_subs_data:
        gdf_tr = data['tr_geo']
        if gdf_tr is not None:
            for sub_id, grupo in gdf_tr.groupby('SUB'):
                if len(grupo) >= 3:
                    # Convex Hull: a 'casca' que envolve todos os pontos daquela subestação
                    area = grupo.geometry.union_all().convex_hull
                    poligonos_reais.append({'COD_ID': sub_id, 'geometry': area})

    if not poligonos_reais:
        print("DEBUG ERROR: Não foi possível gerar áreas reais. Verifique as camadas de transformadores.")
        return

    gdf_areas = gpd.GeoDataFrame(poligonos_reais, crs=all_subs_data[0]['subs'].crs).to_crs("EPSG:4326")
    
    # 2. Resolver Sobreposições (Abordagem de Prioridade por Potência)
    print("DEBUG: Resolvendo sobreposições territoriais...")
    # Unifica todos os pontos de subestações para pegar a potência
    gdf_subs_all = pd.concat([d['subs'] for d in all_subs_data], ignore_index=True)
    gdf_subs_all['COD_ID'] = gdf_subs_all['COD_ID'].astype(str)
    
    # Merge potência com as áreas para ordenar
    gdf_areas['COD_ID'] = gdf_areas['COD_ID'].astype(str)
    gdf_areas = gdf_areas.merge(gdf_subs_all[['COD_ID', 'POTENCIA_CALCULADA', 'NOM', 'DISTRIBUIDORA']], on='COD_ID', how='left')
    gdf_areas = gdf_areas.sort_values(by='POTENCIA_CALCULADA', ascending=False)
    
    areas_limpas = []
    geometria_acumulada = None
    
    for _, row in gdf_areas.iterrows():
        geom_atual = row.geometry
        if geometria_acumulada is None:
            areas_limpas.append(row)
            geometria_acumulada = geom_atual
        else:
            geom_recortada = geom_atual.difference(geometria_acumulada)
            if not geom_recortada.is_empty:
                row.geometry = geom_recortada
                areas_limpas.append(row)
                geometria_acumulada = geometria_acumulada.union(geom_atual)
    
    gdf_final_geo = gpd.GeoDataFrame(areas_limpas, crs="EPSG:4326")

    # 3. Adicionar Centroides (para os marcadores no mapa)
    print("DEBUG: Mapeando localizações das subestações...")
    gdf_subs_all = gpd.GeoDataFrame(gdf_subs_all, crs=all_subs_data[0]['subs'].crs)
    gdf_subs_all = gdf_subs_all.to_crs("EPSG:31983")
    gdf_subs_all['geometry'] = gdf_subs_all.geometry.centroid
    gdf_subs_all = gdf_subs_all.to_crs("EPSG:4326")
    gdf_subs_all['lat_sub'], gdf_subs_all['lon_sub'] = gdf_subs_all.geometry.y, gdf_subs_all.geometry.x
    
    # Merge das coordenadas de volta para o GeoDataFrame de áreas
    cols_to_merge = ['COD_ID', 'lat_sub', 'lon_sub']
    if 'POT_NOM' in gdf_subs_all.columns:
        cols_to_merge.append('POT_NOM')
        
    gdf_final_geo = gdf_final_geo.merge(
        gdf_subs_all[cols_to_merge], 
        on='COD_ID', how='left'
    )

    # 4. Camadas de Estatísticas (Data Providers)
    rj_shape = geobr.read_municipality(code_muni=3304557, year=2020).to_crs("EPSG:4326")
    bounds = rj_shape.total_bounds
    
    stats_frames = []
    if DATA_PROVIDERS_CONFIG['OSM']: stats_frames.append(provider_osm(gdf_final_geo, bounds))
    if DATA_PROVIDERS_CONFIG['CNEFE']: stats_frames.append(provider_cnefe(gdf_final_geo))

    # 5. Unificação Final
    print("DEBUG: Unificando todas as camadas de dados...")
    for df_stats in stats_frames:
        if not df_stats.empty:
            df_stats.index = df_stats.index.astype(str)
            gdf_final_geo = gdf_final_geo.merge(df_stats, left_on='COD_ID', right_index=True, how='left')

    gdf_final_geo = gdf_final_geo.fillna(0)
    gdf_final_geo.columns = [str(c) for c in gdf_final_geo.columns]
    
    print(f"DEBUG: Salvando arquivo mestre unificado: {ARQUIVO_SAIDA_FINAL}")
    if not os.path.exists(PASTA_SAIDA): os.makedirs(PASTA_SAIDA)
    gdf_final_geo.to_file(ARQUIVO_SAIDA_FINAL, driver='GeoJSON')
    
    for gdb in gdbs: manager.update_mtime(gdb)
    manager.save()
    print("DEBUG: Pipeline de Áreas Reais concluído com sucesso!")

if __name__ == "__main__":
    run_pipeline()
