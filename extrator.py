"""
Este script é o motor de processamento de dados (ETL) do projeto.
Ele extrai dados geográficos de múltiplos GDBs, gera áreas REAIS de influência 
(via Convex Hull dos transformadores), resolve sobreposições territoriais,
preenche buracos entre subestações usando Voronoi e integra diversas camadas 
de estatísticas (CNEFE, OSM, etc.) em um arquivo GeoJSON unificado.

Arquitetura: Modular (Data Providers) para facilitar a adição de novas fontes de dados.
"""

import geopandas as gpd
import geobr
import pandas as pd
from shapely.geometry import box, MultiPoint, Point
from shapely.ops import unary_union, voronoi_diagram
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
        'TR_NOMINAL': 'UNTRAT', # Transformadores de AT para potência da subestação
        'TR_GEOGRAFICO': 'UNTRMT' # Transformadores de MT para área real de atendimento
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

# --- FUNÇÕES DE GEOPROCESSAMENTO AVANÇADO ---

def preencher_buracos_rj(gdf_areas: gpd.GeoDataFrame, gdf_subs_pontos: gpd.GeoDataFrame, rj_shape: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Preenche áreas vazias dentro do estado do RJ seguindo a metodologia:
    1. Buracos com 1 vizinho -> Absorvidos pelo vizinho.
    2. Buracos com >1 vizinho -> Divididos via Voronoi entre as subestações em volta.
    """
    print("DEBUG: [Hole Filler] Iniciando preenchimento de áreas vazias no RJ...")
    target_crs = "EPSG:31983" # SIRGAS 2000 / UTM zone 23S (RJ)
    
    # Backup do CRS original
    original_crs = gdf_areas.crs
    
    # Conversão para CRS projetado para cálculos precisos
    gdf_areas_proj = gdf_areas.to_crs(target_crs)
    gdf_subs_pontos_proj = gdf_subs_pontos.to_crs(target_crs)
    rj_poly = rj_shape.to_crs(target_crs).union_all()
    
    # 1. Identificar buracos (Área do Estado - União das Subestações)
    subs_union = gdf_areas_proj.union_all()
    buracos_total = rj_poly.difference(subs_union)
    
    if buracos_total.is_empty:
        print("DEBUG: [Hole Filler] Nenhum buraco encontrado.")
        return gdf_areas
        
    # Explodir MultiPolygon em Polygons individuais
    if hasattr(buracos_total, 'geoms'):
        lista_buracos = [g for g in buracos_total.geoms if g.area > 100] # Ignorar micro-áreas < 100m²
    else:
        lista_buracos = [buracos_total] if buracos_total.area > 100 else []
        
    print(f"DEBUG: [Hole Filler] {len(lista_buracos)} buracos significativos identificados.")
    
    # Dicionário para acumular as novas peças (geometrias) para cada subestação
    pecas_por_sub = {str(sid): [geom] for sid, geom in zip(gdf_areas_proj['COD_ID'], gdf_areas_proj['geometry'])}
    
    for i, buraco in enumerate(lista_buracos):
        # Encontrar subestações vizinhas (que tocam o buraco)
        # Usamos um pequeno buffer de 2m para garantir a detecção de toque na fronteira
        vizinhos = gdf_areas_proj[gdf_areas_proj.intersects(buraco.buffer(2))]
        
        if len(vizinhos) == 1:
            # Caso 2: Fronteira com apenas uma subestação -> Absorção total
            sub_id = str(vizinhos.iloc[0]['COD_ID'])
            pecas_por_sub[sub_id].append(buraco)
            
        elif len(vizinhos) > 1:
            # Caso 3: Fronteira com várias subestações -> Divisão via Voronoi
            ids_vizinhos = vizinhos['COD_ID'].astype(str).tolist()
            pontos_vizinhos = gdf_subs_pontos_proj[gdf_subs_pontos_proj['COD_ID'].astype(str).isin(ids_vizinhos)]
            
            if len(pontos_vizinhos) > 1:
                # Gerar Voronoi baseado nos pontos das subestações vizinhas
                coords = [p.coords[0] for p in pontos_vizinhos.geometry]
                # Envelope para limitar o Voronoi (deve cobrir o buraco com folga)
                envelope = buraco.buffer(5000).envelope
                vor_collection = voronoi_diagram(MultiPoint(coords), envelope=envelope)
                
                # Intersectar cada célula do Voronoi com o buraco
                for celula in vor_collection.geoms:
                    intersecao = celula.intersection(buraco)
                    if not intersecao.is_empty and intersecao.area > 1:
                        # Atribuir a interseção à subestação cujo ponto está dentro desta célula
                        for _, p_row in pontos_vizinhos.iterrows():
                            if celula.contains(p_row.geometry) or celula.distance(p_row.geometry) < 0.1:
                                sid = str(p_row['COD_ID'])
                                pecas_por_sub[sid].append(intersecao)
                                break
            elif len(vizinhos) > 0:
                # Fallback: Se não houver pontos suficientes para Voronoi, atribui ao primeiro vizinho
                sub_id = str(vizinhos.iloc[0]['COD_ID'])
                pecas_por_sub[sub_id].append(buraco)

    # Unificar todas as peças de cada subestação e dissolver linhas internas
    print("DEBUG: [Hole Filler] Dissolvendo fronteiras internas...")
    novas_geoms = {}
    for sid, pecas in pecas_por_sub.items():
        # unary_union funde todas as geometrias em uma só, removendo linhas internas
        # O buffer(0.1).buffer(-0.1) ajuda a eliminar slivers e garantir uma geometria limpa
        geom_unificada = unary_union(pecas).buffer(0.1).buffer(-0.1)
        novas_geoms[sid] = geom_unificada

    # Atualizar o GeoDataFrame com as novas geometrias
    gdf_areas_proj['geometry'] = gdf_areas_proj['COD_ID'].astype(str).map(novas_geoms)
    
    # Corrigir possíveis invalidezes geométricas após uniões
    gdf_areas_proj['geometry'] = gdf_areas_proj['geometry'].make_valid()
    
    print("DEBUG: [Hole Filler] Preenchimento concluído.")
    return gdf_areas_proj.to_crs(original_crs)

# --- CORE PIPELINE ---

def extrair_dados_completos_gdb(caminho_gdb: str) -> Optional[Dict]:
    """Extrai subestações, potências e pontos de transformadores para área real."""
    nome_arquivo = os.path.basename(caminho_gdb).upper()
    dist = 'ENEL' if 'ENEL' in nome_arquivo else 'LIGHT'
    
    try:
        camadas = fiona.listlayers(caminho_gdb)
        cfg = MAPA_CAMADAS_GDB[dist]
        
        if cfg['SUB'] not in camadas: return None
        
        # 1. Subestações (Pontos ou Polígonos)
        gdf_sub = gpd.read_file(caminho_gdb, layer=cfg['SUB'])
        
        # Normalização de colunas: ENEL usa 'NOME', Light usa 'NOM'
        if 'NOME' in gdf_sub.columns and 'NOM' not in gdf_sub.columns:
            print(f"DEBUG: [Normalização] Renomeando 'NOME' para 'NOM' em {dist}")
            gdf_sub = gdf_sub.rename(columns={'NOME': 'NOM'})
        
        # 2. Potência Nominal
        if cfg['TR_NOMINAL'] in camadas:
            gdf_tr_nom = gpd.read_file(caminho_gdb, layer=cfg['TR_NOMINAL'])
            col = 'SUB' if 'SUB' in gdf_tr_nom.columns else None
            if col:
                # BDGD usa POT_NOM para potência nominal
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

    # --- NOVO PASSO: Preencher Buracos no Estado do RJ ---
    print("DEBUG: Obtendo fronteiras do estado do Rio de Janeiro via geobr...")
    rj_state = geobr.read_state(code_state="RJ", year=2020)
    
    # Preparar pontos das subestações para o Voronoi
    gdf_subs_pontos = gdf_subs_all.copy()
    if not isinstance(gdf_subs_pontos, gpd.GeoDataFrame):
        gdf_subs_pontos = gpd.GeoDataFrame(gdf_subs_pontos, crs=all_subs_data[0]['subs'].crs)
    
    # Garantir que temos pontos (centroides se forem polígonos)
    gdf_subs_pontos = gdf_subs_pontos.to_crs("EPSG:31983")
    gdf_subs_pontos['geometry'] = gdf_subs_pontos.geometry.centroid
    gdf_subs_pontos = gdf_subs_pontos.to_crs("EPSG:4326")
    gdf_subs_pontos = gdf_subs_pontos.drop_duplicates(subset=['COD_ID'])

    gdf_final_geo = preencher_buracos_rj(gdf_final_geo, gdf_subs_pontos, rj_state)

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
    # Usamos o shape do RJ para o bounding box das consultas
    bounds = rj_state.to_crs("EPSG:4326").total_bounds
    
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
