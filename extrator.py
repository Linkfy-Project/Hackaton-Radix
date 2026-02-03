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

# --- DICIONÁRIOS DE MAPEAMENTO ANEEL ---
# Mapeamento de códigos TEN_NOM para valores reais em kV (Baseado no PRODIST/BDGD)
# Este mapeamento foi refinado cruzando dados da LIGHT e ENEL RJ.
MAPA_CODIGO_TENSAO = {
    '94': 138.0,
    '91': 69.0,
    '82': 69.0,
    '72': 34.5,
    '67': 13.8,
    '55': 13.8,
    '49': 13.8,
    '46': 13.8,
    '45': 13.2,
    '44': 11.9,
    '42': 11.4,
    '38': 12.7,
    '22': 6.6,
    '10': 2.3,
    '7': 13.8,
    '1': 0.127,
    '2': 0.220,
    '3': 0.380,
    '4': 0.440,
    '01': 0.127,
    '02': 0.220,
    '03': 0.380,
    '04': 0.440
}

# --- CONFIGURAÇÕES GLOBAIS ---
PASTA_DADOS_BRUTOS = "Dados Brutos"
PASTA_SAIDA = "Dados Processados"
ARQUIVO_SAIDA_FINAL = os.path.join(PASTA_SAIDA, "dados_finais_rj.geojson")
ARQUIVO_CONTROLE = os.path.join(PASTA_SAIDA, "controle_processamento.json")

# Registro de Camadas de Dados (Data Providers)
DATA_PROVIDERS_CONFIG = {
    'OSM': True,
    'CNEFE': True,
    'CARGA': True
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
        'TR_GEOGRAFICO': 'UNTRD', # Transformadores para área real
        'CTMT': 'CTMT', # Circuitos de Média Tensão
        'BAR': 'BAR', # Barras
        'SSDAT': 'SSDAT', # Segmentos de Alta Tensão
        'UG_AT': 'UGAT_tab', # Geração Alta Tensão
        'UG_MT': 'UGMT_tab', # Geração Média Tensão
        'UG_BT': 'UGBT_tab', # Geração Baixa Tensão
        'UC_AT': 'UCAT_tab', # Consumidores Alta Tensão
        'UC_MT': 'UCMT_tab', # Consumidores Média Tensão
        'UC_BT': 'UCBT_tab'  # Consumidores Baixa Tensão
    },
    'ENEL': {
        'SUB': 'SUB',
        'TR_NOMINAL': 'UNTRAT', # Transformadores de AT para potência da subestação
        'TR_GEOGRAFICO': 'UNTRMT', # Transformadores de MT para área real de atendimento
        'CTMT': 'CTMT',
        'BAR': 'BAR',
        'SSDAT': 'SSDAT',
        'UG_AT': 'UGAT_tab',
        'UG_MT': 'UGMT_tab',
        'UG_BT': 'UGBT_tab',
        'UC_AT': 'UCAT_tab',
        'UC_MT': 'UCMT_tab',
        'UC_BT': 'UCBT_tab'
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

def provider_carga(caminho_gdb: str, dist: str) -> pd.DataFrame:
    """
    Extrai e agrega dados de carga (consumo e potência instalada) das tabelas UC do GDB.
    Otimizado para processar milhões de registros usando fiona e agregação em memória.
    """
    print(f"DEBUG: [Provider Carga] Processando dados de consumo para {dist} (Otimizado)...")
    cfg = MAPA_CAMADAS_GDB[dist]
    camadas_uc = ['UC_AT', 'UC_MT', 'UC_BT']
    
    # Estrutura de agregação: { (sub_id, cat): [qtd, carga_total, energia_total] }
    agg_data = {}

    def normalizar_cat(cat):
        cat = str(cat).upper()
        if 'RES' in cat: return 'RES'
        if 'IND' in cat: return 'IND'
        if 'COM' in cat: return 'COM'
        if 'RUR' in cat: return 'RUR'
        return 'OUTROS'

    try:
        layers_in_gdb = fiona.listlayers(caminho_gdb)
        for key in camadas_uc:
            layer_name = cfg[key]
            if layer_name in layers_in_gdb:
                print(f"DEBUG: [Provider Carga] Lendo camada {layer_name}...")
                with fiona.open(caminho_gdb, layer=layer_name) as src:
                    # Identificar índices das colunas para acesso rápido
                    cols = list(src.schema['properties'].keys())
                    idx_sub = cols.index('SUB') if 'SUB' in cols else -1
                    idx_tip = cols.index('TIP_CC') if 'TIP_CC' in cols else -1
                    idx_car = cols.index('CAR_INST') if 'CAR_INST' in cols else -1
                    cols_ene = [cols.index(c) for c in cols if c.startswith('ENE_')]

                    if idx_sub == -1 or idx_tip == -1:
                        continue

                    for feat in tqdm(src, desc=f"Agregando {layer_name}", leave=False):
                        props = feat['properties']
                        sub_id = str(props['SUB']).strip()
                        if not sub_id or sub_id == 'None': continue
                        
                        cat = normalizar_cat(props['TIP_CC'])
                        carga = float(props.get('CAR_INST') or 0)
                        
                        # Média das energias
                        ene_vals = [float(props.get(cols[i]) or 0) for i in cols_ene]
                        energia_med = sum(ene_vals) / len(ene_vals) if ene_vals else 0
                        
                        key_agg = (sub_id, cat)
                        if key_agg not in agg_data:
                            agg_data[key_agg] = [0, 0.0, 0.0]
                        
                        agg_data[key_agg][0] += 1
                        agg_data[key_agg][1] += carga
                        agg_data[key_agg][2] += energia_med

        if not agg_data: return pd.DataFrame()

        # Converter para DataFrame
        rows = []
        for (sid, cat), values in agg_data.items():
            rows.append({'COD_ID': sid, 'CAT': cat, 'QTD': values[0], 'CARGA': values[1], 'ENERGIA': values[2]})
        
        final_df = pd.DataFrame(rows)
        
        # Pivotar para ter colunas como CARGA_RES, QTD_RES, ENERGIA_RES, etc.
        pivot_qtd = final_df.pivot(index='COD_ID', columns='CAT', values='QTD').add_prefix('QTD_')
        pivot_carga = final_df.pivot(index='COD_ID', columns='CAT', values='CARGA').add_prefix('CARGA_')
        pivot_energia = final_df.pivot(index='COD_ID', columns='CAT', values='ENERGIA').add_prefix('ENERGIA_')
        
        res = pd.concat([pivot_qtd, pivot_carga, pivot_energia], axis=1).fillna(0)
        res.index = res.index.astype(str)
        return res
        
    except Exception as e:
        print(f"DEBUG ERROR: [Provider Carga] {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()

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

# --- CLASSIFICAÇÃO E RASTREAMENTO ---

def processar_classificacao_e_hierarquia(all_subs_data: List[Dict]) -> pd.DataFrame:
    """
    Aplica a lógica de classificação (Plena, Satélite, etc.) e rastreia quem alimenta quem.
    Utiliza busca recursiva na topologia de Alta Tensão (SSDAT) e integra dados da ONS.
    """
    print("DEBUG: [Classificação] Iniciando classificação e rastreamento hierárquico...")
    
    # 0. Carregar Dados ONS para mapeamento de fronteira (Otimizado)
    path_ons_sub = "Dados Brutos/ONS/SUBESTACAO.csv"
    barra_para_ons = {}
    if os.path.exists(path_ons_sub):
        print("DEBUG: [Classificação] Carregando base de subestações ONS (Otimizado)...")
        # Carrega apenas as colunas necessárias para ganhar velocidade
        df_ons = pd.read_csv(path_ons_sub, sep=';', encoding='latin1', usecols=['num_barra', 'nom_subestacao'])
        df_ons = df_ons.dropna(subset=['num_barra'])
        # Cria o dicionário de mapeamento de forma vetorizada
        barra_para_ons = dict(zip(df_ons['num_barra'].astype(int).astype(str), df_ons['nom_subestacao']))

    todas_classificacoes = []
    classificacao_por_id = {}
    mae_por_id = {}
    
    # Estruturas para o Grafo de Segmentos (Fios)
    pac_to_segs = {} # {pac_id: set(segment_indices)}
    seg_to_subs = {} # {segment_index: set(sub_ids)}
    sub_to_segs = {} # {sub_id: set(segment_indices)}
    seg_data = []    # Lista de todos os segmentos SSDAT para referência
    
    # Passo 1: Classificação Inicial e Mapeamento Geográfico/Topológico
    for data in all_subs_data:
        subs = data['subs']
        untrd = data['tr_geo']
        ctmt = data['ctmt']
        untrs = data['untrs']
        bar = data['bar']
        ssdat = data['ssdat']
        
        # Mapeamentos base do GDB
        circuito_para_mae = ctmt.set_index('COD_ID')['SUB'].to_dict() if ctmt is not None else {}
        subs_com_untrs = set(untrs['SUB'].unique()) if untrs is not None else set()
        
        # Agrupar transformadores para classificação
        untrd_por_sub = {}
        if untrd is not None:
            for sub_id, grupo in untrd.groupby('SUB'):
                untrd_por_sub[str(sub_id).strip()] = grupo

        # Construir Grafo de Segmentos (Fios)
        if ssdat is not None:
            # Join Geográfico para saber quais subestações cada fio toca
            target_crs = "EPSG:31983"
            subs_proj = subs.to_crs(target_crs)
            ssdat_proj = ssdat.to_crs(target_crs)
            subs_buffer = subs_proj.copy()
            subs_buffer['geometry'] = subs_proj.geometry.buffer(50) # 50m de tolerância
            
            spatial_join = gpd.sjoin(ssdat_proj, subs_buffer[['COD_ID', 'geometry']], how='inner', predicate='intersects')
            
            for idx, row in ssdat.iterrows():
                global_idx = len(seg_data)
                seg_data.append(row)
                
                p1, p2 = str(row['PAC_1']), str(row['PAC_2'])
                for p in [p1, p2]:
                    if p not in pac_to_segs: pac_to_segs[p] = set()
                    pac_to_segs[p].add(global_idx)
                
                # Subestações tocadas por este segmento
                if idx in spatial_join.index:
                    col_id = 'COD_ID' if 'COD_ID' in spatial_join.columns else 'COD_ID_right'
                    sids = spatial_join.loc[[idx], col_id].unique()
                    for sid in sids:
                        sid_str = str(sid).strip()
                        if global_idx not in seg_to_subs: seg_to_subs[global_idx] = set()
                        seg_to_subs[global_idx].add(sid_str)
                        if sid_str not in sub_to_segs: sub_to_segs[sid_str] = set()
                        sub_to_segs[sid_str].add(global_idx)

        # Classificar cada subestação
        for _, row in subs.iterrows():
            sid = str(row['COD_ID']).strip()
            meus_untrd = untrd_por_sub.get(sid, pd.DataFrame())
            
            if not meus_untrd.empty:
                circuitos_alimentadores = meus_untrd['CTMT'].unique()
                maes = {str(circuito_para_mae.get(str(c))).strip() for c in circuitos_alimentadores if circuito_para_mae.get(str(c))}
                
                if sid in maes and len(maes) == 1:
                    cat = "1. Distribuição Plena"
                elif len(maes) > 0:
                    cat = "2. Distribuição Satélite"
                    outras_maes = [m for m in maes if m != sid]
                    if outras_maes: mae_por_id[sid] = outras_maes[0]
                else:
                    # Unificado conforme pedido do usuário (Plena mesmo sem circuito mapeado)
                    cat = "1. Distribuição Plena"
            elif sid in subs_com_untrs:
                cat = "3. Transformadora Pura"
            else:
                cat = "4. Transporte/Manobra"
            
            classificacao_por_id[sid] = cat

    # Passo 2: Rastreamento Recursivo via Grafo de Fios (SSDAT)
    print("DEBUG: [Classificação] Realizando busca recursiva na malha de fios (SSDAT)...")
    for sid, cat in classificacao_por_id.items():
        if sid not in mae_por_id and cat in ["3. Transformadora Pura", "4. Transporte/Manobra"]:
            meus_segs = sub_to_segs.get(sid, set())
            visitados_seg = set(meus_segs)
            fila_seg = [(s, 0) for s in meus_segs]
            
            while fila_seg:
                seg_idx, dist = fila_seg.pop(0)
                row_seg = seg_data[seg_idx]
                
                subs_tocadas = seg_to_subs.get(seg_idx, set())
                achou = False
                for s_tocada in subs_tocadas:
                    if s_tocada != sid and classificacao_por_id.get(s_tocada) == "1. Distribuição Plena":
                        mae_por_id[sid] = s_tocada
                        achou = True
                        break
                if achou: break
                
                p1, p2 = str(row_seg['PAC_1']), str(row_seg['PAC_2'])
                for p in [p1, p2]:
                    num_barra = p.replace('EXTERNO:AT_', '').strip()
                    if num_barra in barra_para_ons:
                        mae_por_id[sid] = f"ONS: {barra_para_ons[num_barra]}"
                        achou = True
                        break
                if achou: break
                
                if dist < 50:
                    for p in [p1, p2]:
                        for prox_seg in pac_to_segs.get(p, []):
                            if prox_seg not in visitados_seg:
                                visitados_seg.add(prox_seg)
                                fila_seg.append((prox_seg, dist + 1))

    for sid, cat in classificacao_por_id.items():
        todas_classificacoes.append({
            'COD_ID': sid,
            'CLASSIFICACAO': cat,
            'SUB_MAE': mae_por_id.get(sid)
        })
            
    return pd.DataFrame(todas_classificacoes)

# --- CORE PIPELINE ---

def extrair_dados_completos_gdb(caminho_gdb: str) -> Optional[Dict]:
    """Extrai subestações, potências, circuitos, topologia, geração e tensão nominal."""
    nome_arquivo = os.path.basename(caminho_gdb).upper()
    dist = 'ENEL' if 'ENEL' in nome_arquivo else 'LIGHT'
    
    try:
        camadas = fiona.listlayers(caminho_gdb)
        cfg = MAPA_CAMADAS_GDB[dist]
        
        if cfg['SUB'] not in camadas: return None
        
        gdf_sub = gpd.read_file(caminho_gdb, layer=cfg['SUB'])
        
        if 'NOME' in gdf_sub.columns and 'NOM' not in gdf_sub.columns:
            print(f"DEBUG: [Normalização] Renomeando 'NOME' para 'NOM' em {dist}")
            gdf_sub = gdf_sub.rename(columns={'NOME': 'NOM'})
        
        # 1. Potência de Transformação
        gdf_untrs = None
        if cfg['TR_NOMINAL'] in camadas:
            gdf_untrs = gpd.read_file(caminho_gdb, layer=cfg['TR_NOMINAL'])
            col = 'SUB' if 'SUB' in gdf_untrs.columns else None
            if col:
                pot = gdf_untrs.groupby(col)['POT_NOM'].sum().reset_index()
                pot.columns = ['COD_ID', 'POTENCIA_CALCULADA']
                gdf_sub = gdf_sub.merge(pot, on='COD_ID', how='left').fillna({'POTENCIA_CALCULADA': 0})
        
        # 2. Tensão Nominal (Com Mapeamento de Códigos ANEEL)
        ten_por_sub = {}
        
        # Busca nas Barras
        if cfg['BAR'] in camadas:
            gdf_bar_temp = gpd.read_file(caminho_gdb, layer=cfg['BAR'])
            if 'SUB' in gdf_bar_temp.columns and 'TEN_NOM' in gdf_bar_temp.columns:
                for sub_id, group in gdf_bar_temp.groupby('SUB'):
                    for sub_id, group in gdf_bar_temp.groupby('SUB'):
                        codes = group['TEN_NOM'].astype(str).unique()
                        # Lógica de Proteção: Ignorar tensões de Baixa Tensão (< 2.3kV) para Subestações
                        # Isso evita que serviços auxiliares (127V/220V) sejam confundidos com a tensão da SE.
                        kvs = [MAPA_CODIGO_TENSAO.get(c, 0) for c in codes if MAPA_CODIGO_TENSAO.get(c, 0) >= 2.3]
                        if kvs: ten_por_sub[str(sub_id)] = max(kvs)
    
            # Busca nos Circuitos (MT e AT) como fallback ou complemento
            for camada_circ in ['CTMT', 'SSDAT']:
                if cfg[camada_circ] in camadas:
                    gdf_circ = gpd.read_file(caminho_gdb, layer=cfg[camada_circ])
                    if 'SUB' in gdf_circ.columns and 'TEN_NOM' in gdf_circ.columns:
                        for sub_id, group in gdf_circ.groupby('SUB'):
                            sid_str = str(sub_id)
                            codes = group['TEN_NOM'].astype(str).unique()
                            # Aplicando a mesma proteção para circuitos
                            kvs = [MAPA_CODIGO_TENSAO.get(c, 0) for c in codes if MAPA_CODIGO_TENSAO.get(c, 0) >= 2.3]
                            if kvs:
                                current_max = ten_por_sub.get(sid_str, 0)
                                ten_por_sub[sid_str] = max(current_max, max(kvs))
        # Fallback: Se a subestação não tem tensão nas barras ou circuitos diretos,
        # busca a tensão dos circuitos que alimentam seus transformadores.
        if cfg['TR_GEOGRAFICO'] in camadas and cfg['CTMT'] in camadas:
            gdf_tr_temp = gpd.read_file(caminho_gdb, layer=cfg['TR_GEOGRAFICO'])
            gdf_ctmt_temp = gpd.read_file(caminho_gdb, layer=cfg['CTMT'])
            
            if 'SUB' in gdf_tr_temp.columns and 'CTMT' in gdf_tr_temp.columns and 'TEN_NOM' in gdf_ctmt_temp.columns:
                # Mapeamento de Circuito -> Tensão
                mapa_circ_ten = {}
                for _, c_row in gdf_ctmt_temp.iterrows():
                    c_id = str(c_row['COD_ID'])
                    c_ten_code = str(c_row['TEN_NOM'])
                    c_kv = MAPA_CODIGO_TENSAO.get(c_ten_code, 0)
                    if c_kv >= 2.3:
                        mapa_circ_ten[c_id] = max(mapa_circ_ten.get(c_id, 0), c_kv)
                
                # Para cada subestação sem tensão, olha os transformadores
                for sub_id in gdf_sub['COD_ID'].astype(str).unique():
                    if ten_por_sub.get(sub_id, 0) == 0:
                        meus_trs = gdf_tr_temp[gdf_tr_temp['SUB'].astype(str) == sub_id]
                        if not meus_trs.empty:
                            meus_circs = meus_trs['CTMT'].astype(str).unique()
                            kvs_circs = [mapa_circ_ten.get(c, 0) for c in meus_circs if mapa_circ_ten.get(c, 0) > 0]
                            if kvs_circs:
                                ten_por_sub[sub_id] = max(kvs_circs)

        gdf_sub['TENSAO_NOMINAL'] = gdf_sub['COD_ID'].astype(str).map(ten_por_sub).fillna(0)

        # 3. Geração Distribuída
        pot_geracao_total = pd.DataFrame(columns=['COD_ID', 'POT_INST'])
        for camada_ug in ['UG_AT', 'UG_MT', 'UG_BT']:
            nome_camada = cfg[camada_ug]
            if nome_camada in camadas:
                gdf_ug = gpd.read_file(caminho_gdb, layer=nome_camada)
                if 'SUB' in gdf_ug.columns and 'POT_INST' in gdf_ug.columns:
                    ug_sum = gdf_ug.groupby('SUB')['POT_INST'].sum().reset_index()
                    ug_sum.columns = ['COD_ID', 'POT_INST']
                    pot_geracao_total = pd.concat([pot_geracao_total, ug_sum])
        
        if not pot_geracao_total.empty:
            pot_geracao_final = pot_geracao_total.groupby('COD_ID')['POT_INST'].sum().reset_index()
            pot_geracao_final.columns = ['COD_ID', 'GERACAO_GD_KW']
            gdf_sub = gdf_sub.merge(pot_geracao_final, on='COD_ID', how='left').fillna({'GERACAO_GD_KW': 0})
        else:
            gdf_sub['GERACAO_GD_KW'] = 0

        gdf_tr_geo = None
        if cfg['TR_GEOGRAFICO'] in camadas:
            gdf_tr_geo = gpd.read_file(caminho_gdb, layer=cfg['TR_GEOGRAFICO'])
            gdf_tr_geo['SUB'] = gdf_tr_geo['SUB'].astype(str).str.strip()
        
        gdf_ctmt = None
        if cfg['CTMT'] in camadas:
            gdf_ctmt = gpd.read_file(caminho_gdb, layer=cfg['CTMT'])
            
        gdf_bar = None
        if cfg['BAR'] in camadas:
            gdf_bar = gpd.read_file(caminho_gdb, layer=cfg['BAR'])
            
        gdf_ssdat = None
        if cfg['SSDAT'] in camadas:
            gdf_ssdat = gpd.read_file(caminho_gdb, layer=cfg['SSDAT'])
        
        gdf_sub['DISTRIBUIDORA'] = dist
        gdf_sub['FONTE_GDB'] = os.path.basename(caminho_gdb)
        
        return {
            'subs': gdf_sub,
            'tr_geo': gdf_tr_geo,
            'ctmt': gdf_ctmt,
            'untrs': gdf_untrs,
            'bar': gdf_bar,
            'ssdat': gdf_ssdat,
            'carga': provider_carga(caminho_gdb, dist) if DATA_PROVIDERS_CONFIG.get('CARGA') else None
        }
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

    print("DEBUG: Gerando áreas iniciais de atendimento...")
    poligonos_reais = []
    
    for data in all_subs_data:
        gdf_subs = data['subs']
        gdf_tr = data['tr_geo']
        
        tr_por_sub = {}
        if gdf_tr is not None:
            for sub_id, grupo in gdf_tr.groupby('SUB'):
                tr_por_sub[str(sub_id)] = grupo
        
        for _, sub_row in gdf_subs.iterrows():
            sub_id = str(sub_row['COD_ID'])
            grupo_tr = tr_por_sub.get(sub_id)
            
            if grupo_tr is not None and len(grupo_tr) >= 3:
                area = grupo_tr.geometry.union_all().convex_hull
            else:
                area = sub_row.geometry.centroid.buffer(0.0001)
                
            poligonos_reais.append({'COD_ID': sub_id, 'geometry': area})

    if not poligonos_reais:
        print("DEBUG ERROR: Não foi possível gerar áreas reais. Verifique as camadas de transformadores.")
        return

    gdf_areas = gpd.GeoDataFrame(poligonos_reais, crs=all_subs_data[0]['subs'].crs).to_crs("EPSG:4326")
    
    print("DEBUG: Resolvendo sobreposições territoriais...")
    gdf_subs_all = pd.concat([d['subs'] for d in all_subs_data], ignore_index=True)
    gdf_subs_all['COD_ID'] = gdf_subs_all['COD_ID'].astype(str)
    
    gdf_areas['COD_ID'] = gdf_areas['COD_ID'].astype(str)
    colunas_subs = ['COD_ID', 'POTENCIA_CALCULADA', 'NOM', 'DISTRIBUIDORA', 'TENSAO_NOMINAL', 'GERACAO_GD_KW']
    colunas_existentes = [c for c in colunas_subs if c in gdf_subs_all.columns]
    gdf_areas = gdf_areas.merge(gdf_subs_all[colunas_existentes], on='COD_ID', how='left')
    
    print("DEBUG: Calculando hierarquia de contenção para resolução de conflitos...")
    gdf_subs_pontos_temp = gpd.GeoDataFrame(gdf_subs_all, crs=all_subs_data[0]['subs'].crs)
    gdf_subs_pontos_temp = gdf_subs_pontos_temp.to_crs("EPSG:31983")
    gdf_subs_pontos_temp['geometry'] = gdf_subs_pontos_temp.geometry.centroid
    gdf_subs_pontos_temp = gdf_subs_pontos_temp.to_crs("EPSG:4326")
    gdf_subs_pontos_temp = gdf_subs_pontos_temp.drop_duplicates(subset=['COD_ID'])
    
    sindex = gdf_areas.sindex
    def get_containment_depth(row):
        ponto = gdf_subs_pontos_temp[gdf_subs_pontos_temp['COD_ID'] == row['COD_ID']].geometry
        if ponto.empty: return 0
        ponto = ponto.iloc[0]
        matches = sindex.query(ponto, predicate='within')
        return len(matches) - 1

    gdf_areas['DEPTH'] = gdf_areas.apply(get_containment_depth, axis=1)
    gdf_areas = gdf_areas.sort_values(by=['DEPTH', 'POTENCIA_CALCULADA'], ascending=[False, False])
    
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

    print("DEBUG: Obtendo fronteiras do estado do Rio de Janeiro via geobr...")
    rj_state = geobr.read_state(code_state="RJ", year=2020)
    
    gdf_subs_pontos = gdf_subs_all.copy()
    if not isinstance(gdf_subs_pontos, gpd.GeoDataFrame):
        gdf_subs_pontos = gpd.GeoDataFrame(gdf_subs_pontos, crs=all_subs_data[0]['subs'].crs)
    
    gdf_subs_pontos = gdf_subs_pontos.to_crs("EPSG:31983")
    gdf_subs_pontos['geometry'] = gdf_subs_pontos.geometry.centroid
    gdf_subs_pontos = gdf_subs_pontos.to_crs("EPSG:4326")
    gdf_subs_pontos = gdf_subs_pontos.drop_duplicates(subset=['COD_ID'])

    gdf_final_geo = preencher_buracos_rj(gdf_final_geo, gdf_subs_pontos, rj_state)

    print("DEBUG: Mapeando localizações das subestações...")
    gdf_subs_all = gpd.GeoDataFrame(gdf_subs_all, crs=all_subs_data[0]['subs'].crs)
    gdf_subs_all = gdf_subs_all.to_crs("EPSG:31983")
    gdf_subs_all['geometry'] = gdf_subs_all.geometry.centroid
    gdf_subs_all = gdf_subs_all.to_crs("EPSG:4326")
    gdf_subs_all['lat_sub'], gdf_subs_all['lon_sub'] = gdf_subs_all.geometry.y, gdf_subs_all.geometry.x
    
    cols_to_merge = ['COD_ID', 'lat_sub', 'lon_sub']
    if 'POT_NOM' in gdf_subs_all.columns:
        cols_to_merge.append('POT_NOM')
        
    gdf_final_geo = gdf_final_geo.merge(
        gdf_subs_all[cols_to_merge],
        on='COD_ID', how='left'
    )

    df_class = processar_classificacao_e_hierarquia(all_subs_data)
    gdf_final_geo = gdf_final_geo.merge(df_class, on='COD_ID', how='left')

    bounds = rj_state.to_crs("EPSG:4326").total_bounds
    
    stats_frames = []
    if DATA_PROVIDERS_CONFIG['OSM']: stats_frames.append(provider_osm(gdf_final_geo, bounds))
    if DATA_PROVIDERS_CONFIG['CNEFE']: stats_frames.append(provider_cnefe(gdf_final_geo))
    
    # Integrar dados de carga extraídos dos GDBs
    df_carga_all = pd.concat([d['carga'] for d in all_subs_data if d['carga'] is not None])
    if not df_carga_all.empty:
        # Garantir que o índice seja string para o merge
        df_carga_all.index = df_carga_all.index.astype(str)
        stats_frames.append(df_carga_all)

    print("DEBUG: Unificando todas as camadas de dados...")
    for df_stats in stats_frames:
        if not df_stats.empty:
            df_stats.index = df_stats.index.astype(str)
            gdf_final_geo = gdf_final_geo.merge(df_stats, left_on='COD_ID', right_index=True, how='left')

    gdf_final_geo = gdf_final_geo.fillna(0)
    gdf_final_geo.columns = [str(c) for c in gdf_final_geo.columns]

    print("DEBUG: Aplicando simplificação de geometria (1m de tolerância)...")
    original_crs = gdf_final_geo.crs
    gdf_final_geo = gdf_final_geo.to_crs("EPSG:31983")
    gdf_final_geo['geometry'] = gdf_final_geo.simplify(tolerance=1.0, preserve_topology=True)
    gdf_final_geo = gdf_final_geo.to_crs(original_crs)
    
    print(f"DEBUG: Salvando arquivo mestre unificado: {ARQUIVO_SAIDA_FINAL}")
    if not os.path.exists(PASTA_SAIDA): os.makedirs(PASTA_SAIDA)
    gdf_final_geo.to_file(ARQUIVO_SAIDA_FINAL, driver='GeoJSON')
    
    for gdb in gdbs: manager.update_mtime(gdb)
    manager.save()
    print("DEBUG: Pipeline de Áreas Reais concluído com sucesso!")

if __name__ == "__main__":
    run_pipeline()
