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
}

# Configurações de Caminhos Específicos
PATHS = {
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
        'UGBT': 'UGBT_tab', # Unidade Geradora de Baixa Tensão
        'UGMT': 'UGMT_tab', # Unidade Geradora de Média Tensão
        'UGAT': 'UGAT_tab'  # Unidade Geradora de Alta Tensão
    },
    'ENEL': {
        'SUB': 'SUB',
        'TR_NOMINAL': 'UNTRAT', # Transformadores de AT para potência da subestação
        'TR_GEOGRAFICO': 'UNTRMT', # Transformadores de MT para área real de atendimento
        'CTMT': 'CTMT',
        'BAR': 'BAR',
        'SSDAT': 'SSDAT',
        'UGBT': 'UGBT_tab',
        'UGMT': 'UGMT_tab',
        'UGAT': 'UGAT_tab'
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
                
                # Intersectar cada célula do Voronoi with the buraco
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

    # Atualizar o GeoDataFrame with the novas geometrias
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
    
    # 0. Carregar Dados ONS para mapeamento de fronteira
    path_ons_sub = "Dados Brutos/ONS/SUBESTACAO.csv"
    barra_para_ons = {}
    if os.path.exists(path_ons_sub):
        print("DEBUG: [Classificação] Carregando base de subestações ONS...")
        df_ons = pd.read_csv(path_ons_sub, sep=';', encoding='latin1')
        for _, row in df_ons.iterrows():
            if not pd.isna(row['num_barra']):
                barra_para_ons[str(int(row['num_barra']))] = row['nom_subestacao']

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
        dist = subs.iloc[0]['DISTRIBUIDORA']
        
        # Mapeamentos base do GDB
        circuito_para_mae = ctmt.set_index('COD_ID')['SUB'].to_dict() if ctmt is not None else {}
        subs_com_untrs = set(untrs['SUB'].unique()) if untrs is not None else set()
        pac_to_sub = bar.set_index('PAC')['SUB'].to_dict() if bar is not None else {}
        
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
                    # O sjoin pode renomear a coluna se houver colisão
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
                    cat = "1. Distribuição Plena (Circuito não mapeado)"
            elif sid in subs_com_untrs:
                cat = "3. Transformadora Pura"
            else:
                cat = "4. Transporte/Manobra"
            
            classificacao_por_id[sid] = cat

    # Passo 2: Rastreamento Recursivo via Grafo de Fios (SSDAT)
    print("DEBUG: [Classificação] Realizando busca recursiva na malha de fios (SSDAT)...")
    for sid, cat in classificacao_por_id.items():
        if sid not in mae_por_id and cat in ["3. Transformadora Pura", "4. Transporte/Manobra"]:
            # Inicia BFS a partir dos segmentos que tocam esta subestação
            meus_segs = sub_to_segs.get(sid, set())
            visitados_seg = set(meus_segs)
            fila_seg = [(s, 0) for s in meus_segs]
            
            while fila_seg:
                seg_idx, dist = fila_seg.pop(0)
                row_seg = seg_data[seg_idx]
                
                # 1. Verificar se este fio toca uma subestação PLENA
                subs_tocadas = seg_to_subs.get(seg_idx, set())
                achou = False
                for s_tocada in subs_tocadas:
                    if s_tocada != sid and classificacao_por_id.get(s_tocada) == "1. Distribuição Plena":
                        mae_por_id[sid] = s_tocada
                        achou = True
                        break
                if achou: break
                
                # 2. Verificar se este fio toca um PAC da ONS
                p1, p2 = str(row_seg['PAC_1']), str(row_seg['PAC_2'])
                for p in [p1, p2]:
                    num_barra = p.replace('EXTERNO:AT_', '').strip()
                    if num_barra in barra_para_ons:
                        mae_por_id[sid] = f"ONS: {barra_para_ons[num_barra]}"
                        achou = True
                        break
                if achou: break
                
                # 3. Continuar a busca pelos fios vizinhos
                if dist < 50: # Limite de saltos de fios
                    for p in [p1, p2]:
                        for prox_seg in pac_to_segs.get(p, []):
                            if prox_seg not in visitados_seg:
                                visitados_seg.add(prox_seg)
                                fila_seg.append((prox_seg, dist + 1))

    # Consolidar resultados
    for sid, cat in classificacao_por_id.items():
        todas_classificacoes.append({
            'COD_ID': sid,
            'CLASSIFICACAO': cat,
            'SUB_MAE': mae_por_id.get(sid)
        })
            
    return pd.DataFrame(todas_classificacoes)

# --- CORE PIPELINE ---

def processar_geracao_distribuida(caminho_gdb: str, camadas: List[str], cfg: Dict) -> pd.DataFrame:
    """
    Extrai e agrupa dados de Micro e Minigeração Distribuída (MMGD) por subestação.
    Foca nas tabelas UGBT, UGMT e UGAT filtrando por CODGD ou CEG.
    """
    print(f"DEBUG: [MMGD] Extraindo dados de geração distribuída de {os.path.basename(caminho_gdb)}...")
    dfs_geracao = []
    
    camadas_geracao = ['UGBT', 'UGMT', 'UGAT']
    for cam in camadas_geracao:
        cam_name = cfg.get(cam)
        if cam_name in camadas:
            try:
                # Lendo apenas as colunas necessárias para otimizar
                gdf = gpd.read_file(caminho_gdb, layer=cam_name)
                
                # Filtro: CODGD, CEG_GD ou CEG não nulo/vazio indica MMGD
                filtro_cols = ['CODGD', 'CEG_GD', 'CEG']
                mask = pd.Series(False, index=gdf.index)
                for col in filtro_cols:
                    if col in gdf.columns:
                        mask |= (gdf[col].notna() & (gdf[col].astype(str).str.strip() != ''))
                
                gdf_gd = gdf[mask].copy()
                
                if gdf_gd.empty:
                    continue
                
                # Cálculo da Energia Mensal e Anual
                # Para UGBT e UGMT: ENE_01 a ENE_12
                # Para UGAT: ENE_P_01 a ENE_P_12 e ENE_F_01 a ENE_F_12
                colunas_energia_final = [f'ENE_MMGD_{str(i).zfill(2)}' for i in range(1, 13)]
                
                for i in range(1, 13):
                    mes_str = str(i).zfill(2)
                    col_saida = f'ENE_MMGD_{mes_str}'
                    
                    if cam == 'UGAT':
                        col_p = f'ENE_P_{mes_str}'
                        col_f = f'ENE_F_{mes_str}'
                        val = 0
                        if col_p in gdf_gd.columns: val += gdf_gd[col_p].fillna(0)
                        if col_f in gdf_gd.columns: val += gdf_gd[col_f].fillna(0)
                        gdf_gd[col_saida] = val
                    else:
                        col_e = f'ENE_{mes_str}'
                        if col_e in gdf_gd.columns:
                            gdf_gd[col_saida] = gdf_gd[col_e].fillna(0)
                        else:
                            gdf_gd[col_saida] = 0
                
                gdf_gd['ENERGIA_MMGD_ANUAL'] = gdf_gd[colunas_energia_final].sum(axis=1)
                
                # Selecionar colunas de interesse
                cols_interesse = ['SUB', 'POT_INST', 'ENERGIA_MMGD_ANUAL'] + colunas_energia_final
                dfs_geracao.append(gdf_gd[[c for c in cols_interesse if c in gdf_gd.columns]])
                
            except Exception as e:
                print(f"DEBUG: [MMGD] Erro ao processar camada {cam}: {e}")

    if not dfs_geracao:
        return pd.DataFrame()

    df_total = pd.concat(dfs_geracao, ignore_index=True)
    df_total['SUB'] = df_total['SUB'].astype(str).str.strip()
    
    # Agrupamento por Subestação
    agg_dict = {
        'POT_INST': 'sum',
        'ENERGIA_MMGD_ANUAL': 'sum'
    }
    # Adicionar colunas mensais ao dicionário de agregação
    for i in range(1, 13):
        col = f'ENE_MMGD_{str(i).zfill(2)}'
        if col in df_total.columns:
            agg_dict[col] = 'sum'
            
    df_agrupado = df_total.groupby('SUB').agg(agg_dict).reset_index()
    
    # Adicionar contagem de usinas
    df_count = df_total.groupby('SUB').size().reset_index(name='QTD_USINAS')
    df_agrupado = df_agrupado.merge(df_count, on='SUB')
    
    df_agrupado = df_agrupado.rename(columns={'SUB': 'COD_ID', 'POT_INST': 'TOTAL_MMGD_KW'})
    return df_agrupado

def extrair_dados_completos_gdb(caminho_gdb: str) -> Optional[Dict]:
    """Extrai subestações, potências, circuitos, topologia e MMGD para classificação e rastreamento."""
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
        
        # 2. Potência Nominal (UNTRS or UNTRAT)
        gdf_untrs = None
        if cfg['TR_NOMINAL'] in camadas:
            gdf_untrs = gpd.read_file(caminho_gdb, layer=cfg['TR_NOMINAL'])
            col = 'SUB' if 'SUB' in gdf_untrs.columns else None
            if col:
                pot = gdf_untrs.groupby(col)['POT_NOM'].sum().reset_index()
                pot.columns = ['COD_ID', 'POTENCIA_CALCULADA']
                gdf_sub = gdf_sub.merge(pot, on='COD_ID', how='left').fillna({'POTENCIA_CALCULADA': 0})
        
        # 3. Transformadores Geográficos (UNTRD ou UNTRMT)
        gdf_tr_geo = None
        if cfg['TR_GEOGRAFICO'] in camadas:
            gdf_tr_geo = gpd.read_file(caminho_gdb, layer=cfg['TR_GEOGRAFICO'])
            gdf_tr_geo['SUB'] = gdf_tr_geo['SUB'].astype(str).str.strip()
        
        # 4. Circuitos (CTMT)
        gdf_ctmt = None
        if cfg['CTMT'] in camadas:
            gdf_ctmt = gpd.read_file(caminho_gdb, layer=cfg['CTMT'])
            
        # 5. Topologia (BAR e SSDAT)
        gdf_bar = None
        if cfg['BAR'] in camadas:
            gdf_bar = gpd.read_file(caminho_gdb, layer=cfg['BAR'])
            
        gdf_ssdat = None
        if cfg['SSDAT'] in camadas:
            gdf_ssdat = gpd.read_file(caminho_gdb, layer=cfg['SSDAT'])

        # 6. Geração Distribuída (MMGD)
        df_mmgd = processar_geracao_distribuida(caminho_gdb, camadas, cfg)
        if not df_mmgd.empty:
            gdf_sub = gdf_sub.merge(df_mmgd, on='COD_ID', how='left')
            # Preencher zeros para as novas colunas
            cols_fill = ['TOTAL_MMGD_KW', 'QTD_USINAS', 'ENERGIA_MMGD_ANUAL'] + [f'ENE_MMGD_{str(i).zfill(2)}' for i in range(1, 13)]
            for c in cols_fill:
                if c in gdf_sub.columns:
                    gdf_sub[c] = gdf_sub[c].fillna(0)
        
        gdf_sub['DISTRIBUIDORA'] = dist
        gdf_sub['FONTE_GDB'] = os.path.basename(caminho_gdb)
        
        return {
            'subs': gdf_sub,
            'tr_geo': gdf_tr_geo,
            'ctmt': gdf_ctmt,
            'untrs': gdf_untrs,
            'bar': gdf_bar,
            'ssdat': gdf_ssdat
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

    # 1. Gerar Áreas Reais (Convex Hull ou Ponto Bufferizado)
    print("DEBUG: Gerando áreas iniciais de atendimento...")
    poligonos_reais = []
    
    for data in all_subs_data:
        gdf_subs = data['subs']
        gdf_tr = data['tr_geo']
        
        # Mapear transformadores por subestação para busca rápida
        tr_por_sub = {}
        if gdf_tr is not None:
            for sub_id, grupo in gdf_tr.groupby('SUB'):
                tr_por_sub[str(sub_id)] = grupo
        
        for _, sub_row in gdf_subs.iterrows():
            sub_id = str(sub_row['COD_ID'])
            grupo_tr = tr_por_sub.get(sub_id)
            
            if grupo_tr is not None and len(grupo_tr) >= 3:
                # Caso normal: Convex Hull dos transformadores
                area = grupo_tr.geometry.union_all().convex_hull
            else:
                # Caso especial (ex: Galeão): Subestação sem transformadores georeferenciados suficentes
                # Criamos uma área mínima (buffer de ~10m) para garantir que a subestação exista no processo
                # e possa "reclamar" território via Voronoi posteriormente.
                area = sub_row.geometry.centroid.buffer(0.0001) 
                
            poligonos_reais.append({'COD_ID': sub_id, 'geometry': area})

    if not poligonos_reais:
        print("DEBUG ERROR: Não foi possível gerar áreas reais. Verifique as camadas de transformadores.")
        return

    gdf_areas = gpd.GeoDataFrame(poligonos_reais, crs=all_subs_data[0]['subs'].crs).to_crs("EPSG:4326")
    
    # 2. Resolver Sobreposições (Abordagem de Prioridade por Potência + Contenção)
    print("DEBUG: Resolvendo sobreposições territoriais...")
    # Unifica todos os pontos de subestações para pegar a potência
    gdf_subs_all = pd.concat([d['subs'] for d in all_subs_data], ignore_index=True)
    gdf_subs_all['COD_ID'] = gdf_subs_all['COD_ID'].astype(str)
    
    # Merge potência e MMGD com as áreas para ordenar e enriquecer
    gdf_areas['COD_ID'] = gdf_areas['COD_ID'].astype(str)
    
    # Colunas de MMGD que queremos preservar
    cols_mmgd = ['TOTAL_MMGD_KW', 'QTD_USINAS', 'ENERGIA_MMGD_ANUAL'] + [f'ENE_MMGD_{str(i).zfill(2)}' for i in range(1, 13)]
    cols_to_merge = ['COD_ID', 'POTENCIA_CALCULADA', 'NOM', 'DISTRIBUIDORA'] + [c for c in cols_mmgd if c in gdf_subs_all.columns]
    
    gdf_areas = gdf_areas.merge(gdf_subs_all[cols_to_merge], on='COD_ID', how='left')
    
    # Lógica de Prioridade: Subestações que estão dentro de outras áreas devem ser processadas primeiro
    # para garantir que "esculpam" seu espaço e não sejam absorvidas pela subestação maior.
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
        # Encontra polígonos que contêm este ponto
        matches = sindex.query(ponto, predicate='within')
        return len(matches) - 1 # Desconta o próprio polígono

    gdf_areas['DEPTH'] = gdf_areas.apply(get_containment_depth, axis=1)
    
    # Ordenar por profundidade (mais internas primeiro) e depois por potência
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
    
    # Merge das coordenadas e MMGD de volta para o GeoDataFrame de áreas
    cols_to_merge_final = ['COD_ID', 'lat_sub', 'lon_sub']
    if 'POT_NOM' in gdf_subs_all.columns:
        cols_to_merge_final.append('POT_NOM')
    
    # Adicionar colunas de MMGD ao merge final para garantir que não sejam perdidas
    for c in cols_mmgd:
        if c in gdf_subs_all.columns:
            cols_to_merge_final.append(c)
        
    gdf_final_geo = gdf_final_geo.merge(
        gdf_subs_all[cols_to_merge_final], 
        on='COD_ID', how='left', suffixes=('', '_drop')
    )
    
    # Remover colunas duplicadas se houver
    gdf_final_geo = gdf_final_geo.loc[:, ~gdf_final_geo.columns.str.endswith('_drop')]

    # 3.5 Classificação e Hierarquia
    df_class = processar_classificacao_e_hierarquia(all_subs_data)
    gdf_final_geo = gdf_final_geo.merge(df_class, on='COD_ID', how='left')

    # 4. Unificação Final
    print("DEBUG: Unificando todas as camadas de dados...")

    gdf_final_geo = gdf_final_geo.fillna(0)
    gdf_final_geo.columns = [str(c) for c in gdf_final_geo.columns]

    # --- OTIMIZAÇÃO: Simplificação Ultra-Fina (1 metro) ---
    print("DEBUG: Aplicando simplificação de geometria (1m de tolerância)...")
    original_crs = gdf_final_geo.crs
    # Simplifica em metros para precisão técnica
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
