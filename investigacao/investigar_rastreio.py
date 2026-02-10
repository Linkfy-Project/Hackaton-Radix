
import geopandas as gpd
import pandas as pd
import os
import fiona

def investigar():
    gdb_light = 'Dados Brutos/BDGD ANEEL/LIGHT_382_2021-09-30_M10_20231218-2133.gdb'
    
    print("DEBUG: Carregando camadas para investigação...")
    subs = gpd.read_file(gdb_light, layer='SUB')
    bars = gpd.read_file(gdb_light, layer='BAR')
    ssdat = gpd.read_file(gdb_light, layer='SSDAT')
    
    print("\n--- AMOSTRA SSDAT ---")
    print(ssdat[['PAC_1', 'PAC_2']].head())
    
    print("\n--- AMOSTRA BAR ---")
    print(bars[['PAC', 'SUB']].head())
    
    print(f"DEBUG: Total de Subestações: {len(subs)}")
    print(f"DEBUG: Total de Barras: {len(bars)}")
    print(f"DEBUG: Total de Segmentos AT: {len(ssdat)}")
    
    # Mapeamento PAC -> SUB
    pac_to_sub = bars.set_index('PAC')['SUB'].to_dict()
    
    # Verificar quantos PACs do SSDAT existem no BAR
    pacs_ssdat = set(ssdat['PAC_1'].unique()) | set(ssdat['PAC_2'].unique())
    pacs_no_bar = set(bars['PAC'].unique())
    
    encontrados = pacs_ssdat.intersection(pacs_no_bar)
    print(f"DEBUG: PACs no SSDAT: {len(pacs_ssdat)}")
    print(f"DEBUG: PACs no BAR: {len(pacs_no_bar)}")
    print(f"DEBUG: PACs do SSDAT encontrados no BAR: {len(encontrados)} ({len(encontrados)/len(pacs_ssdat)*100:.1f}%)")
    
    # Construir Grafo de Segmentos via Proximidade Geográfica
    print("DEBUG: Construindo grafo de segmentos via geometria...")
    target_crs = "EPSG:31983"
    subs_proj = subs.to_crs(target_crs)
    ssdat_proj = ssdat.to_crs(target_crs)
    
    # 1. Mapear quais subestações cada segmento toca
    subs_buffer = subs_proj.copy()
    subs_buffer['geometry'] = subs_proj.geometry.buffer(50) # 50m de tolerância
    
    # Join espacial: Segmento -> Subestação
    seg_to_sub = gpd.sjoin(ssdat_proj, subs_buffer[['COD_ID', 'geometry']], how='inner', predicate='intersects')
    col_id = 'COD_ID' if 'COD_ID' in seg_to_sub.columns else 'COD_ID_right'
    seg_to_subs_map = seg_to_sub.groupby(seg_to_sub.index)[col_id].apply(set).to_dict()
    
    # 2. Mapear quais segmentos se tocam (grafo de segmentos)
    # Para ser rápido, vamos usar os PACs como nós de conexão entre segmentos
    grafo_pacs = {}
    pac_to_segs = {}
    for idx, row in ssdat.iterrows():
        p1, p2 = str(row['PAC_1']), str(row['PAC_2'])
        for p in [p1, p2]:
            if p not in pac_to_segs: pac_to_segs[p] = set()
            pac_to_segs[p].add(idx)
            
    print(f"DEBUG: Total de PACs (nós de rede): {len(pac_to_segs)}")
    
    # Verificar classificações (simulando a lógica do extrator)
    # ... simplificado ...
    untrs = gpd.read_file(gdb_light, layer='UNTRS')
    subs_com_untrs = set(untrs['SUB'].unique())
    
    # Identificar subestações de transporte
    transporte_ids = []
    for _, row in subs.iterrows():
        sid = str(row['COD_ID']).strip()
        if sid not in subs_com_untrs: # Simplificação: se não tem UNTRS, é transporte/manobra
            transporte_ids.append(sid)
            
    print(f"DEBUG: Subestações de Transporte identificadas: {len(transporte_ids)}")
    
    # Testar BFS de Segmento em Segmento
    print("DEBUG: Testando rastreamento via grafo de segmentos...")
    
    # Mapear Subestação -> Segmentos que a tocam
    sub_to_segs = {}
    for seg_idx, sids in seg_to_subs_map.items():
        for s in sids:
            sid_str = str(s).strip()
            if sid_str not in sub_to_segs: sub_to_segs[sid_str] = set()
            sub_to_segs[sid_str].add(seg_idx)

    sucessos = 0
    for sid in transporte_ids[:20]:
        meus_segs = sub_to_segs.get(sid, set())
        if not meus_segs:
            print(f"  [AVISO] SE {sid} não toca nenhum segmento SSDAT")
            continue
            
        visitados_seg = set(meus_segs)
        fila_seg = [(s, 0) for s in meus_segs]
        achou = False
        
        while fila_seg:
            seg_atual, dist = fila_seg.pop(0)
            
            # Verifica se este segmento toca OUTRA subestação
            subs_tocadas = seg_to_subs_map.get(seg_atual, set())
            for s_tocada in subs_tocadas:
                s_tocada_str = str(s_tocada).strip()
                if s_tocada_str != sid and s_tocada_str in subs_com_untrs:
                    print(f"  [OK] SE {sid} -> Alimentada por {s_tocada_str} (dist: {dist} segs)")
                    sucessos += 1
                    achou = True
                    break
            if achou: break
            
            if dist < 100:
                # Pega os PACs deste segmento
                p1 = str(ssdat.loc[seg_atual, 'PAC_1'])
                p2 = str(ssdat.loc[seg_atual, 'PAC_2'])
                for p in [p1, p2]:
                    # Pega outros segmentos que usam este PAC
                    for prox_seg in pac_to_segs.get(p, []):
                        if prox_seg not in visitados_seg:
                            visitados_seg.add(prox_seg)
                            fila_seg.append((prox_seg, dist + 1))
                            
        if not achou:
            print(f"  [FALHA] SE {sid} não encontrou origem Plena via rede")

if __name__ == "__main__":
    investigar()
