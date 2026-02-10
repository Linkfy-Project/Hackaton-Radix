"""
Script para investigar a conectividade entre subestações na rede de Alta Tensão (AT).
Objetivo: Identificar como subestações sem carga (transporte) são alimentadas.
"""

import geopandas as gpd
import os
import pandas as pd

def investigar_conectividade():
    gdbs = {
        'ENEL': {
            'path': 'Dados Brutos/BDGD ANEEL/ENEL_RJ_383_2022-09-30_V10_20240605-0611.gdb',
            'UNTRS': 'UNTRAT'
        },
        'LIGHT': {
            'path': 'Dados Brutos/BDGD ANEEL/LIGHT_382_2021-09-30_M10_20231218-2133.gdb',
            'UNTRS': 'UNTRS'
        }
    }
    
    for dist, cfg_dist in gdbs.items():
        path = cfg_dist['path']
        if not os.path.exists(path):
            print(f"\nDEBUG: GDB {dist} não encontrado.")
            continue

        print(f"\nDEBUG: Analisando {dist}...")
        
        # 1. Barras e suas subestações
        gdf_bar = gpd.read_file(path, layer='BAR')
        bar_to_sub = gdf_bar.set_index('COD_ID')['SUB'].to_dict()
        pac_to_sub = gdf_bar.set_index('PAC')['SUB'].to_dict()
        
        # 2. Investigar UNTRS (Transformadores de Subestação)
        untrs_layer = cfg_dist['UNTRS']
        print(f"DEBUG: [{dist}] Analisando transformadores de subestação ({untrs_layer})...")
        gdf_untrs = gpd.read_file(path, layer=untrs_layer)
        untrs_con = []
        for _, row in gdf_untrs.iterrows():
            s1 = bar_to_sub.get(row['BARR_1'])
            s2 = bar_to_sub.get(row['BARR_2'])
            if s1 and s2 and s1 != s2:
                untrs_con.append({'SUB_1': s1, 'SUB_2': s2, 'UNTRS_ID': row['COD_ID']})
                
        if untrs_con:
            print(f"\n--- [{dist}] CONEXÕES VIA TRANSFORMADORES ENTRE SUBS ---")
            print(pd.DataFrame(untrs_con).drop_duplicates(subset=['SUB_1', 'SUB_2']).to_string(index=False))
        else:
            print(f"DEBUG: [{dist}] Nenhuma conexão entre subs via transformadores.")

        # 3. Investigar SSDAT (Segmentos de Alta Tensão)
        print(f"DEBUG: [{dist}] Analisando segmentos de rede AT (SSDAT)...")
        gdf_ssdat = gpd.read_file(path, layer='SSDAT')
        ssdat_con = []
        for _, row in gdf_ssdat.iterrows():
            sub1 = pac_to_sub.get(row['PAC_1'])
            sub2 = pac_to_sub.get(row['PAC_2'])
            
            if sub1 and sub2 and sub1 != sub2:
                ssdat_con.append({'SUB_A': sub1, 'SUB_B': sub2, 'SSDAT_ID': row['COD_ID']})
                
        if ssdat_con:
            print(f"\n--- [{dist}] CONEXÕES ENTRE SUBESTAÇÕES VIA SSDAT ---")
            print(pd.DataFrame(ssdat_con).drop_duplicates(subset=['SUB_A', 'SUB_B']).to_string(index=False))
        else:
            print(f"DEBUG: [{dist}] Nenhuma conexão direta entre subestações encontrada via PACs de SSDAT.")

if __name__ == "__main__":
    investigar_conectividade()
