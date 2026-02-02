"""
Script para investigar subestações que possuem potência nominal mas não possuem 
transformadores georeferenciados na camada geográfica (UNTRD/UNTRMT).
"""

import geopandas as gpd
import os
import pandas as pd

def investigar_subs():
    gdbs = {
        'ENEL': 'Dados Brutos/BDGD ANEEL/ENEL_RJ_383_2022-09-30_V10_20240605-0611.gdb',
        'LIGHT': 'Dados Brutos/BDGD ANEEL/LIGHT_382_2021-09-30_M10_20231218-2133.gdb'
    }
    
    config = {
        'LIGHT': {
            'SUB': 'SUB', 
            'TR_NOM': 'UNTRS', 
            'TR_GEO': 'UNTRD'
        },
        'ENEL': {
            'SUB': 'SUB', 
            'TR_NOM': 'UNTRAT', 
            'TR_GEO': 'UNTRMT'
        }
    }
    
    all_results = []
    
    for dist, path in gdbs.items():
        if not os.path.exists(path):
            print(f"DEBUG: GDB {dist} não encontrado em {path}")
            continue
            
        print(f"DEBUG: Analisando {dist}...")
        cfg = config[dist]
        
        # Ler subestações
        gdf_sub = gpd.read_file(path, layer=cfg['SUB'])
        # Normalizar nome da coluna de nome
        nome_col = 'NOM' if 'NOM' in gdf_sub.columns else 'NOME'
        
        # Ler transformadores para potência nominal
        gdf_tr_nom = gpd.read_file(path, layer=cfg['TR_NOM'])
        sub_col_nom = 'SUB' if 'SUB' in gdf_tr_nom.columns else 'COD_ID'
        pot = gdf_tr_nom.groupby(sub_col_nom)['POT_NOM'].sum().reset_index()
        pot.columns = ['COD_ID', 'POT_NOM']
        
        # Ler transformadores geográficos
        gdf_tr_geo = gpd.read_file(path, layer=cfg['TR_GEO'])
        geo_counts = gdf_tr_geo.groupby('SUB').size().reset_index(name='GEO_COUNT')
        geo_counts.columns = ['COD_ID', 'GEO_COUNT']
        
        # Merge
        gdf_sub['COD_ID'] = gdf_sub['COD_ID'].astype(str)
        pot['COD_ID'] = pot['COD_ID'].astype(str)
        geo_counts['COD_ID'] = geo_counts['COD_ID'].astype(str)
        
        merged = gdf_sub[['COD_ID', nome_col]].merge(pot, on='COD_ID', how='left')
        merged = merged.merge(geo_counts, on='COD_ID', how='left')
        merged = merged.fillna(0)
        
        # Filtrar: Sem transformadores geográficos mas com potência > 0
        filtered = merged[(merged['GEO_COUNT'] == 0) & (merged['POT_NOM'] > 0)]
        
        for _, row in filtered.iterrows():
            all_results.append({
                'DISTRIBUIDORA': dist,
                'COD_ID': row['COD_ID'],
                'NOME': row[nome_col],
                'POT_NOM': row['POT_NOM'],
                'GEO_COUNT': row['GEO_COUNT']
            })
            
    if all_results:
        df_final = pd.DataFrame(all_results)
        print("\n--- SUBESTAÇÕES COM POTÊNCIA MAS SEM TRANSFORMADORES GEOGRÁFICOS ---")
        print(df_final.to_string(index=False))
        print(f"\nTotal encontrado: {len(df_final)}")
    else:
        print("\nNenhuma subestação encontrada com esses critérios.")

if __name__ == "__main__":
    investigar_subs()
