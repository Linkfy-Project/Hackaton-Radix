"""
Script para mapear a conectividade entre subestações via circuitos de Alta Tensão (CTAT).
"""

import geopandas as gpd
import os
import pandas as pd

def mapear_conexoes_at():
    path = 'Dados Brutos/BDGD ANEEL/LIGHT_382_2021-09-30_M10_20231218-2133.gdb'
    if not os.path.exists(path): return

    print("DEBUG: Carregando camadas para análise de topologia AT...")
    bars = gpd.read_file(path, layer='BAR')
    ssdat = gpd.read_file(path, layer='SSDAT')
    subs = gpd.read_file(path, layer='SUB')
    sub_names = subs.set_index('COD_ID')['NOM'].to_dict()
    
    # Mapear PAC para Subestação
    pac_to_sub = bars.set_index('PAC')['SUB'].to_dict()
    
    print("DEBUG: Analisando conexões SSDAT via PACs...")
    conexoes = []
    for _, row in ssdat.iterrows():
        s1 = pac_to_sub.get(row['PAC_1'])
        s2 = pac_to_sub.get(row['PAC_2'])
        
        if s1 and s2 and s1 != s2:
            conexoes.append({'DE': s1, 'PARA': s2, 'SEGMENTO': row['COD_ID']})
            
    if conexoes:
        df = pd.DataFrame(conexoes).drop_duplicates(subset=['DE', 'PARA'])
        print("\n--- CONEXÕES ENTRE SUBESTAÇÕES (TOPOLOGIA SSDAT) ---")
        df['NOME_DE'] = df['DE'].map(sub_names)
        df['NOME_PARA'] = df['PARA'].map(sub_names)
        print(df[['DE', 'NOME_DE', 'PARA', 'NOME_PARA']].to_string(index=False))
    else:
        print("Nenhuma conexão direta encontrada via PACs.")
        
    # Tentativa 2: Proximidade Geográfica
    print("\nDEBUG: Tentando via proximidade geográfica (Linha toca Ponto da Sub)...")
    # Usar CRS projetado para buffer preciso
    target_crs = "EPSG:31983"
    subs_proj = subs.to_crs(target_crs)
    ssdat_proj = ssdat.to_crs(target_crs)
    
    # Criar um buffer pequeno em volta das subs (10m)
    subs_buffer = subs_proj.copy()
    subs_buffer['geometry'] = subs_proj.geometry.buffer(10)
    
    spatial_join = gpd.sjoin(ssdat_proj, subs_buffer[['COD_ID', 'NOM', 'geometry']], how='inner', predicate='intersects')
    
    # Agrupar por segmento para ver quais subs ele toca
    segmento_subs = spatial_join.groupby('COD_ID_left')['COD_ID_right'].unique()
    
    conexoes_geo = []
    for seg_id, sub_ids in segmento_subs.items():
        if len(sub_ids) > 1:
            # Este segmento toca mais de uma subestação!
            for i in range(len(sub_ids)):
                for j in range(i + 1, len(sub_ids)):
                    conexoes_geo.append({'SUB_A': sub_ids[i], 'SUB_B': sub_ids[j]})
    
    if conexoes_geo:
        df_geo = pd.DataFrame(conexoes_geo).drop_duplicates()
        df_geo['NOME_A'] = df_geo['SUB_A'].map(sub_names)
        df_geo['NOME_B'] = df_geo['SUB_B'].map(sub_names)
        print("\n--- CONEXÕES ENTRE SUBESTAÇÕES (PROXIMIDADE GEOGRÁFICA SSDAT) ---")
        print(df_geo.to_string(index=False))
    else:
        print("Nenhuma conexão encontrada via proximidade geográfica.")

if __name__ == "__main__":
    mapear_conexoes_at()
