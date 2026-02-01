"""
Este script verifica se as subestações vazias possuem barramentos (BAR) e PACs associados.
Isso ajuda a entender por que a rastreabilidade topológica está falhando.
"""

import geopandas as gpd
import os
import pandas as pd

def verificar_barras_vazias():
    gdb_path = 'Dados Brutos/BDGD ANEEL/LIGHT_382_2021-09-30_M10_20231218-2133.gdb'
    
    subs = gpd.read_file(gdb_path, layer='SUB')
    bars = gpd.read_file(gdb_path, layer='BAR')
    untrd = gpd.read_file(gdb_path, layer='UNTRD')
    untrs = gpd.read_file(gdb_path, layer='UNTRS')
    
    subs_com_carga = set(untrd['SUB'].unique())
    subs_com_untrs = set(untrs['SUB'].unique())
    
    vazias = subs[~subs['COD_ID'].isin(subs_com_carga) & ~subs['COD_ID'].isin(subs_com_untrs)]
    
    print(f"DEBUG: Analisando {len(vazias)} subestações vazias...")
    
    resultados = []
    for _, row in vazias.iterrows():
        sid = row['COD_ID']
        nome = row['NOM'] if 'NOM' in row else row['NOME']
        
        # Barras vinculadas
        sub_bars = bars[bars['SUB'] == sid]
        pacs = sub_bars['PAC'].unique().tolist()
        
        resultados.append({
            'ID': sid,
            'NOME': nome,
            'QTD_BARRAS': len(sub_bars),
            'PACS': ", ".join(pacs[:5]) + ("..." if len(pacs) > 5 else "")
        })
        
    df_res = pd.DataFrame(resultados)
    print("\n=== ANÁLISE DE BARRAS EM SUBESTAÇÕES VAZIAS ===")
    print(df_res.head(20).to_string(index=False))
    
    print(f"\nSubestações vazias SEM NENHUMA barra no GDB: {len(df_res[df_res['QTD_BARRAS'] == 0])}")

if __name__ == "__main__":
    verificar_barras_vazias()
