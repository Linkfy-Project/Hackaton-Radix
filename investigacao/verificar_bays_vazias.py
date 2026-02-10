"""
Este script verifica se as subestações sem barras possuem vãos (BAY) associados.
"""

import geopandas as gpd
import os
import pandas as pd

def verificar_bays_vazias():
    gdb_path = 'Dados Brutos/BDGD ANEEL/LIGHT_382_2021-09-30_M10_20231218-2133.gdb'
    
    subs = gpd.read_file(gdb_path, layer='SUB')
    bars = gpd.read_file(gdb_path, layer='BAR')
    bays = gpd.read_file(gdb_path, layer='BAY')
    untrd = gpd.read_file(gdb_path, layer='UNTRD')
    untrs = gpd.read_file(gdb_path, layer='UNTRS')
    
    subs_com_carga = set(untrd['SUB'].unique())
    subs_com_untrs = set(untrs['SUB'].unique())
    
    vazias = subs[~subs['COD_ID'].isin(subs_com_carga) & ~subs['COD_ID'].isin(subs_com_untrs)]
    vazias_sem_barras = vazias[~vazias['COD_ID'].isin(bars['SUB'].unique())]
    
    print(f"DEBUG: Analisando {len(vazias_sem_barras)} subestações vazias e sem barras...")
    
    resultados = []
    for _, row in vazias_sem_barras.iterrows():
        sid = row['COD_ID']
        nome = row['NOM'] if 'NOM' in row else row['NOME']
        
        # Bays vinculadas
        sub_bays = bays[bays['SUB'] == sid]
        
        resultados.append({
            'ID': sid,
            'NOME': nome,
            'QTD_BAYS': len(sub_bays)
        })
        
    df_res = pd.DataFrame(resultados)
    print("\n=== ANÁLISE DE BAYS EM SUBESTAÇÕES SEM BARRAS ===")
    print(df_res[df_res['QTD_BAYS'] > 0].to_string(index=False))
    
    print(f"\nSubestações vazias e sem barras que possuem BAYS: {len(df_res[df_res['QTD_BAYS'] > 0])}")

if __name__ == "__main__":
    verificar_bays_vazias()
