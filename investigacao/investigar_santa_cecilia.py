"""
Este script investiga especificamente a subestação SETD SANTA CECILIA.
Ele busca entender por que ela aparece isolada na hierarquia.
"""

import geopandas as gpd
import os
import pandas as pd

def investigar_santa_cecilia():
    gdb_path = 'Dados Brutos/BDGD ANEEL/LIGHT_382_2021-09-30_M10_20231218-2133.gdb'
    
    print("DEBUG: Buscando SETD SANTA CECILIA na camada SUB...")
    subs = gpd.read_file(gdb_path, layer='SUB')
    
    # Localizar Santa Cecilia
    sc = subs[subs['NOM'].str.contains('SANTA CECILIA', na=False, case=False)]
    if sc.empty:
        print("ERRO: Subestação SETD SANTA CECILIA não encontrada.")
        return
    
    sid = sc.iloc[0]['COD_ID']
    nome = sc.iloc[0]['NOM']
    print(f"DEBUG: Encontrada: {nome} (ID: {sid})")
    
    # 1. Verificar Barras e PACs
    print("\nDEBUG: Verificando Barras...")
    bars = gpd.read_file(gdb_path, layer='BAR')
    sc_bars = bars[bars['SUB'] == sid]
    print(f"Quantidade de barras: {len(sc_bars)}")
    pacs = sc_bars['PAC'].unique().tolist()
    print(f"PACs: {pacs}")

    # 2. Verificar SSDAT (Alta Tensão)
    print("\nDEBUG: Verificando conexões SSDAT...")
    ssdat = gpd.read_file(gdb_path, layer='SSDAT')
    conexoes = ssdat[(ssdat['PAC_1'].isin(pacs)) | (ssdat['PAC_2'].isin(pacs))]
    print(f"Quantidade de conexões SSDAT: {len(conexoes)}")
    
    if not conexoes.empty:
        pac_to_sub = bars.set_index('PAC')['SUB'].to_dict()
        sub_names = subs.set_index('COD_ID')['NOM'].to_dict()
        
        for _, row in conexoes.iterrows():
            p1, p2 = row['PAC_1'], row['PAC_2']
            v1 = pac_to_sub.get(p1)
            v2 = pac_to_sub.get(p2)
            
            n1 = sub_names.get(v1, f"EXTERNO:{p1}")
            n2 = sub_names.get(v2, f"EXTERNO:{p2}")
            
            print(f"Conexão: {n1} <-> {n2}")

    # 3. Verificar se ela recebe energia de um gerador (UGAT)
    print("\nDEBUG: Verificando conexões com geradores (UGAT)...")
    ugat = gpd.read_file(gdb_path, layer='UGAT_tab')
    sc_ugat = ugat[ugat['SUB'] == sid]
    print(f"Quantidade de geradores UGAT vinculados: {len(sc_ugat)}")
    if not sc_ugat.empty:
        print(sc_ugat[['COD_ID', 'DESCR', 'POT_INST']].to_string(index=False))

if __name__ == "__main__":
    investigar_santa_cecilia()
