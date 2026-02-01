"""
Este script investiga especificamente a subestação SESD MARMELO.
Ele busca entender por que ela aparece com potência zerada apesar de possuir transformadores.
"""

import geopandas as gpd
import os
import pandas as pd

def investigar_marmelo():
    gdb_path = 'Dados Brutos/BDGD ANEEL/LIGHT_382_2021-09-30_M10_20231218-2133.gdb'
    
    print("DEBUG: Buscando SESD MARMELO na camada SUB...")
    subs = gpd.read_file(gdb_path, layer='SUB')
    
    # Localizar Marmelo
    marmelo = subs[subs['NOM'].str.contains('MARMELO', na=False, case=False)]
    if marmelo.empty:
        print("ERRO: Subestação SESD MARMELO não encontrada.")
        return
    
    sid = marmelo.iloc[0]['COD_ID']
    nome = marmelo.iloc[0]['NOM']
    print(f"DEBUG: Encontrada: {nome} (ID: {sid})")
    
    # 1. Verificar UNTRD (Transformadores de Distribuição)
    print("\nDEBUG: Verificando UNTRD...")
    untrd = gpd.read_file(gdb_path, layer='untrd') # Usando minúsculo para testar se o geopandas resolve
    marmelo_untrd = untrd[untrd['SUB'] == sid]
    print(f"Quantidade de transformadores UNTRD vinculados: {len(marmelo_untrd)}")
    if not marmelo_untrd.empty:
        print(marmelo_untrd[['COD_ID', 'POT_NOM', 'SIT_ATIV']].to_string(index=False))
        print(f"Soma da Potência UNTRD: {marmelo_untrd['POT_NOM'].sum()}")

    # 2. Verificar UNTRS (Transformadores de Subestação)
    print("\nDEBUG: Verificando UNTRS...")
    untrs = gpd.read_file(gdb_path, layer='untrs')
    marmelo_untrs = untrs[untrs['SUB'] == sid]
    print(f"Quantidade de transformadores UNTRS vinculados: {len(marmelo_untrs)}")
    if not marmelo_untrs.empty:
        print(marmelo_untrs[['COD_ID', 'POT_NOM', 'SIT_ATIV']].to_string(index=False))
        print(f"Soma da Potência UNTRS: {marmelo_untrs['POT_NOM'].sum()}")

    # 3. Verificar se existem transformadores em outras camadas (UNTRMT)
    try:
        untrmt = gpd.read_file(gdb_path, layer='UNTRMT')
        marmelo_untrmt = untrmt[untrmt['SUB'] == sid]
        print(f"\nQuantidade de transformadores UNTRMT vinculados: {len(marmelo_untrmt)}")
        if not marmelo_untrmt.empty:
            print(marmelo_untrmt[['COD_ID', 'POT_NOM', 'SIT_ATIV']].to_string(index=False))
    except:
        print("\nCamada UNTRMT não disponível.")

    # 4. Verificar Barras e PACs
    print("\nDEBUG: Verificando Barras...")
    bars = gpd.read_file(gdb_path, layer='BAR')
    marmelo_bars = bars[bars['SUB'] == sid]
    print(f"Quantidade de barras: {len(marmelo_bars)}")
    if not marmelo_bars.empty:
        print(marmelo_bars[['COD_ID', 'PAC', 'TEN_NOM']].to_string(index=False))

if __name__ == "__main__":
    investigar_marmelo()
