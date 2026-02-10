"""
Este script investiga as conexões da ENEL com a ONS e com a LIGHT.
Ele busca por PACs (Pontos de Acoplamento Comum) que indiquem fronteiras elétricas.
"""

import geopandas as gpd
import pandas as pd
import os

def investigar_enel():
    path_enel = 'Dados Brutos/BDGD ANEEL/ENEL_RJ_383_2022-09-30_V10_20240605-0611.gdb'
    path_light = 'Dados Brutos/BDGD ANEEL/LIGHT_382_2021-09-30_M10_20231218-2133.gdb'
    path_ons = "Dados Brutos/ONS/LINHA_TRANSMISSAO.csv"

    print("DEBUG: Carregando dados da ENEL...")
    bars_enel = gpd.read_file(path_enel, layer='BAR')
    subs_enel = gpd.read_file(path_enel, layer='SUB')
    sub_names_enel = subs_enel.set_index('COD_ID')['NOME'].to_dict()

    print("DEBUG: Carregando dados da LIGHT...")
    bars_light = gpd.read_file(path_light, layer='BAR')
    subs_light = gpd.read_file(path_light, layer='SUB')
    sub_names_light = subs_light.set_index('COD_ID')['NOM'].to_dict()

    print("DEBUG: Carregando dados da ONS...")
    df_ons = pd.read_csv(path_ons, sep=';', encoding='latin1')
    ons_bars = set(df_ons['num_barra_de'].astype(str).str.split('.').str[0].unique()) | \
               set(df_ons['num_barra_para'].astype(str).str.split('.').str[0].unique())

    # 1. Buscar PACs Externos na ENEL (padrão EXTERNO:AT_XXXX)
    pacs_enel = bars_enel['PAC'].dropna().unique()
    pacs_externos_enel = [p for p in pacs_enel if 'EXTERNO' in str(p)]
    
    print(f"DEBUG: Encontrados {len(pacs_externos_enel)} PACs externos na ENEL.")
    for p in pacs_externos_enel[:10]:
        print(f"DEBUG: Exemplo PAC Externo ENEL: {p}")

    # 2. Verificar se algum PAC da ENEL existe na LIGHT
    pacs_light = set(bars_light['PAC'].dropna().unique())
    intersecao_enel_light = set(pacs_enel) & pacs_light
    
    print(f"DEBUG: {len(intersecao_enel_light)} PACs compartilhados entre ENEL e LIGHT.")
    for p in list(intersecao_enel_light)[:10]:
        sub_enel_id = bars_enel[bars_enel['PAC'] == p]['SUB'].iloc[0]
        sub_light_id = bars_light[bars_light['PAC'] == p]['SUB'].iloc[0]
        print(f"DEBUG: PAC compartilhado: {p} | ENEL: {sub_names_enel.get(sub_enel_id)} | LIGHT: {sub_names_light.get(sub_light_id)}")

    # 3. Verificar se algum PAC da ENEL (número) bate com ONS
    conexoes_ons_enel = []
    for p in pacs_enel:
        # Tentar extrair número do PAC
        import re
        match = re.search(r'(\d+)', str(p))
        if match:
            num_p = match.group(1)
            if num_p in ons_bars:
                sub_id = bars_enel[bars_enel['PAC'] == p]['SUB'].iloc[0]
                conexoes_ons_enel.append({
                    'PAC': p,
                    'SUB_ID': sub_id,
                    'SUB_NOME': sub_names_enel.get(sub_id),
                    'BARRA_ONS': num_p
                })

    print(f"DEBUG: Encontradas {len(conexoes_ons_enel)} possíveis conexões ENEL -> ONS via número de PAC.")
    df_ons_enel = pd.DataFrame(conexoes_ons_enel).drop_duplicates()
    if not df_ons_enel.empty:
        print(df_ons_enel.head(20))
        df_ons_enel.to_csv("investigacao/conexoes_ons_enel.csv", index=False, sep=';')

if __name__ == "__main__":
    investigar_enel()
