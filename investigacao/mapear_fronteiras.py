"""
Este script mapeia as fronteiras entre ONS, ENEL e LIGHT.
Ele busca por conexões baseadas nos IDs de barras (números nos PACs).
"""

import geopandas as gpd
import pandas as pd
import os
import re

def extrair_numero(pac):
    if pd.isna(pac): return None
    match = re.search(r'(\d+)', str(pac))
    return match.group(1) if match else None

def mapear_fronteiras():
    path_enel = 'Dados Brutos/BDGD ANEEL/ENEL_RJ_383_2022-09-30_V10_20240605-0611.gdb'
    path_light = 'Dados Brutos/BDGD ANEEL/LIGHT_382_2021-09-30_M10_20231218-2133.gdb'
    path_ons = "Dados Brutos/ONS/LINHA_TRANSMISSAO.csv"

    print("DEBUG: Carregando ONS...")
    df_ons = pd.read_csv(path_ons, sep=';', encoding='latin1')
    # Filtrar ONS para Rio de Janeiro
    df_ons_rj = df_ons[(df_ons['nom_estado_de'] == 'RIO DE JANEIRO') | (df_ons['nom_estado_para'] == 'RIO DE JANEIRO')].copy()
    
    print(f"DEBUG: ONS RJ possui {len(df_ons_rj)} linhas.")

    # Mapeamento de Barra -> Subestação ONS (apenas RJ)
    barra_para_ons = {}
    for _, row in df_ons_rj.iterrows():
        b_de = str(row['num_barra_de']).split('.')[0]
        b_para = str(row['num_barra_para']).split('.')[0]
        barra_para_ons[b_de] = row['nom_subestacao_de']
        barra_para_ons[b_para] = row['nom_subestacao_para']

    print("DEBUG: Carregando ENEL...")
    bars_enel = gpd.read_file(path_enel, layer='BAR')
    subs_enel = gpd.read_file(path_enel, layer='SUB')
    sub_names_enel = subs_enel.set_index('COD_ID')['NOME'].to_dict()

    print("DEBUG: Carregando LIGHT...")
    bars_light = gpd.read_file(path_light, layer='BAR')
    subs_light = gpd.read_file(path_light, layer='SUB')
    sub_names_light = subs_light.set_index('COD_ID')['NOM'].to_dict()

    # 1. Conexões ENEL -> ONS (PAC + Nome)
    conexoes_enel_ons = []
    
    # Via PAC
    for _, row in bars_enel.iterrows():
        num = extrair_numero(row['PAC'])
        if num and num in barra_para_ons:
            sub_nome = sub_names_enel.get(row['SUB'])
            conexoes_enel_ons.append({
                'DISTRIBUIDORA': 'ENEL',
                'SUB_LOCAL': sub_nome,
                'SUB_ONS': barra_para_ons[num],
                'PAC': row['PAC'],
                'BARRA': num,
                'METODO': 'PAC'
            })
    
    # Via Nome (Fuzzy/Exact)
    ons_names_rj = set(barra_para_ons.values())
    for sid, nome in sub_names_enel.items():
        nome_upper = str(nome).upper()
        # Tentar match exato ou parcial
        for ons_n in ons_names_rj:
            if ons_n in nome_upper or nome_upper in ons_n:
                conexoes_enel_ons.append({
                    'DISTRIBUIDORA': 'ENEL',
                    'SUB_LOCAL': nome,
                    'SUB_ONS': ons_n,
                    'PAC': 'N/A',
                    'BARRA': 'N/A',
                    'METODO': 'NOME'
                })

    df_enel_ons = pd.DataFrame(conexoes_enel_ons).drop_duplicates(subset=['SUB_LOCAL', 'SUB_ONS'])
    print(f"DEBUG: Encontradas {len(df_enel_ons)} conexões ENEL -> ONS.")
    if not df_enel_ons.empty:
        print(df_enel_ons.head(20))

    # 2. Conexões LIGHT -> ONS
    conexoes_light_ons = []
    for _, row in bars_light.iterrows():
        num = extrair_numero(row['PAC'])
        if num in barra_para_ons:
            sub_nome = sub_names_light.get(row['SUB'])
            conexoes_light_ons.append({
                'DISTRIBUIDORA': 'LIGHT',
                'SUB_LOCAL': sub_nome,
                'SUB_ONS': barra_para_ons[num],
                'PAC': row['PAC'],
                'BARRA': num
            })
    
    df_light_ons = pd.DataFrame(conexoes_light_ons).drop_duplicates()
    print(f"DEBUG: Encontradas {len(df_light_ons)} conexões LIGHT -> ONS.")
    if not df_light_ons.empty:
        print(df_light_ons.head(20))

    # 3. Conexões ENEL <-> LIGHT (Fronteira Direta)
    # Procurar por PACs que aparecem em ambas
    pacs_enel = set(bars_enel['PAC'].dropna().unique())
    pacs_light = set(bars_light['PAC'].dropna().unique())
    fronteira_direta = pacs_enel & pacs_light
    
    conexoes_fronteira = []
    for pac in fronteira_direta:
        sub_enel_id = bars_enel[bars_enel['PAC'] == pac]['SUB'].iloc[0]
        sub_light_id = bars_light[bars_light['PAC'] == pac]['SUB'].iloc[0]
        conexoes_fronteira.append({
            'PAC': pac,
            'SUB_ENEL': sub_names_enel.get(sub_enel_id),
            'SUB_LIGHT': sub_names_light.get(sub_light_id)
        })
    
    df_fronteira = pd.DataFrame(conexoes_fronteira)
    print(f"DEBUG: Encontradas {len(df_fronteira)} conexões de fronteira direta ENEL <-> LIGHT.")
    if not df_fronteira.empty:
        print(df_fronteira)

    # Salvar resultados
    if not df_enel_ons.empty: df_enel_ons.to_csv("investigacao/conexoes_enel_ons.csv", index=False, sep=';')
    if not df_light_ons.empty: df_light_ons.to_csv("investigacao/conexoes_light_ons.csv", index=False, sep=';')
    if not df_fronteira.empty: df_fronteira.to_csv("investigacao/fronteira_enel_light.csv", index=False, sep=';')

if __name__ == "__main__":
    mapear_fronteiras()
