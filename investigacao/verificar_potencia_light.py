"""
Este script investiga a potência das subestações na base da LIGHT.
Ele identifica subestações que não possuem transformadores associados ou que possuem potência nominal zerada.
"""

import geopandas as gpd
import os
import pandas as pd

def investigar_potencia_light():
    gdb_path = 'Dados Brutos/BDGD ANEEL/LIGHT_382_2021-09-30_M10_20231218-2133.gdb'
    
    if not os.path.exists(gdb_path):
        print(f"DEBUG: GDB da LIGHT não encontrado em {gdb_path}")
        return

    print(f"DEBUG: Lendo dados da LIGHT para investigação...")
    
    # 1. Carregar Subestações
    subs = gpd.read_file(gdb_path, layer='SUB')
    print(f"DEBUG: Total de subestações na camada SUB: {len(subs)}")
    
    # 2. Carregar Unidades Transformadoras (Distribuição e Média Tensão)
    # Tentamos carregar UNTRD e UNTRMT
    try:
        untrd = gpd.read_file(gdb_path, layer='UNTRD')
        print(f"DEBUG: Total de transformadores na camada UNTRD: {len(untrd)}")
    except Exception as e:
        print(f"DEBUG: Camada UNTRD não encontrada ou erro ao ler: {e}")
        untrd = pd.DataFrame()

    try:
        untrmt = gpd.read_file(gdb_path, layer='UNTRMT')
        print(f"DEBUG: Total de transformadores na camada UNTRMT: {len(untrmt)}")
    except Exception as e:
        print(f"DEBUG: Camada UNTRMT não encontrada ou erro ao ler: {e}")
        untrmt = pd.DataFrame()

    # 3. Mapear potência por subestação
    # Somamos a potência nominal (POT_NOM) de todos os transformadores vinculados a cada subestação
    potencia_por_sub = {}
    
    # Processar UNTRD
    if not untrd.empty:
        for _, row in untrd.iterrows():
            sub_id = row['SUB']
            pot = row['POT_NOM'] if 'POT_NOM' in row else 0
            potencia_por_sub[sub_id] = potencia_por_sub.get(sub_id, 0) + pot
            
    # Processar UNTRMT
    if not untrmt.empty:
        for _, row in untrmt.iterrows():
            sub_id = row['SUB']
            pot = row['POT_NOM'] if 'POT_NOM' in row else 0
            potencia_por_sub[sub_id] = potencia_por_sub.get(sub_id, 0) + pot

    # 4. Analisar resultados
    subs_com_potencia = []
    subs_zeradas = []
    subs_sem_transformador = []

    nome_col = 'NOM' if 'NOM' in subs.columns else 'NOME'

    for _, row in subs.iterrows():
        sub_id = row['COD_ID']
        nome = row[nome_col]
        
        if sub_id in potencia_por_sub:
            pot_total = potencia_por_sub[sub_id]
            if pot_total > 0:
                subs_com_potencia.append({'ID': sub_id, 'NOME': nome, 'POTENCIA': pot_total})
            else:
                subs_zeradas.append({'ID': sub_id, 'NOME': nome, 'POTENCIA': 0})
        else:
            subs_sem_transformador.append({'ID': sub_id, 'NOME': nome, 'POTENCIA': None})

    # 5. Relatório Final
    print("\n=== RELATÓRIO DE INVESTIGAÇÃO DE POTÊNCIA (LIGHT) ===")
    print(f"Total de Subestações: {len(subs)}")
    print(f"Subestações com Potência > 0: {len(subs_com_potencia)}")
    print(f"Subestações com Potência Zerada (tem transformador mas POT=0): {len(subs_zeradas)}")
    print(f"Subestações SEM Transformador vinculado: {len(subs_sem_transformador)}")
    print(f"Total de Subestações 'Vazias' (Transporte/Manobra): {len(subs_zeradas) + len(subs_sem_transformador)}")
    
    print("\nExemplos de Subestações sem carga (primeiras 20):")
    vazias = subs_zeradas + subs_sem_transformador
    df_vazias = pd.DataFrame(vazias)
    if not df_vazias.empty:
        print(df_vazias.head(20).to_string(index=False))

if __name__ == "__main__":
    investigar_potencia_light()
