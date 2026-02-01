"""
Este script realiza a classificação final de todas as subestações da LIGHT.
Categorias:
1. Distribuição (Carga): Possui transformadores UNTRD.
2. Transformadora: Possui transformadores UNTRS, mas não UNTRD.
3. Transporte (Com Topologia): Sem transformadores, mas possui barras (BAR).
4. Transporte (Sem Topologia): Sem transformadores e sem barras (apenas geográfica).
"""

import geopandas as gpd
import os
import pandas as pd

def classificar_subestacoes_light():
    gdb_path = 'Dados Brutos/BDGD ANEEL/LIGHT_382_2021-09-30_M10_20231218-2133.gdb'
    
    subs = gpd.read_file(gdb_path, layer='SUB')
    bars = gpd.read_file(gdb_path, layer='BAR')
    untrd = gpd.read_file(gdb_path, layer='UNTRD')
    untrs = gpd.read_file(gdb_path, layer='UNTRS')
    
    subs_com_carga = set(untrd['SUB'].unique())
    subs_com_untrs = set(untrs['SUB'].unique())
    subs_com_barras = set(bars['SUB'].unique())
    
    classificacoes = []
    
    for _, row in subs.iterrows():
        sid = row['COD_ID']
        nome = row['NOM'] if 'NOM' in row else row['NOME']
        
        if sid in subs_com_carga:
            cat = "1. Distribuição (Carga)"
        elif sid in subs_com_untrs:
            cat = "2. Transformadora"
        elif sid in subs_com_barras:
            cat = "3. Transporte (Com Topologia)"
        else:
            cat = "4. Transporte (Sem Topologia)"
            
        classificacoes.append({
            'ID': sid,
            'NOME': nome,
            'CLASSIFICACAO': cat
        })
        
    df_res = pd.DataFrame(classificacoes)
    
    print("\n=== CLASSIFICAÇÃO FINAL DAS SUBESTAÇÕES (LIGHT) ===")
    resumo = df_res['CLASSIFICACAO'].value_counts().sort_index()
    print(resumo.to_string())
    print(f"\nTOTAL: {len(df_res)}")
    
    # Salvar resultado
    output = "investigacao/classificacao_final_light.csv"
    df_res.to_csv(output, index=False, sep=';', encoding='utf-8-sig')
    print(f"\nDEBUG: Classificação salva em {output}")

if __name__ == "__main__":
    classificar_subestacoes_light()
