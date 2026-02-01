"""
Este script realiza a classificação final e refinada de todas as subestações da LIGHT.
Ele utiliza a lógica de circuitos (CTMT) para identificar subestações satélites.

Categorias:
1. Distribuição Plena: Possui transformadores UNTRD alimentados por seus próprios circuitos.
2. Distribuição Satélite: Possui transformadores UNTRD alimentados por circuitos de OUTRA subestação.
3. Transformadora Pura: Possui transformadores UNTRS, mas não possui UNTRD.
4. Transporte/Manobra: Sem transformadores (UNTRD/UNTRS).
"""

import geopandas as gpd
import os
import pandas as pd

def classificar_final_v3_light():
    gdb_path = 'Dados Brutos/BDGD ANEEL/LIGHT_382_2021-09-30_M10_20231218-2133.gdb'
    
    print("DEBUG: Carregando dados para classificação final...")
    subs = gpd.read_file(gdb_path, layer='SUB')
    ctmt = gpd.read_file(gdb_path, layer='CTMT')
    untrd = gpd.read_file(gdb_path, layer='UNTRD')
    untrs = gpd.read_file(gdb_path, layer='UNTRS')
    
    # Mapeamentos base
    circuito_para_mae = ctmt.set_index('COD_ID')['SUB'].to_dict()
    subs_com_untrs = set(untrs['SUB'].unique())
    
    # Analisar cada subestação
    classificacoes = []
    nome_col = 'NOM' if 'NOM' in subs.columns else 'NOME'
    
    for _, row in subs.iterrows():
        sid = str(row['COD_ID']).strip()
        nome = row[nome_col]
        
        # Transformadores vinculados a esta subestação
        meus_untrd = untrd[untrd['SUB'] == sid]
        
        if not meus_untrd.empty:
            # Verificar de onde vem a energia desses transformadores
            circuitos_alimentadores = meus_untrd['CTMT'].unique()
            maes = {circuito_para_mae.get(str(c)) for c in circuitos_alimentadores if circuito_para_mae.get(str(c))}
            
            # Se todos os circuitos pertencem a ela mesma, é Plena.
            # Se algum circuito pertence a outra, é Satélite.
            if sid in maes and len(maes) == 1:
                cat = "1. Distribuição Plena"
            elif len(maes) > 0:
                cat = "2. Distribuição Satélite"
            else:
                # Caso raro: tem transformador mas o circuito não foi encontrado
                cat = "1. Distribuição Plena (Circuito não mapeado)"
        elif sid in subs_com_untrs:
            cat = "3. Transformadora Pura"
        else:
            cat = "4. Transporte/Manobra"
            
        classificacoes.append({
            'ID': sid,
            'NOME': nome,
            'CLASSIFICACAO': cat,
            'QTD_UNTRD': len(meus_untrd)
        })
        
    df_res = pd.DataFrame(classificacoes)
    
    print("\n=== CLASSIFICAÇÃO FINAL V3 (LÓGICA DE CIRCUITOS) - LIGHT ===")
    resumo = df_res['CLASSIFICACAO'].value_counts().sort_index()
    print(resumo.to_string())
    print(f"\nTOTAL: {len(df_res)}")
    
    # Mostrar exemplos de Satélites
    satelites = df_res[df_res['CLASSIFICACAO'] == "2. Distribuição Satélite"]
    if not satelites.empty:
        print("\nExemplos de Subestações Satélites (Carga de Terceiros):")
        print(satelites[['ID', 'NOME', 'QTD_UNTRD']].head(20).to_string(index=False))

    # Salvar resultado
    output = "investigacao/classificacao_final_v3_light.csv"
    df_res.to_csv(output, index=False, sep=';', encoding='utf-8-sig')
    print(f"\nDEBUG: Classificação final salva em {output}")

if __name__ == "__main__":
    classificar_final_v3_light()
