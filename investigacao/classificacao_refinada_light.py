"""
Este script realiza a classificação refinada de todas as subestações da LIGHT,
distinguindo entre subestações com carga real e subestações com transformadores de potência zero.
"""

import geopandas as gpd
import os
import pandas as pd

def classificar_refinado_light():
    gdb_path = 'Dados Brutos/BDGD ANEEL/LIGHT_382_2021-09-30_M10_20231218-2133.gdb'
    
    subs = gpd.read_file(gdb_path, layer='SUB')
    bars = gpd.read_file(gdb_path, layer='BAR')
    untrd = gpd.read_file(gdb_path, layer='UNTRD')
    untrs = gpd.read_file(gdb_path, layer='UNTRS')
    
    # Calcular potência por subestação em UNTRD
    pot_untrd = untrd.groupby('SUB')['POT_NOM'].sum().to_dict()
    subs_com_untrd = set(untrd['SUB'].unique())
    
    # Calcular potência por subestação em UNTRS
    pot_untrs = untrs.groupby('SUB')['POT_NOM'].sum().to_dict()
    subs_com_untrs = set(untrs['SUB'].unique())
    
    subs_com_barras = set(bars['SUB'].unique())
    
    classificacoes = []
    
    for _, row in subs.iterrows():
        sid = row['COD_ID']
        nome = row['NOM'] if 'NOM' in row else row['NOME']
        
        p_untrd = pot_untrd.get(sid, 0)
        p_untrs = pot_untrs.get(sid, 0)
        
        if sid in subs_com_untrd:
            if p_untrd > 0:
                cat = "1a. Distribuição (Carga Real > 0)"
            else:
                cat = "1b. Distribuição (Carga Zerada)"
        elif sid in subs_com_untrs:
            if p_untrs > 0:
                cat = "2a. Transformadora (Potência > 0)"
            else:
                cat = "2b. Transformadora (Potência Zerada)"
        elif sid in subs_com_barras:
            cat = "3. Transporte (Com Topologia)"
        else:
            cat = "4. Transporte (Sem Topologia)"
            
        classificacoes.append({
            'ID': sid,
            'NOME': nome,
            'CLASSIFICACAO': cat,
            'POT_UNTRD': p_untrd,
            'POT_UNTRS': p_untrs
        })
        
    df_res = pd.DataFrame(classificacoes)
    
    print("\n=== CLASSIFICAÇÃO REFINADA DAS SUBESTAÇÕES (LIGHT) ===")
    resumo = df_res['CLASSIFICACAO'].value_counts().sort_index()
    print(resumo.to_string())
    print(f"\nTOTAL: {len(df_res)}")
    
    # Mostrar exemplos de carga zerada
    zeradas = df_res[df_res['CLASSIFICACAO'] == "1b. Distribuição (Carga Zerada)"]
    if not zeradas.empty:
        print("\nExemplos de Subestações com UNTRD mas Potência Total Zerada:")
        print(zeradas[['ID', 'NOME', 'POT_UNTRD']].head(10).to_string(index=False))

    # Salvar resultado
    output = "investigacao/classificacao_refinada_light.csv"
    df_res.to_csv(output, index=False, sep=';', encoding='utf-8-sig')
    print(f"\nDEBUG: Classificação refinada salva em {output}")

if __name__ == "__main__":
    classificar_refinado_light()
