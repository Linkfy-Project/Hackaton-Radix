"""
Este script investiga a origem da energia (Alta Tensão) para as subestações da ENEL.
Ele busca identificar se elas são alimentadas por outras subestações ou por pontos externos (ONS).
"""

import geopandas as gpd
import os
import pandas as pd

def investigar_origem_enel():
    gdb_path = 'Dados Brutos/BDGD ANEEL/ENEL_RJ_383_2022-09-30_V10_20240605-0611.gdb'
    
    print(f"DEBUG: Analisando origem para subestações ENEL...")
    
    # 1. Carregar camadas de rede
    subs = gpd.read_file(gdb_path, layer='SUB')
    bars = gpd.read_file(gdb_path, layer='BAR')
    ssdat = gpd.read_file(gdb_path, layer='SSDAT')
    
    nome_col = 'NOME'
    sub_names = subs.set_index('COD_ID')[nome_col].to_dict()
    pac_to_sub = bars.set_index('PAC')['SUB'].to_dict()
    
    # 2. Mapear conexões SSDAT (Alta Tensão)
    origens = []
    
    for sid in subs['COD_ID'].unique():
        # Encontrar PACs desta subestação
        meus_pacs = set(bars[bars['SUB'] == sid]['PAC'].unique())
        
        # Encontrar segmentos SSDAT conectados a esses PACs
        conexoes_at = ssdat[(ssdat['PAC_1'].isin(meus_pacs)) | (ssdat['PAC_2'].isin(meus_pacs))]
        
        vizinhos = set()
        for _, row in conexoes_at.iterrows():
            p1, p2 = str(row['PAC_1']), str(row['PAC_2'])
            v1 = pac_to_sub.get(p1)
            v2 = pac_to_sub.get(p2)
            
            if v1 and v1 != sid: vizinhos.add(v1)
            if v2 and v2 != sid: vizinhos.add(v2)
            
            # Se um dos PACs não pertence a nenhuma subestação no GDB, pode ser um ponto ONS
            if not v1 and p1 not in meus_pacs and p1 != 'None': vizinhos.add(f"EXTERNO:{p1}")
            if not v2 and p2 not in meus_pacs and p2 != 'None': vizinhos.add(f"EXTERNO:{p2}")

        if vizinhos:
            origens.append({
                'ID': sid,
                'NOME': sub_names.get(sid, sid),
                'VIZINHOS_AT': ", ".join([sub_names.get(v, v) for v in vizinhos]),
                'QTD_VIZINHOS': len(vizinhos)
            })

    df_origens = pd.DataFrame(origens)
    
    # Filtrar apenas as que têm conexões externas
    externos = df_origens[df_origens['VIZINHOS_AT'].str.contains("EXTERNO", na=False)]
    print(f"\nSubestações ENEL com possíveis conexões externas (ONS/Rede Básica): {len(externos)}")
    print(externos[['NOME', 'VIZINHOS_AT']].head(20).to_string(index=False))
    
    # Salvar
    output = "investigacao/origem_at_enel.csv"
    df_origens.to_csv(output, index=False, sep=';', encoding='utf-8-sig')
    print(f"\nDEBUG: Relatório salvo em {output}")

if __name__ == "__main__":
    investigar_origem_enel()
