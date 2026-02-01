"""
Este script investiga a origem da energia (Alta Tensão) para as 82 subestações Plenas da LIGHT.
Ele busca identificar se elas são alimentadas por outras subestações ou por pontos externos (ONS).
"""

import geopandas as gpd
import os
import pandas as pd

def investigar_origem_plenas():
    gdb_path = 'Dados Brutos/BDGD ANEEL/LIGHT_382_2021-09-30_M10_20231218-2133.gdb'
    
    # 1. Carregar dados de classificação anterior para pegar as Plenas
    class_path = "investigacao/classificacao_final_v3_light.csv"
    if not os.path.exists(class_path):
        print("DEBUG: Execute o script de classificação final v3 primeiro.")
        return
    
    df_class = pd.read_csv(class_path, sep=';')
    plenas_ids = df_class[df_class['CLASSIFICACAO'].str.contains("Plena")]['ID'].astype(str).tolist()
    
    print(f"DEBUG: Analisando origem para {len(plenas_ids)} subestações Plenas...")
    
    # 2. Carregar camadas de rede
    subs = gpd.read_file(gdb_path, layer='SUB')
    bars = gpd.read_file(gdb_path, layer='BAR')
    ssdat = gpd.read_file(gdb_path, layer='SSDAT')
    
    nome_col = 'NOM' if 'NOM' in subs.columns else 'NOME'
    sub_names = subs.set_index('COD_ID')[nome_col].to_dict()
    pac_to_sub = bars.set_index('PAC')['SUB'].to_dict()
    
    # 3. Mapear conexões SSDAT (Alta Tensão)
    # Queremos ver para onde as linhas de AT que saem das Plenas estão indo
    origens = []
    
    for sid in plenas_ids:
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
            if not v1 and p1 not in meus_pacs: vizinhos.add(f"EXTERNO:{p1}")
            if not v2 and p2 not in meus_pacs: vizinhos.add(f"EXTERNO:{p2}")

        origens.append({
            'ID': sid,
            'NOME': sub_names.get(sid, sid),
            'VIZINHOS_AT': ", ".join([sub_names.get(v, v) for v in vizinhos]),
            'QTD_VIZINHOS': len(vizinhos)
        })

    df_origens = pd.DataFrame(origens)
    
    print("\n=== ORIGEM DA ENERGIA (ALTA TENSÃO) - SUBESTAÇÕES PLENAS ===")
    print(df_origens[['NOME', 'VIZINHOS_AT']].head(20).to_string(index=False))
    
    # Contar quantas têm vizinhos externos (possíveis pontos ONS)
    externos = df_origens[df_origens['VIZINHOS_AT'].str.contains("EXTERNO", na=False)]
    print(f"\nSubestações com possíveis conexões externas (ONS/Rede Básica): {len(externos)}")
    
    # Salvar
    output = "investigacao/origem_at_plenas.csv"
    df_origens.to_csv(output, index=False, sep=';', encoding='utf-8-sig')
    print(f"\nDEBUG: Relatório salvo em {output}")

if __name__ == "__main__":
    investigar_origem_plenas()
