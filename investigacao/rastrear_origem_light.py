"""
Este script rastreia a origem da energia para as subestações da LIGHT,
focando especialmente nas 93 subestações identificadas como "vazias".
Ele utiliza as camadas SUB, BAR, SSDAT e UNTRS para mapear a conectividade.
"""

import geopandas as gpd
import os
import pandas as pd

def rastrear_origem_light():
    gdb_path = 'Dados Brutos/BDGD ANEEL/LIGHT_382_2021-09-30_M10_20231218-2133.gdb'
    
    if not os.path.exists(gdb_path):
        print(f"DEBUG: GDB da LIGHT não encontrado.")
        return

    print(f"DEBUG: Carregando camadas para rastreamento...")
    subs = gpd.read_file(gdb_path, layer='SUB')
    bars = gpd.read_file(gdb_path, layer='BAR')
    ssdat = gpd.read_file(gdb_path, layer='SSDAT')
    untrs = gpd.read_file(gdb_path, layer='UNTRS')
    
    # Mapeamento de nomes
    nome_col = 'NOM' if 'NOM' in subs.columns else 'NOME'
    sub_names = subs.set_index('COD_ID')[nome_col].to_dict()
    
    # 1. Identificar subestações com transformadores de subestação (UNTRS)
    # Isso nos diz quais subestações realmente processam energia (mesmo que não distribuam diretamente)
    pot_untrs = untrs.groupby('SUB')['POT_NOM'].sum().to_dict()
    
    # 2. Mapear PACs para Subestações (via BAR)
    pac_to_sub = bars.set_index('PAC')['SUB'].to_dict()
    
    # 3. Construir o grafo de conexões SSDAT
    # Queremos saber, para cada subestação, quais outras subestações estão conectadas a ela via SSDAT
    conexoes = []
    for _, row in ssdat.iterrows():
        s1 = pac_to_sub.get(row['PAC_1'])
        s2 = pac_to_sub.get(row['PAC_2'])
        
        if s1 and s2 and s1 != s2:
            conexoes.append((s1, s2))
            conexoes.append((s2, s1)) # Grafo não direcionado inicialmente

    df_graph = pd.DataFrame(conexoes, columns=['DE', 'PARA']).drop_duplicates()

    # 4. Analisar as 93 subestações "vazias" (sem UNTRD - do script anterior)
    # Vamos carregar a lista de transformadores de distribuição (UNTRD) para marcar quem tem carga
    untrd = gpd.read_file(gdb_path, layer='UNTRD')
    subs_com_carga = set(untrd['SUB'].unique())
    
    resultados = []
    for _, row in subs.iterrows():
        sid = row['COD_ID']
        nome = row[nome_col]
        
        tem_carga = sid in subs_com_carga
        tem_untrs = sid in pot_untrs
        pot_t = pot_untrs.get(sid, 0)
        
        # Buscar vizinhos no grafo SSDAT
        vizinhos = df_graph[df_graph['DE'] == sid]['PARA'].unique()
        vizinhos_nomes = [sub_names.get(v, v) for v in vizinhos]
        
        status = "CARGA" if tem_carga else ("TRANSFORMADORA" if tem_untrs else "TRANSPORTE/VAZIA")
        
        resultados.append({
            'ID': sid,
            'NOME': nome,
            'STATUS': status,
            'POT_UNTRS': pot_t,
            'VIZINHOS': ", ".join(vizinhos_nomes),
            'QTD_VIZINHOS': len(vizinhos)
        })

    df_res = pd.DataFrame(resultados)
    
    print("\n=== ANÁLISE DE CONECTIVIDADE E ORIGEM (LIGHT) ===")
    print(f"Total de Subestações: {len(df_res)}")
    print(df_res['STATUS'].value_counts().to_string())
    
    print("\n--- FOCO NAS SUBESTAÇÕES DE TRANSPORTE/VAZIAS (SEM CARGA E SEM UNTRS) ---")
    vazias_reais = df_res[df_res['STATUS'] == "TRANSPORTE/VAZIA"]
    print(f"Total de subestações puramente de transporte: {len(vazias_reais)}")
    
    # Mostrar de onde vem a energia (vizinhos) para as primeiras 30 vazias
    print("\nRastreamento de vizinhos para subestações de transporte:")
    print(vazias_reais[['NOME', 'VIZINHOS']].head(30).to_string(index=False))

    # Salvar para análise detalhada
    output = "investigacao/rastreamento_origem_light.csv"
    df_res.to_csv(output, index=False, sep=';', encoding='utf-8-sig')
    print(f"\nDEBUG: Análise completa salva em {output}")

if __name__ == "__main__":
    rastrear_origem_light()
