"""
Este script mapeia a hierarquia e conectividade entre subestações (Mãe-Filha).
Ele identifica quais subestações de transporte são alimentadas por quais subestações de carga,
utilizando a topologia da rede de Alta Tensão (SSDAT).
"""

import geopandas as gpd
import os
import pandas as pd

def gerar_relatorio_hierarquia():
    # DEBUG: Iniciando o mapeamento da hierarquia de subestações
    gdbs = {
        'ENEL': 'Dados Brutos/BDGD ANEEL/ENEL_RJ_383_2022-09-30_V10_20240605-0611.gdb',
        'LIGHT': 'Dados Brutos/BDGD ANEEL/LIGHT_382_2021-09-30_M10_20231218-2133.gdb'
    }
    
    target_crs = "EPSG:31983" # SIRGAS 2000 / UTM zone 23S
    relatorios = []

    for dist, path in gdbs.items():
        if not os.path.exists(path): 
            print(f"DEBUG: Arquivo não encontrado: {path}")
            continue
        
        print(f"DEBUG: Processando hierarquia para {dist}...")
        
        # 1. Carregar camadas necessárias do GDB
        subs = gpd.read_file(path, layer='SUB')
        bars = gpd.read_file(path, layer='BAR')
        ssdat = gpd.read_file(path, layer='SSDAT')
        
        # Normalizar nomes das subestações para o mapeamento
        nome_col = 'NOM' if 'NOM' in subs.columns else 'NOME'
        sub_names = subs.set_index('COD_ID')[nome_col].to_dict()
        
        # 2. Mapear PAC (Ponto de Acoplamento Comum) para Subestação
        pac_to_sub = bars.set_index('PAC')['SUB'].to_dict()
        
        # 3. Analisar conexões topológicas (via PAC nos segmentos de AT)
        conexoes = []
        for _, row in ssdat.iterrows():
            s1 = pac_to_sub.get(row['PAC_1'])
            s2 = pac_to_sub.get(row['PAC_2'])
            
            # Se ambos os PACs pertencem a subestações diferentes, temos um vínculo
            if s1 and s2 and s1 != s2:
                conexoes.append({'SUB_MAE': s1, 'SUB_FILHA': s2})
        
        # 4. Analisar conexões geográficas (fallback para quando o PAC não está na barra)
        subs_proj = subs.to_crs(target_crs)
        ssdat_proj = ssdat.to_crs(target_crs)
        subs_buffer = subs_proj.copy()
        subs_buffer['geometry'] = subs_proj.geometry.buffer(15) # 15 metros de tolerância para interseção
        
        # Join espacial entre linhas de AT e buffers das subestações
        spatial_join = gpd.sjoin(ssdat_proj, subs_buffer[['COD_ID', 'geometry']], how='inner', predicate='intersects')
        segmento_subs = spatial_join.groupby('COD_ID_left')['COD_ID_right'].unique()
        
        for sub_ids in segmento_subs:
            if len(sub_ids) > 1:
                # Se um segmento toca mais de uma subestação, mapeia a relação entre elas
                for i in range(len(sub_ids)):
                    for j in range(i + 1, len(sub_ids)):
                        conexoes.append({'SUB_MAE': sub_ids[i], 'SUB_FILHA': sub_ids[j]})
        
        if conexoes:
            df_con = pd.DataFrame(conexoes).drop_duplicates()
            df_con['DISTRIBUIDORA'] = dist
            df_con['MAE'] = df_con['SUB_MAE'].map(sub_names)
            df_con['FILHA'] = df_con['SUB_FILHA'].map(sub_names)
            relatorios.append(df_con)

    if relatorios:
        df_final = pd.concat(relatorios).drop_duplicates()
        output_path = "Organizar/vinculos_maes_filhas.csv"
        df_final.to_csv(output_path, index=False, sep=';', encoding='utf-8-sig')
        
        print(f"\nDEBUG: Relatório de hierarquia salvo em: {output_path}")
        print(f"DEBUG: Total de conexões únicas encontradas: {len(df_final)}")
        print("\n--- LISTA COMPLETA DE CONEXÕES (MÃE -> FILHA) ---")
        # Configura o pandas para mostrar todas as linhas no print
        pd.set_option('display.max_rows', None)
        print(df_final[['DISTRIBUIDORA', 'MAE', 'FILHA']].sort_values(['DISTRIBUIDORA', 'MAE']).to_string(index=False))
        # Reseta a configuração para o padrão
        pd.reset_option('display.max_rows')
    else:
        print("DEBUG: Nenhuma conexão hierárquica detectada.")

if __name__ == "__main__":
    gerar_relatorio_hierarquia()
