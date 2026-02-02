"""
Este script mapeia a hierarquia entre subestações (Mãe-Filha) usando a topologia dos circuitos (CTMT).
Ele identifica casos onde transformadores de rua pertencem geograficamente a uma subestação (Filha),
mas são alimentados eletricamente por um circuito que nasce em outra subestação (Mãe).
"""

import geopandas as gpd
import os
import pandas as pd

def mapear_hierarquia_via_ctmt():
    gdb_path = 'Dados Brutos/BDGD ANEEL/LIGHT_382_2021-09-30_M10_20231218-2133.gdb'
    
    if not os.path.exists(gdb_path):
        print("DEBUG: GDB da LIGHT não encontrado.")
        return

    print("DEBUG: Carregando dados para mapeamento via circuitos...")
    
    # 1. Carregar Subestações (para nomes)
    subs = gpd.read_file(gdb_path, layer='SUB')
    nome_col = 'NOM' if 'NOM' in subs.columns else 'NOME'
    sub_names = subs.set_index('COD_ID')[nome_col].to_dict()
    
    # 2. Carregar Circuitos (CTMT) - Define quem é a MÃE de cada circuito
    ctmt = gpd.read_file(gdb_path, layer='CTMT')
    circuito_para_mae = ctmt.set_index('COD_ID')['SUB'].to_dict()
    
    # 3. Carregar Transformadores (UNTRD) - Define quem é a FILHA e qual o CIRCUITO
    untrd = gpd.read_file(gdb_path, layer='UNTRD')
    
    print("DEBUG: Analisando vínculos entre transformadores, circuitos e subestações...")
    
    vinc_list = []
    for _, row in untrd.iterrows():
        sub_filha = str(row['SUB']).strip()
        id_circuito = str(row['CTMT']).strip()
        
        sub_mae = circuito_para_mae.get(id_circuito)
        
        if sub_mae and sub_mae != sub_filha:
            vinc_list.append({
                'SUB_MAE': sub_mae,
                'SUB_FILHA': sub_filha,
                'CIRCUITO': id_circuito
            })
            
    if not vinc_list:
        print("DEBUG: Nenhuma hierarquia via circuito encontrada.")
        return

    # 4. Consolidar resultados únicos
    df_vinc = pd.DataFrame(vinc_list).drop_duplicates(subset=['SUB_MAE', 'SUB_FILHA'])
    
    # Adicionar nomes
    df_vinc['MAE'] = df_vinc['SUB_MAE'].map(sub_names)
    df_vinc['FILHA'] = df_vinc['SUB_FILHA'].map(sub_names)
    
    # Salvar resultado
    output_path = "Organizar/hierarquia_via_circuitos.csv"
    df_vinc.to_csv(output_path, index=False, sep=';', encoding='utf-8-sig')
    
    print(f"\nDEBUG: Relatório de hierarquia via CTMT salvo em: {output_path}")
    print(f"DEBUG: Total de conexões únicas (Mãe -> Filha) encontradas: {len(df_vinc)}")
    
    print("\n--- LISTA DE HIERARQUIA DETECTADA (VIA CIRCUITOS) ---")
    pd.set_option('display.max_rows', None)
    print(df_vinc[['MAE', 'FILHA', 'CIRCUITO']].sort_values('MAE').to_string(index=False))
    pd.reset_option('display.max_rows')

if __name__ == "__main__":
    mapear_hierarquia_via_ctmt()
