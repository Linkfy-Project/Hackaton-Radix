"""
Este script verifica a similaridade entre dois arquivos CSV (DataRecords.csv e LINHA_TRANSMISSAO.csv).
Ele compara colunas, tipos de dados e tenta encontrar interseções de valores em colunas chave.
"""

import pandas as pd
import os

# Configurações de caminhos
FILE1 = r"Dados Brutos/ONS/DataRecords.csv"
FILE2 = r"Dados Brutos/ONS/LINHA_TRANSMISSAO.csv"

def verificar_similaridade():
    print(f"DEBUG: Iniciando verificação de similaridade entre {FILE1} e {FILE2}")
    
    # Verificar se os arquivos existem
    if not os.path.exists(FILE1) or not os.path.exists(FILE2):
        print("DEBUG: Um ou ambos os arquivos não foram encontrados.")
        return

    # Carregar os arquivos
    # Usando sep=';' e encoding='utf-8-sig' para lidar com BOM se presente
    try:
        df1 = pd.read_csv(FILE1, sep=';', encoding='utf-8-sig', low_memory=False)
        df2 = pd.read_csv(FILE2, sep=';', encoding='utf-8-sig', low_memory=False)
    except Exception as e:
        print(f"DEBUG: Erro ao carregar arquivos: {e}")
        return

    print(f"DEBUG: Arquivo 1 carregado com {df1.shape[0]} linhas e {df1.shape[1]} colunas.")
    print(f"DEBUG: Arquivo 2 carregado com {df2.shape[0]} linhas e {df2.shape[1]} colunas.")

    # 1. Comparação de Colunas
    cols1 = set(df1.columns)
    cols2 = set(df2.columns)
    common_cols = cols1.intersection(cols2)
    
    print("\n--- Comparação de Colunas ---")
    print(f"Colunas em comum: {len(common_cols)}")
    if common_cols:
        print(f"Nomes das colunas em comum: {common_cols}")
    else:
        print("Nenhuma coluna tem o nome idêntico.")

    # 2. Busca por similaridade de conteúdo em colunas que parecem IDs ou Nomes
    # Vamos normalizar os nomes das colunas para facilitar a busca manual de correspondências
    print("\n--- Análise de Conteúdo ---")
    
    # Colunas candidatas para ID/Nome no df1
    candidates1 = ['Id do Equipamento', 'Nome', 'cod_equipamento', 'nom_linhadetransmissao']
    # Colunas candidatas para ID/Nome no df2
    candidates2 = ['cod_equipamento', 'nom_linhadetransmissao', 'Id do Equipamento', 'Nome']

    found_candidates1 = [c for c in candidates1 if c in df1.columns]
    found_candidates2 = [c for c in candidates2 if c in df2.columns]

    if found_candidates1 and found_candidates2:
        for c1 in found_candidates1:
            for c2 in found_candidates2:
                # Limpar espaços e converter para string para comparação
                vals1 = set(df1[c1].astype(str).str.strip().unique())
                vals2 = set(df2[c2].astype(str).str.strip().unique())
                
                intersection = vals1.intersection(vals2)
                if intersection:
                    similarity = (len(intersection) / min(len(vals1), len(vals2))) * 100
                    print(f"Interseção encontrada entre '{c1}' (Arq 1) e '{c2}' (Arq 2):")
                    print(f"  - Valores em comum: {len(intersection)}")
                    print(f"  - Similaridade (baseada em valores únicos): {similarity:.2f}%")
                    if len(intersection) > 0:
                        print(f"  - Exemplo de valores em comum: {list(intersection)[:5]}")
    else:
        print("Não foram encontradas colunas óbvias de ID/Nome para comparação direta automática.")
        print(f"Colunas Arq 1: {list(df1.columns)[:10]}...")
        print(f"Colunas Arq 2: {list(df2.columns)[:10]}...")

    # 3. Verificação de valores de exemplo para ver se os dados "parecem" os mesmos
    print("\n--- Amostra de Dados ---")
    print("Arq 1 (primeira linha):")
    print(df1.iloc[0].to_dict())
    print("\nArq 2 (primeira linha):")
    print(df2.iloc[0].to_dict())

if __name__ == "__main__":
    verificar_similaridade()
