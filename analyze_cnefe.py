"""
Este script lê o arquivo CNEFE_RJ.csv para analisar sua estrutura,
obter uma amostra dos dados e explicar as colunas presentes.
"""

import pandas as pd

# Nome do arquivo de entrada
FILE_PATH = "CNEFE_RJ.csv"

def analyze_cnefe(file_path: str) -> None:
    """
    Lê o arquivo CSV e imprime informações sobre a estrutura e uma amostra.
    """
    print(f"DEBUG: Iniciando leitura do arquivo {file_path}")
    
    try:
        # Lendo apenas as primeiras 100 linhas para análise rápida
        # O separador identificado no 'head' foi ';'
        df = pd.read_csv(file_path, sep=';', nrows=100)
        
        print("DEBUG: Arquivo lido com sucesso.")
        
        # Informações gerais
        print("\n--- Informações Gerais ---")
        print(df.info())
        
        # Amostra dos dados
        print("\n--- Amostra dos Dados (Primeiras 5 linhas) ---")
        print(df.head())
        
        # Contagem de espécies para entender a distribuição
        print("\n--- Distribuição por COD_ESPECIE ---")
        print(df['COD_ESPECIE'].value_counts())
        
        # Verificando a coluna COD_TIPO_ESPECI
        print("\n--- Distribuição por COD_TIPO_ESPECI ---")
        print(df['COD_TIPO_ESPECI'].value_counts())
        
        # Colunas presentes
        print("\n--- Colunas Identificadas ---")
        for col in df.columns:
            print(f"- {col}")
            
    except Exception as e:
        print(f"DEBUG: Erro ao ler o arquivo: {e}")

if __name__ == "__main__":
    analyze_cnefe(FILE_PATH)
