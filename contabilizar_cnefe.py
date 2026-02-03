"""
Este script processa o arquivo CNEFE_RJ.csv para contabilizar o número total de residências
e estimar a população total do Rio de Janeiro com base nos dados do Censo 2022.
"""

import pandas as pd
import os

# --- CONFIGURAÇÕES ---
# Caminho do arquivo de entrada
FILE_PATH = "Dados Brutos/CNFE IBGE/CNEFE_RJ.csv"

# Média de moradores por domicílio no Rio de Janeiro (Censo 2022)
# Fonte: IBGE Censo 2022
MEDIA_MORADORES = 2.60

def contabilizar_cnefe(path: str) -> None:
    """
    Lê o arquivo CNEFE em chunks e realiza a contagem de residências e pessoas.
    
    Args:
        path (str): Caminho para o arquivo CSV do CNEFE.
    """
    print(f"DEBUG: Iniciando processamento do arquivo: {path}")
    
    # Verifica se o arquivo existe
    if not os.path.exists(path):
        print(f"DEBUG: Erro - Arquivo não encontrado em {path}")
        return

    total_residencias = 0
    total_pessoas_estimadas = 0
    total_linhas = 0
    
    # Definindo o tamanho do chunk para não estourar a memória (500k linhas por vez)
    chunk_size = 500000
    
    try:
        # Usamos apenas as colunas necessárias para economizar memória
        # COD_ESPECIE: 1=Particular, 2=Coletivo
        # COD_TIPO_ESPECI: 101=Ocupado, 104=Coletivo com morador
        cols = ['COD_ESPECIE', 'COD_TIPO_ESPECI']
        
        print(f"DEBUG: Lendo arquivo em chunks de {chunk_size} linhas...")
        
        # Iteração em chunks para eficiência de memória
        for i, chunk in enumerate(pd.read_csv(path, sep=';', usecols=cols, chunksize=chunk_size, low_memory=False)):
            # Contabiliza residências (Espécie 1 ou 2)
            # 1: Domicílio particular
            # 2: Domicílio coletivo
            residencias_chunk = chunk[chunk['COD_ESPECIE'].isin([1, 2])]
            total_residencias += len(residencias_chunk)
            
            # Contabiliza pessoas estimadas (Apenas nos domicílios ocupados)
            # COD_TIPO_ESPECI 101 = Domicílio particular ocupado
            # COD_TIPO_ESPECI 104 = Domicílio coletivo com morador
            ocupados = chunk[chunk['COD_TIPO_ESPECI'].isin([101, 104])]
            total_pessoas_estimadas += len(ocupados) * MEDIA_MORADORES
            
            total_linhas += len(chunk)
            
            # Log de progresso a cada 1 milhão de linhas
            if (i + 1) % 2 == 0:
                print(f"DEBUG: Processadas {total_linhas:,} linhas... (Residências: {total_residencias:,})")

        # Impressão dos resultados finais
        print("\n" + "="*50)
        print("      RESULTADOS DA CONTABILIZAÇÃO - RIO DE JANEIRO")
        print("="*50)
        print(f"Total de endereços processados:    {total_linhas:>15,}")
        print(f"Total de residências encontradas:  {total_residencias:>15,}")
        print(f"Total estimado de pessoas:         {int(total_pessoas_estimadas):>15,}")
        print("="*50)
        print(f"Nota: A estimativa de pessoas utiliza a média de {MEDIA_MORADORES} moradores")
        print("por domicílio ocupado, conforme dados do Censo 2022 para o RJ.")
        print("="*50)
        
    except Exception as e:
        print(f"DEBUG: Ocorreu um erro durante o processamento: {e}")

if __name__ == "__main__":
    # Executa a função principal
    contabilizar_cnefe(FILE_PATH)
