"""
Script para ler o arquivo CNEFE_RJ.csv e imprimir as primeiras 1000 linhas
da coluna DSC_ESTABELECIMENTO que não estejam vazias.
"""

import pandas as pd

# --- CONFIGURAÇÕES ---
CAMINHO_CNEFE = "CNEFE_RJ.csv"

def print_establishments():
    print("DEBUG: Lendo DSC_ESTABELECIMENTO do CNEFE...")
    
    count = 0
    chunk_size = 100000
    
    try:
        # Lendo em chunks para ser eficiente com a memória
        for chunk in pd.read_csv(CAMINHO_CNEFE, sep=';', usecols=['DSC_ESTABELECIMENTO'], chunksize=chunk_size):
            # Filtrar linhas onde DSC_ESTABELECIMENTO não é nulo e não é vazio
            valid_rows = chunk[chunk['DSC_ESTABELECIMENTO'].notna() & (chunk['DSC_ESTABELECIMENTO'].str.strip() != "")]
            
            for val in valid_rows['DSC_ESTABELECIMENTO']:
                print(val)
                count += 1
                if count >= 1000:
                    break
            
            if count >= 1000:
                break
                
        print(f"\nDEBUG: Total de {count} nomes de estabelecimentos impressos.")
        
    except Exception as e:
        print(f"DEBUG ERROR: {e}")

if __name__ == "__main__":
    print_establishments()
