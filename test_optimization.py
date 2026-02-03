
import pandas as pd
import time
import os

path_ons_sub = "Dados Brutos/ONS/SUBESTACAO.csv"

def test_original():
    start = time.time()
    if os.path.exists(path_ons_sub):
        print("DEBUG: [Teste] Carregando base original...")
        df_ons = pd.read_csv(path_ons_sub, sep=';', encoding='latin1', usecols=['num_barra', 'nom_subestacao'])
        df_ons = df_ons.dropna(subset=['num_barra'])
        barra_para_ons = dict(zip(df_ons['num_barra'].astype(int).astype(str), df_ons['nom_subestacao']))
        print(f"DEBUG: [Teste] Original levou {time.time() - start:.4f}s")
        return len(barra_para_ons)
    return 0

def test_optimized():
    start = time.time()
    path_parquet = path_ons_sub.replace('.csv', '.parquet')
    
    if os.path.exists(path_parquet):
        print("DEBUG: [Teste] Carregando base Parquet...")
        df_ons = pd.read_parquet(path_parquet, columns=['num_barra', 'nom_subestacao'])
    else:
        print("DEBUG: [Teste] Carregando base original e convertendo para Parquet...")
        df_ons = pd.read_csv(path_ons_sub, sep=';', encoding='latin1', usecols=['num_barra', 'nom_subestacao'])
        df_ons.to_parquet(path_parquet)
    
    df_ons = df_ons.dropna(subset=['num_barra'])
    # Otimização: converter para string antes de criar o dict, ou manter como int se possível
    barra_para_ons = dict(zip(df_ons['num_barra'].astype(int).astype(str), df_ons['nom_subestacao']))
    print(f"DEBUG: [Teste] Otimizado levou {time.time() - start:.4f}s")
    return len(barra_para_ons)

if __name__ == "__main__":
    count1 = test_original()
    count2 = test_optimized()
    print(f"Resultados: {count1} vs {count2}")
