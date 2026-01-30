import geopandas as gpd
import pandas as pd

CAMINHO_GDB = r"LIGHT_382_2021-09-30_M10_20231218-2133.gdb"

def mapear_barras_ons():
    print("🛰️  MAPEANDO INTERFACES ONS (138kV)...")
    
    # 1. Ler as Barras
    df_bar = gpd.read_file(CAMINHO_GDB, layer='BAR', ignore_geometry=True)
    
    # Filtrar TEN_NOM == 94 (Código ANEEL para 138kV)
    barras_138 = df_bar[df_bar['TEN_NOM'] == '94'].copy()
    
    # 2. Ler as Subestações para pegar os nomes (NOM)
    df_sub = gpd.read_file(CAMINHO_GDB, layer='SUB', ignore_geometry=True)
    df_sub['COD_ID'] = df_sub['COD_ID'].astype(str)
    
    # 3. Cruzar
    barras_138['SUB'] = barras_138['SUB'].astype(str)
    resultado = barras_138.merge(df_sub[['COD_ID', 'NOM']], left_on='SUB', right_on='COD_ID')
    
    print("\n" + "="*50)
    print("📍 BARRAS DE INTERFACE ONS DETECTADAS (138kV)")
    print("="*50)
    
    # Listar as TOP 10 Subestações que possuem barras de interface
    top_interfaces = resultado[['SUB', 'NOM', 'DESCR']].drop_duplicates()
    print(top_interfaces.to_string(index=False))
    
    top_interfaces.to_csv("lista_interfaces_ons.csv", index=False)
    print("\n✅ Tabela 'lista_interfaces_ons.csv' gerada.")

if __name__ == "__main__":
    mapear_barras_ons()