"""
Este script investiga a camada BAR (Barramentos) da LIGHT,
focando nas barras de interface (TI=34).
"""

import geopandas as gpd
import os
import pandas as pd

def investigar_barras_interface():
    gdb_path = 'Dados Brutos/BDGD ANEEL/LIGHT_382_2021-09-30_M10_20231218-2133.gdb'
    
    print("DEBUG: Carregando camada BAR...")
    bars = gpd.read_file(gdb_path, layer='BAR')
    
    # Filtrar por TI (Tipo de Instalação)
    # TI=33: Subestação
    # TI=34: Interface
    
    print("\n=== DISTRIBUIÇÃO DE TIPOS DE BARRAMENTO (TI) ===")
    print(bars['TI'].value_counts().to_string())
    
    interface_bars = bars[bars['TI'] == '34']
    
    print(f"\nTotal de Barras de Interface (TI=34): {len(interface_bars)}")
    
    if not interface_bars.empty:
        print("\nExemplos de Barras de Interface:")
        # Colunas interessantes: COD_ID, SUB, PAC, TEN_NOM, DESCR
        print(interface_bars[['COD_ID', 'SUB', 'PAC', 'TEN_NOM', 'DESCR']].head(20).to_string(index=False))
        
        # Verificar a quais subestações elas pertencem
        subs_com_interface = interface_bars['SUB'].unique()
        print(f"\nQuantidade de subestações que possuem barras de interface: {len(subs_com_interface)}")
        
        # Salvar para análise
        output = "investigacao/barras_interface_light.csv"
        interface_bars.to_csv(output, index=False, sep=';', encoding='utf-8-sig')
        print(f"\nDEBUG: Relatório salvo em {output}")
    else:
        print("\nNenhuma barra de interface (TI=34) encontrada no GDB da LIGHT.")

if __name__ == "__main__":
    investigar_barras_interface()
