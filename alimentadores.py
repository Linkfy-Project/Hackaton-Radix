import geopandas as gpd
import pandas as pd
import os

CAMINHO_GDB = r"LIGHT_382_2021-09-30_M10_20231218-2133.gdb"

def consolidacao_mestre_final():
    print("💎 INICIANDO CONSOLIDAÇÃO MESTRE FINAL...")

    # 1. Carregar Dataset Base (o que criamos lá atrás com as 190 áreas)
    # Se não tiver o arquivo, ele tenta ler do passo anterior ou do GDB
    if not os.path.exists("dataset_ia_final_integrado.geojson"):
        print("❌ Arquivo 'dataset_ia_final_integrado.geojson' não encontrado. Rode o script de construção do dataset primeiro.")
        return
    
    gdf = gpd.read_file("dataset_ia_final_integrado.geojson")
    
    # 2. Carregar a Hierarquia (Mãe/Filha) que você descobriu
    if not os.path.exists("vinculos_maes_filhas.csv"):
        print("❌ Arquivo de hierarquia não encontrado. Rode o script de investigação 'Mãe/Filha' primeiro.")
        return
        
    df_hierarquia = pd.read_csv("vinculos_maes_filhas.csv")
    df_hierarquia['SUB_FILHA_ORFA'] = df_hierarquia['SUB_FILHA_ORFA'].astype(str)

    # 3. Aplicar Herança de Potência
    print("🧬 Aplicando herança de potência...")
    # Dicionário de potência das Mães
    dict_pot_maes = gdf[gdf['POT_NOM'] > 5.1].set_index('COD_ID')['POT_NOM'].to_dict()
    # Dicionário Filha -> Potência da Mãe
    dict_heranca = df_hierarquia.set_index('SUB_FILHA_ORFA')['SUB_MAE'].map(dict_pot_maes).to_dict()

    def ajustar_potencia(row):
        # Se for órfã (potência 5.0 ou NaN), tenta pegar da mãe
        if row['POT_NOM'] <= 5.1 and str(row['COD_ID']) in dict_heranca:
            return dict_heranca[str(row['COD_ID'])]
        return row['POT_NOM']

    gdf['POT_CONSOLIDADA'] = gdf.apply(ajustar_potencia, axis=1)

    # 4. Calcular Gargalos via SSDMT (Contagem de Alimentadores)
    print("📏 Calculando limites por alimentador (SSDMT)...")
    ssdmt = gpd.read_file(CAMINHO_GDB, layer='SSDMT', ignore_geometry=True, columns=['SUB', 'CTMT'])
    ssdmt['SUB'] = ssdmt['SUB'].astype(str).str.strip().str.replace('.0', '', regex=False)
    
    contagem_alim = ssdmt.groupby('SUB')['CTMT'].nunique().reset_index(name='QTD_ALIM')
    gdf = gdf.merge(contagem_alim, left_on='COD_ID', right_on='SUB', how='left').drop(columns=['SUB'])
    gdf['QTD_ALIM'] = gdf['QTD_ALIM'].fillna(1)

    # 5. Cálculo do Risco Real (Gargalo de 10MVA por alimentador)
    print("📈 Gerando métricas de risco...")
    gdf['CAPACIDADE_CABOS'] = gdf['QTD_ALIM'] * 10.0
    gdf['LIMITANTE_SISTEMA'] = gdf[['POT_CONSOLIDADA', 'CAPACIDADE_CABOS']].min(axis=1)
    
    # Risco: Geração / (Limite * 1000 p/ transformar MVA em kW)
    gdf['RISCO_PERCENTUAL'] = (gdf['POT_GERADA_KW'] / (gdf['LIMITANTE_SISTEMA'] * 1000)) * 100

    # 6. Salvar Versão Final
    gdf.to_file("DATASET_IA_RADIX_FINAL.geojson", driver="GeoJSON")
    print("\n" + "="*50)
    print("✅ DATASET FINAL CONCLUÍDO COM SUCESSO!")
    print(f"📊 Total de subestações processadas: {len(gdf)}")
    print(f"🔥 Áreas com Risco > 50%: {len(gdf[gdf['RISCO_PERCENTUAL'] > 50])}")
    print("="*50)

if __name__ == "__main__":
    consolidacao_mestre_final()