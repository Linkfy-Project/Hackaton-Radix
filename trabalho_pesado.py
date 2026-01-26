import geopandas as gpd
import pandas as pd
import numpy as np

CAMINHO_GDB = r"LIGHT_382_2021-09-30_M10_20231218-2133.gdb"

def resgate_geometrico_final():
    print("🚀 INICIANDO RESGATE GEOMÉTRICO FINAL (FORÇA BRUTA)...")
    
    # 1. Carregar SUB (Geometria mestre)
    gdf_sub = gpd.read_file(CAMINHO_GDB, layer='SUB')[['COD_ID', 'NOM', 'geometry']]
    gdf_sub['COD_ID'] = gdf_sub['COD_ID'].astype(str).str.strip()

    # 2. Carregar UNTRS com Geometria (Onde você viu que tem POT_NOM)
    print("🌍 Carregando UNTRS com pontos geográficos...")
    gdf_untrs = gpd.read_file(CAMINHO_GDB, layer='UNTRS')
    
    # Garantir que POT_NOM é numérico
    gdf_untrs['POT_NOM'] = pd.to_numeric(gdf_untrs['POT_NOM'], errors='coerce').fillna(0)

    # 3. Garantir que os mapas estão alinhados (CRS)
    if gdf_untrs.crs != gdf_sub.crs:
        gdf_untrs = gdf_untrs.to_crs(gdf_sub.crs)

    # 4. Criar um pequeno Buffer (Margem de erro de 20 metros)
    # Às vezes o ponto do transformador está na calçada, fora do polígono da subestação
    print("📏 Aplicando margem de erro de 20m para captura...")
    gdf_sub_buffer = gdf_sub.copy()
    # Converter para metros para fazer buffer real, depois volta
    gdf_sub_buffer = gdf_sub_buffer.to_crs(epsg=31983) 
    gdf_sub_buffer['geometry'] = gdf_sub_buffer.buffer(20) 
    gdf_sub_buffer = gdf_sub_buffer.to_crs(gdf_sub.crs)

    # 5. Spatial Join: Capturar tudo que está dentro do "espaço" da subestação
    print("🔗 Cruzando pontos com polígonos...")
    # lsuffix='_untrs', rsuffix='_sub' para evitar o KeyError de antes
    gdf_join = gpd.sjoin(gdf_untrs, gdf_sub_buffer, how='inner', predicate='intersects', lsuffix='untrs', rsuffix='sub')

    # 6. Agrupar e Somar
    if not gdf_join.empty:
        # Usamos o COD_ID que veio da camada SUB (agora com sufixo _sub)
        cap_consolidada = gdf_join.groupby('COD_ID_sub')['POT_NOM'].sum().reset_index()
        cap_consolidada.rename(columns={'COD_ID_sub': 'COD_ID', 'POT_NOM': 'CAPACIDADE_REAL'}, inplace=True)
        
        # Merge final
        df_final = gdf_sub.merge(cap_consolidada, on='COD_ID', how='left')
    else:
        print("⚠️  AVISO: O Join Espacial não encontrou NADA. Verificando se UNTRS tem pontos...")
        print(f"Exemplo de geometria em UNTRS: {gdf_untrs['geometry'].head(1)}")
        df_final = gdf_sub.copy()
        df_final['CAPACIDADE_REAL'] = np.nan

    # --- TRATAMENTO DE SEGURANÇA PARA A IA ---
    encontrados = df_final['CAPACIDADE_REAL'].notna().sum()
    print(f"\n✅ RESULTADO: {encontrados} subestações mapeadas com sucesso.")

    # Se ainda houver vazios, vamos usar a média para não quebrar o Voronoi
    mediana_val = df_final['CAPACIDADE_REAL'].median()
    if np.isnan(mediana_val) or mediana_val == 0: mediana_val = 20.0
    
    print(f"🩹 Preenchendo {len(df_final) - encontrados} lacunas com a mediana: {mediana_val} MVA")
    df_final['CAPACIDADE_REAL'] = df_final['CAPACIDADE_REAL'].fillna(mediana_val)

    # Salvar para o próximo passo (IA / Voronoi)
    df_final.to_file("base_pronta_hackathon.geojson", driver="GeoJSON")
    print("💾 Arquivo 'base_pronta_hackathon.geojson' gerado!")

if __name__ == "__main__":
    resgate_geometrico_final()