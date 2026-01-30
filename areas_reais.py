import geopandas as gpd
import pandas as pd
from shapely.ops import unary_union

CAMINHO_GDB = r"Dados Brutos\BDGD ANEEL\LIGHT_382_2021-09-30_M10_20231218-2133.gdb"

def gerar_areas_reais_untrd():
    print("🎯 GERANDO ÁREAS REAIS VIA CAMADA UNTRD...")
    
    # 1. Carregar os Transformadores (Pontos Geográficos)
    # Pegamos a coluna SUB para saber a qual subestação o ponto pertence
    print("📍 Carregando 95k pontos de transformadores...")
    gdf_untrd = gpd.read_file(CAMINHO_GDB, layer='UNTRD', columns=['SUB', 'geometry'])
    
    # Limpeza básica de IDs
    gdf_untrd['SUB'] = gdf_untrd['SUB'].astype(str).str.strip()
    
    # 2. Gerar polígonos por subestação
    print("📐 Calculando envoltórias (Convex Hull)...")
    poligonos_reais = []
    
    # Agrupamos os pontos por subestação e criamos o polígono que os envolve
    for sub_id, grupo in gdf_untrd.groupby('SUB'):
        if len(grupo) >= 3: # Precisa de 3 pontos para formar uma área
            # Convex Hull: a 'casca' que envolve todos os pontos daquela subestação
            area = grupo.unary_union.convex_hull
            poligonos_reais.append({'COD_ID': sub_id, 'geometry': area})

    # 3. Criar o GeoDataFrame com as áreas reais
    gdf_areas = gpd.GeoDataFrame(poligonos_reais, crs=gdf_untrd.crs)

    # 4. Cruzar com a Potência Nominal (UNTRS) que já validamos antes
    print("⚡ Cruzando com dados de potência...")
    df_untrs = gpd.read_file(CAMINHO_GDB, layer='UNTRS', ignore_geometry=True)[['SUB', 'POT_NOM']]
    df_untrs['SUB'] = df_untrs['SUB'].astype(str).str.strip()
    potencia = df_untrs.groupby('SUB')['POT_NOM'].sum().reset_index()
    
    df_final = gdf_areas.merge(potencia, left_on='COD_ID', right_on='SUB', how='left')
    
    # 5. Salvar o Ground Truth
    print(f"✅ Sucesso! Geradas {len(df_final)} áreas reais de atendimento.")
    df_final.to_file(r"Dados Processados\subestacoes_areas_reais.geojson", driver="GeoJSON")
    print("💾 Arquivo salvo: subestacoes_areas_reais.geojson")

if __name__ == "__main__":
    gerar_areas_reais_untrd()