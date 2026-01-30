import geopandas as gpd
import pandas as pd
import os

# Tente instalar se não tiver: pip install geobr
try:
    import geobr
    HAS_GEOBR = True
except ImportError:
    HAS_GEOBR = False

CAMINHO_GDB = r"LIGHT_382_2021-09-30_M10_20231218-2133.gdb"

def filtrar_apenas_municipio_rj():
    print("🏙️  INICIANDO FILTRO GEOGRÁFICO: MUNICÍPIO DO RIO DE JANEIRO...")

    # 1. Carregar o Dataset Integrado que acabamos de criar
    if not os.path.exists("dataset_ia_final_integrado.geojson"):
        print("❌ Arquivo base não encontrado.")
        return
    
    gdf = gpd.read_file("dataset_ia_final_integrado.geojson")

    # 2. Obter a borda do município do Rio de Janeiro
    print("🗺️  Obtendo limites do município do IBGE...")
    if HAS_GEOBR:
        # Busca o município do Rio pelo código IBGE 3304557
        mun = geobr.read_municipality(code_muni=3304557, year=2020)
    else:
        # Fallback caso não tenha geobr: você precisaria de um shapefile local
        print("⚠️ geobr não instalado. Usando filtro por coordenadas aproximadas (Bounding Box)...")
        # Bbox aproximada do Rio Capital
        gdf = gdf.cx[-43.79:-43.10, -23.08:-22.74]
        mun = None

    if mun is not None:
        mun = mun.to_crs(gdf.crs)
        # Filtro Espacial: Mantém apenas o que intersecta o município
        print("✂️  Recortando subestações fora da capital...")
        # Usamos o centroide para o filtro para evitar que áreas de borda sumam
        gdf_rj = gdf[gdf.geometry.centroid.within(mun.union_all())].copy()
    else:
        gdf_rj = gdf

    # 3. Novo Relatório apenas para a Capital
    zeradas_rj = gdf_rj[gdf_rj['POT_GERADA_KW'] == 0]
    
    print("\n" + "="*50)
    print(f"📊 RELATÓRIO FINAL: APENAS MUNICÍPIO DO RIO")
    print(f"📍 Subestações na Capital: {len(gdf_rj)}")
    print(f"✅ Com Geração Solar: {len(gdf_rj) - len(zeradas_rj)}")
    print(f"❌ Com Geração ZERADA: {len(zeradas_rj)}")
    print("="*50)

    if not zeradas_rj.empty:
        print("\n🔍 Subestações SEM Geração na Capital (Top 10):")
        print(zeradas_rj.sort_values(by='QTD_CLIENTES', ascending=False)[['COD_ID', 'QTD_CLIENTES', 'POT_NOM']].head(10))

    # Salvar o dataset purificado
    gdf_rj.to_file("dataset_ia_rio_capital.geojson", driver="GeoJSON")
    print("\n✅ Dataset 'dataset_ia_rio_capital.geojson' pronto e filtrado!")

if __name__ == "__main__":
    filtrar_apenas_municipio_rj()