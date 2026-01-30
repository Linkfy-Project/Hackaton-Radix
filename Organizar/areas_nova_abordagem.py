import geopandas as gpd
import pandas as pd
import random
from shapely.geometry import Point

CAMINHO_GDB = r"LIGHT_382_2021-09-30_M10_20231218-2133.gdb"

def gerar_kml_final_rj():
    print("🚀 INICIANDO PROCESSAMENTO FINAL PARA GOOGLE EARTH...")
    
    # 1. Carregar as áreas reais (GeoJSON gerado anteriormente)
    gdf = gpd.read_file("subestacoes_areas_reais.geojson")
    
    # 2. RESOLVER CONFLITOS (Limpeza de Sobreposição)
    # Prioridade para quem tem mais potência nominal
    gdf = gdf.sort_values(by='POT_NOM', ascending=False)
    areas_limpas = []
    geometria_acumulada = None
    
    print("✂️ Recortando sobreposições territoriais...")
    for _, row in gdf.iterrows():
        geom_atual = row.geometry
        if geometria_acumulada is None:
            areas_limpas.append(row)
            geometria_acumulada = geom_atual
        else:
            geom_recortada = geom_atual.difference(geometria_acumulada)
            if not geom_recortada.is_empty:
                row.geometry = geom_recortada
                areas_limpas.append(row)
                geometria_acumulada = geometria_acumulada.union(geom_atual)
    
    gdf_final = gpd.GeoDataFrame(areas_limpas, crs=gdf.crs).to_crs(epsg=4326)

    # 3. CRIAR PONTOS DAS SUBESTAÇÕES
    # Vamos pegar as coordenadas originais da camada SUB para marcar o ponto central
    print("📍 Mapeando localizações das subestações...")
    gdf_sub_pontos = gpd.read_file(CAMINHO_GDB, layer='SUB')[['COD_ID', 'NOM', 'geometry']]
    gdf_sub_pontos['geometry'] = gdf_sub_pontos.geometry.centroid # Garante que é um ponto
    gdf_sub_pontos = gdf_sub_pontos.to_crs(epsg=4326)

    # 4. EXPORTAR PARA KML COM ESTILIZAÇÃO
    # Para o KML ficar colorido e bonito, vamos criar uma função de estilo
    import simplekml
    kml = simplekml.Kml()

    # Cores aleatórias semitransparentes (formato AABBGGRR)
    def random_kml_color():
        return simplekml.Color.changealpha("99", simplekml.Color.randomcolor())

    print("🎨 Colorindo polígonos e gerando ícones...")
    for _, row in gdf_final.iterrows():
        # Adicionar o Polígono da Área
        pol = kml.newpolygon(name=f"Área: {row['COD_ID']}", 
                             geometry=row.geometry)
        pol.style.polystyle.color = random_kml_color()
        pol.style.linestyle.width = 2
        pol.description = f"Potência: {row['POT_NOM']} MVA"

        # Adicionar o Ponto da Subestação correspondente
        ponto_row = gdf_sub_pontos[gdf_sub_pontos['COD_ID'].astype(str) == str(row['COD_ID'])]
        if not ponto_row.empty:
            pnt = kml.newpoint(name=f"Subestação {ponto_row.iloc[0]['NOM']}", 
                               coords=[(ponto_row.iloc[0].geometry.x, ponto_row.iloc[0].geometry.y)])
            pnt.style.iconstyle.icon.href = 'http://maps.google.com/mapfiles/kml/paddle/red-stars.png'

    # 5. BORDAS DO MUNICÍPIO (Opcional - se você tiver a camada no GDB ou via arquivo externo)
    # Se você não tiver o arquivo de bordas, o Google Earth já mostra os limites administrativos.
    
    kml.save("mapa_final_engenharia_light.kml")
    print("\n✅ SUCESSO! mapa_final_engenharia_light.kml gerado.")
    print("👉 Abra o arquivo no Google Earth para ver o Ground Truth do Rio!")

if __name__ == "__main__":
    gerar_kml_final_rj()