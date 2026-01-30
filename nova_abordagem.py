"""
Este script processa as áreas de cobertura das subestações da LIGHT, resolve sobreposições territoriais
e preenche buracos (áreas sem cobertura) utilizando Diagramas de Voronoi. 
Utiliza a máscara oficial do IBGE (conforme main.py) para garantir que a cobertura respeite 
perfeitamente a linha da costa e os limites municipais.
"""

import geopandas as gpd
import pandas as pd
import random
import simplekml
import geobr
import os
from shapely.ops import voronoi_diagram
from shapely.geometry import MultiPoint, Polygon, MultiPolygon

# DEBUG: Caminho do banco de dados geográfico
CAMINHO_GDB = r"Dados Brutos\BDGD ANEEL\LIGHT_382_2021-09-30_M10_20231218-2133.gdb"

def random_kml_color_hex() -> str:
    """Gera uma cor aleatória no formato KML (AABBGGRR). 99 é a opacidade (~60%)."""
    r = lambda: random.randint(0, 255)
    return f"99{r():02x}{r():02x}{r():02x}"

def obter_mascara_geografica_ibge():
    """Obtém o limite oficial do município do Rio de Janeiro via geobr (conforme main.py)."""
    print("🌍 Baixando limites oficiais do IBGE para o Rio de Janeiro (3304557)...")
    try:
        # 3304557 é o código IBGE para o município do Rio de Janeiro
        gdf_rj = geobr.read_municipality(code_muni=3304557, year=2020)
        return gdf_rj
    except Exception as e:
        print(f"⚠️ Erro ao baixar geobr: {e}. Usando fallback...")
        return None

def gerar_kml_final_rj():
    print("🚀 INICIANDO PROCESSAMENTO FINAL COM MÁSCARA IBGE (PERFEITA)...")
    
    # 1. Carregar as áreas reais
    print("DEBUG: Carregando subestacoes_areas_reais.geojson...")
    gdf = gpd.read_file(r"Dados Processados\subestacoes_areas_reais.geojson")
    
    # 2. Obter Máscara Geográfica (IBGE) - Seguindo o padrão do main.py
    gdf_ibge = obter_mascara_geografica_ibge()
    
    # Projetar tudo para UTM para cálculos precisos
    print("DEBUG: Projetando geometrias para UTM...")
    gdf_utm = gdf.to_crs(epsg=31983)
    
    if gdf_ibge is not None:
        gdf_ibge_utm = gdf_ibge.to_crs(epsg=31983)
        try:
            limite_territorial = gdf_ibge_utm.geometry.union_all()
        except AttributeError:
            limite_territorial = gdf_ibge_utm.unary_union
        print("DEBUG: Usando máscara oficial do IBGE como limite territorial.")
    else:
        # Fallback caso geobr falhe: usa o convex hull das áreas atuais
        print("⚠️ AVISO: Falha ao obter IBGE. Usando convex hull como fallback.")
        try:
            limite_territorial = gdf_utm.geometry.union_all().convex_hull.buffer(5000)
        except AttributeError:
            limite_territorial = gdf_utm.unary_union.convex_hull.buffer(5000)
    
    # 3. RESOLVER CONFLITOS (Limpeza de Sobreposição Inicial)
    gdf_utm = gdf_utm.sort_values(by='POT_NOM', ascending=False)
    areas_limpas = []
    geometria_acumulada = None
    
    print("✂️ Recortando sobreposições territoriais iniciais...")
    for _, row in gdf_utm.iterrows():
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
    
    gdf_limpo_utm = gpd.GeoDataFrame(areas_limpas, crs=gdf_utm.crs)
    
    # 4. PREENCHER BURACOS COM VORONOI
    print("🕳️ Preenchendo buracos territoriais...")
    
    try:
        uniao_atual = gdf_limpo_utm.geometry.union_all()
    except AttributeError:
        uniao_atual = gdf_limpo_utm.unary_union
        
    vazio_territorial = limite_territorial.difference(uniao_atual)
    
    # Carregar pontos das subestações
    gdf_sub_pontos = gpd.read_file(CAMINHO_GDB, layer='SUB')[['COD_ID', 'geometry']]
    gdf_sub_pontos = gdf_sub_pontos.to_crs(epsg=31983)
    gdf_sub_pontos['geometry'] = gdf_sub_pontos.geometry.centroid
    
    ids_presentes = gdf_limpo_utm['COD_ID'].unique()
    gdf_sub_pontos = gdf_sub_pontos[gdf_sub_pontos['COD_ID'].isin(ids_presentes)]
    
    print("DEBUG: Gerando Diagrama de Voronoi...")
    pontos_lista = [p for p in gdf_sub_pontos.geometry if p is not None]
    if pontos_lista:
        pontos_unidos = MultiPoint(pontos_lista)
        regioes_voronoi = voronoi_diagram(pontos_unidos, envelope=limite_territorial.envelope.buffer(10000))
        
        gdf_voronoi = gpd.GeoDataFrame(geometry=list(regioes_voronoi.geoms), crs=gdf_utm.crs)
        gdf_voronoi = gpd.sjoin(gdf_voronoi, gdf_sub_pontos, how='left', predicate='contains')
        
        print("DEBUG: Distribuindo áreas de buracos...")
        areas_expandidas = []
        for _, row in gdf_limpo_utm.iterrows():
            voronoi_da_sub = gdf_voronoi[gdf_voronoi['COD_ID'] == row['COD_ID']]
            
            if not voronoi_da_sub.empty:
                try:
                    geom_voronoi = voronoi_da_sub.geometry.union_all()
                except AttributeError:
                    geom_voronoi = voronoi_da_sub.geometry.unary_union
                
                pedaco_do_vazio = geom_voronoi.intersection(vazio_territorial)
                
                if not pedaco_do_vazio.is_empty:
                    row.geometry = row.geometry.union(pedaco_do_vazio)
            
            areas_expandidas.append(row)
        
        gdf_final_utm = gpd.GeoDataFrame(areas_expandidas, crs=gdf_utm.crs)
    else:
        gdf_final_utm = gdf_limpo_utm

    # 5. CORTE FINAL COM A MÁSCARA (Garantia de Realismo)
    print("✂️ Aplicando corte final com a máscara IBGE...")
    gdf_final_utm['geometry'] = gdf_final_utm.geometry.intersection(limite_territorial)
    gdf_final_utm = gdf_final_utm[~gdf_final_utm.geometry.is_empty]

    # Voltar para Lat/Long
    gdf_final = gdf_final_utm.to_crs(epsg=4326)

    # 6. CRIAR O KML
    kml = simplekml.Kml()
    
    # --- ADICIONAR O LIMITE TERRITORIAL (MÁSCARA) EM AMARELO ---
    print("🟡 Adicionando limite territorial IBGE ao KML...")
    gdf_mask_4326 = gpd.GeoDataFrame(geometry=[limite_territorial], crs=gdf_utm.crs).to_crs(epsg=4326)
    mask_geom = gdf_mask_4326.geometry.iloc[0]
    
    if isinstance(mask_geom, Polygon):
        mask_pol = kml.newpolygon(name="LIMITE IBGE (RIO DE JANEIRO)")
        mask_pol.outerboundaryis = list(mask_geom.exterior.coords)
        mask_pol.style.polystyle.color = "00FFFFFF" # Transparente
        mask_pol.style.linestyle.color = simplekml.Color.yellow
        mask_pol.style.linestyle.width = 4
    elif isinstance(mask_geom, MultiPolygon):
        for i, p in enumerate(mask_geom.geoms):
            mask_pol = kml.newpolygon(name=f"LIMITE IBGE (RIO DE JANEIRO) - Parte {i+1}")
            mask_pol.outerboundaryis = list(p.exterior.coords)
            mask_pol.style.polystyle.color = "00FFFFFF" # Transparente
            mask_pol.style.linestyle.color = simplekml.Color.yellow
            mask_pol.style.linestyle.width = 4

    # --- ADICIONAR AS ÁREAS DAS SUBESTAÇÕES ---
    gdf_sub_icones = gdf_sub_pontos.to_crs(epsg=4326)
    print("🎨 Gerando áreas das subestações no KML...")
    for _, row in gdf_final.iterrows():
        nome_area = f"Área: {row['COD_ID']}"
        
        if isinstance(row.geometry, Polygon):
            pol = kml.newpolygon(name=nome_area)
            pol.outerboundaryis = list(row.geometry.exterior.coords)
        elif isinstance(row.geometry, MultiPolygon):
            maior_pol = max(row.geometry.geoms, key=lambda p: p.area)
            pol = kml.newpolygon(name=nome_area)
            pol.outerboundaryis = list(maior_pol.exterior.coords)

        pol.style.polystyle.color = random_kml_color_hex()
        pol.style.linestyle.width = 1
        pol.description = f"Subestação ID: {row['COD_ID']}\nCapacidade: {row.get('POT_NOM', 'N/A')} MVA"

        ponto_match = gdf_sub_icones[gdf_sub_icones['COD_ID'].astype(str) == str(row['COD_ID'])]
        if not ponto_match.empty:
            pnt = kml.newpoint(name=f"Subestação {row['COD_ID']}", 
                               coords=[(ponto_match.iloc[0].geometry.x, ponto_match.iloc[0].geometry.y)])
            pnt.style.iconstyle.icon.href = 'http://maps.google.com/mapfiles/kml/paddle/ylw-stars.png'

    kml.save("mapa_final_rj_sanitizado.kml")
    print("\n✅ SUCESSO! Arquivo 'mapa_final_rj_sanitizado.kml' gerado com máscara IBGE perfeita.")

if __name__ == "__main__":
    gerar_kml_final_rj()
