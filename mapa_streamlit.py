"""
Este script cria uma interface web usando Streamlit e Folium para exibir um mapa interativo rico.
Ele integra dados geográficos do Rio de Janeiro (via geobr), desenha o contorno da cidade
e gera um diagrama de Voronoi baseado em pontos aleatórios (simulando subestações).
Inclui camadas de satélite e ferramentas avançadas de mapa.
"""

import streamlit as st
import folium
from streamlit_folium import st_folium
import geopandas as gpd
import geobr
import numpy as np
import pandas as pd
from shapely.geometry import box, MultiPoint
from shapely.ops import voronoi_diagram
from folium.plugins import Fullscreen, MousePosition, MeasureControl

# Configuração da página do Streamlit
st.set_page_config(page_title="Mapa Rio de Janeiro - Voronoi & Satélite", layout="wide")

@st.cache_data
def get_rj_data():
    """
    Obtém o contorno do Rio de Janeiro e gera dados de Voronoi.
    Usa cache para evitar downloads repetidos.
    """
    # DEBUG: Baixando contorno do RJ
    print("DEBUG: Baixando contorno do Rio de Janeiro via geobr")
    rj_shape = geobr.read_municipality(code_muni=3304557, year=2020)
    rj_shape = rj_shape.to_crs("EPSG:4326")
    
    # Gerar pontos aleatórios dentro dos limites do RJ
    bounds = rj_shape.total_bounds  # (minx, miny, maxx, maxy)
    
    # Gerar 20 pontos aleatórios
    np.random.seed(42) # Para consistência
    lat_p = np.random.uniform(bounds[1], bounds[3], 20)
    lon_p = np.random.uniform(bounds[0], bounds[2], 20)
    
    gdf_sub = gpd.GeoDataFrame(
        geometry=gpd.points_from_xy(lon_p, lat_p),
        crs="EPSG:4326"
    )
    
    # Filtrar pontos que estão realmente dentro do polígono do RJ
    gdf_sub = gpd.clip(gdf_sub, rj_shape)
    
    # Se após o clip tivermos poucos pontos, vamos gerar mais até ter pelo menos 10
    while len(gdf_sub) < 10:
        lat_p = np.random.uniform(bounds[1], bounds[3], 10)
        lon_p = np.random.uniform(bounds[0], bounds[2], 10)
        new_points = gpd.GeoDataFrame(geometry=gpd.points_from_xy(lon_p, lat_p), crs="EPSG:4326")
        gdf_sub = pd.concat([gdf_sub, new_points], ignore_index=True)
        gdf_sub = gpd.clip(gdf_sub, rj_shape)

    # Criar Voronoi
    pontos_uniao = MultiPoint(gdf_sub.geometry.tolist())
    region_box = box(*bounds)
    voronoi_geo = voronoi_diagram(pontos_uniao, envelope=region_box)
    
    gdf_voronoi = gpd.GeoDataFrame(geometry=list(voronoi_geo.geoms), crs="EPSG:4326")
    
    # Recortar Voronoi com o contorno do RJ
    voronoi_recortado = gpd.clip(gdf_voronoi, rj_shape)
    
    return rj_shape, gdf_sub, voronoi_recortado

def create_map(rj_shape, gdf_sub, voronoi_recortado):
    """
    Cria o mapa Folium com as camadas de contorno, Voronoi e Satélite.
    """
    # DEBUG: Criando mapa Folium
    print("DEBUG: Criando mapa Folium com dados do RJ e Satélite")
    
    # Centralizar no RJ
    m = folium.Map(location=[-22.9068, -43.1729], zoom_start=11, control_scale=True)
    
    # --- ADICIONANDO CAMADAS DE FUNDO (TILES) ---
    folium.TileLayer('openstreetmap', name='OpenStreetMap').add_to(m)
    folium.TileLayer('cartodbpositron', name='CartoDB Positron').add_to(m)
    
    # Camada de Satélite (Esri World Imagery)
    folium.TileLayer(
        tiles='https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',
        attr='Esri',
        name='Satélite (Esri)',
        overlay=False,
        control=True
    ).add_to(m)

    # Camada de Satélite (Google)
    folium.TileLayer(
        tiles='https://mt1.google.com/vt/lyrs=s&x={x}&y={y}&z={z}',
        attr='Google',
        name='Satélite (Google)',
        overlay=False,
        control=True
    ).add_to(m)
    
    # --- ADICIONANDO DADOS GEOGRÁFICOS ---
    
    # Adicionar Contorno do RJ
    folium.GeoJson(
        rj_shape,
        name="Contorno Rio de Janeiro",
        style_function=lambda x: {'fillColor': 'none', 'color': 'yellow', 'weight': 3},
        tooltip="Limite Municipal do Rio de Janeiro"
    ).add_to(m)
    
    # Adicionar Áreas de Voronoi
    # Cores para os polígonos
    colors = ['red', 'blue', 'green', 'purple', 'orange', 'darkred', 'lightred', 'beige', 'darkblue', 'darkgreen', 'cadetblue', 'darkpurple', 'white', 'pink', 'lightblue', 'lightgreen', 'gray', 'black', 'lightgray']
    
    # Criar um FeatureGroup para o Voronoi para não poluir o controle de camadas
    voronoi_group = folium.FeatureGroup(name="Diagrama de Voronoi").add_to(m)
    
    for i, row in voronoi_recortado.iterrows():
        color = colors[i % len(colors)]
        folium.GeoJson(
            row.geometry,
            style_function=lambda x, color=color: {
                'fillColor': color,
                'color': 'white',
                'weight': 1,
                'fillOpacity': 0.3
            },
            tooltip=f"Área de Influência {i+1}"
        ).add_to(voronoi_group)
        
    # Adicionar Pontos (Subestações) em um grupo
    points_group = folium.FeatureGroup(name="Subestações").add_to(m)
    for i, row in gdf_sub.iterrows():
        folium.CircleMarker(
            location=[row.geometry.y, row.geometry.x],
            radius=6,
            color='white',
            weight=2,
            fill=True,
            fill_color='red',
            fill_opacity=1,
            popup=f"Subestação {i+1}",
            tooltip=f"Ponto {i+1}"
        ).add_to(points_group)

    # --- PLUGINS E CONTROLES ---
    Fullscreen(position='topright', title='Tela Cheia', title_cancel='Sair').add_to(m)
    MousePosition(position='bottomright', separator=' | ', prefix='Lat: ', lng_first=False).add_to(m)
    MeasureControl(position='topleft', primary_length_unit='meters').add_to(m)
    
    # Controle de Camadas (collapsed=True para mostrar apenas o ícone)
    folium.LayerControl(collapsed=True, position='topright').add_to(m)
    
    return m

def main():
    st.title("🗺️ Rio de Janeiro - Voronoi & Visão de Satélite")
    st.markdown("""
    Esta interface integra o contorno oficial do RJ, o cálculo de áreas de Voronoi e múltiplas camadas de visualização, 
    incluindo **Imagens de Satélite (Esri e Google)**.
    """)
    
    with st.spinner("Processando dados geográficos e camadas de satélite..."):
        try:
            rj_shape, gdf_sub, voronoi_recortado = get_rj_data()
            m = create_map(rj_shape, gdf_sub, voronoi_recortado)
            
            # Renderizar o mapa
            st_folium(m, width=1200, height=700, returned_objects=[])
            
        except Exception as e:
            st.error(f"Erro ao carregar dados: {e}")
            # DEBUG: Erro detalhado
            import traceback
            print(f"DEBUG ERROR: {e}")
            print(traceback.format_exc())

if __name__ == "__main__":
    main()
