"""
Este script cria uma interface web usando Streamlit e Folium para exibir um mapa interativo rico.
Ele integra dados geográficos reais da LIGHT (via GDB), desenha o contorno da cidade do Rio de Janeiro,
e gera um diagrama de Voronoi baseado na localização real das subestações.
Otimizado para evitar re-processamento durante a navegação no mapa.
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

# --- CONFIGURAÇÕES ---
CAMINHO_GDB = r"LIGHT_382_2021-09-30_M10_20231218-2133.gdb"
NOME_CAMADA_SUB = 'SUB' 
NOME_CAMADA_TR = 'UNTRS'

# Configuração da página do Streamlit
st.set_page_config(page_title="Mapa LIGHT RJ - Voronoi Real", layout="wide")

@st.cache_data
def get_real_data():
    """
    Lê os dados reais do GDB da LIGHT e o contorno do RJ.
    Calcula a potência das subestações e gera o Voronoi.
    """
    # DEBUG: Iniciando carregamento de dados reais
    print("DEBUG: Carregando contorno do Rio de Janeiro...")
    rj_shape = geobr.read_municipality(code_muni=3304557, year=2020)
    rj_shape = rj_shape.to_crs("EPSG:4326")
    
    print(f"DEBUG: Carregando subestações do GDB: {CAMINHO_GDB}")
    try:
        # Carregar subestações
        gdf_sub = gpd.read_file(CAMINHO_GDB, layer=NOME_CAMADA_SUB)
        
        # Carregar transformadores para potência
        gdf_tr = gpd.read_file(CAMINHO_GDB, layer=NOME_CAMADA_TR)
        
        # Calcular potência por SE
        potencia_por_sub = gdf_tr.groupby('SUB')['POT_NOM'].sum().reset_index()
        potencia_por_sub.columns = ['COD_ID', 'POTENCIA_CALCULADA']
        
        # Mesclar dados
        gdf_sub = gdf_sub.merge(potencia_por_sub, on='COD_ID', how='left')
        gdf_sub['POTENCIA_CALCULADA'] = gdf_sub['POTENCIA_CALCULADA'].fillna(0)
        
        # Tratamento de Geometria (Centroides precisos)
        gdf_sub_projected = gdf_sub.to_crs("EPSG:31983")
        gdf_sub['geometry'] = gdf_sub_projected.geometry.centroid.to_crs("EPSG:4326")
        gdf_sub = gdf_sub.to_crs("EPSG:4326")
            
        # Filtrar apenas subestações que estão DENTRO do município do Rio
        gdf_sub_rj = gpd.clip(gdf_sub, rj_shape)
        
        # --- VORONOI REAL ---
        print("DEBUG: Gerando Voronoi baseado em subestações reais...")
        bounds = rj_shape.total_bounds
        pontos_unicos = gdf_sub_rj.geometry.drop_duplicates()
        pontos_uniao = MultiPoint(pontos_unicos.tolist())
        region_box = box(*bounds)
        voronoi_geo = voronoi_diagram(pontos_uniao, envelope=region_box)
        
        gdf_voronoi = gpd.GeoDataFrame(geometry=list(voronoi_geo.geoms), crs="EPSG:4326")
        voronoi_recortado = gpd.clip(gdf_voronoi, rj_shape)
        
        return rj_shape, gdf_sub_rj, voronoi_recortado

    except Exception as e:
        print(f"DEBUG ERROR: {e}")
        raise e

@st.cache_resource
def create_map_object(_rj_shape, _gdf_sub, _voronoi_recortado):
    """
    Cria o objeto de mapa Folium e o armazena em cache.
    O uso de @st.cache_resource evita que o mapa seja reconstruído a cada interação.
    """
    # DEBUG: Construindo objeto de mapa Folium (Cache Miss)
    print("DEBUG: Construindo objeto de mapa Folium...")
    
    m = folium.Map(location=[-22.9068, -43.1729], zoom_start=11, control_scale=True)
    
    # Tiles
    folium.TileLayer('openstreetmap', name='OpenStreetMap').add_to(m)
    folium.TileLayer('cartodbpositron', name='CartoDB Positron').add_to(m)
    folium.TileLayer(
        tiles='https://mt1.google.com/vt/lyrs=s&x={x}&y={y}&z={z}',
        attr='Google',
        name='Satélite (Google)',
        overlay=False,
        control=True
    ).add_to(m)
    
    # Contorno RJ
    folium.GeoJson(
        _rj_shape,
        name="Contorno Rio de Janeiro",
        style_function=lambda x: {'fillColor': 'none', 'color': 'yellow', 'weight': 3}
    ).add_to(m)
    
    # Voronoi
    colors = ['red', 'blue', 'green', 'purple', 'orange', 'darkred', 'cadetblue', 'darkpurple', 'pink', 'lightblue', 'lightgreen', 'gray']
    voronoi_group = folium.FeatureGroup(name="Áreas de Influência (Voronoi)").add_to(m)
    
    for i, row in _voronoi_recortado.iterrows():
        color = colors[i % len(colors)]
        folium.GeoJson(
            row.geometry,
            style_function=lambda x, color=color: {
                'fillColor': color,
                'color': 'white',
                'weight': 1,
                'fillOpacity': 0.3
            },
            tooltip=f"Área {i+1}"
        ).add_to(voronoi_group)
        
    # Subestações Reais
    sub_group = folium.FeatureGroup(name="Subestações LIGHT").add_to(m)
    for i, row in _gdf_sub.iterrows():
        pot = row['POTENCIA_CALCULADA']
        radius = 4 + (pot / 50.0)
        if radius > 15: radius = 15
        
        folium.CircleMarker(
            location=[row.geometry.y, row.geometry.x],
            radius=radius,
            color='white',
            weight=1,
            fill=True,
            fill_color='red',
            fill_opacity=0.9,
            popup=f"<b>{row.get('NOME', 'S/N')}</b><br>Potência: {pot:.2f} MVA<br>ID: {row['COD_ID']}",
            tooltip=row.get('NOME', row['COD_ID'])
        ).add_to(sub_group)

    # Controles
    Fullscreen().add_to(m)
    MousePosition().add_to(m)
    MeasureControl().add_to(m)
    folium.LayerControl(collapsed=True).add_to(m)
    
    return m

def main():
    st.title("⚡ Mapa de Subestações LIGHT - Rio de Janeiro")
    st.markdown("""
    Visualização real das subestações da **LIGHT**. 
    As áreas coloridas representam o **Diagrama de Voronoi**.
    """)
    
    # 1. Carregar dados (Cache de dados)
    try:
        rj_shape, gdf_sub, voronoi_recortado = get_real_data()
        
        # 2. Criar/Recuperar mapa (Cache de recurso)
        m = create_map_object(rj_shape, gdf_sub, voronoi_recortado)
        
        # 3. Renderizar mapa (Otimizado)
        # returned_objects=[] evita que o Streamlit recarregue a página ao clicar no mapa
        st_folium(
            m, 
            width=1200, 
            height=700, 
            returned_objects=[], 
            use_container_width=True
        )
        
    except Exception as e:
        st.error(f"Erro ao processar dados: {e}")
        st.info("Certifique-se de que o arquivo GDB da LIGHT está na pasta raiz.")

if __name__ == "__main__":
    main()
