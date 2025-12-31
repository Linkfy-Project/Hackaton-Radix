"""
Este script cria uma interface web usando Streamlit e Folium para exibir um mapa interativo rico.
Ele integra dados geográficos reais da LIGHT (via GDB), o contorno do Rio de Janeiro (via geobr),
gera um diagrama de Voronoi e utiliza FastMarkerCluster para exibir milhões de pontos do CNEFE_RJ.
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
from folium.plugins import Fullscreen, MousePosition, MeasureControl, FastMarkerCluster

# --- CONFIGURAÇÕES ---
CAMINHO_GDB = r"LIGHT_382_2021-09-30_M10_20231218-2133.gdb"
CAMINHO_CNEFE = "CNEFE_RJ.csv"
NOME_CAMADA_SUB = 'SUB' 
NOME_CAMADA_TR = 'UNTRS'

# Configuração da página do Streamlit
st.set_page_config(page_title="Mapa LIGHT & CNEFE RJ - Big Data", layout="wide")

@st.cache_data
def get_light_data():
    """
    Lê os dados da LIGHT e o contorno do RJ.
    """
    print("DEBUG: Carregando contorno do Rio de Janeiro...")
    rj_shape = geobr.read_municipality(code_muni=3304557, year=2020)
    rj_shape = rj_shape.to_crs("EPSG:4326")
    
    try:
        gdf_sub = gpd.read_file(CAMINHO_GDB, layer=NOME_CAMADA_SUB)
        gdf_tr = gpd.read_file(CAMINHO_GDB, layer=NOME_CAMADA_TR)
        
        potencia_por_sub = gdf_tr.groupby('SUB')['POT_NOM'].sum().reset_index()
        potencia_por_sub.columns = ['COD_ID', 'POTENCIA_CALCULADA']
        
        gdf_sub = gdf_sub.merge(potencia_por_sub, on='COD_ID', how='left')
        gdf_sub['POTENCIA_CALCULADA'] = gdf_sub['POTENCIA_CALCULADA'].fillna(0)
        
        gdf_sub_projected = gdf_sub.to_crs("EPSG:31983")
        gdf_sub['geometry'] = gdf_sub_projected.geometry.centroid.to_crs("EPSG:4326")
        gdf_sub = gdf_sub.to_crs("EPSG:4326")
            
        gdf_sub_rj = gpd.clip(gdf_sub, rj_shape)
        
        # Voronoi
        bounds = rj_shape.total_bounds
        pontos_unicos = gdf_sub_rj.geometry.drop_duplicates()
        pontos_uniao = MultiPoint(pontos_unicos.tolist())
        region_box = box(*bounds)
        voronoi_geo = voronoi_diagram(pontos_uniao, envelope=region_box)
        gdf_voronoi = gpd.GeoDataFrame(geometry=list(voronoi_geo.geoms), crs="EPSG:4326")
        voronoi_recortado = gpd.clip(gdf_voronoi, rj_shape)
        
        return rj_shape, gdf_sub_rj, voronoi_recortado
    except Exception as e:
        print(f"DEBUG ERROR (LIGHT): {e}")
        return rj_shape, None, None

@st.cache_data
def get_cnefe_points(limit=100000):
    """
    Lê as coordenadas do CNEFE. 
    Limitado a 100k pontos para manter a performance do navegador, 
    mas usa FastMarkerCluster para eficiência.
    """
    print(f"DEBUG: Carregando {limit} pontos do CNEFE...")
    try:
        # Lemos apenas as colunas necessárias para economizar memória
        df = pd.read_csv(CAMINHO_CNEFE, sep=';', usecols=['LATITUDE', 'LONGITUDE'], nrows=limit)
        # Remove nulos
        df = df.dropna(subset=['LATITUDE', 'LONGITUDE'])
        # Retorna lista de [lat, lon] para o FastMarkerCluster
        return df[['LATITUDE', 'LONGITUDE']].values.tolist()
    except Exception as e:
        print(f"DEBUG ERROR (CNEFE): {e}")
        return []

@st.cache_resource
def create_map_object(_rj_shape, _gdf_sub, _voronoi_recortado, _cnefe_points):
    """
    Cria o objeto de mapa Folium.
    """
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
    if _voronoi_recortado is not None:
        voronoi_group = folium.FeatureGroup(name="Áreas de Influência (Voronoi)").add_to(m)
        colors = ['red', 'blue', 'green', 'purple', 'orange', 'darkred', 'cadetblue', 'darkpurple', 'pink', 'lightblue', 'lightgreen', 'gray']
        for i, row in _voronoi_recortado.iterrows():
            color = colors[i % len(colors)]
            folium.GeoJson(
                row.geometry,
                style_function=lambda x, color=color: {
                    'fillColor': color, 'color': 'white', 'weight': 1, 'fillOpacity': 0.2
                }
            ).add_to(voronoi_group)
        
    # Subestações LIGHT
    if _gdf_sub is not None:
        sub_group = folium.FeatureGroup(name="Subestações LIGHT").add_to(m)
        for i, row in _gdf_sub.iterrows():
            pot = row['POTENCIA_CALCULADA']
            radius = 4 + (pot / 50.0)
            if radius > 15: radius = 15
            nome_se = str(row.get('NOM') or row.get('NOME') or row.get('COD_ID'))
            folium.CircleMarker(
                location=[row.geometry.y, row.geometry.x],
                radius=radius, color='white', weight=1, fill=True, fill_color='red', fill_opacity=1,
                popup=f"<b>{nome_se}</b><br>Potência: {pot:.2f} MVA",
                tooltip=f"SE: {nome_se}"
            ).add_to(sub_group)

    # CNEFE Big Data (FastMarkerCluster)
    if _cnefe_points:
        # FastMarkerCluster é extremamente eficiente para milhares de pontos
        # Ele agrupa os pontos em clusters que se expandem ao dar zoom
        cluster = FastMarkerCluster(
            data=_cnefe_points,
            name="Endereços CNEFE (Cluster)",
            callback=None # Pode ser customizado para ícones menores
        )
        m.add_child(cluster)

    # Controles
    Fullscreen().add_to(m)
    MousePosition().add_to(m)
    MeasureControl().add_to(m)
    folium.LayerControl(collapsed=True).add_to(m)
    
    return m

def main():
    st.title("⚡ Big Data: LIGHT & CNEFE RJ")
    st.markdown("""
    Visualização de **Big Data**: Subestações reais e **milhares de endereços do CNEFE**.
    Os pontos azuis representam clusters de residências que se expandem conforme você dá zoom.
    """)
    
    # Sidebar para controle de volume de dados
    num_pontos = st.sidebar.slider("Quantidade de pontos CNEFE", 1000, 500000, 100000, step=10000)
    
    with st.spinner("Processando milhões de registros..."):
        try:
            rj_shape, gdf_sub, voronoi_recortado = get_light_data()
            cnefe_points = get_cnefe_points(limit=num_pontos)
            
            m = create_map_object(rj_shape, gdf_sub, voronoi_recortado, cnefe_points)
            
            st_folium(m, width=1200, height=700, returned_objects=[], use_container_width=True)
            
        except Exception as e:
            st.error(f"Erro ao processar dados: {e}")

if __name__ == "__main__":
    main()
