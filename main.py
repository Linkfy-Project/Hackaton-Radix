"""
Este script cria uma interface web usando Streamlit e Folium para exibir um mapa interativo.
Ele utiliza um único arquivo GeoJSON unificado contendo tanto a geografia quanto as estatísticas.
"""

import streamlit as st
import folium
from streamlit_folium import st_folium
import geopandas as gpd
import geobr
from folium.plugins import Fullscreen, MousePosition, MeasureControl
import os
import random

# --- CONFIGURAÇÕES ---
ARQUIVO_UNIFICADO = os.path.join("Dados Processados", "dados_finais_rj.geojson")

# Dicionário de Espécies para o Popup
DIC_ESPECIES = {
    1: "Domicílio Particular",
    2: "Domicílio Coletivo",
    3: "Estabelecimento Agropecuário",
    4: "Estabelecimento de Ensino",
    5: "Estabelecimento de Saúde",
    6: "Estabelecimento de Outras Finalidades",
    7: "Estabelecimento Religioso",
    8: "Unidade em Construção"
}

# Cores por Distribuidora (para os marcadores)
CORES_DISTRIBUIDORA = {
    'LIGHT': 'red',
    'ENEL': 'blue'
}

# Configuração da página do Streamlit
st.set_page_config(page_title="Mapa LIGHT & ENEL RJ - Big Data", layout="wide")

@st.cache_data
def load_data():
    """
    Carrega o contorno do RJ e os dados unificados.
    """
    print("DEBUG: Carregando contorno do Rio de Janeiro...")
    rj_shape = geobr.read_municipality(code_muni=3304557, year=2020)
    rj_shape = rj_shape.to_crs("EPSG:4326")
    
    print(f"DEBUG: Carregando dados unificados de {ARQUIVO_UNIFICADO}...")
    if not os.path.exists(ARQUIVO_UNIFICADO):
        return rj_shape, None
        
    gdf_unificado = gpd.read_file(ARQUIVO_UNIFICADO)
    return rj_shape, gdf_unificado

def gerar_html_popup(row):
    """
    Gera o conteúdo HTML para o popup a partir das propriedades do GeoJSON.
    """
    cod_id = str(row['COD_ID'])
    pot_nom = row.get('POT_NOM', 0)
    dist = row.get('DISTRIBUIDORA', 'N/A')
    classificacao = row.get('CLASSIFICACAO', 'Não Classificada')
    mae = row.get('SUB_MAE', 'N/A')
    
    stats_html = f"<b>Área da SE: {row['NOM'] or cod_id}</b><br>"
    stats_html += f"Distribuidora: {dist}<br>"
    stats_html += f"<b>Classificação: {classificacao}</b><br>"
    if mae and mae != '0' and mae != 'None':
        stats_html += f"Alimentada por (ID): {mae}<br>"
    stats_html += f"Potência: {row['POTENCIA_CALCULADA']:.2f} MVA<br>"
    if pot_nom and pot_nom > 0:
        stats_html += f"Potência Nominal: {pot_nom:.2f} MVA<br>"
    stats_html += "<br><b>Estatísticas CNEFE:</b><br>"
    
    total_consumidores = 0
    residenciais = 0
    comerciais = 0
    
    # Capturar dados do OSM e Outras Finalidades das colunas do GeoJSON
    count_osm_shop = int(row.get('OSM_SHOP', 0))
    count_osm_ind = int(row.get('OSM_INDUSTRIAL', 0))
    count_outras = int(row.get('6', 0)) # Espécie 6

    # Itera pelas espécies do CNEFE (1 a 8)
    for esp_code in range(1, 9):
        col_name = str(float(esp_code)) # O GeoJSON salvou como "1.0", "2.0", etc.
        if col_name not in row:
            col_name = str(esp_code) # Tenta formato inteiro
            
        count = int(row.get(col_name, 0))
        
        if count > 0:
            esp_nome = DIC_ESPECIES.get(esp_code, f"Espécie {esp_code}")
            
            if esp_code != 8:
                total_consumidores += count
                if esp_code in [1, 2]: residenciais += count
                else: comerciais += count
            
            # Listagem detalhada
            if esp_code != 6:
                stats_html += f"- {esp_nome}: {count}<br>"
            else:
                outros_calc = max(0, count_outras - (count_osm_shop + count_osm_ind))
                stats_html += f"- {esp_nome}: {count_outras}<br>"
                stats_html += f"&nbsp;&nbsp;&nbsp;&nbsp;indústria: {count_osm_ind}<br>"
                stats_html += f"&nbsp;&nbsp;&nbsp;&nbsp;comércio: {count_osm_shop}<br>"
                stats_html += f"&nbsp;&nbsp;&nbsp;&nbsp;outros: {outros_calc}<br>"
    
    if total_consumidores > 0:
        stats_html += f"<b>Total de Endereços: {total_consumidores}</b>"
        perc_res = (residenciais / total_consumidores) * 100
        perc_com = (comerciais / total_consumidores) * 100
        stats_html += f"<div style='color: green;'><br><b>- Residencial ({perc_res:.1f}%)</b>: {residenciais}</div>"
        stats_html += f"<div style='color: red;'><b>- Não Residencial ({perc_com:.1f}%)</b>: {comerciais}</div>"
    else:
        stats_html += "Sem dados do CNEFE para esta área.<br>"
        
    return stats_html

@st.cache_resource
def create_map_object(_rj_shape, _gdf_unificado):
    """
    Cria o objeto de mapa Folium.
    """
    m = folium.Map(location=[-22.9068, -43.1729], zoom_start=11, control_scale=True)
    
    # Tiles
    folium.TileLayer('openstreetmap', name='OpenStreetMap').add_to(m)
    folium.TileLayer('cartodbpositron', name='CartoDB Positron').add_to(m)
    folium.TileLayer(
        tiles='https://mt1.google.com/vt/lyrs=s&x={x}&y={y}&z={z}',
        attr='Google', name='Satélite (Google)', overlay=False
    ).add_to(m)
    
    # Contorno RJ
    folium.GeoJson(
        _rj_shape, name="Contorno Rio de Janeiro",
        style_function=lambda x: {'fillColor': 'none', 'color': 'yellow', 'weight': 3}
    ).add_to(m)
    
    if _gdf_unificado is not None:
        areas_group = folium.FeatureGroup(name="Áreas de Atendimento Reais").add_to(m)
        sub_group = folium.FeatureGroup(name="Subestações").add_to(m)
        hierarchy_group = folium.FeatureGroup(name="Hierarquia de Alimentação", show=False).add_to(m)
        
        # Mapear coordenadas das subestações para desenhar as setas
        coords_subs = {str(row['COD_ID']): (row['lat_sub'], row['lon_sub']) for _, row in _gdf_unificado.iterrows()}

        # Lista de cores vibrantes para diferenciar as áreas
        cores_vibrantes = [
            'red', 'blue', 'green', 'purple', 'orange', 'darkred', 
            'lightred', 'beige', 'darkblue', 'darkgreen', 'cadetblue', 
            'darkpurple', 'pink', 'lightblue', 'lightgreen', 
            'gray', 'black'
        ]
        
        for i, row in _gdf_unificado.iterrows():
            dist = row.get('DISTRIBUIDORA', 'LIGHT')
            # Escolhe uma cor baseada no índice para ser consistente
            area_color = cores_vibrantes[i % len(cores_vibrantes)]
            marker_color = CORES_DISTRIBUIDORA.get(dist, 'gray')
            
            # Popup unificado
            popup_content = gerar_html_popup(row)
            
            # Desenha Polígono da Área Real
            folium.GeoJson(
                row.geometry,
                style_function=lambda x, c=area_color: {
                    'fillColor': c, 'color': 'white', 'weight': 1, 'fillOpacity': 0.4
                },
                tooltip=f"Área: {row['NOM']} ({dist})",
                popup=folium.Popup(popup_content, max_width=300)
            ).add_to(areas_group)
            
            # Desenha Marcador da Subestação
            pot = row['POTENCIA_CALCULADA']
            radius = min(15, 4 + (pot / 50.0))
            
            folium.CircleMarker(
                location=[row['lat_sub'], row['lon_sub']],
                radius=radius, color='white', weight=1, fill=True, fill_color=marker_color, fill_opacity=1,
                popup=folium.Popup(popup_content, max_width=300),
                tooltip=f"SE: {row['NOM']} ({dist})"
            ).add_to(sub_group)
            
            # Desenha Seta de Alimentação (Hierarquia)
            mae_id = str(row.get('SUB_MAE', ''))
            if mae_id and mae_id in coords_subs and mae_id != str(row['COD_ID']):
                mae_coords = coords_subs[mae_id]
                filha_coords = (row['lat_sub'], row['lon_sub'])
                
                # Linha com seta
                folium.PolyLine(
                    locations=[mae_coords, filha_coords],
                    color='yellow',
                    weight=2,
                    opacity=0.8,
                    dash_array='5, 10',
                    tooltip=f"Fluxo: {mae_id} -> {row['NOM']}"
                ).add_to(hierarchy_group)
                
                # Adicionar um pequeno círculo na ponta para indicar direção
                folium.CircleMarker(
                    location=filha_coords,
                    radius=3,
                    color='yellow',
                    fill=True,
                    fill_color='yellow'
                ).add_to(hierarchy_group)
            
    # Controles
    Fullscreen().add_to(m)
    MousePosition().add_to(m)
    MeasureControl().add_to(m)
    folium.LayerControl(collapsed=True).add_to(m)
    
    return m

def main():
    st.title("⚡ Big Data: LIGHT & ENEL RJ (Áreas Reais)")
    st.markdown("""
    Visualização de **Áreas Reais de Atendimento** (Convex Hull dos transformadores).
    **Legenda Marcadores:** <span style='color:red'>●</span> LIGHT | <span style='color:blue'>●</span> ENEL
    """, unsafe_allow_html=True)
    
    if not os.path.exists(ARQUIVO_UNIFICADO):
        st.error(f"⚠️ Arquivo {ARQUIVO_UNIFICADO} não encontrado! Execute `python extrator.py` primeiro.")
        return

    with st.spinner("Carregando mapa e dados unificados..."):
        try:
            rj_shape, gdf_unificado = load_data()
            m = create_map_object(rj_shape, gdf_unificado)
            st_folium(m, width=1200, height=700, returned_objects=[], use_container_width=True)
            
        except Exception as e:
            st.error(f"Erro ao carregar mapa: {e}")
            st.exception(e)

if __name__ == "__main__":
    main()
