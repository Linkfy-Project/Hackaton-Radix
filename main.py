"""
Este script cria uma interface web usando Streamlit e Folium para exibir um mapa interativo.
Ele utiliza um único arquivo GeoJSON unificado contendo tanto a geografia quanto as estatísticas.
Otimizado com PNGs rasterizados e renderização em massa para performance profissional.
"""

import streamlit as st
import folium
from streamlit_folium import st_folium
import geopandas as gpd
import geobr
from folium.plugins import Fullscreen, MousePosition, MeasureControl, AntPath
from folium.features import DivIcon
import os
import random
import base64
import re

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
    print("DEBUG: Carregando contorno do Estado do Rio de Janeiro...")
    # Alterado de read_municipality para read_state para pegar o estado inteiro
    rj_shape = geobr.read_state(code_state="RJ", year=2020)
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
    
    stats_html = f'<div style="font-family: sans-serif; min-width: 250px; max-width: 300px;">'
    stats_html += f'<h4 style="margin: 0 0 10px 0; color: #333; border-bottom: 1px solid #ccc; padding-bottom: 5px;">{row["NOM"] or cod_id}</h4>'
    stats_html += f"<b>Distribuidora:</b> {dist}<br>"
    stats_html += f"<b>Classificação:</b> {classificacao}<br>"
    if mae and mae != '0' and mae != 'None':
        stats_html += f"<b>Alimentada por (ID):</b> {mae}<br>"
    stats_html += f"<b>Potência:</b> {row['POTENCIA_CALCULADA']:.2f} MVA<br>"
    if pot_nom and pot_nom > 0:
        stats_html += f"<b>Potência Nominal:</b> {pot_nom:.2f} MVA<br>"
    
    stats_html += "<div style='margin-top: 10px; padding: 8px; background: #f9f9f9; border: 1px solid #eee;'>"
    stats_html += "<b>Estatísticas CNEFE:</b><br>"
    
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
                stats_html += f"• {esp_nome}: {count}<br>"
            else:
                outros_calc = max(0, count_outras - (count_osm_shop + count_osm_ind))
                stats_html += f"• {esp_nome}: {count_outras}<br>"
                stats_html += f"&nbsp;&nbsp;&nbsp;&nbsp;<i>indústria: {count_osm_ind}</i><br>"
                stats_html += f"&nbsp;&nbsp;&nbsp;&nbsp;<i>comércio: {count_osm_shop}</i><br>"
                stats_html += f"&nbsp;&nbsp;&nbsp;&nbsp;<i>outros: {outros_calc}</i><br>"
    
    if total_consumidores > 0:
        stats_html += f"<hr style='margin: 8px 0; border: 0; border-top: 1px solid #ddd;'>"
        stats_html += f"<b>Total de Endereços: {total_consumidores}</b>"
        perc_res = (residenciais / total_consumidores) * 100
        perc_com = (comerciais / total_consumidores) * 100
        stats_html += f"<div style='color: #2e7d32; margin-top: 5px;'><b>Residencial ({perc_res:.1f}%)</b>: {residenciais}</div>"
        stats_html += f"<div style='color: #c62828;'><b>Não Residencial ({perc_com:.1f}%)</b>: {comerciais}</div>"
    else:
        stats_html += "<br><i>Sem dados do CNEFE para esta área.</i>"
    
    stats_html += "</div></div>"
    return stats_html

def get_image_base64(icon_name):
    """
    Converte uma imagem (PNG/SVG) para string Base64.
    """
    path = os.path.join("assets", "icons", icon_name)
    if os.path.exists(path):
        with open(path, "rb") as f:
            data = base64.b64encode(f.read()).decode("utf-8")
            ext = icon_name.split('.')[-1]
            return f"data:image/{ext};base64,{data}"
    return None

@st.cache_resource
def create_map_object(_rj_shape, _gdf_unificado):
    """
    Cria o objeto de mapa Folium otimizado com ícones PNG e popups completos.
    """
    print("DEBUG: Iniciando criação do objeto de mapa otimizado...")
    
    m = folium.Map(
        location=[-22.5, -42.5], # Centralizado mais para o meio do estado
        zoom_start=8, # Zoom reduzido para ver o estado todo
        control_scale=True,
        prefer_canvas=True
    )
    
    # Tiles
    folium.TileLayer('openstreetmap', name='OpenStreetMap').add_to(m)
    folium.TileLayer('cartodbpositron', name='CartoDB Positron').add_to(m)
    folium.TileLayer(
        tiles='https://mt1.google.com/vt/lyrs=s&x={x}&y={y}&z={z}',
        attr='Google', name='Satélite (Google)', overlay=False
    ).add_to(m)
    
    # Contorno RJ
    folium.GeoJson(
        _rj_shape.simplify(0.001), name="Contorno Rio de Janeiro",
        style_function=lambda x: {'fillColor': 'none', 'color': 'yellow', 'weight': 2}
    ).add_to(m)
    
    if _gdf_unificado is not None:
        # Carregar ícones PNG em Base64 e definir proporções reais
        icons_config = {
            "raio": {"url": get_image_base64("raio.png"), "ratio": 73/128},
            "satelite": {"url": get_image_base64("satelite.png"), "ratio": 1.0},
            "transformador": {"url": get_image_base64("transformador.png"), "ratio": 1.0},
            "torre": {"url": get_image_base64("torre.png"), "ratio": 1.0}
        }

        # Mapeamento de Categorias
        categories = {
            "1. Distribuição Plena": ("⚡ SE: Distribuição Plena", icons_config["raio"]),
            "2. Distribuição Satélite": ("📡 SE: Distribuição Satélite", icons_config["satelite"]),
            "3. Transformadora Pura": ("🔄 SE: Transformadora Pura", icons_config["transformador"]),
            "4. Transporte/Manobra": ("🏗️ SE: Transporte/Manobra", icons_config["torre"])
        }

        hierarchy_group = folium.FeatureGroup(name="🔗 Hierarquia de Alimentação", show=False).add_to(m)
        coords_subs = {str(row['COD_ID']): (row['lat_sub'], row['lon_sub']) for _, row in _gdf_unificado.iterrows()}

        cores_vibrantes = [
            '#e6194b', '#3cb44b', '#ffe119', '#4363d8', '#f58231', '#911eb4',
            '#46f0f0', '#f032e6', '#bcf60c', '#fabebe', '#008080', '#e6beff',
            '#9a6324', '#fffac8', '#800000', '#aaffc3', '#808000', '#ffd8b1'
        ]

        # --- RENDERIZAÇÃO ---
        for base_classif, (group_name, icon_config) in categories.items():
            group = folium.FeatureGroup(name=group_name).add_to(m)
            
            mask = _gdf_unificado['CLASSIFICACAO'].str.contains(base_classif, na=False)
            gdf_cat = _gdf_unificado[mask].copy()
            
            if gdf_cat.empty: continue

            # Gerar HTML do popup para cada linha e salvar no GeoDataFrame
            gdf_cat['popup_html'] = gdf_cat.apply(gerar_html_popup, axis=1)

            # Polígonos em massa (Simplificados)
            gdf_cat['geometry'] = gdf_cat['geometry'].simplify(0.0001)
            gdf_cat['color'] = [cores_vibrantes[i % len(cores_vibrantes)] for i in range(len(gdf_cat))]
            
            folium.GeoJson(
                gdf_cat,
                style_function=lambda x: {
                    'fillColor': x['properties']['color'], 'color': 'white', 'weight': 1, 'fillOpacity': 0.3
                },
                tooltip=folium.GeoJsonTooltip(fields=['NOM'], aliases=['Área:']),
                popup=folium.GeoJsonPopup(fields=['popup_html'], labels=False)
            ).add_to(group)

            # Marcadores PNG
            icon_url = icon_config["url"]
            ratio = icon_config["ratio"]

            for _, row in gdf_cat.iterrows():
                pot = row.get('POTENCIA_CALCULADA', 0)
                base_size = 25 + min(45, pot / 10.0)
                
                if icon_url:
                    if ratio <= 1: # Mais alto que largo
                        height = base_size
                        width = base_size * ratio
                    else: # Mais largo que alto
                        width = base_size
                        height = base_size / ratio

                    folium.Marker(
                        location=[row['lat_sub'], row['lon_sub']],
                        icon=folium.CustomIcon(icon_url, icon_size=(int(width), int(height))),
                        popup=folium.Popup(row['popup_html'], max_width=300),
                        tooltip=f"SE: {row['NOM']}"
                    ).add_to(group)
                else:
                    folium.CircleMarker(
                        location=[row['lat_sub'], row['lon_sub']],
                        radius=min(15, 4 + (pot / 50.0)),
                        color='white', weight=1, fill=True, fill_color='gray', fill_opacity=1,
                        popup=folium.Popup(row['popup_html'], max_width=300),
                        tooltip=f"SE: {row['NOM']}"
                    ).add_to(group)

        # 3. Fluxo de Alimentação (AntPath)
        for _, row in _gdf_unificado.iterrows():
            mae_id = str(row.get('SUB_MAE', ''))
            if mae_id and mae_id in coords_subs and mae_id != str(row['COD_ID']):
                mae_coords = coords_subs[mae_id]
                filha_coords = (row['lat_sub'], row['lon_sub'])
                AntPath(
                    locations=[mae_coords, filha_coords],
                    color='white', pulse_color='yellow', weight=3, opacity=0.9,
                    delay=800, dash_array=[10, 20],
                    tooltip=f"Fluxo: {mae_id} -> {row['NOM']}"
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
            
            # --- LEGENDA ABAIXO DO MAPA ---
            st.markdown("---")
            st.subheader("📖 Legenda do Mapa")
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.markdown("**Tipos de Subestação**")
                
                # Exibir ícones reais na legenda com HTML para controle total de proximidade
                def get_legend_html(icon_name, label):
                    b64 = get_image_base64(icon_name)
                    return f'''
                        <div style="display: flex; align-items: center; margin-bottom: 8px;">
                            <img src="{b64}" height="25" style="margin-right: 10px;">
                            <span style="font-size: 14px; font-weight: bold;">{label}</span>
                        </div>
                    '''

                st.markdown(get_legend_html("raio.png", "Distribuição Plena"), unsafe_allow_html=True)
                st.markdown(get_legend_html("satelite.png", "Distribuição Satélite"), unsafe_allow_html=True)
                st.markdown(get_legend_html("transformador.png", "Transformadora Pura"), unsafe_allow_html=True)
                st.markdown(get_legend_html("torre.png", "Transporte / Manobra"), unsafe_allow_html=True)
                
            with col2:
                st.markdown("**Tamanho do Ícone**")
                st.info("O tamanho do ícone é proporcional à **Potência Calculada** da subestação. Quanto maior o ícone, maior a capacidade de carga.")
                
            with col3:
                st.markdown("**Distribuidoras**")
                st.markdown("""
                - <span style='color:red'>●</span> **LIGHT**: Marcadores Vermelhos
                - <span style='color:blue'>●</span> **ENEL**: Marcadores Azuis
                """, unsafe_allow_html=True)
                
            with col4:
                st.markdown("**Camadas e Áreas**")
                st.write("Use o controle no canto superior direito do mapa para alternar a visibilidade das áreas de atendimento e da hierarquia de fluxo.")

        except Exception as e:
            st.error(f"Erro ao carregar mapa: {e}")
            st.exception(e)

if __name__ == "__main__":
    main()
