"""
Este script cria uma interface web usando Streamlit e Folium para exibir um mapa interativo rico.
Ele integra dados geográficos reais da LIGHT (via GDB), o contorno do Rio de Janeiro (via geobr),
gera um diagrama de Voronoi e utiliza dados pré-processados do CNEFE para exibir estatísticas por área.
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
import os

# --- CONFIGURAÇÕES ---
CAMINHO_GDB = r"LIGHT_382_2021-09-30_M10_20231218-2133.gdb"
ARQUIVO_STATS = "cnefe_stats_by_sub.csv"
NOME_CAMADA_SUB = 'SUB' 
NOME_CAMADA_TR = 'UNTRS'

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
        
        # Garantir que POT_NOM existe (pode vir do GDB ou ser calculada)
        if 'POT_NOM' not in gdf_sub.columns:
            gdf_sub['POT_NOM'] = 0
        
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
        
        # Associar Voronoi ao COD_ID
        voronoi_com_sub = gpd.sjoin(voronoi_recortado, gdf_sub_rj[['COD_ID', 'NOM', 'POTENCIA_CALCULADA', 'POT_NOM', 'geometry']], how='left', predicate='contains')
        
        return rj_shape, gdf_sub_rj, voronoi_com_sub
    except Exception as e:
        print(f"DEBUG ERROR (LIGHT): {e}")
        return rj_shape, None, None

@st.cache_data
def get_preprocessed_cnefe():
    """
    Lê o arquivo de estatísticas pré-processado.
    """
    print("DEBUG: Carregando dados pré-processados do CNEFE...")
    try:
        if not os.path.exists(ARQUIVO_STATS):
            return None
            
        df_stats = pd.read_csv(ARQUIVO_STATS, index_col='COD_ID')
        df_stats.index = df_stats.index.astype(str)
        
        return df_stats
    except Exception as e:
        print(f"DEBUG ERROR (CNEFE Load): {e}")
        return None

@st.cache_resource
def create_map_object(_rj_shape, _gdf_sub, _voronoi_recortado, _cnefe_stats):
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
            
            cod_id = str(row['COD_ID'])
            pot_nom = row.get('POT_NOM', 0)
            
            stats_html = f"<b>Área da SE: {row['NOM'] or cod_id}</b><br>"
            stats_html += f"Potência: {row['POTENCIA_CALCULADA']:.2f} MVA<br>"
            if pot_nom and pot_nom > 0:
                stats_html += f"Potência Nominal: {pot_nom:.2f} MVA<br>"
            stats_html += "<br><b>Estatísticas CNEFE:</b><br>"
            
            if _cnefe_stats is not None and cod_id in _cnefe_stats.index:
                row_stats = _cnefe_stats.loc[cod_id]
                total_consumidores = 0
                
                # Categorias para o resumo
                residenciais = 0
                comerciais = 0
                
                # Detalhamento por espécie
                detalhes_res = ""
                detalhes_com = ""
                
                # Variáveis para o detalhamento de "Outras Finalidades"
                count_outras = 0
                count_osm_shop = 0
                count_osm_ind = 0
                
                # Primeiro pass: capturar dados do OSM e Outras Finalidades
                for col in row_stats.index:
                    val = row_stats[col]
                    if col == 'OSM_SHOP':
                        count_osm_shop = int(val)
                    elif col == 'OSM_INDUSTRIAL':
                        count_osm_ind = int(val)
                    elif col == '6':
                        count_outras = int(val)

                # Segundo pass: construir o HTML
                for esp_code in row_stats.index:
                    count = row_stats[esp_code]
                    if count > 0:
                        # Pular colunas do OSM na listagem principal, elas serão usadas no detalhamento da espécie 6
                        if esp_code in ['OSM_SHOP', 'OSM_INDUSTRIAL']:
                            continue
                            
                        try:
                            esp_code_int = int(float(esp_code))
                        except ValueError:
                            continue
                            
                        esp_nome = DIC_ESPECIES.get(esp_code_int, f"Espécie {esp_code}")
                        
                        # Somar ao total se não for espécie 8 (Unidade em Construção)
                        if esp_code_int != 8:
                            total_consumidores += int(count)
                            
                            # Classificação
                            if esp_code_int in [1, 2]:
                                residenciais += int(count)
                                detalhes_res += f"&nbsp;&nbsp;&nbsp;&nbsp;{esp_nome}: {int(count)}<br>"
                            else:
                                comerciais += int(count)
                                if esp_code_int == 6: # Estabelecimento de Outras Finalidades
                                    # Detalhamento customizado no resumo
                                    outros_calc = count_outras - (count_osm_shop + count_osm_ind)
                                    if outros_calc < 0: outros_calc = 0
                                    
                                    detalhes_com += f"&nbsp;&nbsp;&nbsp;&nbsp;{esp_nome}: {count_outras}<br>"
                                    detalhes_com += f"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;indústria: {count_osm_ind}<br>"
                                    detalhes_com += f"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;comércio: {count_osm_shop}<br>"
                                    detalhes_com += f"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;outros: {outros_calc}<br>"
                                else:
                                    detalhes_com += f"&nbsp;&nbsp;&nbsp;&nbsp;{esp_nome}: {int(count)}<br>"
                        
                        # Listagem original
                        if esp_code_int != 6:
                             stats_html += f"- {esp_nome}: {int(count)}<br>"
                        else:
                             outros_calc = count_outras - (count_osm_shop + count_osm_ind)
                             if outros_calc < 0: outros_calc = 0
                             
                             stats_html += f"- {esp_nome}: {count_outras}<br>"
                             stats_html += f"&nbsp;&nbsp;&nbsp;&nbsp;indústria: {count_osm_ind}<br>"
                             stats_html += f"&nbsp;&nbsp;&nbsp;&nbsp;comércio: {count_osm_shop}<br>"
                             stats_html += f"&nbsp;&nbsp;&nbsp;&nbsp;outros: {outros_calc}<br>"
                
                stats_html += f"<b>Total de Endereços (Consumidores): {total_consumidores}</b>"
                
                # Adicionar Resumo por Classe com cores (Simplificado)
                if total_consumidores > 0:
                    perc_res = (residenciais / total_consumidores) * 100
                    perc_com = (comerciais / total_consumidores) * 100
                    
                    stats_html += f"<div style='color: green;'><br><b>- Classe Residencial ({perc_res:.1f}%)</b><br>"
                    stats_html += f"Subtotal Residencial: {residenciais}</div>"
                    
                    stats_html += f"<div style='color: red;'><b>- Não Residencial ({perc_com:.1f}%)</b><br>"
                    
                    # Detalhamento de Comercial/Não Residencial com percentuais
                    outros_calc = count_outras - (count_osm_shop + count_osm_ind)
                    if outros_calc < 0: outros_calc = 0
                    
                    # Outros comércios/serviços do CNEFE (espécies 3, 4, 5, 7)
                    outros_cnefe = comerciais - count_outras
                    total_outros_final = outros_calc + outros_cnefe
                    
                    perc_ind = (count_osm_ind / total_consumidores) * 100
                    perc_shop = (count_osm_shop / total_consumidores) * 100
                    perc_outros = (total_outros_final / total_consumidores) * 100
                    
                    stats_html += f"&nbsp;&nbsp;&nbsp;&nbsp;Industrial: {count_osm_ind} ({perc_ind:.1f}%)<br>"
                    stats_html += f"&nbsp;&nbsp;&nbsp;&nbsp;Comercial: {count_osm_shop} ({perc_shop:.1f}%)<br>"
                    stats_html += f"&nbsp;&nbsp;&nbsp;&nbsp;Outros: {total_outros_final} ({perc_outros:.1f}%)<br>"
                    stats_html += f"Subtotal Comercial/Não Residencial: {comerciais}</div>"
            else:
                stats_html += "Sem dados processados para esta área.<br>"

            folium.GeoJson(
                row.geometry,
                style_function=lambda x, color=color: {
                    'fillColor': color, 'color': 'white', 'weight': 1, 'fillOpacity': 0.2
                },
                tooltip=f"Área: {row['NOM'] or cod_id}",
                popup=folium.Popup(stats_html, max_width=300)
            ).add_to(voronoi_group)
        
    # Subestações LIGHT
    if _gdf_sub is not None:
        sub_group = folium.FeatureGroup(name="Subestações LIGHT").add_to(m)
        for i, row in _gdf_sub.iterrows():
            pot = row['POTENCIA_CALCULADA']
            radius = 4 + (pot / 50.0)
            if radius > 15: radius = 15
            nome_se = str(row.get('NOM') or row.get('NOME') or row.get('COD_ID'))
            cod_id = str(row['COD_ID'])
            pot_nom = row.get('POT_NOM', 0) # Tenta pegar a potência nominal original se existir
            
            stats_html = f"<b>SE: {nome_se}</b><br>Potência Calculada: {pot:.2f} MVA<br>"
            if pot_nom > 0:
                stats_html += f"Potência Nominal: {pot_nom:.2f} MVA<br>"
            stats_html += "<br><b>Estatísticas CNEFE na Área:</b><br>"
            
            if _cnefe_stats is not None and cod_id in _cnefe_stats.index:
                row_stats = _cnefe_stats.loc[cod_id]
                total_consumidores = 0
                
                # Categorias para o resumo
                residenciais = 0
                comerciais = 0
                
                # Detalhamento por espécie
                detalhes_res = ""
                detalhes_com = ""
                
                # Variáveis para o detalhamento de "Outras Finalidades"
                count_outras = 0
                count_osm_shop = 0
                count_osm_ind = 0
                
                # Primeiro pass: capturar dados do OSM e Outras Finalidades
                for col in row_stats.index:
                    val = row_stats[col]
                    if col == 'OSM_SHOP':
                        count_osm_shop = int(val)
                    elif col == 'OSM_INDUSTRIAL':
                        count_osm_ind = int(val)
                    elif col == '6':
                        count_outras = int(val)

                # Segundo pass: construir o HTML
                for esp_code in row_stats.index:
                    count = row_stats[esp_code]
                    if count > 0:
                        # Pular colunas do OSM na listagem principal, elas serão usadas no detalhamento da espécie 6
                        if esp_code in ['OSM_SHOP', 'OSM_INDUSTRIAL']:
                            continue
                            
                        try:
                            esp_code_int = int(float(esp_code))
                        except ValueError:
                            continue
                            
                        esp_nome = DIC_ESPECIES.get(esp_code_int, f"Espécie {esp_code}")
                        
                        # Somar ao total se não for espécie 8 (Unidade em Construção)
                        if esp_code_int != 8:
                            total_consumidores += int(count)
                            
                            # Classificação
                            if esp_code_int in [1, 2]:
                                residenciais += int(count)
                                detalhes_res += f"&nbsp;&nbsp;&nbsp;&nbsp;{esp_nome}: {int(count)}<br>"
                            else:
                                comerciais += int(count)
                                if esp_code_int == 6: # Estabelecimento de Outras Finalidades
                                    # Detalhamento customizado no resumo
                                    outros_calc = count_outras - (count_osm_shop + count_osm_ind)
                                    if outros_calc < 0: outros_calc = 0
                                    
                                    detalhes_com += f"&nbsp;&nbsp;&nbsp;&nbsp;{esp_nome}: {count_outras}<br>"
                                    detalhes_com += f"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;indústria: {count_osm_ind}<br>"
                                    detalhes_com += f"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;comércio: {count_osm_shop}<br>"
                                    detalhes_com += f"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;outros: {outros_calc}<br>"
                                else:
                                    detalhes_com += f"&nbsp;&nbsp;&nbsp;&nbsp;{esp_nome}: {int(count)}<br>"
                        
                        # Listagem original
                        if esp_code_int != 6:
                             stats_html += f"- {esp_nome}: {int(count)}<br>"
                        else:
                             outros_calc = count_outras - (count_osm_shop + count_osm_ind)
                             if outros_calc < 0: outros_calc = 0
                             
                             stats_html += f"- {esp_nome}: {count_outras}<br>"
                             stats_html += f"&nbsp;&nbsp;&nbsp;&nbsp;indústria: {count_osm_ind}<br>"
                             stats_html += f"&nbsp;&nbsp;&nbsp;&nbsp;comércio: {count_osm_shop}<br>"
                             stats_html += f"&nbsp;&nbsp;&nbsp;&nbsp;outros: {outros_calc}<br>"
                
                stats_html += f"<b>Total de Endereços (Consumidores): {total_consumidores}</b>"
                
                # Adicionar Resumo por Classe com cores (Simplificado)
                if total_consumidores > 0:
                    perc_res = (residenciais / total_consumidores) * 100
                    perc_com = (comerciais / total_consumidores) * 100
                    
                    stats_html += f"<div style='color: green;'><br><b>- Classe Residencial ({perc_res:.1f}%)</b><br>"
                    stats_html += f"Subtotal Residencial: {residenciais}</div>"
                    
                    stats_html += f"<div style='color: red;'><b>- Classe Comercial / Não Residencial ({perc_com:.1f}%)</b><br>"
                    
                    # Detalhamento de Comercial/Não Residencial com percentuais
                    outros_calc = count_outras - (count_osm_shop + count_osm_ind)
                    if outros_calc < 0: outros_calc = 0
                    
                    # Outros comércios/serviços do CNEFE (espécies 3, 4, 5, 7)
                    outros_cnefe = comerciais - count_outras
                    total_outros_final = outros_calc + outros_cnefe
                    
                    perc_ind = (count_osm_ind / total_consumidores) * 100
                    perc_shop = (count_osm_shop / total_consumidores) * 100
                    perc_outros = (total_outros_final / total_consumidores) * 100
                    
                    stats_html += f"&nbsp;&nbsp;&nbsp;&nbsp;Industrial: {count_osm_ind} ({perc_ind:.1f}%)<br>"
                    stats_html += f"&nbsp;&nbsp;&nbsp;&nbsp;Comercial: {count_osm_shop} ({perc_shop:.1f}%)<br>"
                    stats_html += f"&nbsp;&nbsp;&nbsp;&nbsp;Outros: {total_outros_final} ({perc_outros:.1f}%)<br>"
                    stats_html += f"Subtotal Comercial/Não Residencial: {comerciais}</div>"
            else:
                stats_html += "Sem dados processados.<br>"

            folium.CircleMarker(
                location=[row.geometry.y, row.geometry.x],
                radius=radius, color='white', weight=1, fill=True, fill_color='red', fill_opacity=1,
                popup=folium.Popup(stats_html, max_width=300),
                tooltip=f"SE: {nome_se}"
            ).add_to(sub_group)

    # Controles
    Fullscreen().add_to(m)
    MousePosition().add_to(m)
    MeasureControl().add_to(m)
    folium.LayerControl(collapsed=True).add_to(m)
    
    return m

def main():
    st.title("⚡ Big Data: LIGHT & CNEFE RJ (Otimizado)")
    st.markdown("""
    Visualização de **Big Data** com dados pré-processados. 
    Clique nas áreas de Voronoi ou nas subestações para ver a **contagem de espécies** de endereços.
    """)
    
    if not os.path.exists(ARQUIVO_STATS):
        st.warning("⚠️ Arquivos pré-processados não encontrados! Execute `python pre_process_cnefe.py` primeiro.")
        return

    with st.spinner("Carregando mapa e estatísticas..."):
        try:
            rj_shape, gdf_sub, voronoi_recortado = get_light_data()
            cnefe_stats = get_preprocessed_cnefe()
            
            m = create_map_object(rj_shape, gdf_sub, voronoi_recortado, cnefe_stats)
            
            st_folium(m, width=1200, height=700, returned_objects=[], use_container_width=True)
            
        except Exception as e:
            st.error(f"Erro ao carregar mapa: {e}")
            st.exception(e)

if __name__ == "__main__":
    main()
