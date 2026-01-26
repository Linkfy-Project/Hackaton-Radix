"""
Este script gera um diagrama de Sankey para visualizar a hierarquia sistêmica de impacto (GD Solar).
Ele mapeia o fluxo de potência gerada desde os setores (filhas) até as interfaces e, finalmente, para a Rede Básica.
Utiliza a biblioteca Plotly para uma visualização interativa e profissional.
"""

import pandas as pd
import geopandas as gpd
import os
import plotly.graph_objects as go

def gerar_visualizacao_vitoria():
    print("DEBUG: 🎨 Iniciando geração do Diagrama de Sankey...")
    
    # 1. Carregar dados com verificação de existência
    arquivos = {
        "gdf": "DATASET_IA_RADIX_FINAL.geojson",
        "df_int": "lista_interfaces_ons.csv",
        "df_h": "vinculos_maes_filhas.csv"
    }
    
    for nome, path in arquivos.items():
        if not os.path.exists(path):
            print(f"DEBUG: ❌ Erro: Arquivo {path} não encontrado!")
            return

    print("DEBUG: Carregando arquivos...")
    gdf = gpd.read_file(arquivos["gdf"])
    df_int = pd.read_csv(arquivos["df_int"])
    df_h = pd.read_csv(arquivos["df_h"])

    # Normalizar IDs para string
    gdf['COD_ID'] = gdf['COD_ID'].astype(str)
    df_int['SUB'] = df_int['SUB'].astype(str)
    df_h['SUB_MAE'] = df_h['SUB_MAE'].astype(str)
    df_h['SUB_FILHA_ORFA'] = df_h['SUB_FILHA_ORFA'].astype(str)

    root_label = "REDE BÁSICA (ONS/IPE)"
    
    # Definir qual coluna de nome usar (NOM ou COD_ID)
    nome_col = 'NOM' if 'NOM' in gdf.columns else 'COD_ID'
    
    # Pegar as interfaces que aparecem no nosso dataset
    interfaces_presentes = df_int[df_int['SUB'].isin(gdf['COD_ID'].values)]
    
    # Listas para o Sankey
    sources = []
    targets = []
    values = []
    labels = []
    
    # Dicionário para mapear labels para índices
    label_to_index = {}

    def get_node_index(label):
        if label not in label_to_index:
            label_to_index[label] = len(labels)
            labels.append(label)
        return label_to_index[label]

    root_idx = get_node_index(root_label)

    print(f"DEBUG: Processando {len(interfaces_presentes['SUB'].unique()[:10])} interfaces...")

    for sub_id in interfaces_presentes['SUB'].unique()[:10]: # Limitado para 10 interfaces para clareza
        # Busca o nome da SE
        match_se = gdf[gdf['COD_ID'] == sub_id]
        nome_se = match_se[nome_col].iloc[0] if not match_se.empty else f"SE {sub_id}"
        int_label = f"INTERFACE: {nome_se}"
        int_idx = get_node_index(int_label)
        
        # Fluxo Interface -> Rede Básica
        # Somamos a geração da interface
        pot_interface = match_se['POT_GERADA_KW'].sum() / 1000 # Convertendo para MW
        
        # Fluxo Bairros (Filhas) -> Interface
        filhas = df_h[df_h['SUB_MAE'] == sub_id]['SUB_FILHA_ORFA'].tolist()
        total_pot_filhas = 0
        
        for f_id in filhas[:5]: # Top 5 filhas por interface
            match_f = gdf[gdf['COD_ID'] == f_id]
            if not match_f.empty:
                nome_f = match_f[nome_col].iloc[0]
                pot_f = match_f['POT_GERADA_KW'].iloc[0] / 1000
                f_label = f"SETOR: {nome_f}"
                f_idx = get_node_index(f_label)
                
                sources.append(f_idx)
                targets.append(int_idx)
                values.append(max(0.01, pot_f))
                total_pot_filhas += pot_f

        # O fluxo da interface para a rede básica deve ser pelo menos a soma das filhas + sua própria geração
        fluxo_total_interface = max(0.1, pot_interface + total_pot_filhas)
        
        sources.append(int_idx)
        targets.append(root_idx)
        values.append(fluxo_total_interface)

    if not sources:
        print("DEBUG: ⚠️ Nenhuma conexão encontrada. Verifique os dados.")
        return

    print("DEBUG: Criando figura Plotly...")
    # Criar o Diagrama de Sankey
    fig = go.Figure(data=[go.Sankey(
        node = dict(
          pad = 15,
          thickness = 20,
          line = dict(color = "black", width = 0.5),
          label = labels,
          color = "skyblue"
        ),
        link = dict(
          source = sources,
          target = targets,
          value = values,
          color = "rgba(255, 165, 0, 0.4)" # Laranja semi-transparente para os fluxos
      ))])

    fig.update_layout(
        title_text="Fluxo Hierárquico de Impacto Sistêmico (Geração Distribuída Solar - MW)",
        font_size=10,
        width=1200,
        height=800
    )

    # Salvar como HTML (interativo) e tentar exportar imagem se possível
    output_html = "HIERARQUIA_SANKKEY_INTERATIVO.html"
    fig.write_html(output_html)
    print(f"DEBUG: ✅ Sucesso! Arquivo interativo '{output_html}' gerado.")
    
    # Tentar salvar como imagem estática (requer kaleido)
    try:
        fig.write_image("HIERARQUIA_TECNICA_FINAL.png")
        print("DEBUG: ✅ Imagem estática 'HIERARQUIA_TECNICA_FINAL.png' gerada.")
    except Exception as e:
        print(f"DEBUG: ⚠️ Não foi possível gerar a imagem estática (requer 'kaleido'): {e}")

if __name__ == "__main__":
    gerar_visualizacao_vitoria()
