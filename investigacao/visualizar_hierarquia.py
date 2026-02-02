"""
Este script gera uma visualização interativa da hierarquia entre subestações.
Ele combina dados de:
1. ONS -> Subestações Plenas (via PACs Externos e Nomes)
2. Subestações Plenas -> Subestações Satélites (via Circuitos MT)
3. Conexões de Alta Tensão (via Topologia SSDAT)
4. Integração ENEL e LIGHT via Rede Básica ONS
"""

import pandas as pd
from pyvis.network import Network
import os
import re

def carregar_conexoes_ons():
    """Mapeia conexões ONS -> Distribuidoras e ONS -> ONS usando Linhas e Subestações."""
    path_light = "investigacao/origem_at_plenas.csv"
    path_enel = "investigacao/conexoes_enel_ons.csv"
    path_ons_lt = "Dados Brutos/ONS/LINHA_TRANSMISSAO.csv"
    path_ons_sub = "Dados Brutos/ONS/SUBESTACAO.csv"
    
    if not os.path.exists(path_ons_lt) or not os.path.exists(path_ons_sub):
        print("DEBUG ERROR: Arquivos ONS não encontrados.")
        return pd.DataFrame()

    print("DEBUG: Carregando dados ONS (Linhas e Subestações)...")
    df_lt = pd.read_csv(path_ons_lt, sep=';', encoding='latin1')
    df_sub = pd.read_csv(path_ons_sub, sep=';', encoding='latin1')
    
    # Filtrar ONS para Rio de Janeiro para o Backbone
    df_lt_rj = df_lt[(df_lt['nom_estado_de'] == 'RIO DE JANEIRO') | (df_lt['nom_estado_para'] == 'RIO DE JANEIRO')].copy()
    
    # Criar mapeamento de Barra -> Subestação ONS (Priorizando RJ para evitar falsos positivos)
    barra_para_ons = {}
    
    # 1. Primeiro mapeia tudo que é do RJ
    df_sub_rj = df_sub[df_sub['nom_estado'] == 'Rio de Janeiro']
    for _, row in df_sub_rj.iterrows():
        if pd.isna(row['num_barra']): continue
        barra_para_ons[str(int(row['num_barra']))] = row['nom_subestacao']
        
    # 2. Depois mapeia o resto (sem sobrescrever RJ)
    for _, row in df_sub.iterrows():
        if pd.isna(row['num_barra']): continue
        b_str = str(int(row['num_barra']))
        if b_str not in barra_para_ons:
            barra_para_ons[b_str] = row['nom_subestacao']

    conexoes = []

    # 1. ONS -> ONS (Backbone no RJ)
    for _, row in df_lt_rj.iterrows():
        conexoes.append({
            'MAE': f"ONS: {row['nom_subestacao_de']}",
            'FILHA': f"ONS: {row['nom_subestacao_para']}",
            'DISTRIBUIDORA': 'ONS',
            'TIPO': 'Rede Básica (Backbone)'
        })

    # 2. ONS -> LIGHT (Incluindo 138kV como Santa Cecília)
    if os.path.exists(path_light):
        df_light = pd.read_csv(path_light, sep=';')
        for _, row in df_light.iterrows():
            pacs_ext = str(row['VIZINHOS_AT']).split(',')
            for pac in pacs_ext:
                if 'EXTERNO:AT_' in pac:
                    num_barra = pac.replace('EXTERNO:AT_', '').strip()
                    nome_ons = barra_para_ons.get(num_barra)
                    
                    if nome_ons:
                        # Se for um gerador conhecido (ex: Simplicio, Nilo Peçanha)
                        tipo = 'Rede Básica -> LIGHT'
                        if num_barra in ['9501', '251', '252']:
                            tipo = 'Gerador Regional (138kV) -> LIGHT'
                        
                        conexoes.append({
                            'MAE': f"ONS: {nome_ons}",
                            'FILHA': row['NOME'],
                            'DISTRIBUIDORA': 'LIGHT',
                            'TIPO': tipo
                        })

    # 3. ONS -> ENEL
    if os.path.exists(path_enel):
        df_enel = pd.read_csv(path_enel, sep=';')
        for _, row in df_enel.iterrows():
            conexoes.append({
                'MAE': f"ONS: {row['SUB_ONS']}",
                'FILHA': row['SUB_LOCAL'],
                'DISTRIBUIDORA': 'ENEL',
                'TIPO': 'Rede Básica -> ENEL'
            })
    
    return pd.DataFrame(conexoes).drop_duplicates()

def gerar_arvore_interativa_total():
    path_at = "Organizar/vinculos_maes_filhas.csv"
    path_mt = "Organizar/hierarquia_via_circuitos.csv"
    output_html = "Organizar/HIERARQUIA_SUBESTACOES_INTERATIVO.html"
    
    dfs = []
    
    # 1. Nível ONS e Fronteiras
    df_ons = carregar_conexoes_ons()
    if not df_ons.empty:
        dfs.append(df_ons)
    
    # 2. Nível Alta Tensão (Topologia)
    if os.path.exists(path_at):
        print(f"DEBUG: Lendo dados AT de {path_at}...")
        df_at = pd.read_csv(path_at, sep=';')
        df_at = df_at[['MAE', 'FILHA', 'DISTRIBUIDORA']].copy()
        df_at['TIPO'] = 'Alta Tensão (Topologia)'
        dfs.append(df_at)
    
    # 3. Nível Média Tensão (Circuitos)
    if os.path.exists(path_mt):
        print(f"DEBUG: Lendo dados MT de {path_mt}...")
        df_mt = pd.read_csv(path_mt, sep=';')
        df_mt = df_mt[['MAE', 'FILHA']].copy()
        df_mt['DISTRIBUIDORA'] = 'LIGHT'
        df_mt['TIPO'] = 'Média Tensão (Circuitos)'
        dfs.append(df_mt)

    if not dfs:
        print("DEBUG ERROR: Nenhum dado de hierarquia encontrado.")
        return

    # Combinar tudo
    df_final = pd.concat(dfs).drop_duplicates(subset=['MAE', 'FILHA'])
    
    # Criar a rede interativa
    net = Network(height="850px", width="100%", bgcolor="#222222", font_color="white", directed=True)
    
    # Configurações de física
    net.barnes_hut(gravity=-10000, central_gravity=0.3, spring_length=200)
    
    print(f"DEBUG: Processando {len(df_final)} conexões totais...")
    
    # Adicionar nós únicos
    subs_unicas = set(df_final['MAE'].dropna().tolist() + df_final['FILHA'].dropna().tolist())
    
    for sub in subs_unicas:
        # Determinar cor e tamanho baseado na função e distribuidora
        if sub.startswith("ONS:"):
            color = "#9933ff" # Roxo para ONS
            size = 45
            label = sub.replace("ONS: ", "")
        else:
            # Tentar descobrir a distribuidora do nó
            row_dist = df_final[(df_final['MAE'] == sub) | (df_final['FILHA'] == sub)]
            dist = row_dist['DISTRIBUIDORA'].iloc[0] if not row_dist.empty else 'DESCONHECIDA'
            
            is_mae = sub in df_final['MAE'].values
            is_filha = sub in df_final['FILHA'].values
            
            if dist == 'ENEL':
                color = "#00ff00" # Verde para ENEL
                size = 25 if is_mae else 15
            elif dist == 'LIGHT':
                if is_mae and not is_filha:
                    color = "#ff4444" # Vermelho para Raízes LIGHT
                    size = 30
                elif is_mae and is_filha:
                    color = "#ffcc00" # Amarelo para Intermediárias LIGHT
                    size = 20
                else:
                    color = "#00ccff" # Azul para Pontas LIGHT
                    size = 15
            else:
                color = "#ffffff"
                size = 10
            label = sub
            
        net.add_node(sub, label=label, color=color, size=size, title=f"Subestação: {sub}")

    # Adicionar conexões
    for _, row in df_final.iterrows():
        if pd.isna(row['MAE']) or pd.isna(row['FILHA']): continue
        
        if 'Gerador' in row['TIPO']:
            color_edge = "#00ffcc" # Ciano para Geradores Regionais
            width = 4
        elif 'Rede Básica' in row['TIPO']:
            color_edge = "#9933ff"
            width = 3
        elif "Alta" in row['TIPO']:
            color_edge = "#ffffff"
            width = 2
        else:
            color_edge = "#aaaaaa"
            width = 1
            
        net.add_edge(row['MAE'], row['FILHA'], title=f"{row['TIPO']} ({row['DISTRIBUIDORA']})", color=color_edge, width=width)

    # Salvar
    print(f"DEBUG: Salvando visualização em {output_html}...")
    net.save_graph(output_html)
    print("DEBUG: Sucesso! A árvore hierárquica TOTAL (ONS -> ENEL/LIGHT) foi gerada.")

if __name__ == "__main__":
    gerar_arvore_interativa_total()
