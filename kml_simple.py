"""
Este script lê dados de subestações de um arquivo GDB, calcula a potência total somando os transformadores
associados (camadas UNTRAT e UNTRMT) e gera um arquivo KML estilizado com ícones e escala baseada na potência.
"""
import geopandas as gpd
import simplekml
import pandas as pd

# --- CONFIGURAÇÕES ---
# Caminho para o arquivo Geodatabase (GDB)
CAMINHO_GDB = r"ENEL_RJ_383_2022-09-30_V10_20240605-0611.gdb"
# Nome das camadas
NOME_CAMADA_SUB = 'SUB' 
NOME_CAMADA_TR_AT = 'UNTRAT'
NOME_CAMADA_TR_MT = 'UNTRMT'
# Nome do arquivo KML de saída
ARQUIVO_SAIDA = "subestacoes_com_icones.kml"

# Link de um ícone de raio (padrão do Google)
URL_ICONE = "https://cdn-icons-png.freepik.com/256/2550/2550433.png"

def gerar_kml_estilizado() -> None:
    """
    Lê a camada de subestações e transformadores, calcula a potência total por SE e salva um KML.
    """
    print("DEBUG: Iniciando carregamento de dados...")
    try:
        # Carregar subestações (geometria)
        gdf_sub = gpd.read_file(CAMINHO_GDB, layer=NOME_CAMADA_SUB)
        print(f"DEBUG: {len(gdf_sub)} subestações carregadas.")

        # Carregar transformadores AT e MT para obter a potência
        print("DEBUG: Carregando dados de potência dos transformadores...")
        gdf_tr_at = gpd.read_file(CAMINHO_GDB, layer=NOME_CAMADA_TR_AT)
        gdf_tr_mt = gpd.read_file(CAMINHO_GDB, layer=NOME_CAMADA_TR_MT)

        # Combinar as tabelas de transformadores
        # Precisamos das colunas 'SUB', 'POT_NOM' e 'TIP_UNID'
        df_pot_at = gdf_tr_at[['SUB', 'POT_NOM', 'TIP_UNID']].copy()
        df_pot_mt = gdf_tr_mt[['SUB', 'POT_NOM', 'TIP_UNID']].copy()
        df_pot_total = pd.concat([df_pot_at, df_pot_mt])

        # Somar potência por subestação
        # Nota: 'SUB' no transformador corresponde ao 'COD_ID' na camada SUB
        # Filtramos apenas transformadores com TIP_UNID == 54 ou 41 (Transformadores de SE)
        # Se não houver nenhum desses, pegamos o maior valor de POT_NOM associado à SE
        
        def calcular_potencia_se(group):
            # Tenta filtrar pelos tipos conhecidos de transformadores de SE (54 e 41)
            # Convertemos para string para evitar problemas de tipo
            se_trafos = group[group['TIP_UNID'].astype(str).isin(['54', '41'])]
            if not se_trafos.empty:
                return se_trafos['POT_NOM'].sum()
            else:
                # Se não achar transformadores de SE, pega o maior transformador de distribuição
                # (Isso evita somar milhares de trafos de poste, o que daria um valor irreal)
                return group['POT_NOM'].max()

        potencia_por_sub = df_pot_total.groupby('SUB').apply(calcular_potencia_se).reset_index()
        potencia_por_sub.columns = ['COD_ID', 'POTENCIA_CALCULADA']

        # Mesclar a potência calculada de volta no GeoDataFrame das subestações
        gdf = gdf_sub.merge(potencia_por_sub, on='COD_ID', how='left')
        gdf['POTENCIA_CALCULADA'] = gdf['POTENCIA_CALCULADA'].fillna(0)

        print(f"DEBUG: Potência calculada e mesclada para {len(gdf)} subestações.")

    except Exception as e:
        print(f"DEBUG: Erro ao processar dados do GDB: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # Converter para Lat/Lon (WGS84) se necessário para o KML
    if gdf.crs and gdf.crs.to_string() != 'EPSG:4326':
        print(f"DEBUG: Convertendo CRS de {gdf.crs} para EPSG:4326")
        gdf = gdf.to_crs("EPSG:4326")

    # Criar objeto KML
    kml = simplekml.Kml()
    print(f"Gerando KML para {len(gdf)} subestações...")

    for index, row in gdf.iterrows():
        # Tenta pegar o nome da subestação (coluna NOME ou COD_ID)
        nome_se = row.get('NOME') or row.get('COD_ID') or f"Subestação {index}"
        nome = str(nome_se)
        
        # Cria o ponto no KML
        pnt = kml.newpoint(name=nome)
        
        # Geometria
        if row.geometry is None:
            print(f"DEBUG: Geometria nula para {nome}")
            continue

        if row.geometry.geom_type in ['Polygon', 'MultiPolygon']:
            centroide = row.geometry.centroid
            pnt.coords = [(centroide.x, centroide.y)]
        elif row.geometry.geom_type == 'Point':
            pnt.coords = [(row.geometry.x, row.geometry.y)]
        else:
            centroide = row.geometry.centroid
            pnt.coords = [(centroide.x, centroide.y)]
        
        # Potência calculada
        potencia = row['POTENCIA_CALCULADA']
        pnt.description = f"Potência Total: {potencia:.2f} MVA\nID: {row.get('COD_ID', 'N/A')}\nDescrição: {row.get('DESCR', '')}"
        
        # --- ESTILIZAÇÃO ---
        pnt.style.iconstyle.icon.href = URL_ICONE
        pnt.style.iconstyle.color = simplekml.Color.yellow
        
        # Escala dinâmica baseada na potência
        try:
            val_potencia = float(potencia)
            # Ajuste da escala para ser visível mas não exagerada
            # Se potência for 0, escala mínima de 0.8
            scale = 0.8 + (val_potencia / 200.0) 
            if scale > 4.0: scale = 4.0 
            pnt.style.iconstyle.scale = scale
        except (ValueError, TypeError):
            pnt.style.iconstyle.scale = 1.0

    # Salva o arquivo final
    try:
        kml.save(ARQUIVO_SAIDA)
        print(f"Arquivo '{ARQUIVO_SAIDA}' salvo com sucesso!")
    except Exception as e:
        print(f"DEBUG: Erro ao salvar o arquivo KML: {e}")

if __name__ == "__main__":
    gerar_kml_estilizado()
