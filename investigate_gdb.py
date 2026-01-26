
import geopandas as gpd
import pandas as pd

CAMINHO_GDB = r"LIGHT_382_2021-09-30_M10_20231218-2133.gdb"
NOME_CAMADA_SUB = 'SUB' 
NOME_CAMADA_TR = 'UNTRS'

def investigate():
    print("DEBUG: Carregando dados do GDB...")
    try:
        gdf_sub = gpd.read_file(CAMINHO_GDB, layer=NOME_CAMADA_SUB)
        gdf_tr = gpd.read_file(CAMINHO_GDB, layer=NOME_CAMADA_TR)
        
        print(f"DEBUG: Colunas em SUB: {gdf_sub.columns.tolist()}")
        print(f"DEBUG: Colunas em UNTRS: {gdf_tr.columns.tolist()}")
        
        potencia_por_sub = gdf_tr.groupby('SUB')['POT_NOM'].sum().reset_index()
        potencia_por_sub.columns = ['COD_ID', 'POTENCIA_CALCULADA']
        
        gdf_sub_merged = gdf_sub.merge(potencia_por_sub, on='COD_ID', how='left')
        
        subs_sem_tr = gdf_sub_merged[gdf_sub_merged['POTENCIA_CALCULADA'].isna()]
        print(f"DEBUG: Subestações sem correspondência em UNTRS: {len(subs_sem_tr)}")
        if len(subs_sem_tr) > 0:
            print("DEBUG: Exemplo de subs sem correspondência:")
            print(subs_sem_tr[['COD_ID', 'NOM']].head(10))

        # Verificar se os IDs batem em termos de tipo (string vs int)
        print(f"DEBUG: Tipo de COD_ID em SUB: {gdf_sub['COD_ID'].dtype}")
        print(f"DEBUG: Tipo de SUB em UNTRS: {gdf_tr['SUB'].dtype}")
        
        print(f"DEBUG: Exemplo de COD_ID em SUB: {gdf_sub['COD_ID'].iloc[0]}")
        print(f"DEBUG: Exemplo de SUB em UNTRS: {gdf_tr['SUB'].iloc[0]}")

        # Verificar se há IDs em UNTRS que não estão em SUB
        subs_em_tr = set(gdf_tr['SUB'].unique())
        subs_em_sub = set(gdf_sub['COD_ID'].unique())
        
        ids_faltando_em_sub = subs_em_tr - subs_em_sub
        print(f"DEBUG: IDs em UNTRS que não existem em SUB: {len(ids_faltando_em_sub)}")
        if len(ids_faltando_em_sub) > 0:
            print(f"DEBUG: Exemplo de IDs faltando: {list(ids_faltando_em_sub)[:5]}")

        # Verificar se há espaços ou diferenças sutis
        exemplo_tr_id = str(gdf_tr['SUB'].iloc[0])
        exemplo_sub_id = str(gdf_sub['COD_ID'].iloc[0])
        print(f"DEBUG: Representação do ID em TR: '{exemplo_tr_id}'")
        print(f"DEBUG: Representação do ID em SUB: '{exemplo_sub_id}'")

        # Verificar se há subestações em SUB que estão fora do Rio de Janeiro
        import geobr
        rj_shape = geobr.read_municipality(code_muni=3304557, year=2020)
        rj_shape = rj_shape.to_crs("EPSG:4326")
        
        gdf_sub = gdf_sub.to_crs("EPSG:4326")
        gdf_sub_rj = gpd.clip(gdf_sub, rj_shape)
        
        print(f"DEBUG: Total de subestações no RJ: {len(gdf_sub_rj)}")
        
        gdf_sub_rj_merged = gdf_sub_rj.merge(potencia_por_sub, on='COD_ID', how='left')
        subs_rj_sem_tr = gdf_sub_rj_merged[gdf_sub_rj_merged['POTENCIA_CALCULADA'].isna()]
        print(f"DEBUG: Subestações no RJ sem correspondência em UNTRS: {len(subs_rj_sem_tr)}")
        if len(subs_rj_sem_tr) > 0:
            print("DEBUG: Exemplo de subs no RJ sem correspondência:")
            print(subs_rj_sem_tr[['COD_ID', 'NOM']].head(10))

        # Verificar se há outras camadas que podem conter transformadores
        import fiona
        layers = fiona.listlayers(CAMINHO_GDB)
        print(f"DEBUG: Camadas disponíveis no GDB: {layers}")

        # Verificar UNTRD (Unidades Transformadoras de Distribuição?)
        if 'UNTRD' in layers:
            gdf_untrd = gpd.read_file(CAMINHO_GDB, layer='UNTRD')
            print(f"DEBUG: Total em UNTRD: {len(gdf_untrd)}")
            if 'SUB' in gdf_untrd.columns:
                pot_untrd = gdf_untrd.groupby('SUB')['POT_NOM'].sum().reset_index()
                print(f"DEBUG: Subestações encontradas em UNTRD: {len(pot_untrd)}")
                
                # Ver se alguma das subs que faltavam em UNTRS está em UNTRD
                subs_faltando = subs_rj_sem_tr['COD_ID'].unique()
                encontradas_em_untrd = pot_untrd[pot_untrd['SUB'].isin(subs_faltando)]
                print(f"DEBUG: Subestações que faltavam e foram encontradas em UNTRD: {len(encontradas_em_untrd)}")

        # Verificar EQTRS (Equipamentos de Transformação de Subestação?)
        if 'EQTRS' in layers:
            gdf_eqtrs = gpd.read_file(CAMINHO_GDB, layer='EQTRS')
            print(f"DEBUG: Total em EQTRS: {len(gdf_eqtrs)}")
            # Verificar colunas de EQTRS
            print(f"DEBUG: Colunas em EQTRS: {gdf_eqtrs.columns.tolist()}")
            
            # Tentar encontrar relação com SUB
            # Geralmente EQTRS tem uma chave estrangeira para UNTRS, e UNTRS para SUB
            # Ou EQTRS pode ter direto o campo SUB ou similar.
            
            # Verificar se COD_ID de EQTRS bate com COD_ID de UNTRS
            ids_eqtrs = set(gdf_eqtrs['COD_ID'].unique())
            ids_untrs = set(gdf_tr['COD_ID'].unique())
            intersecao = ids_eqtrs.intersection(ids_untrs)
            print(f"DEBUG: Interseção entre EQTRS e UNTRS (COD_ID): {len(intersecao)}")
            
            if len(intersecao) > 0:
                # Se batem, podemos pegar a potência de EQTRS e levar para UNTRS
                gdf_tr_merged = gdf_tr.merge(gdf_eqtrs[['COD_ID', 'POT_NOM']], on='COD_ID', how='left', suffixes=('', '_EQ'))
                print(f"DEBUG: UNTRS com POT_NOM original zero mas com POT_NOM em EQTRS: {len(gdf_tr_merged[(gdf_tr_merged['POT_NOM'] == 0) & (gdf_tr_merged['POT_NOM_EQ'] > 0)])}")

    except Exception as e:
        print(f"DEBUG ERROR: {e}")

if __name__ == "__main__":
    investigate()
