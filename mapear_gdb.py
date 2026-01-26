import geopandas as gpd
import pandas as pd
import fiona
import numpy as np

# --- CONFIGURAÇÃO ---
CAMINHO_GDB = r"LIGHT_382_2021-09-30_M10_20231218-2133.gdb"

# Dicionário das camadas e colunas que queremos investigar para a IA
# Formato: 'Nome_Camada': ['Coluna_ID', 'Coluna_Potencia', 'Coluna_Carga']
CAMADAS_ALVO = {
    'SUB': ['COD_ID', 'NOM', 'geometry'],               # O Centro Geográfico
    'UNTRS': ['SUB', 'POT_NOM', 'ENET_01'],             # Carga Real (Alta Tensão)
    'EQTRS': ['COD_ID', 'POT_NOM', 'PAC_1'],            # Capacidade Física (Peso W)
    'UNTRD': ['SUB', 'POT_NOM', 'ENET_01'],             # Carga Real (Distribuição)
    'UGMT_tab': ['SUB', 'POT_INST', 'ENE_01']           # Geração Distribuída (O Risco)
}

def analisar_dados():
    print(f"🔬 INICIANDO ANÁLISE DE INTEGRIDADE: {CAMINHO_GDB}\n")
    
    # Lista de todos os IDs de subestações reais para validar cruzamentos
    ids_validos = set()

    # 1. Primeiro carregamos a SUB para ter a referência
    try:
        print(f"{'='*60}")
        print(f"📂 Lendo Camada Mestre: SUB")
        gdf_sub = gpd.read_file(CAMINHO_GDB, layer='SUB')
        
        # Guardar IDs para validação (convertendo para string para garantir)
        ids_validos = set(gdf_sub['COD_ID'].astype(str))
        
        print(f"✅ Total de Subestações encontradas: {len(gdf_sub)}")
        print(f"👀 Exemplo de dados:\n{gdf_sub[['COD_ID', 'NOM']].head(3).to_string(index=False)}")
        print(f"{'='*60}\n")
    except Exception as e:
        print(f"❌ Erro crítico ao ler SUB: {e}")
        return

    # 2. Loop pelas outras camadas
    for camada, colunas in CAMADAS_ALVO.items():
        if camada == 'SUB': continue # Já lemos
        
        print(f"📂 Analisando Camada: {camada}")
        try:
            # Ler a camada (se for _tab, o geopandas lê como tabela sem geometria)
            # ignore_geometry=True acelera a leitura se não precisarmos desenhar agora
            df = gpd.read_file(CAMINHO_GDB, layer=camada, ignore_geometry=True)
            
            # Filtra apenas colunas que existem (para evitar erro se nome mudou)
            cols_existentes = [c for c in colunas if c in df.columns]
            df_filtrado = df[cols_existentes].copy()
            
            # --- RELATÓRIO DE AMOSTRA ---
            print(f"📊 Primeiros 5 registros:")
            print(df_filtrado.head().to_string(index=False))
            
            # --- RELATÓRIO DE QUALIDADE ---
            print(f"\n⚠️  Relatório de Falhas ({len(df)} registros totais):")
            for col in cols_existentes:
                # Converter para numérico para achar zeros (força erros a virarem NaN)
                if 'POT' in col or 'ENE' in col:
                    df_filtrado[col] = pd.to_numeric(df_filtrado[col], errors='coerce')
                
                qtd_nan = df_filtrado[col].isna().sum()
                # Conta zeros apenas se for numérico
                if pd.api.types.is_numeric_dtype(df_filtrado[col]):
                    qtd_zeros = (df_filtrado[col] == 0).sum()
                    txt_zeros = f"| Zeros: {qtd_zeros} ({(qtd_zeros/len(df))*100:.1f}%)"
                else:
                    txt_zeros = ""
                
                print(f"   > Coluna '{col}': NaNs: {qtd_nan} {txt_zeros}")

            # --- VERIFICAÇÃO DE VÍNCULO (CRUCIAL PARA O VORONOI) ---
            if 'SUB' in df.columns:
                # Converte para string para comparar com o set ids_validos
                df['SUB_STR'] = df['SUB'].astype(str)
                orfaos = df[~df['SUB_STR'].isin(ids_validos)]
                qtd_orfaos = len(orfaos)
                print(f"\n🔗 Integridade de Vínculo:")
                print(f"   > Registros com 'SUB' que NÃO existem na camada SUB: {qtd_orfaos}")
                if qtd_orfaos > 0:
                    print(f"   > Exemplo de IDs órfãos: {orfaos['SUB'].unique()[:5]}")
            
            print(f"{'-'*60}\n")

        except Exception as e:
            print(f"❌ Erro ao ler {camada}: {e}\n")

if __name__ == "__main__":
    analisar_dados()