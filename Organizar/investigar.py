import geopandas as gpd
import pandas as pd

CAMINHO_GDB = r"LIGHT_382_2021-09-30_M10_20231218-2133.gdb"

def super_investigacao_hierarquia():
    print("🕵️‍♂️ INICIANDO SUPER INVESTIGAÇÃO DE TOPOLOGIA...")

    # 1. Carregar Potências Reais (UNTRS) para identificar as "Mães"
    print("⚡ Identificando Subestações com potência real (Mães)...")
    untrs = gpd.read_file(CAMINHO_GDB, layer='UNTRS', ignore_geometry=True, columns=['SUB', 'POT_NOM'])
    untrs['SUB'] = untrs['SUB'].astype(str).str.strip().str.replace('.0', '', regex=False)
    # Lista de IDs que têm transformador de potência
    lista_maes = untrs[untrs['POT_NOM'] > 0]['SUB'].unique().tolist()

    # 2. Carregar Conectividade (SSDMT)
    print("🛣️  Lendo trechos de média tensão...")
    ssdmt = gpd.read_file(CAMINHO_GDB, layer='SSDMT', ignore_geometry=True, columns=['SUB', 'CTMT'])
    ssdmt['SUB'] = ssdmt['SUB'].astype(str).str.strip().str.replace('.0', '', regex=False)
    ssdmt['CTMT'] = ssdmt['CTMT'].astype(str).str.strip()

    # 3. Agrupar por Alimentador e analisar os membros
    print("🔍 Analisando DNA dos alimentadores...")
    relacoes = []
    
    for alim, grupo in ssdmt.groupby('CTMT'):
        subs_no_alim = grupo['SUB'].unique().tolist()
        
        if len(subs_no_alim) > 1:
            # Identificar quem neste grupo é "Mãe" (tem potência) e quem é "Filha" (está vazia)
            maes_no_alim = [s for s in subs_no_alim if s in lista_maes]
            filhas_no_alim = [s for s in subs_no_alim if s not in lista_maes]
            
            if maes_no_alim and filhas_no_alim:
                for f in filhas_no_alim:
                    relacoes.append({
                        'ALIMENTADOR': alim,
                        'SUB_MAE': maes_no_alim[0], # Pega a primeira mãe encontrada
                        'SUB_FILHA_ORFA': f
                    })

    df_hierarquia = pd.DataFrame(relacoes)

    print("\n" + "="*50)
    print(f"📊 RESULTADO DA PERÍCIA")
    if not df_hierarquia.empty:
        print(f"Foram encontradas {len(df_hierarquia)} subestações 'órfãs' que têm uma 'mãe' no mesmo cabo!")
        print("="*50)
        print("\nExemplos de Vínculos Descobertos:")
        print(df_hierarquia.head(20).to_string(index=False))
        
        # Salvar para usar no dataset final
        df_hierarquia.to_csv("vinculos_maes_filhas.csv", index=False)
    else:
        print("Nenhum vínculo direto 'Mãe-Filha' encontrado nos mesmos alimentadores.")
        print("Isso reforça que os IDs estão realmente isolados no sistema.")

if __name__ == "__main__":
    super_investigacao_hierarquia()