import fiona
import pandas as pd
import os
import glob
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor
import time
import multiprocessing

# --- CONFIGURAÇÕES ---
PASTA_DADOS_BRUTOS = "Dados Brutos"
PASTA_SAIDA = "Dados Processados"
ARQUIVO_SAIDA = os.path.join(PASTA_SAIDA, "perfis_consumo.csv")

def simplificar_classe(clas_sub: str) -> str:
    """
    DEBUG: Classifica a Unidade Consumidora com base na coluna CLAS_SUB (Padrão ANEEL).
    """
    if not clas_sub:
        return "OUTROS"
    
    c = str(clas_sub).upper().strip()
    
    # Regras baseadas no Manual da BDGD (Módulo 10 PRODIST)
    if c.startswith('RE') or c == 'RU3':
        return 'RESIDENCIAL'
    elif c.startswith('IN') or c == 'RU5':
        return 'INDUSTRIAL'
    elif c.startswith('CO'):
        return 'COMERCIAL'
    elif c.startswith('RU'): # RU1, RU2, RU4, RU6, RU7, RU8
        return 'RURAL'
    elif c.startswith('PP'):
        return 'PODER_PUBLICO'
    elif c.startswith('SP'):
        return 'SERVICO_PUBLICO'
    elif c == 'IP':
        return 'ILUMINACAO_PUBLICA'
    else:
        return 'OUTROS' # CPR, CSPS, etc.

def processar_camada_inteira(args):
    """
    Processa uma camada inteira de um GDB em um único processo.
    """
    gdb_path, layer_name, position = args
    stats_camada = {}
    
    try:
        with fiona.open(gdb_path, layer=layer_name) as src:
            nome_gdb = "ENEL" if "ENEL" in gdb_path.upper() else "LIGHT"
            total_records = len(src)
            desc = f"{nome_gdb} | {layer_name}"
            
            with tqdm(total=total_records, desc=desc, position=position, leave=True) as pbar:
                for feature in src:
                    props = feature['properties']
                    sub_id = str(props.get('SUB')).strip()
                    
                    if not sub_id or sub_id == 'None':
                        pbar.update(1)
                        continue
                    
                    # MUDANÇA CRUCIAL: Usando CLAS_SUB em vez de TIP_CC
                    classe = simplificar_classe(props.get('CLAS_SUB'))
                    chave = (sub_id, classe)
                    
                    if chave not in stats_camada:
                        stats_camada[chave] = {
                            'QTD_CLIENTES': 0,
                            'SOMA_CAR_INST': 0.0,
                            'ENE_01': 0.0, 'ENE_02': 0.0, 'ENE_03': 0.0, 'ENE_04': 0.0,
                            'ENE_05': 0.0, 'ENE_06': 0.0, 'ENE_07': 0.0, 'ENE_08': 0.0,
                            'ENE_09': 0.0, 'ENE_10': 0.0, 'ENE_11': 0.0, 'ENE_12': 0.0
                        }
                    
                    s = stats_camada[chave]
                    s['QTD_CLIENTES'] += 1
                    s['SOMA_CAR_INST'] += float(props.get('CAR_INST') or 0)
                    
                    if layer_name == 'UCAT_tab':
                        for i in range(1, 13):
                            mes = f"{i:02d}"
                            s[f'ENE_{mes}'] += (float(props.get(f'ENE_P_{mes}') or 0) + float(props.get(f'ENE_F_{mes}') or 0))
                    else:
                        for i in range(1, 13):
                            mes = f"{i:02d}"
                            s[f'ENE_{mes}'] += float(props.get(f'ENE_{mes}') or 0)
                    
                    pbar.update(1)
    except Exception as e:
        print(f"\nDEBUG ERROR: Erro em {layer_name}: {e}")
        
    return stats_camada

def extrair_estatisticas_paralelo():
    start_time = time.time()
    print("DEBUG: Iniciando extração paralela com mapeamento CLAS_SUB...")
    
    gdbs = glob.glob(os.path.join(PASTA_DADOS_BRUTOS, "**", "*.gdb"), recursive=True)
    camadas_alvo = ['UCBT_tab', 'UCMT_tab', 'UCAT_tab']
    
    tarefas = []
    pos = 0
    for gdb in gdbs:
        try:
            layers = fiona.listlayers(gdb)
            for camada in camadas_alvo:
                if camada in layers:
                    tarefas.append((gdb, camada, pos))
                    pos += 1
        except Exception as e:
            print(f"DEBUG ERROR: Erro ao ler {gdb}: {e}")

    print(f"DEBUG: {len(tarefas)} tarefas enviadas para os núcleos.")

    acumulador_global = {}
    with ProcessPoolExecutor(max_workers=len(tarefas)) as executor:
        resultados = list(executor.map(processar_camada_inteira, tarefas))

    print("\n" * (len(tarefas) + 1))
    print("DEBUG: Consolidando dados finais...")
    
    for stats_parcial in resultados:
        for chave, dados in stats_parcial.items():
            if chave not in acumulador_global:
                acumulador_global[chave] = dados
            else:
                g = acumulador_global[chave]
                g['QTD_CLIENTES'] += dados['QTD_CLIENTES']
                g['SOMA_CAR_INST'] += dados['SOMA_CAR_INST']
                for i in range(1, 13):
                    mes = f"ENE_{i:02d}"
                    g[mes] += dados[mes]

    rows = []
    for (sub_id, classe), stats in acumulador_global.items():
        row = {'COD_ID': sub_id, 'CLASSE': classe}
        row.update(stats)
        rows.append(row)

    df_final = pd.DataFrame(rows)
    if not os.path.exists(PASTA_SAIDA): os.makedirs(PASTA_SAIDA)
    df_final.to_csv(ARQUIVO_SAIDA, index=False, sep=';')
    
    duracao = (time.time() - start_time) / 60
    print(f"\nDEBUG: Extração concluída em {duracao:.2f} minutos.")
    print("\n=== RESULTADO FINAL (POR CLASSE ANEEL) ===")
    print(df_final.groupby('CLASSE')['QTD_CLIENTES'].sum().sort_values(ascending=False))

def main():
    """Entrada amigável para Streamlit: gera `Dados Processados/perfis_consumo.csv`."""
    multiprocessing.freeze_support()
    extrair_estatisticas_paralelo()


if __name__ == "__main__":
    multiprocessing.freeze_support()
    extrair_estatisticas_paralelo()
