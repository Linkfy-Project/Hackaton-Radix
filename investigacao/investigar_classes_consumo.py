"""
Este script investiga a possibilidade de obter classes de consumo por subestação
nos bancos de dados BDGD da ENEL e da LIGHT.
Ele conta as unidades consumidoras por tipo (TIP_CC) para uma subestação de exemplo.
"""

import fiona
import pandas as pd

def investigar_classes_consumo(gdb_path, sub_id_exemplo):
    print(f"\nDEBUG: Investigando GDB: {gdb_path}")
    
    # Camadas de Unidades Consumidoras
    camadas_uc = ['UCBT_tab', 'UCMT_tab', 'UCAT_tab']
    
    stats = []
    
    try:
        for camada in camadas_uc:
            print(f"DEBUG: Lendo camada {camada}...")
            try:
                with fiona.open(gdb_path, layer=camada) as src:
                    count = 0
                    for feature in src:
                        props = feature['properties']
                        # No BDGD, a coluna SUB identifica a subestação
                        if str(props.get('SUB')) == str(sub_id_exemplo):
                            stats.append({
                                'Camada': camada,
                                'Classe': props.get('TIP_CC'),
                                'CNAE': props.get('CNAE')
                            })
                            count += 1
                    print(f"DEBUG: Encontradas {count} UCs para a subestação {sub_id_exemplo} na camada {camada}")
            except Exception as e:
                print(f"DEBUG: Erro ao ler camada {camada}: {e}")
        
        if stats:
            df = pd.DataFrame(stats)
            resumo = df.groupby(['Camada', 'Classe']).size().reset_index(name='Quantidade')
            print(f"\nResultado para Subestação {sub_id_exemplo}:")
            print(resumo)
        else:
            print(f"\nNenhuma unidade consumidora encontrada para a subestação {sub_id_exemplo}.")
            
    except Exception as e:
        print(f"DEBUG: Erro geral na investigação: {e}")

if __name__ == "__main__":
    # Caminhos dos GDBs (ajustados conforme list_files)
    gdb_enel = "Dados Brutos/BDGD ANEEL/ENEL_RJ_383_2022-09-30_V10_20240605-0611.gdb"
    gdb_light = "Dados Brutos/BDGD ANEEL/LIGHT_382_2021-09-30_M10_20231218-2133.gdb"
    
    # IDs de exemplo baseados nos arquivos de mapeamento
    # ENEL: 'VPA' (Vila de Paiva?)
    # LIGHT: '1240' (SESD?)
    
    print("=== INVESTIGAÇÃO ENEL ===")
    #investigar_classes_consumo(gdb_enel, "VPA")
    
    print("\n=== INVESTIGAÇÃO LIGHT ===")
    investigar_classes_consumo(gdb_light, "1240")
