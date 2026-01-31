"""
Este script realiza o mapeamento completo de todos os bancos de dados File Geodatabase (.gdb)
encontrados em uma pasta específica.
Para cada GDB, ele gera:
1. Uma árvore inicial com os nomes das camadas ordenados alfabeticamente.
2. O detalhamento da estrutura de colunas.
3. Uma prévia dos primeiros 5 registros de cada camada.
Os resultados são salvos em arquivos .txt na mesma pasta dos arquivos originais.
"""

import fiona
import pandas as pd
import os
import glob

# Configurações globais
# Pasta contendo os bancos de dados GDB
BASE_DIR = r"Dados Brutos\BDGD ANEEL"

def gerar_nome_saida(caminho_original: str, parametros: str) -> str:
    """
    Gera o nome do arquivo de saída seguindo a convenção:
    nome_original + "processed" + parâmetros.
    O arquivo será salvo na mesma pasta do original.
    """
    diretorio = os.path.dirname(caminho_original)
    nome_base = os.path.splitext(os.path.basename(caminho_original))[0]
    nome_arquivo = f"{nome_base}_processed_{parametros}.txt"
    return os.path.join(diretorio, nome_arquivo)

def mapear_gdb(caminho_gdb: str) -> None:
    """
    Mapeia e salva a estrutura de um único arquivo GDB.
    """
    print(f"DEBUG: Processando banco de dados: {caminho_gdb}")
    
    # Define o nome do arquivo de saída
    arquivo_saida = gerar_nome_saida(caminho_gdb, "mapeamento_detalhado")
    
    try:
        # Lista todas as camadas presentes no GDB e as ordena alfabeticamente
        camadas = sorted(fiona.listlayers(caminho_gdb))
        print(f"DEBUG: {len(camadas)} camadas encontradas e ordenadas em {os.path.basename(caminho_gdb)}")
        
        with open(arquivo_saida, "w", encoding="utf-8") as f:
            f.write("="*80 + "\n")
            f.write(f"MAPEAMENTO DO BANCO DE DADOS: {os.path.basename(caminho_gdb)}\n")
            f.write("="*80 + "\n\n")

            # --- PARTE 1: ÁRVORE DE CAMADAS (RESUMO ORDENADO) ---
            f.write("🌳 ÁRVORE DE CAMADAS (RESUMO - ORDEM ALFABÉTICA):\n")
            f.write(f"└── {os.path.basename(caminho_gdb)}\n")
            for i, nome_camada in enumerate(camadas):
                simbolo = "└──" if i == len(camadas) - 1 else "├──"
                f.write(f"    {simbolo} {nome_camada}\n")
            
            f.write("\n" + "="*80 + "\n")
            f.write("🔍 DETALHAMENTO DAS CAMADAS\n")
            f.write("="*80 + "\n")

            # --- PARTE 2: DETALHAMENTO (COLUNAS E PRÉVIA) ---
            for nome_camada in camadas:
                print(f"DEBUG: Detalhando camada: {nome_camada}")
                f.write(f"\n📂 CAMADA: {nome_camada}\n")
                
                try:
                    # Abre a camada para ler o esquema e os dados
                    with fiona.open(caminho_gdb, layer=nome_camada) as camada:
                        # Exibe a estrutura de colunas (Schema)
                        f.write("  │\n")
                        f.write("  ├── ESTRUTURA DAS COLUNAS:\n")
                        propriedades = camada.schema.get('properties', {})
                        
                        if propriedades:
                            items = list(propriedades.items())
                            for i, (coluna, tipo) in enumerate(items):
                                simbolo = "└──" if i == len(items) - 1 else "├──"
                                f.write(f"  │   {simbolo} {coluna} ({tipo})\n")
                        else:
                            f.write("  │   └── (Sem colunas de atributos)\n")

                        # Coleta os 5 primeiros registros para a prévia
                        f.write("  │\n")
                        f.write("  ├── PRÉVIA DOS DADOS (Top 5):\n")
                        
                        registros = []
                        for j, feature in enumerate(camada):
                            if j >= 5:
                                break
                            registros.append(feature['properties'])
                        
                        if registros:
                            df_previa = pd.DataFrame(registros)
                            string_previa = df_previa.to_string(index=False)
                            for linha in string_previa.split('\n'):
                                f.write(f"  │   {linha}\n")
                        else:
                            f.write("  │   └── (Camada sem registros)\n")
                            
                except Exception as e:
                    f.write(f"  │   └── DEBUG: Erro ao ler camada {nome_camada}: {e}\n")
                
                f.write("  " + "─"*40 + "\n")

        print(f"DEBUG: Arquivo gerado com sucesso: {arquivo_saida}")

    except Exception as e:
        print(f"DEBUG: Erro ao processar {caminho_gdb}: {e}")

def processar_todos_gdbs(pasta_base: str) -> None:
    """
    Localiza todos os arquivos .gdb na pasta e inicia o mapeamento.
    """
    print(f"DEBUG: Buscando arquivos .gdb em: {pasta_base}")
    
    # Busca por diretórios que terminam com .gdb
    padrao = os.path.join(pasta_base, "*.gdb")
    lista_gdbs = glob.glob(padrao)
    
    if not lista_gdbs:
        print("DEBUG: Nenhum arquivo .gdb encontrado na pasta especificada.")
        return

    print(f"DEBUG: Encontrados {len(lista_gdbs)} arquivos GDB.")
    
    for gdb in lista_gdbs:
        mapear_gdb(gdb)

if __name__ == "__main__":
    # Inicia o processamento em lote
    processar_todos_gdbs(BASE_DIR)
