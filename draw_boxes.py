"""
Este script lê coordenadas de bounding boxes de painéis solares de um arquivo JSON (gemini.txt),
desenha essas caixas sobre uma imagem de satélite (chunk_direto.png) e salva o resultado.
As coordenadas no JSON estão em formato normalizado (0 a 1).
"""

import json
from PIL import Image, ImageDraw
import os

# Configurações de arquivos
ARQUIVO_DADOS = "gemini.txt"
IMAGEM_ENTRADA = "chunk_direto.png"
IMAGEM_SAIDA = "chunk_com_paineis.png"

def desenhar_bounding_boxes():
    """
    Lê os dados do gemini.txt e desenha as caixas na imagem chunk_direto.png.
    """
    print(f"DEBUG: Iniciando o processamento de {ARQUIVO_DADOS}")

    # Verifica se os arquivos existem
    if not os.path.exists(ARQUIVO_DADOS):
        print(f"DEBUG: Erro - Arquivo {ARQUIVO_DADOS} não encontrado.")
        return
    
    if not os.path.exists(IMAGEM_ENTRADA):
        print(f"DEBUG: Erro - Imagem {IMAGEM_ENTRADA} não encontrada.")
        return

    # Carrega os dados JSON do arquivo gemini.txt
    try:
        with open(ARQUIVO_DADOS, 'r', encoding='utf-8') as f:
            dados = json.load(f)
    except Exception as e:
        print(f"DEBUG: Erro ao ler o arquivo JSON: {e}")
        return

    # Carrega a imagem original
    try:
        img = Image.open(IMAGEM_ENTRADA).convert("RGB")
        largura, altura = img.size
        print(f"DEBUG: Imagem carregada. Dimensões: {largura}x{altura}")
    except Exception as e:
        print(f"DEBUG: Erro ao abrir a imagem: {e}")
        return

    # Cria um objeto para desenhar na imagem
    draw = ImageDraw.Draw(img)

    # Itera sobre os painéis solares encontrados
    paineis = dados.get("solar_panels", [])
    print(f"DEBUG: Encontrados {len(paineis)} painéis solares.")

    for i, painel in enumerate(paineis):
        # O formato do Gemini costuma ser [ymin, xmin, ymax, xmax] em valores normalizados (0-1000 ou 0-1)
        # Analisando o gemini.txt, os valores parecem estar entre 0 e 1.
        box = painel.get("box_2d", [])
        if len(box) == 4:
            ymin, xmin, ymax, xmax = box
            
            # Converte coordenadas normalizadas para pixels
            # Nota: Multiplicamos pela largura/altura da imagem
            left = xmin * largura
            top = ymin * altura
            right = xmax * largura
            bottom = ymax * altura

            # Desenha o retângulo (Bounding Box)
            # Usamos a cor vermelha (255, 0, 0) e largura de 3 pixels
            draw.rectangle([left, top, right, bottom], outline="red", width=3)
            print(f"DEBUG: Desenhando caixa {i+1}: [{left:.1f}, {top:.1f}, {right:.1f}, {bottom:.1f}]")

    # Salva o resultado
    try:
        img.save(IMAGEM_SAIDA)
        print(f"DEBUG: Resultado salvo com sucesso em: {IMAGEM_SAIDA}")
    except Exception as e:
        print(f"DEBUG: Erro ao salvar a imagem de saída: {e}")

if __name__ == "__main__":
    desenhar_bounding_boxes()
