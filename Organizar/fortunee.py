import numpy as np
import matplotlib.pyplot as plt
from scipy.spatial import cKDTree

def gerar_e_plotar_voronoi_colorido(n_pontos=30, tamanho_area=100, resolucao_grid=500, nome_arquivo='voronoi_areas.png'):
    """
    Gera pontos aleatórios, calcula as áreas de Voronoi (via vizinho mais próximo em grid),
    colora cada área diferentemente, plota, exibe e salva a imagem.

    Args:
        n_pontos (int): Número de pontos (sementes) para gerar.
        tamanho_area (int): O tamanho do quadrado do mapa (ex: 100x100).
        resolucao_grid (int): Densidade da imagem para colorir (maior = bordas mais suaves, mais lento).
        nome_arquivo (str): Nome do arquivo para salvar a imagem.
    """
    print(f"Gerando {n_pontos} pontos aleatórios...")

    # 1. Gerar Pontos Aleatórios (Sementes)
    # Cria coordenadas X e Y entre 0 e o tamanho_area
    pontos = np.random.rand(n_pontos, 2) * tamanho_area

    print("Calculando áreas usando método de grid (Vizinho Mais Próximo)...")

    # 2. Configurar o Grid para visualização colorida
    # Criamos uma malha fina de pontos sobre toda a área
    x_grid = np.linspace(0, tamanho_area, resolucao_grid)
    y_grid = np.linspace(0, tamanho_area, resolucao_grid)
    xx, yy = np.meshgrid(x_grid, y_grid)

    # Achatar a grade para criar uma lista longa de coordenadas (pixels)
    grid_points = np.c_[xx.ravel(), yy.ravel()]

    # 3. Usar cKDTree para encontrar o vizinho mais próximo rapidamente
    # O cKDTree é uma estrutura de dados super eficiente para busca espacial.
    # Ele faz o trabalho pesado de definir "quem está mais perto de quem".
    tree = cKDTree(pontos)

    # Para cada ponto na nossa grade, encontramos o índice do ponto original mais próximo.
    # 'indices' conterá um número de 0 a (n_pontos-1) para cada pixel do grid.
    _, indices = tree.query(grid_points)

    # Remodelar os índices de volta para o formato quadrado da imagem (grid)
    region_map = indices.reshape(xx.shape)

    print("Plotando o resultado...")

    # 4. Plotagem
    fig, ax = plt.subplots(figsize=(10, 10))

    # Usamos pcolormesh para pintar a grade baseado no índice da região.
    # Escolhemos um colormap (cmap) com cores distintas (ex: 'tab20', 'nipy_spectral')
    cmap = plt.cm.get_cmap('tab20', n_pontos)
    
    # Plotar as áreas coloridas
    ax.pcolormesh(xx, yy, region_map, cmap=cmap, shading='nearest', alpha=0.7)

    # Plotar os pontos originais (sementes) por cima como bolinhas pretas
    ax.scatter(pontos[:, 0], pontos[:, 1], c='black', s=60, edgecolors='white', zorder=10, label='Pontos Originais')

    # Configurações finais do gráfico
    ax.set_xlim(0, tamanho_area)
    ax.set_ylim(0, tamanho_area)
    ax.set_title(f"Diagrama de Voronoi: Áreas Definidas para {n_pontos} Pontos", fontsize=14)
    ax.set_aspect('equal', adjustable='box') # Garante que não fique esticado
    ax.legend()
    plt.axis('off') # Remove eixos numéricos para um visual mais limpo

    # 5. Salvar e Exibir
    plt.tight_layout()
    plt.savefig(nome_arquivo, dpi=150, bbox_inches='tight')
    print(f"Imagem salva com sucesso como '{nome_arquivo}'")

    plt.show()

# --- Executar o script ---
if __name__ == "__main__":
    # Você pode ajustar os parâmetros aqui
    gerar_e_plotar_voronoi_colorido(
        n_pontos=30,         # Tente mudar para 10 ou 100
        tamanho_area=100,    # Tamanho do "mundo"
        resolucao_grid=600,  # Se ficar lento, diminua para 300
        nome_arquivo='meu_voronoi_colorido.png'
    )