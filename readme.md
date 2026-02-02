# Projeto de Mapeamento de Subestações - LIGHT & ENEL RJ

Este projeto consiste em uma ferramenta de Big Data e Geoprocessamento para extrair, processar e visualizar a infraestrutura elétrica das distribuidoras LIGHT e ENEL no estado do Rio de Janeiro.

## 🚀 Como Executar

### 1. Instalação de Dependências
Certifique-se de ter o Python instalado e execute o comando abaixo para instalar todas as bibliotecas necessárias:

```bash
pip install -r requirements.txt
```

### 2. Processamento de Dados (ETL)
O primeiro passo é processar os dados brutos (arquivos .gdb) para gerar a base unificada. O script `extrator.py` realiza todo o trabalho pesado de geoprocessamento.

```bash
python extrator.py
```
*   **O que ele faz:**
    *   Lê arquivos GDB na pasta `Dados Brutos`.
    *   Gera áreas de influência reais baseadas na localização dos transformadores.
    *   Resolve sobreposições de território entre subestações.
    *   Preenche áreas vazias no estado do RJ usando diagramas de Voronoi.
    *   Enriquece os dados com estatísticas do CNEFE (IBGE) e OpenStreetMap (OSM).
    *   Classifica as subestações e rastreia a hierarquia de alimentação.
    *   Salva o resultado em `Dados Processados/dados_finais_rj.geojson`.

### 3. Visualização no Mapa
Após o processamento, você pode visualizar os dados em um mapa interativo usando o Streamlit.

```bash
streamlit run main.py
```
*   **O que ele faz:**
    *   Cria uma interface web interativa.
    *   Exibe as áreas de atendimento coloridas por subestação.
    *   Mostra ícones personalizados para cada tipo de subestação (Plena, Satélite, etc).
    *   Apresenta popups detalhados com estatísticas de consumo e tipos de estabelecimentos.
    *   Desenha linhas animadas (AntPath) mostrando o fluxo de energia entre subestações "mães" e "filhas".

## 📁 Estrutura de Arquivos Principal

*   `extrator.py`: Motor de processamento geográfico e integração de dados.
*   `main.py`: Interface de visualização e dashboard.
*   `requirements.txt`: Lista de bibliotecas Python necessárias.
*   `Dados Brutos/`: Pasta onde devem estar os arquivos .gdb das distribuidoras.
*   `Dados Processados/`: Pasta onde o arquivo final unificado é gerado.
*   `assets/icons/`: Ícones utilizados na visualização do mapa.

## 🛠️ Tecnologias Utilizadas

*   **Python**: Linguagem base.
*   **GeoPandas & Shapely**: Processamento geográfico avançado.
*   **Streamlit & Folium**: Interface web e mapas interativos.
*   **Geobr**: Integração com malhas territoriais oficiais do IBGE.
*   **Requests & Overpass API**: Coleta de dados do OpenStreetMap.
