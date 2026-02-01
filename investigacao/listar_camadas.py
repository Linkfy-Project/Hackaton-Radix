import fiona
import os

def listar_camadas_light():
    gdb_path = 'Dados Brutos/BDGD ANEEL/LIGHT_382_2021-09-30_M10_20231218-2133.gdb'
    if not os.path.exists(gdb_path):
        print(f"Erro: GDB não encontrado em {gdb_path}")
        return

    layers = fiona.listlayers(gdb_path)
    print(f"Camadas encontradas no GDB da LIGHT ({len(layers)}):")
    for layer in sorted(layers):
        print(f"- {layer}")

if __name__ == "__main__":
    listar_camadas_light()
