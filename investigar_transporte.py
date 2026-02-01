"""
Script para identificar subestações de transporte (sem carga direta) e rastrear sua alimentação.
"""

import geopandas as gpd
import os
import pandas as pd

def investigar_transporte():
    gdbs = {
        'ENEL': {
            'path': 'Dados Brutos/BDGD ANEEL/ENEL_RJ_383_2022-09-30_V10_20240605-0611.gdb',
            'UNTRS': 'UNTRAT'
        },
        'LIGHT': {
            'path': 'Dados Brutos/BDGD ANEEL/LIGHT_382_2021-09-30_M10_20231218-2133.gdb',
            'UNTRS': 'UNTRS'
        }
    }
    
    for dist, cfg in gdbs.items():
        path = cfg['path']
        if not os.path.exists(path): continue
        
        print(f"\n--- ANALISANDO {dist} ---")
        
        # 1. Carregar dados básicos
        subs = gpd.read_file(path, layer='SUB')
        bars = gpd.read_file(path, layer='BAR')
        ctmt = gpd.read_file(path, layer='CTMT')
        untrs = gpd.read_file(path, layer='UNTRS' if dist == 'LIGHT' else 'UNTRAT')
        
        # 2. Calcular potência por subestação
        pot = untrs.groupby('SUB')['POT_NOM'].sum().reset_index()
        pot.columns = ['COD_ID', 'POT_TOTAL']
        
        # 3. Contar circuitos MT por subestação
        ct_counts = ctmt.groupby('SUB').size().reset_index(name='CT_COUNT')
        ct_counts.columns = ['COD_ID', 'CT_COUNT']
        
        # 4. Merge
        subs['COD_ID'] = subs['COD_ID'].astype(str)
        pot['COD_ID'] = pot['COD_ID'].astype(str)
        ct_counts['COD_ID'] = ct_counts['COD_ID'].astype(str)
        
        merged = subs[['COD_ID', 'NOM' if 'NOM' in subs.columns else 'NOME']].merge(pot, on='COD_ID', how='left').merge(ct_counts, on='COD_ID', how='left')
        merged = merged.fillna(0)
        
        # 5. Identificar subs de transporte (Potência 0 e 0 circuitos MT)
        transporte = merged[(merged['POT_TOTAL'] == 0) & (merged['CT_COUNT'] == 0)]
        
        print(f"Subestações de Transporte identificadas: {len(transporte)}")
        if not transporte.empty:
            print(transporte[['COD_ID', 'NOM' if 'NOM' in subs.columns else 'NOME']].to_string(index=False))
            
            # 6. Tentar rastrear alimentação via SSDAT ou UNTRS
            # (Já vimos que UNTRS raramente conecta subs diferentes)
            # Vamos ver se há circuitos AT (CTAT) que ligam essas subs a outras.
            # Infelizmente CTAT não tem coluna SUB.
            # Mas podemos ver se há Barras dessas subs conectadas a Barras de outras subs via SSDAT.
            
            pac_to_sub = bars.set_index('PAC')['SUB'].to_dict()
            ssdat = gpd.read_file(path, layer='SSDAT')
            
            print("\nRastreando alimentação via rede de Alta Tensão (SSDAT)...")
            for _, sub_row in transporte.iterrows():
                sid = sub_row['COD_ID']
                # Encontrar PACs desta subestação
                sub_pacs = bars[bars['SUB'] == sid]['PAC'].unique()
                
                # Encontrar conexões SSDAT que envolvem esses PACs
                conexoes = ssdat[(ssdat['PAC_1'].isin(sub_pacs)) | (ssdat['PAC_2'].isin(sub_pacs))]
                
                alimentadores = set()
                for _, con in conexoes.iterrows():
                    p1, p2 = con['PAC_1'], con['PAC_2']
                    other_pac = p2 if p1 in sub_pacs else p1
                    other_sub = pac_to_sub.get(other_pac)
                    if other_sub and other_sub != sid:
                        alimentadores.add(other_sub)
                
                if alimentadores:
                    print(f"Subestação {sid} ({sub_row.get('NOM', sub_row.get('NOME'))}) é alimentada por: {alimentadores}")
                else:
                    # Tentar via nome do circuito AT se possível
                    pass

if __name__ == "__main__":
    investigar_transporte()
