#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Script para analisar arquivos CSV e identificar linhas malformadas."""

import sys
import csv
from pathlib import Path
from collections import defaultdict
import page

# Garantir encoding UTF-8
if sys.platform == 'win32':
    try:
        if sys.stdout.encoding != 'utf-8':
            sys.stdout.reconfigure(encoding='utf-8')
        if sys.stderr.encoding != 'utf-8':
            sys.stderr.reconfigure(encoding='utf-8')
    except:
        pass

def analisar_arquivo_csv(filepath, table_info):
    """Analisa um arquivo CSV e retorna estatísticas de linhas malformadas."""
    table_name = table_info["table"]
    columns = table_info["columns"]
    num_cols_esperado = len(columns)
    
    total_linhas = 0
    linhas_ok = 0
    linhas_malformadas = 0
    linhas_com_poucas_colunas = 0
    linhas_com_muitas_colunas = 0
    exemplos_problemas = []
    
    try:
        with open(filepath, 'r', encoding='utf-8', errors='replace', newline='') as f:
            reader = csv.reader(page.NullByteStripper(f), delimiter=';', quotechar='"')
            
            for linha_num, row in enumerate(reader, start=1):
                total_linhas += 1
                num_cols_real = len(row)
                
                if num_cols_real == num_cols_esperado:
                    linhas_ok += 1
                else:
                    linhas_malformadas += 1
                    
                    if num_cols_real < num_cols_esperado:
                        linhas_com_poucas_colunas += 1
                        if len(exemplos_problemas) < 5:
                            # Mostrar primeiros campos da linha problemática
                            preview = ';'.join(row[:3]) if row else ''
                            if len(preview) > 100:
                                preview = preview[:100] + '...'
                            exemplos_problemas.append({
                                'linha': linha_num,
                                'colunas_esperadas': num_cols_esperado,
                                'colunas_encontradas': num_cols_real,
                                'preview': preview
                            })
                    else:
                        linhas_com_muitas_colunas += 1
                        if len(exemplos_problemas) < 5:
                            preview = ';'.join(row[:3]) if row else ''
                            if len(preview) > 100:
                                preview = preview[:100] + '...'
                            exemplos_problemas.append({
                                'linha': linha_num,
                                'colunas_esperadas': num_cols_esperado,
                                'colunas_encontradas': num_cols_real,
                                'preview': preview
                            })
                
                # Mostrar progresso a cada 100k linhas
                if linha_num % 100000 == 0:
                    print(f"    Processadas {linha_num:,} linhas...", end='\r')
    
    except Exception as e:
        print(f"    ERRO ao processar {filepath.name}: {e}")
        return None
    
    return {
        'arquivo': filepath.name,
        'tabela': table_name,
        'total_linhas': total_linhas,
        'linhas_ok': linhas_ok,
        'linhas_malformadas': linhas_malformadas,
        'linhas_com_poucas_colunas': linhas_com_poucas_colunas,
        'linhas_com_muitas_colunas': linhas_com_muitas_colunas,
        'exemplos_problemas': exemplos_problemas,
        'percentual_problemas': (linhas_malformadas / total_linhas * 100) if total_linhas > 0 else 0
    }

def main():
    print("="*80)
    print("ANÁLISE DE ARQUIVOS CSV - IDENTIFICAÇÃO DE LINHAS MALFORMADAS")
    print("="*80)
    print()
    
    # Verificar se DATA_DIR existe
    if not page.DATA_DIR.exists():
        print(f"ERRO: Diretório {page.DATA_DIR} não existe!")
        return
    
    print(f"Analisando arquivos em: {page.DATA_DIR}")
    print()
    
    # Encontrar todos os arquivos CSV (com extensão .csv ou sem extensão mas com nomes que indicam CSV)
    files_csv = list(page.DATA_DIR.rglob("*.csv"))
    # Também procurar arquivos sem extensão .csv mas que são CSVs (EMPRECSV, ESTABELE, etc)
    files_other = []
    for ext in ['EMPRECSV', 'ESTABELE', 'SOCIOCSV', 'CNAECSV', 'MOTICSV', 'MUNICCSV', 
                'NATJUCSV', 'PAISCSV', 'QUALSCSV']:
        files_other.extend(list(page.DATA_DIR.rglob(f"*.{ext}")))
        files_other.extend(list(page.DATA_DIR.rglob(f"*{ext}")))
    
    # Procurar também arquivos que terminam com SIMPLES.CSV
    files_other.extend(list(page.DATA_DIR.rglob("*SIMPLES*")))
    
    files = files_csv + files_other
    files = [f for f in files if f.is_file() and not f.name.startswith('.')]
    
    if not files:
        print("Nenhum arquivo CSV encontrado!")
        return
    
    print(f"Encontrados {len(files)} arquivo(s) CSV")
    print()
    
    # Agrupar por tipo de tabela
    resultados_por_tabela = defaultdict(list)
    resultados_gerais = []
    
    for filepath in files:
        filename = filepath.name.upper()
        table_info = None
        
        # Identificar tipo de arquivo
        if "EMPRE" in filename and "CSV" in filename:
            table_info = page.FILES_TABLES_MAP["Empresas"]
        elif "ESTABELE" in filename:
            table_info = page.FILES_TABLES_MAP["Estabelecimentos"]
        elif "SOCIO" in filename:
            table_info = page.FILES_TABLES_MAP["Socios"]
        elif "SIMPLES" in filename:
            table_info = page.FILES_TABLES_MAP["Simples"]
        elif "CNAE" in filename:
            table_info = page.FILES_TABLES_MAP["Cnaes"]
        elif "MOTI" in filename:
            table_info = page.FILES_TABLES_MAP["Motivos"]
        elif "MUNIC" in filename:
            table_info = page.FILES_TABLES_MAP["Municipios"]
        elif "NATJU" in filename:
            table_info = page.FILES_TABLES_MAP["Naturezas"]
        elif "PAIS" in filename:
            table_info = page.FILES_TABLES_MAP["Paises"]
        elif "QUALS" in filename:
            table_info = page.FILES_TABLES_MAP["Qualificacoes"]
        
        if table_info:
            print(f"Analisando: {filepath.name} ({table_info['table']})...")
            resultado = analisar_arquivo_csv(filepath, table_info)
            if resultado:
                resultados_por_tabela[table_info['table']].append(resultado)
                resultados_gerais.append(resultado)
            print()  # Linha em branco
    
    # Relatório por tabela
    print("="*80)
    print("RELATÓRIO POR TABELA")
    print("="*80)
    print()
    
    total_geral_linhas = 0
    total_geral_ok = 0
    total_geral_malformadas = 0
    
    for tabela in sorted(resultados_por_tabela.keys()):
        resultados = resultados_por_tabela[tabela]
        total_linhas_tabela = sum(r['total_linhas'] for r in resultados)
        total_ok_tabela = sum(r['linhas_ok'] for r in resultados)
        total_malformadas_tabela = sum(r['linhas_malformadas'] for r in resultados)
        
        total_geral_linhas += total_linhas_tabela
        total_geral_ok += total_ok_tabela
        total_geral_malformadas += total_malformadas_tabela
        
        percentual = (total_malformadas_tabela / total_linhas_tabela * 100) if total_linhas_tabela > 0 else 0
        
        print(f"📊 {tabela.upper()}")
        print(f"   Total de linhas: {total_linhas_tabela:,}")
        print(f"   Linhas OK: {total_ok_tabela:,} ({100-percentual:.2f}%)")
        print(f"   Linhas malformadas: {total_malformadas_tabela:,} ({percentual:.2f}%)")
        print()
    
    # Resumo geral
    print("="*80)
    print("RESUMO GERAL")
    print("="*80)
    print()
    print(f"Total de arquivos analisados: {len(resultados_gerais)}")
    print(f"Total de linhas: {total_geral_linhas:,}")
    print(f"Linhas OK: {total_geral_ok:,} ({100-(total_geral_malformadas/total_geral_linhas*100) if total_geral_linhas > 0 else 0:.2f}%)")
    print(f"Linhas malformadas: {total_geral_malformadas:,} ({(total_geral_malformadas/total_geral_linhas*100) if total_geral_linhas > 0 else 0:.2f}%)")
    print()
    
    # Detalhes de arquivos com problemas
    arquivos_com_problemas = [r for r in resultados_gerais if r['linhas_malformadas'] > 0]
    
    if arquivos_com_problemas:
        print("="*80)
        print("ARQUIVOS COM PROBLEMAS")
        print("="*80)
        print()
        
        for resultado in sorted(arquivos_com_problemas, key=lambda x: x['linhas_malformadas'], reverse=True):
            print(f"📁 {resultado['arquivo']} ({resultado['tabela']})")
            print(f"   Total: {resultado['total_linhas']:,} linhas")
            print(f"   Problemas: {resultado['linhas_malformadas']:,} ({resultado['percentual_problemas']:.2f}%)")
            print(f"   - Poucas colunas: {resultado['linhas_com_poucas_colunas']:,}")
            print(f"   - Muitas colunas: {resultado['linhas_com_muitas_colunas']:,}")
            
            if resultado['exemplos_problemas']:
                print("   Exemplos de linhas problemáticas:")
                for ex in resultado['exemplos_problemas']:
                    print(f"      Linha {ex['linha']:,}: esperado {ex['colunas_esperadas']} colunas, encontrado {ex['colunas_encontradas']}")
                    print(f"         Preview: {ex['preview']}")
            print()
    else:
        print("✅ Nenhum arquivo com problemas encontrado!")
        print()

if __name__ == "__main__":
    main()

