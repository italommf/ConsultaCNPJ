"""Utilitários para importação"""
import os
import csv
from pathlib import Path
from typing import List, Optional, Tuple

import polars as pl

def encontrar_arquivos_csv(diretorio: Path, padrao: str) -> List[Path]:
    """
    Encontra arquivos CSV que correspondem ao padrão.
    Padrões: EMPRE, ESTABELE, SOCIO, SIMPLES, CNAE, MOTI, MUNIC, NATJU, PAIS, QUALS
    """
    arquivos = []
    
    if not diretorio.exists():
        return arquivos
    
    # Buscar recursivamente
    for arquivo in diretorio.rglob("*"):
        if not arquivo.is_file():
            continue
        
        nome_upper = arquivo.name.upper()
        
        # Verificar padrão
        if padrao.upper() == "EMPRE" and ("EMPRE" in nome_upper or "EMPRESAS" in nome_upper):
            if "CSV" in nome_upper or nome_upper.endswith("EMPRECSV"):
                arquivos.append(arquivo)
        elif padrao.upper() == "ESTABELE" and ("ESTABELE" in nome_upper or "ESTABELECIMENTOS" in nome_upper):
            arquivos.append(arquivo)
        elif padrao.upper() == "SOCIO" and ("SOCIO" in nome_upper or "SOCIOS" in nome_upper):
            if "CSV" in nome_upper or nome_upper.endswith("SOCIOCSV"):
                arquivos.append(arquivo)
        elif padrao.upper() == "SIMPLES" and "SIMPLES" in nome_upper:
            if "CSV" in nome_upper:
                arquivos.append(arquivo)
        elif padrao.upper() == "CNAE" and "CNAE" in nome_upper:
            if "CSV" in nome_upper or nome_upper.endswith("CNAECSV"):
                arquivos.append(arquivo)
        elif padrao.upper() == "MOTI" and "MOTI" in nome_upper:
            if "CSV" in nome_upper or nome_upper.endswith("MOTICSV"):
                arquivos.append(arquivo)
        elif padrao.upper() == "MUNIC" and "MUNIC" in nome_upper:
            if "CSV" in nome_upper or nome_upper.endswith("MUNICCSV"):
                arquivos.append(arquivo)
        elif padrao.upper() == "NATJU" and "NATJU" in nome_upper:
            if "CSV" in nome_upper or nome_upper.endswith("NATJUCSV"):
                arquivos.append(arquivo)
        elif padrao.upper() == "PAIS" and "PAIS" in nome_upper:
            if "CSV" in nome_upper or nome_upper.endswith("PAISCSV"):
                arquivos.append(arquivo)
        elif padrao.upper() == "QUALS" and "QUALS" in nome_upper:
            if "CSV" in nome_upper or nome_upper.endswith("QUALSCSV"):
                arquivos.append(arquivo)
    
    return sorted(arquivos)


def validar_arquivo(arquivo: Path) -> bool:
    """Valida se arquivo existe e é legível"""
    if not arquivo.exists():
        return False
    if not arquivo.is_file():
        return False
    if arquivo.stat().st_size == 0:
        return False
    return True


def contar_linhas_csv(arquivo: Path, num_colunas_esperadas: int, delimiter: str = ';') -> Tuple[int, int]:
    """
    Conta linhas válidas e problemáticas de um arquivo CSV.
    Retorna: (linhas_validas, linhas_problematicas)
    
    Uma linha é considerada:
    - VÁLIDA: se tiver exatamente o número esperado de colunas OU até 10% a mais (colunas extras)
    - PROBLEMÁTICA: se tiver MENOS colunas que o esperado (quebra de linha ou dados incompletos)
    
    Nota: Durante a importação, linhas com menos colunas são preenchidas com valores vazios,
    mas são marcadas como problemáticas aqui para indicar que há dados incompletos no arquivo.
    """
    # Aceitar até 10% a mais de colunas (para arquivos que podem ter colunas extras)
    max_colunas = int(num_colunas_esperadas * 1.1)

    # Tentar caminho rápido com Polars (vetorizado e em C)
    try:
        # Tentar UTF-8 primeiro, depois latin-1 para preservar acentos
        try:
            df = pl.read_csv(
                arquivo,
                separator=delimiter,
                has_header=False,
                infer_schema_length=0,
                ignore_errors=True,
                encoding="utf-8",
            )
        except Exception:
            df = pl.read_csv(
                arquivo,
                separator=delimiter,
                has_header=False,
                infer_schema_length=0,
                ignore_errors=True,
                encoding="latin-1",
            )

        cols = df.columns
        if not cols:
            return 0, 0

        # Aproximação do número de colunas por linha: quantidade de valores não nulos
        non_null_count = pl.sum_horizontal(
            [pl.col(c).is_not_null().cast(pl.Int32) for c in cols]
        )

        stats = df.select(
            total=pl.len(),
            valid=((non_null_count >= num_colunas_esperadas) & (non_null_count <= max_colunas)).sum(),
        ).row(0)

        total_linhas = int(stats[0])
        linhas_validas = int(stats[1])
        linhas_problematicas = total_linhas - linhas_validas
        return linhas_validas, linhas_problematicas

    except Exception as e:
        # Fallback para implementação antiga com csv.reader se algo der errado com Polars
        print(f"  ⚠ Falha ao contar com Polars em {arquivo.name}, usando csv.reader: {e}")

    linhas_validas = 0
    linhas_problematicas = 0

    try:
        # Tentar UTF-8 primeiro, depois latin-1 para preservar acentos
        try:
            with open(arquivo, 'r', encoding='utf-8', errors='strict', newline='') as f:
                reader = csv.reader(f, delimiter=delimiter, quotechar='"')
                for linha_num, row in enumerate(reader, start=1):
                    # Linha vazia
                    if not row or (len(row) == 1 and not row[0].strip()):
                        continue
                    # Verificar número de colunas
                    num_colunas = len(row)
                    # Linha válida: tem exatamente o número esperado ou até 10% a mais
                    if num_colunas == num_colunas_esperadas or (num_colunas_esperadas < num_colunas <= max_colunas):
                        linhas_validas += 1
                    # Linha problemática: tem menos colunas que o esperado
                    else:
                        linhas_problematicas += 1
        except UnicodeDecodeError:
            # Se UTF-8 falhar, tentar latin-1
            with open(arquivo, 'r', encoding='latin-1', errors='strict', newline='') as f:
                reader = csv.reader(f, delimiter=delimiter, quotechar='"')
                for linha_num, row in enumerate(reader, start=1):
                    # Linha vazia
                    if not row or (len(row) == 1 and not row[0].strip()):
                        continue
                    # Verificar número de colunas
                    num_colunas = len(row)
                    # Linha válida: tem exatamente o número esperado ou até 10% a mais
                    if num_colunas == num_colunas_esperadas or (num_colunas_esperadas < num_colunas <= max_colunas):
                        linhas_validas += 1
                    # Linha problemática: tem menos colunas que o esperado (quebra de linha ou dados incompletos)
                    elif num_colunas < num_colunas_esperadas:
                        linhas_problematicas += 1
                    # Linha com muitas colunas extras também é problemática
                else:
                    linhas_problematicas += 1

    except Exception as e:
        print(f"  ⚠ Erro ao contar linhas de {arquivo.name}: {e}")

    return linhas_validas, linhas_problematicas


def verificar_arquivos_baixados(downloads_dir: Path, data_dir: Path) -> dict:
    """
    Verifica quais arquivos foram baixados e descompactados.
    Retorna dicionário com status.
    """
    status = {
        'zips_baixados': 0,
        'zips_faltando': [],
        'arquivos_descompactados': 0,
        'arquivos_faltando': []
    }
    
    # Contar ZIPs baixados
    zip_files = list(downloads_dir.glob("*.zip"))
    status['zips_baixados'] = len(zip_files)
    
    # Verificar arquivos descompactados
    for subdir in ['dominio', 'empresas', 'estabelecimentos', 'socios', 'simples']:
        subdir_path = data_dir / subdir
        if subdir_path.exists():
            arquivos = list(subdir_path.glob("*"))
            status['arquivos_descompactados'] += len(arquivos)
    
    return status
