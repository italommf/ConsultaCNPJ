"""Importador CSV otimizado para ClickHouse"""
import sys
from pathlib import Path
from typing import Optional
from datetime import date

import polars as pl
from clickhouse_driver import Client

# Ajuste de path para suportar execução direta
BASE_DIR = Path(__file__).resolve().parents[1]
UTILS_DIR = BASE_DIR / "utilities"

for path_candidate in (BASE_DIR, UTILS_DIR):
    path_str = str(path_candidate)
    if path_candidate.exists() and path_str not in sys.path:
        sys.path.insert(0, path_str)

from utilities.normalizador import (
    normalizar_cnpj,
    normalizar_data,
    normalizar_capital_social,
    limpar_string,
    normalizar_codigo,
)
from utilities.utils import encontrar_arquivos_csv, validar_arquivo

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ClickHouseImporter:
    """Importador otimizado para ClickHouse - insere arquivo completo de uma vez"""
    
    def __init__(self, client: Client):
        self.client = client
        # Configurar timeouts aumentados
        try:
            client.execute("SET send_timeout = 3600")  # 1 hora
            client.execute("SET receive_timeout = 3600")  # 1 hora
            # Permitir muitos partitions por bloco de INSERT (necessário para estabelecimentos)
            client.execute("SET max_partitions_per_insert_block = 10000")
        except:
            pass  # Ignorar se não conseguir configurar
    
    def _normalize_date(self, data_str: Optional[str]) -> date:
        """Normaliza data para objeto date que o ClickHouse driver espera"""
        if not data_str or data_str == '':
            return date(1970, 1, 1)
        # Se já é um objeto date, retornar direto
        if isinstance(data_str, date):
            return data_str
        # Se é string, converter para date
        if isinstance(data_str, str):
            data_str = data_str.strip()
            # Se já está no formato YYYY-MM-DD
            if len(data_str) == 10 and data_str[4] == '-' and data_str[7] == '-':
                try:
                    from datetime import datetime
                    return datetime.strptime(data_str, "%Y-%m-%d").date()
                except ValueError:
                    return date(1970, 1, 1)
            # Tentar formato YYYYMMDD
            elif len(data_str) == 8 and data_str.isdigit():
                try:
                    from datetime import datetime
                    return datetime.strptime(data_str, "%Y%m%d").date()
                except ValueError:
                    return date(1970, 1, 1)
        return date(1970, 1, 1)
    
    def importar_empresas(self, arquivo: Path) -> int:
        """Importa arquivo completo de empresas de uma vez (Polars vetorizado, sem iter_rows)"""
        logger.info(f"Importando empresas de {arquivo.name}...")
        
        linhas_processadas = 0
        
        try:
            df = pl.read_csv(
                arquivo,
                separator=';',
                has_header=False,
                infer_schema_length=0,
                ignore_errors=True,
                encoding="utf8-lossy",  # arquivos da Receita podem ter bytes inválidos
            )

            # Renomear colunas para padrão col0, col1, ...
            df = df.rename({name: f"col{i}" for i, name in enumerate(df.columns)})

            # Garantir pelo menos 7 colunas
            for i in range(7):
                col_name = f"col{i}"
                if col_name not in df.columns:
                    df = df.with_columns(pl.lit("").alias(col_name))

            # Helpers vetorizados simples
            def clean_col(name: str) -> pl.Expr:
                return (
                    pl.col(name)
                    .cast(pl.Utf8)
                    .fill_null("")
                    .str.replace("\x00", "")
                    .str.strip_chars()
                )

            # Normalizações vetorizadas, reutilizando suas funções Python
            df = df.with_columns([
                pl.col("col0")
                    .map_elements(lambda x: normalizar_codigo(x, 8) or "", return_dtype=pl.Utf8)
                    .alias("cnpj_basico"),
                clean_col("col1")
                    .map_elements(lambda x: limpar_string(x) or "", return_dtype=pl.Utf8)
                    .alias("razao_social"),
                pl.col("col2")
                    .map_elements(lambda x: normalizar_codigo(x, 4) or "", return_dtype=pl.Utf8)
                    .alias("natureza_juridica"),
                pl.col("col3")
                    .map_elements(lambda x: normalizar_codigo(x, 2) or "", return_dtype=pl.Utf8)
                    .alias("qualificacao_do_responsavel"),
                pl.col("col4")
                    .map_elements(lambda x: normalizar_capital_social(x) or 0, return_dtype=pl.Int64)
                    .alias("capital_social"),
                pl.col("col5")
                    .map_elements(lambda x: normalizar_codigo(x, 2) or "", return_dtype=pl.Utf8)
                    .alias("porte"),
                clean_col("col6")
                    .map_elements(lambda x: limpar_string(x) or "", return_dtype=pl.Utf8)
                    .alias("ente_federativo"),
            ])

            colunas_ordem = [
                "cnpj_basico",
                "razao_social",
                "natureza_juridica",
                "qualificacao_do_responsavel",
                "capital_social",
                "porte",
                "ente_federativo",
            ]

            if df.height > 0:
                tabela_df = df.select(colunas_ordem)
                BATCH_SIZE = 500_000

                for offset in range(0, tabela_df.height, BATCH_SIZE):
                    chunk = tabela_df.slice(offset, BATCH_SIZE)
                    batch = chunk.to_numpy().tolist()
                    if not batch:
                        continue
                    self.client.execute("INSERT INTO empresas VALUES", batch)
                    linhas_processadas += len(batch)
                    logger.info(f"  Inseridas {linhas_processadas:,} linhas de empresas até agora...")
        
        except Exception as e:
            logger.error(f"Erro ao importar {arquivo.name}: {e}")
            raise
        
        return linhas_processadas
    
    def importar_estabelecimentos(self, arquivo: Path) -> int:
        """Importa arquivo completo de estabelecimentos de uma vez"""
        logger.info(f"Importando estabelecimentos de {arquivo.name}...")
        
        linhas_processadas = 0
        dados = []
        
        try:
            df = pl.read_csv(
                arquivo,
                separator=';',
                has_header=False,
                infer_schema_length=0,
                ignore_errors=True,
                encoding="utf8-lossy",
            )

            # Renomear colunas para um padrão conhecido (col0, col1, ...)
            df = df.rename({name: f"col{i}" for i, name in enumerate(df.columns)})

            # Garantir que temos ao menos 29 colunas (preencher ausentes com string vazia)
            for i in range(29):
                col_name = f"col{i}"
                if col_name not in df.columns:
                    df = df.with_columns(pl.lit("").alias(col_name))

            # Helpers vetorizados
            def zfill_col(name: str, size: int) -> pl.Expr:
                return (
                    pl.col(name)
                    .cast(pl.Utf8)
                    .fill_null("")
                    .str.replace("\x00", "")
                    .str.strip_chars()
                    .str.zfill(size)
                    .str.slice(0, size)
                )

            def clean_col(name: str) -> pl.Expr:
                return (
                    pl.col(name)
                    .cast(pl.Utf8)
                    .fill_null("")
                    .str.replace("\x00", "")
                    .str.strip_chars()
                )

            default_date = date(1970, 1, 1)

            def parse_date(name: str) -> pl.Expr:
                base = pl.col(name).cast(pl.Utf8).str.strip_chars()
                # Tentar primeiro YYYY-MM-DD, depois YYYYMMDD; fallback 1970-01-01
                d1 = base.str.strptime(pl.Date, "%Y-%m-%d", strict=False)
                d2 = base.str.strptime(pl.Date, "%Y%m%d", strict=False)
                return pl.coalesce(d1, d2, pl.lit(default_date))

            # Normalizações vetorizadas
            df = df.with_columns([
                zfill_col("col0", 8).alias("cnpj_basico"),
                zfill_col("col1", 4).alias("cnpj_ordem"),
                zfill_col("col2", 2).alias("cnpj_dv"),
            ])

            df = df.with_columns([
                (pl.col("cnpj_basico") + pl.col("cnpj_ordem") + pl.col("cnpj_dv")).alias("cnpj"),
                zfill_col("col3", 1).alias("matriz_filial"),
                clean_col("col4").alias("nome_fantasia"),
                zfill_col("col5", 2).alias("situacao_cadastral"),
                parse_date("col6").alias("data_situacao"),
                zfill_col("col7", 2).alias("motivo_situacao"),
                clean_col("col8").alias("cidade_exterior"),
                zfill_col("col9", 3).alias("pais"),
                parse_date("col10").alias("data_inicio"),
                zfill_col("col11", 7).alias("cnae_fiscal"),
                clean_col("col12").alias("cnae_fiscal_secundaria"),
                clean_col("col13").alias("tipo_logradouro"),
                clean_col("col14").alias("logradouro"),
                clean_col("col15").alias("numero"),
                clean_col("col16").alias("complemento"),
                clean_col("col17").alias("bairro"),
                zfill_col("col18", 8).alias("cep"),
                zfill_col("col19", 2).alias("uf"),
                zfill_col("col20", 4).alias("municipio"),
                zfill_col("col21", 2).alias("ddd_1"),
                clean_col("col22").alias("telefone_1"),
                zfill_col("col23", 2).alias("ddd_2"),
                clean_col("col24").alias("telefone_2"),
                zfill_col("col25", 2).alias("ddd_fax"),
                clean_col("col26").alias("fax"),
                clean_col("col27").alias("email"),
                clean_col("col28").alias("situacao_especial"),
                parse_date("col28").alias("data_situacao_especial"),
            ])

            # Extrair na ordem exata do schema do ClickHouse
            colunas_ordem = [
                "cnpj_basico",
                "cnpj_ordem",
                "cnpj_dv",
                "cnpj",
                "matriz_filial",
                "nome_fantasia",
                "situacao_cadastral",
                "data_situacao",
                "motivo_situacao",
                "cidade_exterior",
                "pais",
                "data_inicio",
                "cnae_fiscal",
                "cnae_fiscal_secundaria",
                "tipo_logradouro",
                "logradouro",
                "numero",
                "complemento",
                "bairro",
                "cep",
                "uf",
                "municipio",
                "ddd_1",
                "telefone_1",
                "ddd_2",
                "telefone_2",
                "ddd_fax",
                "fax",
                "email",
                "situacao_especial",
                "data_situacao_especial",
            ]

            if df.height > 0:
                # Inserir em chunks usando apenas operações vetorizadas (sem iter_rows)
                tabela_df = df.select(colunas_ordem)
                BATCH_SIZE = 500_000

                for offset in range(0, tabela_df.height, BATCH_SIZE):
                    chunk = tabela_df.slice(offset, BATCH_SIZE)
                    batch = chunk.to_numpy().tolist()
                    if not batch:
                        continue
                    self.client.execute("INSERT INTO estabelecimentos VALUES", batch)
                    linhas_processadas += len(batch)
                    logger.info(f"  Inseridas {linhas_processadas:,} linhas de estabelecimentos até agora...")
        
        except Exception as e:
            logger.error(f"Erro ao importar {arquivo.name}: {e}")
            raise
        
        return linhas_processadas
    
    def importar_socios(self, arquivo: Path) -> int:
        """Importa arquivo completo de sócios de uma vez (Polars vetorizado, sem iter_rows)"""
        logger.info(f"Importando sócios de {arquivo.name}...")
        
        linhas_processadas = 0
        
        try:
            df = pl.read_csv(
                arquivo,
                separator=';',
                has_header=False,
                infer_schema_length=0,
                ignore_errors=True,
                encoding="utf8-lossy",
            )

            df = df.rename({name: f"col{i}" for i, name in enumerate(df.columns)})

            # Garantir ao menos 11 colunas
            for i in range(11):
                col_name = f"col{i}"
                if col_name not in df.columns:
                    df = df.with_columns(pl.lit("").alias(col_name))

            def clean_col(name: str) -> pl.Expr:
                return (
                    pl.col(name)
                    .cast(pl.Utf8)
                    .fill_null("")
                    .str.replace("\x00", "")
                    .str.strip_chars()
                )

            default_date = date(1970, 1, 1)

            def parse_date(name: str) -> pl.Expr:
                base = pl.col(name).cast(pl.Utf8).str.strip_chars()
                d1 = base.str.strptime(pl.Date, "%Y-%m-%d", strict=False)
                d2 = base.str.strptime(pl.Date, "%Y%m%d", strict=False)
                return pl.coalesce(d1, d2, pl.lit(default_date))

            df = df.with_columns([
                pl.col("col0")
                    .map_elements(lambda x: normalizar_codigo(x, 8) or "", return_dtype=pl.Utf8)
                    .alias("cnpj_basico"),
                pl.col("col1")
                    .map_elements(lambda x: normalizar_codigo(x, 1) or "", return_dtype=pl.Utf8)
                    .alias("identificador_socio"),
                clean_col("col2")
                    .map_elements(lambda x: limpar_string(x) or "", return_dtype=pl.Utf8)
                    .alias("nome_socio"),
                clean_col("col3")
                    .map_elements(lambda x: limpar_string(x) or "", return_dtype=pl.Utf8)
                    .alias("cnpj_cpf_socio"),
                pl.col("col4")
                    .map_elements(lambda x: normalizar_codigo(x, 2) or "", return_dtype=pl.Utf8)
                    .alias("qualificacao_socio"),
                parse_date("col5").alias("data_entrada_sociedade"),
                pl.col("col6")
                    .map_elements(lambda x: normalizar_codigo(x, 3) or "", return_dtype=pl.Utf8)
                    .alias("pais"),
                pl.col("col7")
                    .map_elements(lambda x: normalizar_codigo(x, 1) or "", return_dtype=pl.Utf8)
                    .alias("representante_legal"),
                clean_col("col8")
                    .map_elements(lambda x: limpar_string(x) or "", return_dtype=pl.Utf8)
                    .alias("nome_representante"),
                pl.col("col9")
                    .map_elements(lambda x: normalizar_codigo(x, 2) or "", return_dtype=pl.Utf8)
                    .alias("qualificacao_representante"),
                pl.col("col10")
                    .map_elements(lambda x: normalizar_codigo(x, 1) or "", return_dtype=pl.Utf8)
                    .alias("faixa_etaria"),
            ])

            colunas_ordem = [
                "cnpj_basico",
                "identificador_socio",
                "nome_socio",
                "cnpj_cpf_socio",
                "qualificacao_socio",
                "data_entrada_sociedade",
                "pais",
                "representante_legal",
                "nome_representante",
                "qualificacao_representante",
                "faixa_etaria",
            ]

            if df.height > 0:
                tabela_df = df.select(colunas_ordem)
                BATCH_SIZE = 500_000

                for offset in range(0, tabela_df.height, BATCH_SIZE):
                    chunk = tabela_df.slice(offset, BATCH_SIZE)
                    batch = chunk.to_numpy().tolist()
                    if not batch:
                        continue
                    self.client.execute("INSERT INTO socios VALUES", batch)
                    linhas_processadas += len(batch)
                    logger.info(f"  Inseridas {linhas_processadas:,} linhas de sócios até agora...")
        
        except Exception as e:
            logger.error(f"Erro ao importar {arquivo.name}: {e}")
            raise
        
        return linhas_processadas
    
    def importar_simples(self, arquivo: Path) -> int:
        """Importa arquivo completo de simples de uma vez (Polars vetorizado, sem iter_rows)"""
        logger.info(f"Importando simples de {arquivo.name}...")
        
        linhas_processadas = 0
        
        try:
            df = pl.read_csv(
                arquivo,
                separator=';',
                has_header=False,
                infer_schema_length=0,
                ignore_errors=True,
                encoding="utf8-lossy",
            )

            # Renomear colunas para padrão col0, col1, ...
            df = df.rename({name: f"col{i}" for i, name in enumerate(df.columns)})

            # Garantir pelo menos 7 colunas
            for i in range(7):
                col_name = f"col{i}"
                if col_name not in df.columns:
                    df = df.with_columns(pl.lit("").alias(col_name))

            default_date = date(1970, 1, 1)

            def parse_date(name: str) -> pl.Expr:
                base = pl.col(name).cast(pl.Utf8).str.strip_chars()
                d1 = base.str.strptime(pl.Date, "%Y-%m-%d", strict=False)
                d2 = base.str.strptime(pl.Date, "%Y%m%d", strict=False)
                return pl.coalesce(d1, d2, pl.lit(default_date))

            # Normalizações vetorizadas
            df = df.with_columns([
                pl.col("col0")
                    .map_elements(lambda x: normalizar_codigo(x, 8) or "", return_dtype=pl.Utf8)
                    .alias("cnpj_basico"),
                pl.col("col1")
                    .map_elements(lambda x: normalizar_codigo(x, 1) or "", return_dtype=pl.Utf8)
                    .alias("opcao_simples"),
                parse_date("col2").alias("data_opcao_simples"),
                parse_date("col3").alias("data_exclusao_simples"),
                pl.col("col4")
                    .map_elements(lambda x: normalizar_codigo(x, 1) or "", return_dtype=pl.Utf8)
                    .alias("opcao_mei"),
                parse_date("col5").alias("data_opcao_mei"),
                parse_date("col6").alias("data_exclusao_mei"),
            ])

            colunas_ordem = [
                "cnpj_basico",
                "opcao_simples",
                "data_opcao_simples",
                "data_exclusao_simples",
                "opcao_mei",
                "data_opcao_mei",
                "data_exclusao_mei",
            ]

            if df.height > 0:
                # Inserir em chunks de 500k linhas usando operações vetorizadas
                tabela_df = df.select(colunas_ordem)
                BATCH_SIZE = 500_000

                for offset in range(0, tabela_df.height, BATCH_SIZE):
                    chunk = tabela_df.slice(offset, BATCH_SIZE)
                    batch = chunk.to_numpy().tolist()
                    if not batch:
                        continue
                    self.client.execute("INSERT INTO simples VALUES", batch)
                    linhas_processadas += len(batch)
                    logger.info(f"  Inseridas {linhas_processadas:,} linhas de simples até agora...")
        
        except Exception as e:
            logger.error(f"Erro ao importar {arquivo.name}: {e}")
            raise
        
        return linhas_processadas
    
    def importar_dominio(self, arquivo: Path, tabela: str) -> int:
        """Importa tabela de domínio completa de uma vez (cnaes, motivos, municipios, etc)"""
        logger.info(f"Importando {tabela} de {arquivo.name}...")
        
        linhas_processadas = 0
        dados = []
        
        # Determinar tamanho do código baseado na tabela (conforme schema.sql)
        codigo_size = {
            'cnaes': 7,          # FixedString(7)
            'motivos': 2,         # FixedString(2)
            'municipios': 4,      # FixedString(4) - código IBGE tem 4 dígitos
            'naturezas': 4,       # FixedString(4)
            'paises': 3,          # FixedString(3)
            'qualificacoes': 2    # FixedString(2)
        }.get(tabela, 4)
        
        try:
            df = pl.read_csv(
                arquivo,
                separator=';',
                has_header=False,
                infer_schema_length=0,
                ignore_errors=True,
                encoding="utf8-lossy",
            )

            for linha_num, row in enumerate(df.iter_rows(), start=1):
                row = list(row)

                if len(row) < 2:
                    row.extend([''] * (2 - len(row)))
                
                dados.append([
                    normalizar_codigo(row[0], codigo_size) or '',
                    limpar_string(row[1]) or ''
                ])
            
            # Inserir tudo de uma vez
            if dados:
                logger.info(f"  Inserindo {len(dados):,} registros no banco...")
                try:
                    self.client.execute(f"INSERT INTO {tabela} VALUES", dados)
                    linhas_processadas = len(dados)
                    logger.info(f"✓ Importados {linhas_processadas:,} registros de {tabela} de {arquivo.name}")
                except Exception as e:
                    logger.error(f"✗ Erro ao inserir {tabela}: {e}")
                    raise
        
        except Exception as e:
            logger.error(f"Erro ao importar {arquivo.name}: {e}")
            raise
        
        return linhas_processadas
