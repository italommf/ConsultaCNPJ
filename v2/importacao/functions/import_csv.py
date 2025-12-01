"""Importador CSV otimizado para ClickHouse"""
import csv
import sys
from pathlib import Path
from typing import Optional
from datetime import date
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
        # Configurar limite de partições e timeouts aumentados
        try:
            client.execute("SET max_partitions_per_insert_block = 10000")
            client.execute("SET send_timeout = 3600")  # 1 hora
            client.execute("SET receive_timeout = 3600")  # 1 hora
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
        """Importa arquivo completo de empresas de uma vez"""
        logger.info(f"Importando empresas de {arquivo.name}...")
        
        linhas_processadas = 0
        dados = []
        
        try:
            with open(arquivo, 'r', encoding='utf-8', errors='replace', newline='') as f:
                reader = csv.reader(f, delimiter=';', quotechar='"')
                
                for linha_num, row in enumerate(reader, start=1):
                    # Tratar linhas quebradas
                    if len(row) < 7:
                        # Preencher colunas faltantes
                        while len(row) < 7:
                            row.append('')
                    
                    # Normalizar dados
                    cnpj_basico = normalizar_codigo(row[0], 8) or ''
                    razao_social = limpar_string(row[1]) or ''
                    natureza_juridica = normalizar_codigo(row[2], 4) or ''
                    qualificacao_do_responsavel = normalizar_codigo(row[3], 2) or ''
                    capital_social = normalizar_capital_social(row[4]) or 0
                    porte = normalizar_codigo(row[5], 2) or ''
                    ente_federativo = limpar_string(row[6]) or ''
                    
                    dados.append([
                        str(cnpj_basico),
                        str(razao_social),
                        str(natureza_juridica),
                        str(qualificacao_do_responsavel),
                        capital_social,
                        str(porte),
                        str(ente_federativo)
                    ])
                    
                    # Log de progresso a cada 100k linhas
                    if len(dados) % 100_000 == 0:
                        logger.info(f"  Lidas {len(dados):,} linhas...")
            
            # Inserir tudo de uma vez
            if dados:
                logger.info(f"  Inserindo {len(dados):,} registros no banco...")
                try:
                    self.client.execute("INSERT INTO empresas VALUES", dados)
                    linhas_processadas = len(dados)
                    logger.info(f"✓ Importadas {linhas_processadas:,} empresas de {arquivo.name}")
                except Exception as e:
                    logger.error(f"✗ Erro ao inserir empresas: {e}")
                    raise
        
        except Exception as e:
            logger.error(f"Erro ao importar {arquivo.name}: {e}")
            raise
        
        return linhas_processadas
    
    def importar_estabelecimentos(self, arquivo: Path) -> int:
        """Importa arquivo completo de estabelecimentos de uma vez"""
        logger.info(f"Importando estabelecimentos de {arquivo.name}...")
        
        linhas_processadas = 0
        dados = []
        
        # Funções inline otimizadas
        def quick_zfill(s, size):
            if not s:
                return '0' * size
            return s.strip().zfill(size)[:size]
        
        def quick_clean(s):
            if not s:
                return ''
            return s.strip().replace('\x00', '')
        
        try:
            with open(arquivo, 'r', encoding='utf-8', errors='replace', newline='') as f:
                reader = csv.reader(f, delimiter=';', quotechar='"')
                
                for linha_num, row in enumerate(reader, start=1):
                    # Preencher colunas faltantes
                    if len(row) < 29:
                        row.extend([''] * (29 - len(row)))
                    
                    # Normalização otimizada
                    cnpj_basico = quick_zfill(row[0], 8)
                    cnpj_ordem = quick_zfill(row[1], 4)
                    cnpj_dv = quick_zfill(row[2], 2)
                    cnpj = f"{cnpj_basico}{cnpj_ordem}{cnpj_dv}"
                    
                    # Datas
                    data_situacao = self._normalize_date(normalizar_data(row[6]) if row[6] else None)
                    data_inicio = self._normalize_date(normalizar_data(row[10]) if row[10] else None)
                    data_situacao_especial = self._normalize_date(normalizar_data(row[28]) if len(row) > 28 and row[28] else None)
                    
                    dados.append([
                        cnpj_basico, cnpj_ordem, cnpj_dv, cnpj,
                        quick_zfill(row[3], 1), quick_clean(row[4]), quick_zfill(row[5], 2),
                        data_situacao, quick_zfill(row[7], 2), quick_clean(row[8]),
                        quick_zfill(row[9], 3), data_inicio, quick_zfill(row[11], 7),
                        quick_clean(row[12]), quick_clean(row[13]), quick_clean(row[14]),
                        quick_clean(row[15]), quick_clean(row[16]), quick_clean(row[17]),
                        quick_zfill(row[18], 8), quick_zfill(row[19], 2), quick_zfill(row[20], 4),
                        quick_zfill(row[21], 2), quick_clean(row[22]), quick_zfill(row[23], 2),
                        quick_clean(row[24]), quick_zfill(row[25], 2), quick_clean(row[26]),
                        quick_clean(row[27]), quick_clean(row[28]) if len(row) > 28 else '',
                        data_situacao_especial
                    ])
                    
                    # Log de progresso a cada 100k linhas
                    if len(dados) % 100_000 == 0:
                        logger.info(f"  Lidas {len(dados):,} linhas...")
            
            # Inserir tudo de uma vez
            if dados:
                logger.info(f"  Inserindo {len(dados):,} registros no banco...")
                try:
                    self.client.execute("INSERT INTO estabelecimentos VALUES", dados)
                    linhas_processadas = len(dados)
                    logger.info(f"✓ Importados {linhas_processadas:,} estabelecimentos de {arquivo.name}")
                except Exception as e:
                    logger.error(f"✗ Erro ao inserir estabelecimentos: {e}")
                    raise
        
        except Exception as e:
            logger.error(f"Erro ao importar {arquivo.name}: {e}")
            raise
        
        return linhas_processadas
    
    def importar_socios(self, arquivo: Path) -> int:
        """Importa arquivo completo de sócios de uma vez"""
        logger.info(f"Importando sócios de {arquivo.name}...")
        
        linhas_processadas = 0
        dados = []
        
        try:
            with open(arquivo, 'r', encoding='utf-8', errors='replace', newline='') as f:
                reader = csv.reader(f, delimiter=';', quotechar='"')
                
                for linha_num, row in enumerate(reader, start=1):
                    if len(row) < 11:
                        while len(row) < 11:
                            row.append('')
                    
                    # Normalizar datas
                    data_entrada_raw = normalizar_data(row[5]) if len(row) > 5 else None
                    data_entrada = self._normalize_date(data_entrada_raw)
                    
                    dados.append([
                        str(normalizar_codigo(row[0], 8) or ''),
                        str(normalizar_codigo(row[1], 1) or ''),
                        str(limpar_string(row[2]) or ''),
                        str(limpar_string(row[3]) or ''),
                        str(normalizar_codigo(row[4], 2) or ''),
                        data_entrada,
                        str(normalizar_codigo(row[6], 3) or ''),
                        str(normalizar_codigo(row[7], 1) or ''),
                        str(limpar_string(row[8]) or ''),
                        str(normalizar_codigo(row[9], 2) or ''),
                        str(normalizar_codigo(row[10], 1) or '')
                    ])
                    
                    # Log de progresso a cada 100k linhas
                    if len(dados) % 100_000 == 0:
                        logger.info(f"  Lidas {len(dados):,} linhas...")
            
            # Inserir tudo de uma vez
            if dados:
                logger.info(f"  Inserindo {len(dados):,} registros no banco...")
                try:
                    self.client.execute("INSERT INTO socios VALUES", dados)
                    linhas_processadas = len(dados)
                    logger.info(f"✓ Importados {linhas_processadas:,} sócios de {arquivo.name}")
                except Exception as e:
                    logger.error(f"✗ Erro ao inserir sócios: {e}")
                    raise
        
        except Exception as e:
            logger.error(f"Erro ao importar {arquivo.name}: {e}")
            raise
        
        return linhas_processadas
    
    def importar_simples(self, arquivo: Path) -> int:
        """Importa arquivo completo de simples de uma vez"""
        logger.info(f"Importando simples de {arquivo.name}...")
        
        linhas_processadas = 0
        dados = []
        
        try:
            with open(arquivo, 'r', encoding='utf-8', errors='replace', newline='') as f:
                reader = csv.reader(f, delimiter=';', quotechar='"')
                
                for linha_num, row in enumerate(reader, start=1):
                    if len(row) < 7:
                        while len(row) < 7:
                            row.append('')
                    
                    # Normalizar datas para objetos date
                    opcao_simples_raw = normalizar_data(row[2]) if len(row) > 2 else None
                    data_opcao_simples = self._normalize_date(opcao_simples_raw)
                    
                    data_exclusao_simples_raw = normalizar_data(row[3]) if len(row) > 3 else None
                    data_exclusao_simples = self._normalize_date(data_exclusao_simples_raw)
                    
                    data_opcao_mei_raw = normalizar_data(row[5]) if len(row) > 5 else None
                    data_opcao_mei = self._normalize_date(data_opcao_mei_raw)
                    
                    data_exclusao_mei_raw = normalizar_data(row[6]) if len(row) > 6 else None
                    data_exclusao_mei = self._normalize_date(data_exclusao_mei_raw)
                    
                    dados.append([
                        normalizar_codigo(row[0], 8) or '',
                        normalizar_codigo(row[1], 1) or '',
                        data_opcao_simples,  # opcao_simples (objeto date)
                        data_exclusao_simples,  # data_exclusao_simples (objeto date)
                        normalizar_codigo(row[4], 1) or '',
                        data_opcao_mei,  # opcao_mei (objeto date)
                        data_exclusao_mei  # data_exclusao_mei (objeto date)
                    ])
                    
                    # Log de progresso a cada 100k linhas
                    if len(dados) % 100_000 == 0:
                        logger.info(f"  Lidas {len(dados):,} linhas...")
            
            # Inserir tudo de uma vez
            if dados:
                logger.info(f"  Inserindo {len(dados):,} registros no banco...")
                try:
                    self.client.execute("INSERT INTO simples VALUES", dados)
                    linhas_processadas = len(dados)
                    logger.info(f"✓ Importados {linhas_processadas:,} registros de simples de {arquivo.name}")
                except Exception as e:
                    logger.error(f"✗ Erro ao inserir simples: {e}")
                    raise
        
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
            with open(arquivo, 'r', encoding='utf-8', errors='replace', newline='') as f:
                reader = csv.reader(f, delimiter=';', quotechar='"')
                
                for linha_num, row in enumerate(reader, start=1):
                    if len(row) < 2:
                        while len(row) < 2:
                            row.append('')
                    
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
