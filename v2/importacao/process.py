"""Processo principal de importação - orquestra todas as etapas"""
import sys
import time
import logging
from pathlib import Path
from datetime import datetime
from functions.import_csv import ClickHouseImporter

from dotenv import load_dotenv
from utilities.output import (
    imprimir_estatisticas_finais,
    imprimir_resumo_contagens,
    print_header,
    print_step,
)
from utilities.clickhouse import (
    carregar_config,
    configurar_sessao_clickhouse,
    conectar_clickhouse,
    criar_banco_e_schema,
    limpar_banco_dados,
    verificar_importacao,
)
from utilities.csv_stats import contar_linhas_arquivos
from utilities.utils import encontrar_arquivos_csv, validar_arquivo
from utilities.config import garantir_encoding_windows, resolver_diretorios
from utilities.downloader import baixar_arquivos_mes_atual, descompactar_arquivos

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent

def executar():

    inicio = time.time()
    data_dir, downloads_dir = resolver_diretorios(BASE_DIR)
    schema_file = BASE_DIR.parent / "clickhouse" / "schema.sql"

    load_dotenv()
    garantir_encoding_windows()

    print_header("PROCESSAMENTO COMPLETO DE DADOS CNPJ - CLICKHOUSE OTIMIZADO")
    print(f"Data/Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Sistema: {sys.platform}")
    print(f"Diretório de dados: {data_dir}")
    print(f"Diretório de downloads: {downloads_dir}")

    # Etapa 1: Conectar ao ClickHouse
    print_step(1, 7, "Conectando ao ClickHouse")
    config = carregar_config()
    client = conectar_clickhouse(config)

    # Etapa 2: Download de arquivos
    print_step(2, 7, "Download de Arquivos")
    garantir_downloads(downloads_dir)

    # Etapa 3: Descompactação de arquivos
    print_step(3, 7, "Descompactação de Arquivos")
    garantir_descompactacao(downloads_dir, data_dir)

    # Etapa 4: Contagem de linhas dos arquivos CSV (comentado para testes)
    print_step(4, 7, "Contagem de Linhas dos Arquivos CSV")
    contagens_csv = contar_linhas_arquivos(data_dir)
    imprimir_resumo_contagens(contagens_csv)

    # Etapa 5: Preparação do banco de dados
    print_step(5, 7, "Preparação do Banco de Dados")
    logger.info("Removendo completamente todas as tabelas existentes...")
    if not limpar_banco_dados(client):
        logger.error("✗ Falha ao limpar banco de dados. Abortando importação.")
        return
    
    # Verificar que o banco está realmente vazio
    try:
        tabelas_existentes = client.execute("SHOW TABLES")
        if tabelas_existentes:
            logger.warning("⚠ Ainda existem tabelas no banco: %s", [t[0] for t in tabelas_existentes])
            logger.warning("  Tentando remover novamente...")
            if not limpar_banco_dados(client):
                logger.error("✗ Falha ao limpar banco de dados na segunda tentativa. Abortando importação.")
                return
    except Exception as exc:
        # Se der erro ao verificar, pode ser que o banco não exista ainda - isso é OK
        logger.debug("Banco pode não existir ainda: %s", exc)
    
    logger.info("Criando banco de dados e schema do zero...")
    criar_banco_e_schema(client, schema_file)
    configurar_sessao_clickhouse(client)

    # Etapa 6: Importação de dados
    print_step(6, 7, "Importação de Dados")
    executar_importacoes(client, data_dir)

    # Etapa 7: Verificação final
    print_step(7, 7, "Verificação Final")
    verificar_importacao(client, contagens_csv)
    imprimir_estatisticas_finais(client, config.database, inicio)


def garantir_downloads(downloads_dir: Path) -> None:
    """Garante que os arquivos foram baixados"""
    zip_files = list(downloads_dir.glob("*.zip"))
    if zip_files:
        logger.info("✓ Encontrados %s arquivos ZIP já baixados", len(zip_files))
        return

    logger.info("Baixando arquivos do mês atual...")
    if baixar_arquivos_mes_atual(downloads_dir):
        logger.info("✓ Download concluído")
    else:
        logger.warning("⚠ Nenhum arquivo foi baixado")


def garantir_descompactacao(downloads_dir: Path, data_dir: Path) -> None:
    """Garante que os arquivos foram descompactados"""
    subdirs = ["dominio", "empresas", "estabelecimentos", "socios", "simples"]
    if any((data_dir / subdir).exists() and any((data_dir / subdir).iterdir()) for subdir in subdirs):
        logger.info("✓ Arquivos já descompactados")
        return

    logger.info("Descompactando arquivos...")
    if descompactar_arquivos(downloads_dir, data_dir):
        logger.info("✓ Descompactação concluída")
    else:
        logger.warning("⚠ Nenhum arquivo foi descompactado")


def executar_importacoes(client, data_dir: Path) -> None:
    """Executa todas as importações de dados"""
    importer = ClickHouseImporter(client)

    # Comentado para teste - importação de domínio
    logger.info("\n📋 Importando tabelas de domínio...")
    dominio_tabelas = {
        "CNAE": "cnaes",
        "MOTI": "motivos",
        "MUNIC": "municipios",
        "NATJU": "naturezas",
        "PAIS": "paises",
        "QUALS": "qualificacoes",
    }
    for padrao, tabela in dominio_tabelas.items():
        for arquivo in encontrar_arquivos_csv(data_dir, padrao):
            if validar_arquivo(arquivo):
                try:
                    importer.importar_dominio(arquivo, tabela)
                except Exception as exc:
                    logger.error("✗ Erro ao importar %s: %s", arquivo.name, exc)

    # Comentado para teste - importação de empresas
    logger.info("\n🏢 Importando empresas...")
    importar_lista(importer.importar_empresas, data_dir, "EMPRE")

    logger.info("\n🏪 Importando estabelecimentos...")
    importar_lista(importer.importar_estabelecimentos, data_dir, "ESTABELE")

    logger.info("\n👥 Importando sócios...")
    importar_lista(importer.importar_socios, data_dir, "SOCIO")

    logger.info("\n📄 Importando simples...")
    for arquivo in encontrar_arquivos_csv(data_dir, "SIMPLES"):
        if validar_arquivo(arquivo):
            try:
                importer.importar_simples(arquivo)
            except Exception as exc:
                logger.error("✗ Erro ao importar %s: %s", arquivo.name, exc)


def importar_lista(func_import, data_dir: Path, padrao: str) -> None:
    """Importa uma lista de arquivos do mesmo tipo"""
    arquivos = encontrar_arquivos_csv(data_dir, padrao)
    total = 0
    for idx, arquivo in enumerate(arquivos, 1):
        if not validar_arquivo(arquivo):
            continue
        try:
            logger.info("  [%s/%s] %s", idx, len(arquivos), arquivo.name)
            linhas = func_import(arquivo)
            total += linhas
            logger.info("  ✓ Total acumulado: %s", f"{total:,}")
        except Exception as exc:
            logger.error("  ✗ Erro ao importar %s: %s", arquivo.name, exc)
