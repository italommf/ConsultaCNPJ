"""Funções para gerenciamento do ClickHouse"""
import logging
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

from clickhouse_driver import Client
from dotenv import load_dotenv

logger = logging.getLogger(__name__)


@dataclass
class ClickHouseConfig:
    host: str = "localhost"
    port: int = 9000
    user: str = "default"
    password: str = ""
    database: str = "cnpj"
    max_retries: int = 10
    retry_delay: int = 3


def carregar_config() -> ClickHouseConfig:
    """Carrega configuração do ClickHouse do ambiente"""
    load_dotenv()
    return ClickHouseConfig(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_PORT", "9000")),
        user=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", "") or "",
        database=os.getenv("CLICKHOUSE_DATABASE", "cnpj"),
    )


def conectar_clickhouse(config: ClickHouseConfig) -> Client:
    """Conecta ao ClickHouse com retry automático"""
    connect_kwargs = {
        "host": config.host,
        "port": config.port,
        "user": config.user,
        "connect_timeout": 10,
    }
    if config.password and config.password.strip() and config.password.strip().lower() != "none":
        connect_kwargs["password"] = config.password.strip()

    for attempt in range(config.max_retries):
        try:
            client_temp = Client(**connect_kwargs)
            client_temp.execute(f"CREATE DATABASE IF NOT EXISTS {config.database}")

            connect_kwargs["database"] = config.database
            client = Client(**connect_kwargs)
            client.execute("SELECT 1")

            logger.info("✓ Conectado ao ClickHouse")
            return client
        except Exception as exc:
            if attempt < config.max_retries - 1:
                logger.warning(
                    "Tentativa %s/%s falhou (%s). Aguardando %ss...",
                    attempt + 1,
                    config.max_retries,
                    exc,
                    config.retry_delay,
                )
                time.sleep(config.retry_delay)
            else:
                logger.error(
                    "Erro ao conectar ao ClickHouse após %s tentativas: %s",
                    config.max_retries,
                    exc,
                )
                raise
    raise RuntimeError("Não foi possível conectar ao ClickHouse")


def criar_banco_e_schema(client: Client, schema_file: Path) -> bool:
    """Cria banco de dados e schema no ClickHouse"""
    if not schema_file.exists():
        logger.error("Arquivo de schema não encontrado: %s", schema_file)
        return False

    logger.info("Criando banco de dados e schema (%s)...", schema_file.name)
    try:
        # Habilitar LowCardinality (necessário para esta versão do ClickHouse)
        # try:
        #     client.execute("SET allow_experimental_low_cardinality_type = 1")
        #     logger.debug("LowCardinality habilitado")
        # except Exception:
        #     logger.warning("Não foi possível habilitar LowCardinality (pode não ser necessário)")
        
        with open(schema_file, "r", encoding="utf-8") as file:
            schema_sql = file.read()

        lines = []
        for line in schema_sql.split("\n"):
            if "--" in line:
                comment_pos = line.find("--")
                in_string = False
                for char in line[:comment_pos]:
                    if char in ('"', "'"):
                        in_string = not in_string
                if not in_string:
                    line = line[:comment_pos]
            lines.append(line)

        statements: List[str] = []
        current_statement = ""
        for line in ("\n".join(lines)).split("\n"):
            stripped = line.strip()
            if not stripped:
                continue
            current_statement += stripped + " "
            if stripped.endswith(";"):
                statements.append(current_statement.strip())
                current_statement = ""

        # Adicionar qualquer statement restante
        if current_statement.strip():
            statements.append(current_statement.strip())

        tabelas_criadas = 0
        for statement in statements:
            if not statement:
                continue
            # Pular comandos USE - já estamos conectados ao banco correto
            if statement.strip().upper().startswith("USE "):
                logger.debug("Pulando comando USE (já conectado ao banco)")
                continue
            try:
                client.execute(statement)
                # Verificar se é um CREATE TABLE
                if statement.strip().upper().startswith("CREATE TABLE"):
                    tabelas_criadas += 1
                    logger.debug("Tabela criada/verificada")
            except Exception as exc:
                error_str = str(exc).lower()
                # Ignorar apenas erros de "already exists" ou "table already exists"
                if "already exists" in error_str or "table already exists" in error_str:
                    logger.debug("Tabela já existe (ignorando): %s", statement[:50])
                    # Contar como criada mesmo se já existir
                    if statement.strip().upper().startswith("CREATE TABLE"):
                        tabelas_criadas += 1
                else:
                    logger.error("Erro ao executar statement: %s", exc)
                    logger.error("Statement problemático: %s", statement[:100])
                    # Não retornar False aqui, continuar tentando criar outras tabelas

        logger.info("  ✓ Schema processado (%s tabelas criadas/verificadas)", tabelas_criadas)
        
        # Verificar se as tabelas foram criadas
        try:
            tabelas = client.execute("SHOW TABLES")
            logger.info("  ✓ Tabelas no banco: %s", [t[0] for t in tabelas] if tabelas else "nenhuma")
        except Exception as exc:
            logger.warning("Não foi possível verificar tabelas: %s", exc)
        
        return True
    except Exception as exc:
        logger.error("Erro ao criar schema: %s", exc)
        import traceback
        logger.error(traceback.format_exc())
        return False


def limpar_banco_dados(client: Client, tabelas: Optional[List[str]] = None) -> bool:
    """
    Remove completamente todas as tabelas do banco de dados.
    Se houver tabelas com partes quebradas que não podem ser removidas,
    remove o banco inteiro e o recria.
    """
    tabelas_padrao = [
        "empresas",
        "estabelecimentos",
        "socios",
        "simples",
        "cnaes",
        "motivos",
        "municipios",
        "naturezas",
        "paises",
        "qualificacoes",
    ]

    tabelas_alvo = tabelas or tabelas_padrao
    logger.info("Removendo completamente todas as tabelas do banco de dados (%s tabelas)...", len(tabelas_alvo))

    # Obter nome do banco de dados atual
    try:
        database_name = client.execute("SELECT currentDatabase()")[0][0]
    except Exception:
        # Se não conseguir, usar o padrão
        database_name = "cnpj"

    # Aumentar limite de partes quebradas para permitir remoção de tabelas corrompidas
    # try:
    #     client.execute("SET max_suspicious_broken_parts = 10000")
    # except Exception:
    #     pass  # Se não conseguir, continua mesmo assim

    tabelas_removidas = []
    tabelas_com_erro = []

    try:
        for tabela in tabelas_alvo:
            removida = False
            
            # Estratégia 1: DROP direto (mais agressivo - remove tudo)
            try:
                client.execute(f"DROP TABLE IF EXISTS {tabela}")
                tabelas_removidas.append(tabela)
                logger.info("  ✓ %s removida completamente", tabela)
                removida = True
            except Exception as drop_exc:
                error_str = str(drop_exc).lower()
                
                # Se falhar por partes quebradas, marcar para remoção do banco inteiro
                if any(keyword in error_str for keyword in ("broken parts", "suspiciously", "cannot attach", "too_many_unexpected_data_parts")):
                    logger.warning("  ⚠ Tabela %s tem partes quebradas e não pode ser removida individualmente", tabela)
                    tabelas_com_erro.append(tabela)
                else:
                    # Outro tipo de erro
                    logger.error("  ✗ Erro ao remover %s: %s", tabela, drop_exc)
                    tabelas_com_erro.append(tabela)

        # Se houver tabelas com erro (partes quebradas), remover o banco inteiro
        if tabelas_com_erro:
            logger.warning("⚠ %s tabela(s) não puderam ser removidas: %s", len(tabelas_com_erro), ", ".join(tabelas_com_erro))
            logger.info("  Removendo banco de dados inteiro para limpar dados corrompidos...")
            
            try:
                # Criar um novo cliente sem especificar o banco para poder dropar o banco corrompido
                from clickhouse_driver import Client as ClickHouseClient
                config = carregar_config()
                
                # Conectar sem especificar o banco
                connect_kwargs = {
                    "host": config.host,
                    "port": config.port,
                    "user": config.user,
                    "connect_timeout": 10,
                }
                if config.password and config.password.strip() and config.password.strip().lower() != "none":
                    connect_kwargs["password"] = config.password.strip()
                
                # Conectar ao banco 'default'
                client_default = ClickHouseClient(**connect_kwargs)
                client_default.execute("USE default")
                
                # Tentar aumentar o limite antes de dropar
                try:
                    client_default.execute("SET max_suspicious_broken_parts = 10000")
                except Exception:
                    pass
                
                # Remover banco inteiro (isso remove todas as tabelas e dados, incluindo corrompidos)
                logger.info("  Executando DROP DATABASE...")
                client_default.execute(f"DROP DATABASE IF EXISTS {database_name}")
                logger.info("  ✓ Banco '%s' removido completamente", database_name)
                
                # Recriar o banco
                client_default.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
                logger.info("  ✓ Banco '%s' recriado", database_name)
                
                # Fechar conexão temporária
                client_default.disconnect()
                
                # Atualizar o cliente original para usar o banco recriado
                connect_kwargs["database"] = database_name
                client_new = ClickHouseClient(**connect_kwargs)
                client_new.execute("SELECT 1")
                
                # Substituir o cliente (não podemos fazer isso diretamente, então retornamos True e o processo continuará)
                logger.info("  ✓ Banco de dados completamente limpo (banco removido e recriado)")
                logger.warning("⚠ É necessário reconectar ao banco. O processo continuará na próxima etapa.")
                return True
                
            except Exception as db_exc:
                error_str = str(db_exc).lower()
                if "too_many_unexpected_data_parts" in error_str or "suspiciously many" in error_str:
                    logger.error("✗ Erro ao remover banco: partes quebradas detectadas")
                    logger.error("")
                    logger.error("  ⚠ SOLUÇÃO NECESSÁRIA:")
                    logger.error("  O ClickHouse precisa ser reiniciado para aplicar o max_suspicious_broken_parts.")
                    logger.error("")
                    logger.error("  Execute um dos comandos abaixo:")
                    logger.error("    docker restart clickhouse-cnpj")
                    logger.error("    docker-compose restart clickhouse")
                    logger.error("    docker restart <nome_do_container_clickhouse>")
                    logger.error("")
                    logger.error("  Após reiniciar, execute o script novamente.")
                    logger.error("")
                    
                    # Verificar se o banco já está vazio (caso o usuário tenha reiniciado manualmente)
                    try:
                        client_check = ClickHouseClient(**connect_kwargs)
                        client_check.execute(f"USE {database_name}")
                        tabelas_check = client_check.execute("SHOW TABLES")
                        client_check.disconnect()
                        if not tabelas_check:
                            logger.info("  ℹ Verificação: Banco parece estar vazio. Continuando...")
                            return True
                    except Exception:
                        pass
                    
                    return False
                else:
                    logger.error("✗ Erro ao remover/recriar banco: %s", db_exc)
                    return False

        logger.info("  ✓ Banco de dados completamente limpo (%s tabelas removidas)", len(tabelas_removidas))
        return True
        
    except Exception as exc:
        logger.error("✗ Erro ao limpar banco: %s", exc)
        return False


def configurar_sessao_clickhouse(client: Client) -> None:
    """Configura otimizações de sessão do ClickHouse"""
    try:
        # max_partitions_per_insert_block não existe nesta versão, removido
        client.execute("SET send_timeout = 600")
        client.execute("SET receive_timeout = 600")
        # Habilitar LowCardinality se necessário
        # try:
        #     client.execute("SET allow_experimental_low_cardinality_type = 1")
        # except Exception:
        #     pass
        logger.info("✓ Otimizações de sessão aplicadas")
    except Exception as exc:
        logger.warning("⚠ Erro ao configurar sessão: %s", exc)


def verificar_importacao(client: Client, contagens_csv: Dict[str, dict]) -> bool:
    """Verifica se a quantidade de registros no banco bate com os arquivos CSV"""
    print("\n" + "=" * 80)
    print("VERIFICAÇÃO DE IMPORTAÇÃO")
    print("=" * 80)

    if not contagens_csv:
        print("⚠ Contagem de linhas não foi realizada. Mostrando apenas contagens do banco:")
        tabelas = ["empresas", "estabelecimentos", "socios", "simples", "cnaes", "motivos", "municipios", "naturezas", "paises", "qualificacoes"]
        for tabela in tabelas:
            try:
                count_db = client.execute(f"SELECT count() FROM {tabela}")[0][0]
                print(f"  {tabela:20s} | DB: {count_db:>15,}")
            except Exception as exc:
                print(f"  ✗ {tabela:20s} | Erro: {exc}")
        print("=" * 80)
        return True

    tudo_ok = True
    for tabela, dados in contagens_csv.items():
        if dados["validas"] == 0:
            continue
        try:
            count_db = client.execute(f"SELECT count() FROM {tabela}")[0][0]
            count_csv = dados["validas"]
            status = "✓" if count_db == count_csv else "✗"
            tudo_ok = tudo_ok and (status == "✓")
            diff = count_db - count_csv
            print(f"{status} {tabela:20s} | CSV: {count_csv:>15,} | DB: {count_db:>15,} | Diff: {diff:>10,}")
        except Exception as exc:
            print(f"✗ {tabela:20s} | Erro ao verificar: {exc}")
            tudo_ok = False

    print("=" * 80)
    if tudo_ok:
        print("✓ Todas as importações estão corretas!")
    else:
        print("⚠ Algumas importações não bateram. Verifique os logs acima.")
    return tudo_ok


def obter_estatisticas(client: Client, tabelas: List[str]) -> Dict[str, int]:
    """Obtém estatísticas de contagem de registros por tabela"""
    stats: Dict[str, int] = {}
    for tabela in tabelas:
        try:
            stats[tabela] = client.execute(f"SELECT count() FROM {tabela}")[0][0]
        except Exception:
            stats[tabela] = 0
    return stats


def obter_tamanho_banco(client: Client, database: str) -> Optional[str]:
    """Obtém o tamanho total do banco de dados"""
    try:
        tamanho = client.execute(
            "SELECT formatReadableSize(sum(bytes)) "
            "FROM system.parts WHERE database = %(db)s AND active = 1",
            {"db": database},
        )[0][0]
        return tamanho
    except Exception:
        return None

