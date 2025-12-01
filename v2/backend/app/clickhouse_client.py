"""Cliente ClickHouse reutilizável com pool de conexões"""
from clickhouse_driver import Client
from typing import Optional
import logging
from .config import settings

logger = logging.getLogger(__name__)

# Cliente global (singleton)
_client: Optional[Client] = None


def get_clickhouse_client() -> Client:
    """Retorna cliente ClickHouse (singleton)"""
    global _client
    
    if _client is None:
        try:
            _client = Client(
                host=settings.CLICKHOUSE_HOST,
                port=settings.CLICKHOUSE_PORT,
                user=settings.CLICKHOUSE_USER,
                password=settings.CLICKHOUSE_PASSWORD,
                database=settings.CLICKHOUSE_DATABASE,
                connect_timeout=10,
                send_receive_timeout=300,
                sync_request_timeout=300,
                compression=True,  # Compressão de rede
            )
            # Testar conexão
            _client.execute("SELECT 1")
            logger.info("ClickHouse client conectado com sucesso")
        except Exception as e:
            logger.error(f"Erro ao conectar ao ClickHouse: {e}")
            raise
    
    return _client


def close_clickhouse_client():
    """Fecha conexão com ClickHouse"""
    global _client
    if _client is not None:
        try:
            _client.disconnect()
            _client = None
            logger.info("Conexão ClickHouse fechada")
        except Exception as e:
            logger.error(f"Erro ao fechar conexão ClickHouse: {e}")




