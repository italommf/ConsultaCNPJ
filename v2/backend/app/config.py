"""Configurações da aplicação FastAPI"""
from pydantic_settings import BaseSettings
from typing import List


class Settings(BaseSettings):
    # ClickHouse
    CLICKHOUSE_HOST: str = "localhost"
    CLICKHOUSE_PORT: int = 9000
    CLICKHOUSE_USER: str = "default"
    CLICKHOUSE_PASSWORD: str = ""
    CLICKHOUSE_DATABASE: str = "cnpj"
    
    # API
    API_TITLE: str = "CNPJ Search API"
    API_VERSION: str = "2.0.0"
    API_DESCRIPTION: str = "API otimizada para busca de dados CNPJ usando ClickHouse"
    
    # CORS
    CORS_ORIGINS: List[str] = ["http://localhost:3000", "http://localhost:5173", "http://127.0.0.1:3000"]
    CORS_ALLOW_CREDENTIALS: bool = True
    CORS_ALLOW_METHODS: List[str] = ["*"]
    CORS_ALLOW_HEADERS: List[str] = ["*"]
    
    # Autenticação
    SECRET_KEY: str = "your-secret-key-change-in-production"  # Mudar em produção!
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 1440  # 24 horas
    
    # Rate Limiting (opcional)
    RATE_LIMIT_PER_MINUTE: int = 100
    
    class Config:
        env_file = ".env"
        case_sensitive = True


settings = Settings()




