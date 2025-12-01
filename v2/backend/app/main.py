"""FastAPI Application Main"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import logging

from .config import settings
from .routes import auth, companies, cnaes, municipios

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Criar app FastAPI
app = FastAPI(
    title=settings.API_TITLE,
    version=settings.API_VERSION,
    description=settings.API_DESCRIPTION,
    docs_url="/docs",
    redoc_url="/redoc"
)

# Configurar CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=settings.CORS_ALLOW_CREDENTIALS,
    allow_methods=settings.CORS_ALLOW_METHODS,
    allow_headers=settings.CORS_ALLOW_HEADERS,
)

# Incluir routers
app.include_router(auth.router)
app.include_router(companies.router)
app.include_router(cnaes.router)
app.include_router(municipios.router)


@app.get("/")
async def root():
    """Health check endpoint"""
    return {
        "status": "ok",
        "api": settings.API_TITLE,
        "version": settings.API_VERSION
    }


@app.get("/health")
async def health():
    """Health check detalhado"""
    from .clickhouse_client import get_clickhouse_client
    try:
        client = get_clickhouse_client()
        client.execute("SELECT 1")
        return {
            "status": "healthy",
            "clickhouse": "connected"
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return JSONResponse(
            status_code=503,
            content={
                "status": "unhealthy",
                "clickhouse": "disconnected",
                "error": str(e)
            }
        )


@app.on_event("startup")
async def startup_event():
    """Evento de inicialização"""
    logger.info("Iniciando aplicação FastAPI...")
    try:
        from .clickhouse_client import get_clickhouse_client
        client = get_clickhouse_client()
        logger.info("ClickHouse conectado com sucesso")
    except Exception as e:
        logger.error(f"Erro ao conectar ClickHouse na inicialização: {e}")


@app.on_event("shutdown")
async def shutdown_event():
    """Evento de encerramento"""
    logger.info("Encerrando aplicação FastAPI...")
    from .clickhouse_client import close_clickhouse_client
    close_clickhouse_client()




