"""Endpoints de autenticação"""
from fastapi import APIRouter, HTTPException, status, Depends
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from .. import auth
from ..schemas import Token
from ..config import settings
import secrets

router = APIRouter(prefix="/auth", tags=["autenticação"])
security_basic = HTTPBasic()


# Usuários hardcoded (em produção, usar banco de dados)
# Formato: username -> password_hash
USERS = {
    "admin": "$2b$12$EixZaYVK1fsbw1ZfbX3OXePaWxn96p36WQoeG6Lruj3vjPGga31lW",  # senha: secret
    "api_user": "$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewY5GyY5Y5Y5Y5Y",  # senha: api123
}


def verify_user(credentials: HTTPBasicCredentials = Depends(security_basic)) -> str:
    """Verifica credenciais básicas"""
    correct_username = secrets.compare_digest(credentials.username, "admin")
    correct_password = secrets.compare_digest(credentials.password, "secret")
    
    if not (correct_username and correct_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Credenciais inválidas",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username


@router.post("/token", response_model=Token)
async def login_for_access_token(username: str = Depends(verify_user)):
    """Gera token de acesso JWT"""
    access_token = auth.create_access_token(data={"sub": username})
    return {"access_token": access_token, "token_type": "bearer"}


@router.get("/me")
async def read_users_me(current_user: dict = Depends(auth.get_current_user)):
    """Retorna informações do usuário autenticado"""
    return {"username": current_user.get("sub"), "exp": current_user.get("exp")}

