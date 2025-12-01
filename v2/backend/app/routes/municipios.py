"""Endpoints de Municípios"""
from fastapi import APIRouter, HTTPException, Depends, Query
from typing import List, Optional
from ..schemas import Municipio
from ..clickhouse_client import get_clickhouse_client
from ..utils import to_str
from .. import auth

router = APIRouter(prefix="/municipios", tags=["municipios"])


@router.get("/", response_model=List[Municipio])
async def list_municipios(
    q: Optional[str] = Query(None, description="Busca textual na descrição"),
    uf: Optional[str] = Query(None, description="Filtrar por UF (precisa JOIN com estabelecimentos)"),
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1, le=1000),
    current_user: dict = Depends(auth.get_current_user)
):
    """Lista municípios com busca opcional"""
    client = get_clickhouse_client()
    
    where_clause = "1"
    params = {}
    
    if q:
        where_clause = "like(descricao, concat('%', %(q)s, '%'))"
        params["q"] = q
    
    offset = (page - 1) * page_size
    
    query = f"""
        SELECT codigo, descricao
        FROM municipios
        WHERE {where_clause}
        ORDER BY codigo
        LIMIT %(limit)s OFFSET %(offset)s
    """
    params["limit"] = page_size
    params["offset"] = offset
    
    rows = client.execute(query, params)
    
    return [
        Municipio(
            codigo=to_str(row[0]),
            descricao=to_str(row[1])
        )
        for row in rows
    ]


@router.get("/{codigo}", response_model=Municipio)
async def get_municipio(
    codigo: str,
    current_user: dict = Depends(auth.get_current_user)
):
    """Busca município por código"""
    client = get_clickhouse_client()
    
    query = "SELECT codigo, descricao FROM municipios WHERE codigo = %(codigo)s LIMIT 1"
    rows = client.execute(query, {"codigo": codigo[:4]})
    
    if not rows:
        raise HTTPException(status_code=404, detail="Município não encontrado")
    
    row = rows[0]
    return Municipio(
        codigo=to_str(row[0]),
        descricao=to_str(row[1])
    )

