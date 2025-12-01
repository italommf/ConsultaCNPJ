"""Endpoints de CNAEs"""
from fastapi import APIRouter, HTTPException, Depends, Query
from typing import List, Optional
from ..schemas import Cnae
from ..clickhouse_client import get_clickhouse_client
from ..utils import to_str
from .. import auth

router = APIRouter(prefix="/cnaes", tags=["cnaes"])


@router.get("/", response_model=List[Cnae])
async def list_cnaes(
    q: Optional[str] = Query(None, description="Busca textual na descrição"),
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1, le=1000),
    current_user: dict = Depends(auth.get_current_user)
):
    """Lista CNAEs com busca opcional"""
    client = get_clickhouse_client()
    
    where_clause = "1"
    params = {}
    
    if q:
        where_clause = "like(descricao, concat('%', %(q)s, '%'))"
        params["q"] = q
    
    offset = (page - 1) * page_size
    
    query = f"""
        SELECT codigo, descricao
        FROM cnaes
        WHERE {where_clause}
        ORDER BY codigo
        LIMIT %(limit)s OFFSET %(offset)s
    """
    params["limit"] = page_size
    params["offset"] = offset
    
    rows = client.execute(query, params)
    
    return [
        Cnae(
            codigo=to_str(row[0]),
            descricao=to_str(row[1])
        )
        for row in rows
    ]


@router.get("/{codigo}", response_model=Cnae)
async def get_cnae(
    codigo: str,
    current_user: dict = Depends(auth.get_current_user)
):
    """Busca CNAE por código"""
    client = get_clickhouse_client()
    
    query = "SELECT codigo, descricao FROM cnaes WHERE codigo = %(codigo)s LIMIT 1"
    rows = client.execute(query, {"codigo": codigo[:7]})
    
    if not rows:
        raise HTTPException(status_code=404, detail="CNAE não encontrado")
    
    row = rows[0]
    return Cnae(
        codigo=to_str(row[0]),
        descricao=to_str(row[1])
    )

