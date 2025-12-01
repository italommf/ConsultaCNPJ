"""Endpoints de empresas e estabelecimentos"""
from fastapi import APIRouter, HTTPException, Depends, Query
from typing import Optional
from ..schemas import (
    CompanyDetailResponse,
    Estabelecimento,
    SearchRequest,
    SearchResponse,
    Empresa,
    Simples,
    Socio
)
from ..clickhouse_client import get_clickhouse_client
from ..utils import to_str, format_date, format_capital_social
from .. import auth
import logging

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/companies", tags=["empresas"])


@router.get("/cnpj/{cnpj}", response_model=CompanyDetailResponse)
async def buscar_por_cnpj(
    cnpj: str,
    current_user: dict = Depends(auth.get_current_user)
):
    """
    Busca empresa completa por CNPJ.
    Retorna estabelecimento, empresa, sócios e simples nacional.
    """
    # Limpar e validar CNPJ
    cnpj_clean = "".join(filter(str.isdigit, cnpj))
    if len(cnpj_clean) != 14:
        raise HTTPException(status_code=400, detail="CNPJ deve ter 14 dígitos")
    
    client = get_clickhouse_client()
    
    try:
        # Buscar estabelecimento
        query_est = """
            SELECT 
                cnpj, cnpj_basico, cnpj_ordem, cnpj_dv, matriz_filial, nome_fantasia,
                situacao_cadastral, toString(data_situacao) as data_situacao,
                motivo_situacao, cidade_exterior, pais, toString(data_inicio) as data_inicio,
                cnae_fiscal, cnae_fiscal_secundaria, tipo_logradouro, logradouro,
                numero, complemento, bairro, cep, uf, municipio,
                ddd_1, telefone_1, ddd_2, telefone_2, ddd_fax, fax,
                email, situacao_especial, toString(data_situacao_especial) as data_situacao_especial
            FROM estabelecimentos
            WHERE cnpj = %(cnpj)s
            LIMIT 1
        """
        est_rows = client.execute(query_est, {"cnpj": cnpj_clean})
        
        if not est_rows:
            raise HTTPException(status_code=404, detail="CNPJ não encontrado")
        
        est_data = est_rows[0]
        cnpj_basico = to_str(est_data[1])
        if not cnpj_basico:
            raise HTTPException(status_code=404, detail="CNPJ não encontrado")
        
        # Buscar empresa
        query_emp = """
            SELECT 
                cnpj_basico, razao_social, natureza_juridica, qualificacao_do_responsavel,
                capital_social, porte, ente_federativo
            FROM empresas
            WHERE cnpj_basico = %(cnpj_basico)s
            LIMIT 1
        """
        emp_rows = client.execute(query_emp, {"cnpj_basico": cnpj_basico})
        empresa = None
        if emp_rows:
            emp_data = emp_rows[0]
            empresa = Empresa(
                cnpj_basico=to_str(emp_data[0]),
                razao_social=to_str(emp_data[1]),
                natureza_juridica=to_str(emp_data[2]),
                qualificacao_do_responsavel=to_str(emp_data[3]),
                capital_social=format_capital_social(emp_data[4]),
                porte=to_str(emp_data[5]),
                ente_federativo=to_str(emp_data[6])
            )
        
        # Buscar simples
        query_simp = """
            SELECT 
                opcao_simples, toString(data_opcao_simples) as data_opcao_simples,
                toString(data_exclusao_simples) as data_exclusao_simples,
                opcao_mei, toString(data_opcao_mei) as data_opcao_mei,
                toString(data_exclusao_mei) as data_exclusao_mei
            FROM simples
            WHERE cnpj_basico = %(cnpj_basico)s
            LIMIT 1
        """
        simp_rows = client.execute(query_simp, {"cnpj_basico": cnpj_basico})
        simples = None
        if simp_rows:
            simp_data = simp_rows[0]
            simples = Simples(
                opcao_simples=to_str(simp_data[0]),
                data_opcao_simples=format_date(to_str(simp_data[1])),
                data_exclusao_simples=format_date(to_str(simp_data[2])),
                opcao_mei=to_str(simp_data[3]),
                data_opcao_mei=format_date(to_str(simp_data[4])),
                data_exclusao_mei=format_date(to_str(simp_data[5]))
            )
        
        # Buscar sócios
        query_soc = """
            SELECT 
                identificador_socio, nome_socio, cnpj_cpf_socio, qualificacao_socio,
                toString(data_entrada_sociedade) as data_entrada_sociedade, pais,
                representante_legal, nome_representante, qualificacao_representante, faixa_etaria
            FROM socios
            WHERE cnpj_basico = %(cnpj_basico)s
        """
        soc_rows = client.execute(query_soc, {"cnpj_basico": cnpj_basico})
        socios = []
        for soc_data in soc_rows:
            socios.append(Socio(
                identificador_socio=to_str(soc_data[0]),
                nome_socio=to_str(soc_data[1]),
                cnpj_cpf_socio=to_str(soc_data[2]),
                qualificacao_socio=to_str(soc_data[3]),
                data_entrada_sociedade=format_date(to_str(soc_data[4])),
                pais=to_str(soc_data[5]),
                representante_legal=to_str(soc_data[6]),
                nome_representante=to_str(soc_data[7]),
                qualificacao_representante=to_str(soc_data[8]),
                faixa_etaria=to_str(soc_data[9])
            ))
        
        # Buscar dados de domínio
        cnae_desc = None
        if est_data[12]:  # cnae_fiscal
            cnae_cod = to_str(est_data[12])
            if cnae_cod:
                cnae_rows = client.execute("SELECT descricao FROM cnaes WHERE codigo = %(codigo)s", {"codigo": cnae_cod})
                if cnae_rows:
                    cnae_desc = to_str(cnae_rows[0][0])
        
        municipio_desc = None
        if est_data[21]:  # municipio
            mun_cod = to_str(est_data[21])
            if mun_cod:
                mun_rows = client.execute("SELECT descricao FROM municipios WHERE codigo = %(codigo)s", {"codigo": mun_cod})
                if mun_rows:
                    municipio_desc = to_str(mun_rows[0][0])
        
        # Montar resposta
        estabelecimento = Estabelecimento(
            cnpj=to_str(est_data[0]),
            cnpj_basico=cnpj_basico,
            cnpj_ordem=to_str(est_data[2]),
            cnpj_dv=to_str(est_data[3]),
            matriz_filial=to_str(est_data[4]),
            nome_fantasia=to_str(est_data[5]),
            situacao_cadastral=to_str(est_data[6]),
            data_situacao=format_date(to_str(est_data[7])),
            motivo_situacao=to_str(est_data[8]),
            cidade_exterior=to_str(est_data[9]),
            pais=to_str(est_data[10]),
            data_inicio=format_date(to_str(est_data[11])),
            cnae_fiscal=to_str(est_data[12]),
            cnae_fiscal_secundaria=to_str(est_data[13]),
            tipo_logradouro=to_str(est_data[14]),
            logradouro=to_str(est_data[15]),
            numero=to_str(est_data[16]),
            complemento=to_str(est_data[17]),
            bairro=to_str(est_data[18]),
            cep=to_str(est_data[19]),
            uf=to_str(est_data[20]),
            municipio=to_str(est_data[21]),
            ddd_1=to_str(est_data[22]),
            telefone_1=to_str(est_data[23]),
            ddd_2=to_str(est_data[24]),
            telefone_2=to_str(est_data[25]),
            ddd_fax=to_str(est_data[26]),
            fax=to_str(est_data[27]),
            email=to_str(est_data[28]),
            situacao_especial=to_str(est_data[29]),
            data_situacao_especial=format_date(to_str(est_data[30]))
        )
        
        return CompanyDetailResponse(
            estabelecimento=estabelecimento,
            empresa=empresa,
            simples=simples,
            socios=socios,
            cnae_principal_desc=cnae_desc,
            municipio_desc=municipio_desc
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao buscar CNPJ {cnpj}: {e}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")


@router.get("/search", response_model=SearchResponse)
async def search_companies(
    q: Optional[str] = Query(None, description="Busca textual"),
    cnpj: Optional[str] = Query(None, description="CNPJ completo"),
    uf: Optional[str] = Query(None, description="UF"),
    municipio: Optional[str] = Query(None, description="Código município"),
    cnae_fiscal: Optional[str] = Query(None, description="CNAE fiscal"),
    situacao_cadastral: Optional[str] = Query(None, description="Situação cadastral"),
    matriz_filial: Optional[str] = Query(None, description="1=Matriz, 2=Filial"),
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1, le=1000),
    current_user: dict = Depends(auth.get_current_user)
):
    """
    Busca empresas com múltiplos filtros.
    Todos os filtros são indexados para performance máxima.
    """
    client = get_clickhouse_client()
    
    # Construir WHERE clause
    where_conditions = []
    params = {}
    
    if cnpj:
        cnpj_clean = "".join(filter(str.isdigit, cnpj))
        if len(cnpj_clean) == 14:
            where_conditions.append("cnpj = %(cnpj)s")
            params["cnpj"] = cnpj_clean
    
    if uf:
        where_conditions.append("uf = %(uf)s")
        params["uf"] = uf.upper()[:2]
    
    if municipio:
        where_conditions.append("municipio = %(municipio)s")
        params["municipio"] = municipio[:4]
    
    if cnae_fiscal:
        where_conditions.append("cnae_fiscal = %(cnae_fiscal)s")
        params["cnae_fiscal"] = cnae_fiscal[:7]
    
    if situacao_cadastral:
        where_conditions.append("situacao_cadastral = %(situacao_cadastral)s")
        params["situacao_cadastral"] = situacao_cadastral[:2]
    
    if matriz_filial:
        where_conditions.append("matriz_filial = %(matriz_filial)s")
        params["matriz_filial"] = matriz_filial[:1]
    
    if q:
        # Busca fuzzy em nome_fantasia (usando like para performance)
        where_conditions.append("(like(nome_fantasia, concat('%', %(q)s, '%')) OR like(toString(cnpj), concat('%', %(q)s, '%')))")
        params["q"] = q
    
    where_clause = " AND ".join(where_conditions) if where_conditions else "1"
    
    # Calcular offset
    offset = (page - 1) * page_size
    
    # Query de contagem
    count_query = f"SELECT count() FROM estabelecimentos WHERE {where_clause}"
    total = client.execute(count_query, params)[0][0]
    
    # Query de dados
    data_query = f"""
        SELECT 
            cnpj, cnpj_basico, cnpj_ordem, cnpj_dv, matriz_filial, nome_fantasia,
            situacao_cadastral, toString(data_situacao) as data_situacao,
            motivo_situacao, cidade_exterior, pais, toString(data_inicio) as data_inicio,
            cnae_fiscal, cnae_fiscal_secundaria, tipo_logradouro, logradouro,
            numero, complemento, bairro, cep, uf, municipio,
            ddd_1, telefone_1, ddd_2, telefone_2, ddd_fax, fax,
            email, situacao_especial, toString(data_situacao_especial) as data_situacao_especial
        FROM estabelecimentos
        WHERE {where_clause}
        ORDER BY cnpj
        LIMIT %(limit)s OFFSET %(offset)s
    """
    params["limit"] = page_size
    params["offset"] = offset
    
    rows = client.execute(data_query, params)
    
    results = []
    for row in rows:
        results.append(Estabelecimento(
            cnpj=to_str(row[0]),
            cnpj_basico=to_str(row[1]),
            cnpj_ordem=to_str(row[2]),
            cnpj_dv=to_str(row[3]),
            matriz_filial=to_str(row[4]),
            nome_fantasia=to_str(row[5]),
            situacao_cadastral=to_str(row[6]),
            data_situacao=format_date(to_str(row[7])),
            motivo_situacao=to_str(row[8]),
            cidade_exterior=to_str(row[9]),
            pais=to_str(row[10]),
            data_inicio=format_date(to_str(row[11])),
            cnae_fiscal=to_str(row[12]),
            cnae_fiscal_secundaria=to_str(row[13]),
            tipo_logradouro=to_str(row[14]),
            logradouro=to_str(row[15]),
            numero=to_str(row[16]),
            complemento=to_str(row[17]),
            bairro=to_str(row[18]),
            cep=to_str(row[19]),
            uf=to_str(row[20]),
            municipio=to_str(row[21]),
            ddd_1=to_str(row[22]),
            telefone_1=to_str(row[23]),
            ddd_2=to_str(row[24]),
            telefone_2=to_str(row[25]),
            ddd_fax=to_str(row[26]),
            fax=to_str(row[27]),
            email=to_str(row[28]),
            situacao_especial=to_str(row[29]),
            data_situacao_especial=format_date(to_str(row[30]))
        ))
    
    total_pages = (total + page_size - 1) // page_size
    
    return SearchResponse(
        total=total,
        page=page,
        page_size=page_size,
        total_pages=total_pages,
        results=results
    )

