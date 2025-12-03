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
    Socio,
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
    Retorna estabelecimento, empresa, sócios e simples nacional com estrutura completa e descrições.
    """
    # Limpar e validar CNPJ
    cnpj_clean = "".join(filter(str.isdigit, cnpj))
    if len(cnpj_clean) != 14:
        raise HTTPException(status_code=400, detail="CNPJ deve ter 14 dígitos")
    
    client = get_clickhouse_client()
    
    try:
        # 1. Buscar estabelecimento (query principal - mais rápida, sem JOINs)
        query_est = """
            SELECT 
                cnpj, cnpj_basico, matriz_filial, nome_fantasia,
                situacao_cadastral, motivo_situacao,
                toString(data_situacao) AS data_situacao,
                toString(data_inicio) AS data_abertura,
                cnae_fiscal, cnae_fiscal_secundaria,
                tipo_logradouro, logradouro, numero, complemento, bairro, cep,
                uf, municipio, pais,
                ddd_1, telefone_1, ddd_2, telefone_2, ddd_fax, fax, email,
                situacao_especial, toString(data_situacao_especial) AS data_situacao_especial,
                cidade_exterior
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
        
        # Preparar dados base
        data = {
            'cnpj': to_str(est_data[0]),
            'matriz_filial': to_str(est_data[2]),
            'nome_fantasia': to_str(est_data[3]),
            'situacao_cadastral': to_str(est_data[4]),
            'motivo_situacao': to_str(est_data[5]),
            'data_situacao': to_str(est_data[6]),
            'data_abertura': to_str(est_data[7]),
            'cnae_fiscal': to_str(est_data[8]),
            'cnae_fiscal_secundaria': to_str(est_data[9]),
            'tipo_logradouro': to_str(est_data[10]),
            'logradouro': to_str(est_data[11]),
            'numero': to_str(est_data[12]),
            'complemento': to_str(est_data[13]),
            'bairro': to_str(est_data[14]),
            'cep': to_str(est_data[15]),
            'uf': to_str(est_data[16]),
            'municipio_codigo': to_str(est_data[17]),
            'pais_estabelecimento_cod': to_str(est_data[18]),
            'ddd_1': to_str(est_data[19]),
            'telefone_1': to_str(est_data[20]),
            'ddd_2': to_str(est_data[21]),
            'telefone_2': to_str(est_data[22]),
            'ddd_fax': to_str(est_data[23]),
            'fax': to_str(est_data[24]),
            'email': to_str(est_data[25]),
            'situacao_especial': to_str(est_data[26]),
            'data_situacao_especial': to_str(est_data[27]),
            'cidade_exterior': to_str(est_data[28]),
        }
        
        # 2. Buscar empresa (query simples e rápida)
        query_emp = """
            SELECT 
                razao_social, capital_social, porte, natureza_juridica,
                ente_federativo, qualificacao_do_responsavel
            FROM empresas
            WHERE cnpj_basico = %(cnpj_basico)s
            LIMIT 1
        """
        emp_rows = client.execute(query_emp, {"cnpj_basico": cnpj_basico})
        if emp_rows:
            emp_data = emp_rows[0]
            data['razao_social'] = to_str(emp_data[0])
            data['capital_social'] = emp_data[1]
            data['porte'] = to_str(emp_data[2])
            data['natureza_juridica_cod'] = to_str(emp_data[3])
            data['ente_federativo'] = to_str(emp_data[4])
            data['qualif_resp_empresa_cod'] = to_str(emp_data[5])
        
        # 3. Buscar simples (query simples e rápida)
        query_simp = """
            SELECT 
                opcao_simples,
                toString(data_opcao_simples) AS data_opcao_simples,
                toString(data_exclusao_simples) AS data_exclusao_simples,
                opcao_mei,
                toString(data_opcao_mei) AS data_opcao_mei,
                toString(data_exclusao_mei) AS data_exclusao_mei
            FROM simples
            WHERE cnpj_basico = %(cnpj_basico)s
            LIMIT 1
        """
        simp_rows = client.execute(query_simp, {"cnpj_basico": cnpj_basico})
        if simp_rows:
            simp_data = simp_rows[0]
            data['opcao_simples'] = to_str(simp_data[0])
            data['data_opcao_simples'] = to_str(simp_data[1])
            data['data_exclusao_simples'] = to_str(simp_data[2])
            data['opcao_mei'] = to_str(simp_data[3])
            data['data_opcao_mei'] = to_str(simp_data[4])
            data['data_exclusao_mei'] = to_str(simp_data[5])
        
        # 4. Buscar sócios (query simples, sem JOINs)
        query_soc = """
            SELECT 
                identificador_socio, nome_socio, cnpj_cpf_socio,
                faixa_etaria, toString(data_entrada_sociedade) AS data_entrada_sociedade,
                qualificacao_socio, pais, representante_legal,
                nome_representante, qualificacao_representante
            FROM socios
            WHERE cnpj_basico = %(cnpj_basico)s
        """
        soc_rows = client.execute(query_soc, {"cnpj_basico": cnpj_basico})
        
        # 5. Buscar todas as descrições em batch (queries otimizadas, sem JOINs)
        # Coletar códigos únicos para buscar em batch
        codigos_unicos = {
            'cnae': set(),
            'municipio': set(),
            'motivo': set(),
            'natureza': set(),
            'pais_est': set(),
            'qual_resp': set(),
            'qual_soc': set(),
            'pais_soc': set(),
            'qual_rep': set(),
        }
        
        # Adicionar códigos do estabelecimento
        if data.get('cnae_fiscal'):
            codigos_unicos['cnae'].add(data['cnae_fiscal'])
        if data.get('municipio_codigo'):
            codigos_unicos['municipio'].add(data['municipio_codigo'])
        if data.get('motivo_situacao'):
            codigos_unicos['motivo'].add(data['motivo_situacao'])
        if data.get('pais_estabelecimento_cod'):
            codigos_unicos['pais_est'].add(data['pais_estabelecimento_cod'])
        
        # Adicionar códigos da empresa
        if data.get('natureza_juridica_cod'):
            codigos_unicos['natureza'].add(data['natureza_juridica_cod'])
        if data.get('qualif_resp_empresa_cod'):
            codigos_unicos['qual_resp'].add(data['qualif_resp_empresa_cod'])
        
        # Adicionar códigos dos sócios
        for soc_row in soc_rows:
            if soc_row[5]:  # qualificacao_socio
                codigos_unicos['qual_soc'].add(to_str(soc_row[5]))
            if soc_row[6]:  # pais
                codigos_unicos['pais_soc'].add(to_str(soc_row[6]))
            if soc_row[9]:  # qualificacao_representante
                codigos_unicos['qual_rep'].add(to_str(soc_row[9]))
        
        # Buscar todas as descrições em batch (queries paralelas otimizadas)
        descricoes = {}
        
        # CNAE principal
        if codigos_unicos['cnae']:
            cnae_cod = list(codigos_unicos['cnae'])[0]
            cnae_row = client.execute("SELECT descricao FROM cnaes WHERE codigo = %(codigo)s", {"codigo": cnae_cod})
            if cnae_row:
                data['cnae_principal_desc'] = to_str(cnae_row[0][0])
        
        # CNAEs secundários (processar antes de passar para processar_dados_empresa)
        cnaes_secundarios_list = []
        cnae_secundaria_str = to_str(data.get('cnae_fiscal_secundaria', ''))
        if cnae_secundaria_str and cnae_secundaria_str.strip():
            cnae_codes = [c.strip() for c in cnae_secundaria_str.split(',') if c.strip() and len(c.strip()) == 7]
            if cnae_codes:
                # Buscar descrições para cada CNAE secundário
                if len(cnae_codes) == 1:
                    cnae_rows = client.execute(
                        "SELECT codigo, descricao FROM cnaes WHERE codigo = %(codigo)s",
                        {"codigo": cnae_codes[0]}
                    )
                else:
                    # Para múltiplos valores, usar OR (mais compatível)
                    conditions = " OR ".join([f"codigo = %(codigo{i})s" for i in range(len(cnae_codes))])
                    params = {f"codigo{i}": cnae_codes[i] for i in range(len(cnae_codes))}
                    cnae_rows = client.execute(
                        f"SELECT codigo, descricao FROM cnaes WHERE {conditions}",
                        params
                    )
                cnae_desc_map = {to_str(row[0]): to_str(row[1]) for row in cnae_rows}
                
                for code in cnae_codes:
                    cnaes_secundarios_list.append({
                        'codigo': code,
                        'descricao': cnae_desc_map.get(code)
                    })
        data['cnaes_secundarios'] = cnaes_secundarios_list
        
        # Município
        if codigos_unicos['municipio']:
            mun_cod = list(codigos_unicos['municipio'])[0]
            mun_row = client.execute("SELECT descricao FROM municipios WHERE codigo = %(codigo)s", {"codigo": mun_cod})
            if mun_row:
                data['municipio_desc'] = to_str(mun_row[0][0])
        
        # Motivo situação
        if codigos_unicos['motivo']:
            mot_cod = list(codigos_unicos['motivo'])[0]
            mot_row = client.execute("SELECT descricao FROM motivos WHERE codigo = %(codigo)s", {"codigo": mot_cod})
            if mot_row:
                data['situacao_motivo_desc'] = to_str(mot_row[0][0])
        
        # Natureza jurídica
        if codigos_unicos['natureza']:
            nat_cod = list(codigos_unicos['natureza'])[0]
            nat_row = client.execute("SELECT descricao FROM naturezas WHERE codigo = %(codigo)s", {"codigo": nat_cod})
            if nat_row:
                data['natureza_juridica_desc'] = to_str(nat_row[0][0])
        
        # País estabelecimento
        if codigos_unicos['pais_est']:
            pais_est_cod = list(codigos_unicos['pais_est'])[0]
            pais_est_row = client.execute("SELECT descricao FROM paises WHERE codigo = %(codigo)s", {"codigo": pais_est_cod})
            if pais_est_row:
                data['pais_estabelecimento_desc'] = to_str(pais_est_row[0][0])
        
        # Qualificação responsável empresa
        if codigos_unicos['qual_resp']:
            qual_resp_cod = list(codigos_unicos['qual_resp'])[0]
            qual_resp_row = client.execute("SELECT descricao FROM qualificacoes WHERE codigo = %(codigo)s", {"codigo": qual_resp_cod})
            if qual_resp_row:
                data['qualif_resp_empresa_desc'] = to_str(qual_resp_row[0][0])
        
        # Buscar descrições dos sócios em batch (muito mais rápido)
        qual_soc_map = {}
        if codigos_unicos['qual_soc']:
            qual_soc_list = list(codigos_unicos['qual_soc'])
            # Construir query com valores diretamente (mais rápido e compatível)
            if len(qual_soc_list) == 1:
                qual_soc_rows = client.execute(
                    "SELECT codigo, descricao FROM qualificacoes WHERE codigo = %(codigo)s",
                    {"codigo": qual_soc_list[0]}
                )
            else:
                # Para múltiplos valores, usar OR (mais compatível que IN com tupla)
                conditions = " OR ".join([f"codigo = %(codigo{i})s" for i in range(len(qual_soc_list))])
                params = {f"codigo{i}": qual_soc_list[i] for i in range(len(qual_soc_list))}
                qual_soc_rows = client.execute(
                    f"SELECT codigo, descricao FROM qualificacoes WHERE {conditions}",
                    params
                )
            qual_soc_map = {to_str(row[0]): to_str(row[1]) for row in qual_soc_rows}
        
        pais_soc_map = {}
        if codigos_unicos['pais_soc']:
            pais_soc_list = list(codigos_unicos['pais_soc'])
            if len(pais_soc_list) == 1:
                pais_soc_rows = client.execute(
                    "SELECT codigo, descricao FROM paises WHERE codigo = %(codigo)s",
                    {"codigo": pais_soc_list[0]}
                )
            else:
                conditions = " OR ".join([f"codigo = %(codigo{i})s" for i in range(len(pais_soc_list))])
                params = {f"codigo{i}": pais_soc_list[i] for i in range(len(pais_soc_list))}
                pais_soc_rows = client.execute(
                    f"SELECT codigo, descricao FROM paises WHERE {conditions}",
                    params
                )
            pais_soc_map = {to_str(row[0]): to_str(row[1]) for row in pais_soc_rows}
        
        qual_rep_map = {}
        if codigos_unicos['qual_rep']:
            qual_rep_list = list(codigos_unicos['qual_rep'])
            if len(qual_rep_list) == 1:
                qual_rep_rows = client.execute(
                    "SELECT codigo, descricao FROM qualificacoes WHERE codigo = %(codigo)s",
                    {"codigo": qual_rep_list[0]}
                )
            else:
                conditions = " OR ".join([f"codigo = %(codigo{i})s" for i in range(len(qual_rep_list))])
                params = {f"codigo{i}": qual_rep_list[i] for i in range(len(qual_rep_list))}
                qual_rep_rows = client.execute(
                    f"SELECT codigo, descricao FROM qualificacoes WHERE {conditions}",
                    params
                )
            qual_rep_map = {to_str(row[0]): to_str(row[1]) for row in qual_rep_rows}
        
        # Processar sócios com descrições (já buscadas em batch)
        socios_list = []
        for soc_row in soc_rows:
            qual_soc_cod = to_str(soc_row[5])
            pais_soc_cod = to_str(soc_row[6])
            qual_rep_cod = to_str(soc_row[9])
            
            socios_list.append({
                'identificador_socio': to_str(soc_row[0]),
                'nome_socio': to_str(soc_row[1]),
                'cnpj_cpf_socio': to_str(soc_row[2]),
                'faixa_etaria': to_str(soc_row[3]),
                'data_entrada_sociedade': to_str(soc_row[4]),
                'qualif_socio_cod': qual_soc_cod,
                'qualif_socio_desc': qual_soc_map.get(qual_soc_cod),
                'pais_socio_cod': pais_soc_cod,
                'pais_socio_desc': pais_soc_map.get(pais_soc_cod),
                'representante_legal': to_str(soc_row[7]),
                'nome_representante': to_str(soc_row[8]),
                'qualif_rep_legal_cod': qual_rep_cod,
                'qualif_rep_legal_desc': qual_rep_map.get(qual_rep_cod)
            })
        
        data['socios'] = socios_list
        
        # Processar dados usando função auxiliar
        from ..process_data import processar_dados_empresa
        return processar_dados_empresa(data)
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao buscar CNPJ {cnpj}: {e}")
        import traceback
        traceback.print_exc()
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


@router.get("/cnae/{cnae}", response_model=SearchResponse)
async def search_by_cnae(
    cnae: str,
    cnae_sec: bool = Query(
        False,
        description="Se true, busca também em CNAEs secundários (cnae_fiscal_secundaria)",
    ),
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1, le=1000),
    current_user: dict = Depends(auth.get_current_user),
):
    """
    Busca empresas por CNAE.

    - `cnae`: código CNAE de 7 dígitos.
    - `cnae_sec=true`: inclui buscas em CNAEs secundários (campo `cnae_fiscal_secundaria`).
    """
    # Limpar e validar CNAE
    cnae_clean = "".join(filter(str.isdigit, cnae))[:7]
    if len(cnae_clean) != 7:
        raise HTTPException(status_code=400, detail="CNAE deve ter 7 dígitos")

    client = get_clickhouse_client()

    try:
        params = {"cnae": cnae_clean}

        if cnae_sec:
            where_clause = """
                (
                    cnae_fiscal = %(cnae)s
                    OR cnae_fiscal_secundaria = %(cnae)s
                    OR like(cnae_fiscal_secundaria, concat(%(cnae)s, ',%'))
                    OR like(cnae_fiscal_secundaria, concat('%,', %(cnae)s, ',%'))
                    OR like(cnae_fiscal_secundaria, concat('%,', %(cnae)s))
                )
            """
        else:
            where_clause = "cnae_fiscal = %(cnae)s"

        # Paginação
        offset = (page - 1) * page_size

        # Contagem total (número de estabelecimentos / empresas para esse CNAE)
        count_query = f"SELECT count() FROM estabelecimentos WHERE {where_clause}"
        total = client.execute(count_query, params)[0][0]

        # Dados paginados (mesma seleção de campos do /companies/search)
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
            results.append(
                Estabelecimento(
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
                    data_situacao_especial=format_date(to_str(row[30])),
                )
            )

        total_pages = (total + page_size - 1) // page_size

        return SearchResponse(
            total=total,
            page=page,
            page_size=page_size,
            total_pages=total_pages,
            results=results,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao buscar empresas por CNAE {cnae_clean}: {e}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")

