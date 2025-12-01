from rest_framework import viewsets, filters
from rest_framework.response import Response
from rest_framework.decorators import action
from django_filters.rest_framework import DjangoFilterBackend
from django.db.models import Subquery, OuterRef, Prefetch, Q
from django.db import connection
from .models import Estabelecimentos, Cnaes, Municipios, Motivos, Paises, Naturezas, Qualificacoes, Simples, Socios, Empresas
from .serializers import EstabelecimentosSerializer, CompanyDetailSerializer, CnaesSerializer, MunicipiosSerializer

class CompanyViewSet(viewsets.ReadOnlyModelViewSet):
    """
    ViewSet for companies (Estabelecimentos).
    Endpoints:
    - /api/companies/cnpj/{cnpj}/ - Busca empresa por CNPJ
    - /api/companies/search/ - Busca geral com múltiplos filtros (paginado, padrão: 1000 por página)
    """
    filter_backends = [DjangoFilterBackend, filters.SearchFilter]
    filterset_fields = {
        'uf': ['exact'],
        'municipio': ['exact'],
        'cnae_fiscal': ['exact'],
        'situacao_cadastral': ['exact'],
        'matriz_filial': ['exact'],
    }
    search_fields = ['nome_fantasia', 'cnpj_basico__razao_social']
    
    def list(self, request, *args, **kwargs):
        """Endpoint desabilitado. Use /api/companies/search/ para busca geral"""
        from rest_framework.exceptions import MethodNotAllowed
        raise MethodNotAllowed("GET", detail="Use /api/companies/search/ para busca geral com filtros ou /api/companies/cnpj/{cnpj}/ para buscar por CNPJ")

    @action(detail=False, methods=['get'], url_path='cnpj/(?P<cnpj>[^/.]+)')
    def buscar_cnpj(self, request, cnpj=None):
        """
        Busca empresa completa por CNPJ.
        Endpoint: /api/companies/cnpj/{cnpj}/
        """
        if not cnpj:
            from rest_framework.exceptions import ValidationError
            raise ValidationError("CNPJ é obrigatório")
        
        return self._buscar_por_cnpj(cnpj)
    
    @action(detail=False, methods=['get'], url_path='search')
    def search(self, request):
        """
        Busca geral de empresas com múltiplos filtros.
        Endpoint: /api/companies/search/
        
        Query parameters opcionais:
        - page: Número da página (padrão: 1)
        - page_size: Tamanho da página (padrão: 1000)
        - cnae_principal: CNAE principal (7 dígitos)
        - cnae_secundario: CNAE secundário (7 dígitos)
        - uf: Estado (sigla de 2 letras)
        - municipio: Código do município
        - capital_social_min: Capital social mínimo
        - capital_social_max: Capital social máximo
        - qtd_socios_min: Quantidade mínima de sócios
        - qtd_socios_max: Quantidade máxima de sócios
        - situacao_cadastral: Situação cadastral
        - matriz_filial: Matriz ou Filial (1=Matriz, 2=Filial)
        - porte: Porte da empresa (00, 01, 03, 05)
        - natureza_juridica: Código da natureza jurídica
        - simples: Opção pelo Simples Nacional (S ou N)
        - mei: Opção pelo MEI (S ou N)
        """
        # Parâmetros de paginação
        page = request.query_params.get('page', '1')
        page_size = request.query_params.get('page_size', '1000')
        
        try:
            page = int(page)
            if page < 1:
                page = 1
        except (ValueError, TypeError):
            page = 1
        
        try:
            page_size = int(page_size)
            if page_size < 1:
                page_size = 1000
        except (ValueError, TypeError):
            page_size = 1000
        
        # Coletar todos os filtros
        filters = {
            'cnae_principal': request.query_params.get('cnae_principal'),
            'cnae_secundario': request.query_params.get('cnae_secundario'),
            'uf': request.query_params.get('uf'),
            'municipio': request.query_params.get('municipio'),
            'capital_social_min': request.query_params.get('capital_social_min'),
            'capital_social_max': request.query_params.get('capital_social_max'),
            'qtd_socios_min': request.query_params.get('qtd_socios_min'),
            'qtd_socios_max': request.query_params.get('qtd_socios_max'),
            'situacao_cadastral': request.query_params.get('situacao_cadastral'),
            'matriz_filial': request.query_params.get('matriz_filial'),
            'porte': request.query_params.get('porte'),
            'natureza_juridica': request.query_params.get('natureza_juridica'),
            'simples': request.query_params.get('simples'),
            'mei': request.query_params.get('mei'),
        }
        
        return self._buscar_geral(page, page_size, filters)
    
    @action(detail=False, methods=['get'], url_path='cnae/(?P<cnae>[^/.]+)')
    def buscar_cnae(self, request, cnae=None):
        """
        Busca empresas por CNAE.
        Endpoint: /api/companies/cnae/{cnae}/
        Query parameters opcionais:
        - limit: Limita quantidade de resultados (máximo 1000)
        - cnae_sec: Se True, busca também em CNAEs secundários (padrão: False)
        """
        if not cnae:
            from rest_framework.exceptions import ValidationError
            raise ValidationError("CNAE é obrigatório")
        
        # Parâmetros de paginação
        page = request.query_params.get('page', '1')
        page_size = request.query_params.get('page_size', '1000')
        
        try:
            page = int(page)
            if page < 1:
                page = 1
        except (ValueError, TypeError):
            page = 1
        
        try:
            page_size = int(page_size)
            if page_size < 1:
                page_size = 1000
        except (ValueError, TypeError):
            page_size = 1000
        
        # Parâmetro cnae_sec: False por padrão
        cnae_sec = request.query_params.get('cnae_sec', 'false').lower()
        cnae_sec = cnae_sec in ('true', '1', 'yes', 'on')
        
        return self._buscar_por_cnae(cnae, page, page_size, cnae_sec)
    
    def _buscar_por_cnpj(self, cnpj):
        """Busca empresa completa por CNPJ"""
        # Limpar e validar CNPJ
        cnpj = cnpj.strip().replace('.', '').replace('/', '').replace('-', '')
        if len(cnpj) != 14 or not cnpj.isdigit():
            from rest_framework.exceptions import ValidationError
            raise ValidationError("CNPJ deve ter 14 dígitos")
        
        try:
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT 
                        -- ========================================================================
                        -- 1. DADOS DO ESTABELECIMENTO
                        -- ========================================================================
                        est.cnpj,
                        est.matriz_filial,
                        est.nome_fantasia,
                        est.situacao_cadastral,
                        mot.descricao AS situacao_motivo_desc,
                        CASE 
                            WHEN est.data_situacao IS NULL THEN NULL::text
                            ELSE COALESCE(
                                NULLIF(TO_CHAR(est.data_situacao, 'DD/MM/YYYY'), ''),
                                est.data_situacao::text
                            )
                        END AS data_situacao,
                        CASE 
                            WHEN est.data_inicio IS NULL THEN NULL::text
                            ELSE COALESCE(
                                NULLIF(TO_CHAR(est.data_inicio, 'DD/MM/YYYY'), ''),
                                est.data_inicio::text
                            )
                        END AS data_abertura,
                        est.cnae_fiscal,
                        cnae.descricao AS cnae_principal_desc,
                        est.cnae_fiscal_secundaria,
                        est.tipo_logradouro,
                        est.logradouro,
                        est.numero,
                        est.complemento,
                        est.bairro,
                        est.cep,
                        est.uf,
                        est.municipio AS municipio_codigo,
                        mun.descricao AS municipio_desc,
                        est.ddd_1, est.telefone_1,
                        est.ddd_2, est.telefone_2,
                        est.email,
                        est.pais AS pais_estabelecimento_cod,
                        pais_est.descricao AS pais_estabelecimento_desc,
                        est.situacao_especial,
                        CASE 
                            WHEN est.data_situacao_especial IS NULL THEN NULL::text
                            ELSE COALESCE(
                                NULLIF(TO_CHAR(est.data_situacao_especial, 'DD/MM/YYYY'), ''),
                                est.data_situacao_especial::text
                            )
                        END AS data_situacao_especial,
                        est.cidade_exterior,
                        est.ddd_fax,
                        est.fax,
                        -- ========================================================================
                        -- 2. DADOS DA EMPRESA
                        -- ========================================================================
                        emp.razao_social,
                        emp.capital_social,
                        emp.porte,
                        CASE 
                            WHEN emp.porte = '00' THEN 'Não informado'
                            WHEN emp.porte = '01' THEN 'Micro empresa'
                            WHEN emp.porte = '03' THEN 'Empresa de pequeno porte'
                            WHEN emp.porte = '05' THEN 'Demais'
                            ELSE NULL
                        END AS porte_desc,
                        emp.natureza_juridica AS natureza_juridica_cod,
                        nat.descricao AS natureza_juridica_desc,
                        emp.ente_federativo,
                        emp.qualificacao_do_responsavel AS qualif_resp_empresa_cod,
                        qual_resp.descricao AS qualif_resp_empresa_desc,
                        -- ========================================================================
                        -- 3. SIMPLES NACIONAL
                        -- ========================================================================
                        simp.opcao_simples,
                        CASE 
                            WHEN simp.data_opcao_simples IS NULL THEN NULL::text
                            WHEN simp.data_opcao_simples::text = '' OR simp.data_opcao_simples::text LIKE '%%BC%%' THEN NULL::text
                            ELSE COALESCE(
                                NULLIF(TO_CHAR(simp.data_opcao_simples, 'DD/MM/YYYY'), ''),
                                simp.data_opcao_simples::text
                            )
                        END AS data_opcao_simples,
                        CASE 
                            WHEN simp.data_exclusao_simples IS NULL THEN NULL::text
                            WHEN simp.data_exclusao_simples::text = '' OR simp.data_exclusao_simples::text LIKE '%%BC%%' THEN NULL::text
                            ELSE COALESCE(
                                NULLIF(TO_CHAR(simp.data_exclusao_simples, 'DD/MM/YYYY'), ''),
                                simp.data_exclusao_simples::text
                            )
                        END AS data_exclusao_simples,
                        simp.opcao_mei,
                        CASE 
                            WHEN simp.data_opcao_mei IS NULL THEN NULL::text
                            WHEN simp.data_opcao_mei::text = '' OR simp.data_opcao_mei::text LIKE '%%BC%%' THEN NULL::text
                            ELSE COALESCE(
                                NULLIF(TO_CHAR(simp.data_opcao_mei, 'DD/MM/YYYY'), ''),
                                simp.data_opcao_mei::text
                            )
                        END AS data_opcao_mei,
                        CASE 
                            WHEN simp.data_exclusao_mei IS NULL THEN NULL::text
                            WHEN simp.data_exclusao_mei::text = '' OR simp.data_exclusao_mei::text LIKE '%%BC%%' THEN NULL::text
                            ELSE COALESCE(
                                NULLIF(TO_CHAR(simp.data_exclusao_mei, 'DD/MM/YYYY'), ''),
                                simp.data_exclusao_mei::text
                            )
                        END AS data_exclusao_mei,
                        -- ========================================================================
                        -- 4. LISTA AGREGADA DE SÓCIOS (JSON)
                        -- ========================================================================
                        COALESCE(
                            json_agg(
                                jsonb_build_object(
                                    'identificador_socio', soc.identificador_socio,
                                    'nome_socio', soc.nome_socio,
                                    'cnpj_cpf_socio', soc.cnpj_cpf_socio,
                                    'faixa_etaria', soc.faixa_etaria,
                                    'faixa_etaria_desc', 
                                    CASE 
                                        WHEN soc.faixa_etaria = '1' THEN 'Entre 0 a 12 anos'
                                        WHEN soc.faixa_etaria = '2' THEN 'Entre 13 a 20 anos'
                                        WHEN soc.faixa_etaria = '3' THEN 'Entre 21 a 30 anos'
                                        WHEN soc.faixa_etaria = '4' THEN 'Entre 31 a 40 anos'
                                        WHEN soc.faixa_etaria = '5' THEN 'Entre 41 a 50 anos'
                                        WHEN soc.faixa_etaria = '6' THEN 'Entre 51 a 60 anos'
                                        WHEN soc.faixa_etaria = '7' THEN 'Entre 61 a 70 anos'
                                        WHEN soc.faixa_etaria = '8' THEN 'Entre 71 a 80 anos'
                                        WHEN soc.faixa_etaria = '9' THEN 'Maior de 80 anos'
                                        WHEN soc.faixa_etaria = '0' THEN 'Não informada'
                                        ELSE NULL
                                    END,
                                    'data_entrada_sociedade', 
                                    CASE 
                                        WHEN soc.data_entrada_sociedade IS NULL THEN NULL
                                        WHEN soc.data_entrada_sociedade::text LIKE '%%BC%%' THEN NULL
                                        WHEN soc.data_entrada_sociedade::text = '' THEN NULL
                                        ELSE COALESCE(
                                            NULLIF(TO_CHAR(soc.data_entrada_sociedade, 'DD/MM/YYYY'), ''),
                                            soc.data_entrada_sociedade::text
                                        )
                                    END,
                                    'qualif_socio_cod', soc.qualificacao_socio,
                                    'qualif_socio_desc', qual_soc.descricao,
                                    'pais_socio_cod', soc.pais,
                                    'pais_socio_desc', pais_soc.descricao,
                                    'representante_legal', soc.representante_legal,
                                    'nome_representante', soc.nome_representante,
                                    'qualif_rep_legal_cod', soc.qualificacao_representante,
                                    'qualif_rep_legal_desc', qual_rep.descricao
                                )
                            ) FILTER (WHERE soc.cnpj_basico IS NOT NULL),
                            '[]'::json
                        ) AS socios
                    FROM estabelecimentos est
                    LEFT JOIN empresas emp ON est.cnpj_basico = emp.cnpj_basico
                    LEFT JOIN simples simp ON est.cnpj_basico = simp.cnpj_basico
                    LEFT JOIN socios soc ON est.cnpj_basico = soc.cnpj_basico
                    LEFT JOIN municipios mun ON est.municipio = mun.codigo
                    LEFT JOIN cnaes cnae ON est.cnae_fiscal = cnae.codigo
                    LEFT JOIN motivos mot ON est.motivo_situacao = mot.codigo
                    LEFT JOIN naturezas nat ON emp.natureza_juridica = nat.codigo
                    LEFT JOIN paises pais_est ON est.pais = pais_est.codigo
                    LEFT JOIN qualificacoes qual_resp ON emp.qualificacao_do_responsavel = qual_resp.codigo
                    LEFT JOIN paises pais_soc ON soc.pais = pais_soc.codigo
                    LEFT JOIN qualificacoes qual_soc ON soc.qualificacao_socio = qual_soc.codigo
                    LEFT JOIN qualificacoes qual_rep ON soc.qualificacao_representante = qual_rep.codigo
                    WHERE est.cnpj = %s
                    GROUP BY
                        est.cnpj, est.matriz_filial, est.nome_fantasia, est.situacao_cadastral,
                        mot.descricao, est.data_situacao, est.data_inicio, est.cnae_fiscal,
                        cnae.descricao, est.cnae_fiscal_secundaria, est.tipo_logradouro,
                        est.logradouro, est.numero, est.complemento, est.bairro, est.cep,
                        est.uf, est.municipio, mun.descricao, est.ddd_1, est.telefone_1,
                        est.ddd_2, est.telefone_2, est.email, est.pais, pais_est.descricao,
                        est.situacao_especial, est.data_situacao_especial, est.cidade_exterior,
                        est.ddd_fax, est.fax,
                        emp.razao_social, emp.capital_social, emp.porte,
                        CASE 
                            WHEN emp.porte = '00' THEN 'Não informado'
                            WHEN emp.porte = '01' THEN 'Micro empresa'
                            WHEN emp.porte = '03' THEN 'Empresa de pequeno porte'
                            WHEN emp.porte = '05' THEN 'Demais'
                            ELSE NULL
                        END,
                        emp.natureza_juridica,
                        nat.descricao, emp.ente_federativo, emp.qualificacao_do_responsavel,
                        qual_resp.descricao,
                        simp.opcao_simples, simp.data_opcao_simples, simp.data_exclusao_simples,
                        simp.opcao_mei, simp.data_opcao_mei, simp.data_exclusao_mei
                """, [cnpj])
                
                row = cursor.fetchone()
                if not row:
                    from rest_framework.exceptions import NotFound
                    raise NotFound("CNPJ não encontrado")
                
                # Mapear colunas para valores
                columns = [col[0] for col in cursor.description]
                data = dict(zip(columns, row))
                
                # Processar dados usando função auxiliar
                response_data = self._processar_dados_empresa(data)
                return Response(response_data)
        except Exception as e:
            import traceback
            print(f"Erro ao executar query SQL: {e}")
            traceback.print_exc()
            from rest_framework.exceptions import APIException
            raise APIException(f"Erro ao buscar dados: {str(e)}")
    
    def _listar_todas_empresas(self, page=1, page_size=1000):
        """
        Lista todas as empresas com paginação.
        :param page: Número da página (padrão: 1)
        :param page_size: Tamanho da página (padrão: 1000)
        """
        try:
            with connection.cursor() as cursor:
                # Query para buscar todas as empresas
                query = """
                    SELECT
                        est.cnpj,
                        est.matriz_filial,
                        est.nome_fantasia,
                        est.situacao_cadastral,
                        mot.descricao AS situacao_motivo_desc,
                        CASE 
                            WHEN est.data_situacao IS NULL THEN NULL::text
                            ELSE COALESCE(
                                NULLIF(TO_CHAR(est.data_situacao, 'DD/MM/YYYY'), ''),
                                est.data_situacao::text
                            )
                        END AS data_situacao,
                        CASE 
                            WHEN est.data_inicio IS NULL THEN NULL::text
                            ELSE COALESCE(
                                NULLIF(TO_CHAR(est.data_inicio, 'DD/MM/YYYY'), ''),
                                est.data_inicio::text
                            )
                        END AS data_abertura,
                        est.cnae_fiscal,
                        cnae.descricao AS cnae_principal_desc,
                        est.cnae_fiscal_secundaria,
                        est.tipo_logradouro,
                        est.logradouro,
                        est.numero,
                        est.complemento,
                        est.bairro,
                        est.cep,
                        est.uf,
                        est.municipio AS municipio_codigo,
                        mun.descricao AS municipio_desc,
                        est.ddd_1, est.telefone_1,
                        est.ddd_2, est.telefone_2,
                        est.email,
                        est.pais AS pais_estabelecimento_cod,
                        pais_est.descricao AS pais_estabelecimento_desc,
                        est.situacao_especial,
                        CASE 
                            WHEN est.data_situacao_especial IS NULL THEN NULL::text
                            ELSE COALESCE(
                                NULLIF(TO_CHAR(est.data_situacao_especial, 'DD/MM/YYYY'), ''),
                                est.data_situacao_especial::text
                            )
                        END AS data_situacao_especial,
                        est.cidade_exterior,
                        est.ddd_fax,
                        est.fax,
                        emp.razao_social,
                        emp.capital_social,
                        emp.porte,
                        CASE 
                            WHEN emp.porte = '00' THEN 'Não informado'
                            WHEN emp.porte = '01' THEN 'Micro empresa'
                            WHEN emp.porte = '03' THEN 'Empresa de pequeno porte'
                            WHEN emp.porte = '05' THEN 'Demais'
                            ELSE NULL
                        END AS porte_desc,
                        emp.natureza_juridica AS natureza_juridica_cod,
                        nat.descricao AS natureza_juridica_desc,
                        emp.ente_federativo,
                        emp.qualificacao_do_responsavel AS qualif_resp_empresa_cod,
                        qual_resp.descricao AS qualif_resp_empresa_desc,
                        simp.opcao_simples,
                        CASE 
                            WHEN simp.data_opcao_simples IS NULL THEN NULL::text
                            WHEN simp.data_opcao_simples::text = '' OR simp.data_opcao_simples::text LIKE '%%BC%%' THEN NULL::text
                            ELSE COALESCE(
                                NULLIF(TO_CHAR(simp.data_opcao_simples, 'DD/MM/YYYY'), ''),
                                simp.data_opcao_simples::text
                            )
                        END AS data_opcao_simples,
                        CASE 
                            WHEN simp.data_exclusao_simples IS NULL THEN NULL::text
                            WHEN simp.data_exclusao_simples::text = '' OR simp.data_exclusao_simples::text LIKE '%%BC%%' THEN NULL::text
                            ELSE COALESCE(
                                NULLIF(TO_CHAR(simp.data_exclusao_simples, 'DD/MM/YYYY'), ''),
                                simp.data_exclusao_simples::text
                            )
                        END AS data_exclusao_simples,
                        simp.opcao_mei,
                        CASE 
                            WHEN simp.data_opcao_mei IS NULL THEN NULL::text
                            WHEN simp.data_opcao_mei::text = '' OR simp.data_opcao_mei::text LIKE '%%BC%%' THEN NULL::text
                            ELSE COALESCE(
                                NULLIF(TO_CHAR(simp.data_opcao_mei, 'DD/MM/YYYY'), ''),
                                simp.data_opcao_mei::text
                            )
                        END AS data_opcao_mei,
                        CASE 
                            WHEN simp.data_exclusao_mei IS NULL THEN NULL::text
                            WHEN simp.data_exclusao_mei::text = '' OR simp.data_exclusao_mei::text LIKE '%%BC%%' THEN NULL::text
                            ELSE COALESCE(
                                NULLIF(TO_CHAR(simp.data_exclusao_mei, 'DD/MM/YYYY'), ''),
                                simp.data_exclusao_mei::text
                            )
                        END AS data_exclusao_mei,
                        COALESCE(
                            json_agg(
                                jsonb_build_object(
                                    'identificador_socio', soc.identificador_socio,
                                    'nome_socio', soc.nome_socio,
                                    'cnpj_cpf_socio', soc.cnpj_cpf_socio,
                                    'faixa_etaria', soc.faixa_etaria,
                                    'faixa_etaria_desc', 
                                    CASE 
                                        WHEN soc.faixa_etaria = '1' THEN 'Entre 0 a 12 anos'
                                        WHEN soc.faixa_etaria = '2' THEN 'Entre 13 a 20 anos'
                                        WHEN soc.faixa_etaria = '3' THEN 'Entre 21 a 30 anos'
                                        WHEN soc.faixa_etaria = '4' THEN 'Entre 31 a 40 anos'
                                        WHEN soc.faixa_etaria = '5' THEN 'Entre 41 a 50 anos'
                                        WHEN soc.faixa_etaria = '6' THEN 'Entre 51 a 60 anos'
                                        WHEN soc.faixa_etaria = '7' THEN 'Entre 61 a 70 anos'
                                        WHEN soc.faixa_etaria = '8' THEN 'Entre 71 a 80 anos'
                                        WHEN soc.faixa_etaria = '9' THEN 'Maior de 80 anos'
                                        WHEN soc.faixa_etaria = '0' THEN 'Não informada'
                                        ELSE NULL
                                    END,
                                    'data_entrada_sociedade', 
                                    CASE 
                                        WHEN soc.data_entrada_sociedade IS NULL THEN NULL
                                        WHEN soc.data_entrada_sociedade::text LIKE '%%BC%%' THEN NULL
                                        WHEN soc.data_entrada_sociedade::text = '' THEN NULL
                                        ELSE COALESCE(
                                            NULLIF(TO_CHAR(soc.data_entrada_sociedade, 'DD/MM/YYYY'), ''),
                                            soc.data_entrada_sociedade::text
                                        )
                                    END,
                                    'qualif_socio_cod', soc.qualificacao_socio,
                                    'qualif_socio_desc', qual_soc.descricao,
                                    'pais_socio_cod', soc.pais,
                                    'pais_socio_desc', pais_soc.descricao,
                                    'representante_legal', soc.representante_legal,
                                    'nome_representante', soc.nome_representante,
                                    'qualif_rep_legal_cod', soc.qualificacao_representante,
                                    'qualif_rep_legal_desc', qual_rep.descricao
                                )
                            ) FILTER (WHERE soc.cnpj_basico IS NOT NULL),
                            '[]'::json
                        ) AS socios
                    FROM estabelecimentos est
                    LEFT JOIN empresas emp ON est.cnpj_basico = emp.cnpj_basico
                    LEFT JOIN simples simp ON est.cnpj_basico = simp.cnpj_basico
                    LEFT JOIN socios soc ON est.cnpj_basico = soc.cnpj_basico
                    LEFT JOIN municipios mun ON est.municipio = mun.codigo
                    LEFT JOIN cnaes cnae ON est.cnae_fiscal = cnae.codigo
                    LEFT JOIN motivos mot ON est.motivo_situacao = mot.codigo
                    LEFT JOIN naturezas nat ON emp.natureza_juridica = nat.codigo
                    LEFT JOIN paises pais_est ON est.pais = pais_est.codigo
                    LEFT JOIN qualificacoes qual_resp ON emp.qualificacao_do_responsavel = qual_resp.codigo
                    LEFT JOIN paises pais_soc ON soc.pais = pais_soc.codigo
                    LEFT JOIN qualificacoes qual_soc ON soc.qualificacao_socio = qual_soc.codigo
                    LEFT JOIN qualificacoes qual_rep ON soc.qualificacao_representante = qual_rep.codigo
                    GROUP BY
                        est.cnpj, est.matriz_filial, est.nome_fantasia, est.situacao_cadastral,
                        mot.descricao, est.data_situacao, est.data_inicio, est.cnae_fiscal,
                        cnae.descricao, est.cnae_fiscal_secundaria, est.tipo_logradouro,
                        est.logradouro, est.numero, est.complemento, est.bairro, est.cep,
                        est.uf, est.municipio, mun.descricao, est.ddd_1, est.telefone_1,
                        est.ddd_2, est.telefone_2, est.email, est.pais, pais_est.descricao,
                        est.situacao_especial, est.data_situacao_especial, est.cidade_exterior,
                        est.ddd_fax, est.fax,
                        emp.razao_social, emp.capital_social, emp.porte,
                        CASE 
                            WHEN emp.porte = '00' THEN 'Não informado'
                            WHEN emp.porte = '01' THEN 'Micro empresa'
                            WHEN emp.porte = '03' THEN 'Empresa de pequeno porte'
                            WHEN emp.porte = '05' THEN 'Demais'
                            ELSE NULL
                        END,
                        emp.natureza_juridica,
                        nat.descricao, emp.ente_federativo, emp.qualificacao_do_responsavel,
                        qual_resp.descricao,
                        simp.opcao_simples, simp.data_opcao_simples, simp.data_exclusao_simples,
                        simp.opcao_mei, simp.data_opcao_mei, simp.data_exclusao_mei
                    ORDER BY est.cnpj
                """
                
                # Contar total de registros
                count_query = "SELECT COUNT(DISTINCT est.cnpj) FROM estabelecimentos est"
                cursor.execute(count_query)
                total_count = cursor.fetchone()[0]
                
                # Calcular offset para paginação
                offset = (page - 1) * page_size
                
                # Adicionar LIMIT e OFFSET para paginação
                query += f" LIMIT {page_size} OFFSET {offset}"
                
                cursor.execute(query)
                
                rows = cursor.fetchall()
                
                # Processar cada resultado
                columns = [col[0] for col in cursor.description]
                empresas_list = []
                
                for row in rows:
                    data = dict(zip(columns, row))
                    empresa_data = self._processar_dados_empresa(data)
                    empresas_list.append(empresa_data)
                
                # Calcular informações de paginação
                total_pages = (total_count + page_size - 1) // page_size if total_count > 0 else 0
                
                return Response({
                    'count': len(empresas_list),
                    'total_count': total_count,
                    'page': page,
                    'page_size': page_size,
                    'total_pages': total_pages,
                    'results': empresas_list
                })
                
        except Exception as e:
            import traceback
            print(f"Erro ao listar empresas: {e}")
            traceback.print_exc()
            from rest_framework.exceptions import APIException
            raise APIException(f"Erro ao listar empresas: {str(e)}")
    
    def _buscar_geral(self, page=1, page_size=1000, filters=None):
        """
        Busca geral de empresas com múltiplos filtros.
        :param page: Número da página (padrão: 1)
        :param page_size: Tamanho da página (padrão: 1000)
        :param filters: Dicionário com os filtros a aplicar
        """
        if filters is None:
            filters = {}
        
        try:
            with connection.cursor() as cursor:
                # Construir condições WHERE dinamicamente
                where_conditions = []
                params = []
                
                # CNAE Principal
                if filters.get('cnae_principal'):
                    cnae_principal = filters['cnae_principal'].strip()
                    if len(cnae_principal) == 7 and cnae_principal.isdigit():
                        where_conditions.append("est.cnae_fiscal = %s")
                        params.append(cnae_principal)
                
                # CNAE Secundário
                if filters.get('cnae_secundario'):
                    cnae_secundario = filters['cnae_secundario'].strip()
                    if len(cnae_secundario) == 7 and cnae_secundario.isdigit():
                        where_conditions.append("""
                            (est.cnae_fiscal_secundaria = %s
                            OR est.cnae_fiscal_secundaria LIKE %s
                            OR est.cnae_fiscal_secundaria LIKE %s
                            OR est.cnae_fiscal_secundaria LIKE %s)
                        """)
                        params.extend([cnae_secundario, f'{cnae_secundario},%', f'%,{cnae_secundario},%', f'%,{cnae_secundario}'])
                
                # UF (Estado)
                if filters.get('uf'):
                    uf = filters['uf'].strip().upper()
                    if len(uf) == 2:
                        where_conditions.append("est.uf = %s")
                        params.append(uf)
                
                # Município
                if filters.get('municipio'):
                    municipio = filters['municipio'].strip()
                    where_conditions.append("est.municipio = %s")
                    params.append(municipio)
                
                # Capital Social - Mínimo
                if filters.get('capital_social_min'):
                    try:
                        capital_min = float(filters['capital_social_min'])
                        where_conditions.append("emp.capital_social >= %s")
                        params.append(capital_min)
                    except (ValueError, TypeError):
                        pass
                
                # Capital Social - Máximo
                if filters.get('capital_social_max'):
                    try:
                        capital_max = float(filters['capital_social_max'])
                        where_conditions.append("emp.capital_social <= %s")
                        params.append(capital_max)
                    except (ValueError, TypeError):
                        pass
                
                # Situação Cadastral
                if filters.get('situacao_cadastral'):
                    situacao = filters['situacao_cadastral'].strip()
                    where_conditions.append("est.situacao_cadastral = %s")
                    params.append(situacao)
                
                # Matriz/Filial
                if filters.get('matriz_filial'):
                    matriz_filial = filters['matriz_filial'].strip()
                    if matriz_filial in ('1', '2'):
                        where_conditions.append("est.matriz_filial = %s")
                        params.append(matriz_filial)
                
                # Porte
                if filters.get('porte'):
                    porte = filters['porte'].strip()
                    if porte in ('00', '01', '03', '05'):
                        where_conditions.append("emp.porte = %s")
                        params.append(porte)
                
                # Natureza Jurídica
                if filters.get('natureza_juridica'):
                    natureza = filters['natureza_juridica'].strip()
                    where_conditions.append("emp.natureza_juridica = %s")
                    params.append(natureza)
                
                # Simples Nacional
                if filters.get('simples'):
                    simples = filters['simples'].strip().upper()
                    if simples in ('S', 'N'):
                        where_conditions.append("simp.opcao_simples = %s")
                        params.append(simples)
                
                # MEI
                if filters.get('mei'):
                    mei = filters['mei'].strip().upper()
                    if mei in ('S', 'N'):
                        where_conditions.append("simp.opcao_mei = %s")
                        params.append(mei)
                
                # Construir WHERE clause
                where_clause = ""
                if where_conditions:
                    where_clause = "WHERE " + " AND ".join(where_conditions)
                
                # Query base
                query = """
                    SELECT
                        est.cnpj,
                        est.matriz_filial,
                        est.nome_fantasia,
                        est.situacao_cadastral,
                        mot.descricao AS situacao_motivo_desc,
                        CASE 
                            WHEN est.data_situacao IS NULL THEN NULL::text
                            ELSE COALESCE(
                                NULLIF(TO_CHAR(est.data_situacao, 'DD/MM/YYYY'), ''),
                                est.data_situacao::text
                            )
                        END AS data_situacao,
                        CASE 
                            WHEN est.data_inicio IS NULL THEN NULL::text
                            ELSE COALESCE(
                                NULLIF(TO_CHAR(est.data_inicio, 'DD/MM/YYYY'), ''),
                                est.data_inicio::text
                            )
                        END AS data_abertura,
                        est.cnae_fiscal,
                        cnae.descricao AS cnae_principal_desc,
                        est.cnae_fiscal_secundaria,
                        est.tipo_logradouro,
                        est.logradouro,
                        est.numero,
                        est.complemento,
                        est.bairro,
                        est.cep,
                        est.uf,
                        est.municipio AS municipio_codigo,
                        mun.descricao AS municipio_desc,
                        est.ddd_1, est.telefone_1,
                        est.ddd_2, est.telefone_2,
                        est.email,
                        est.pais AS pais_estabelecimento_cod,
                        pais_est.descricao AS pais_estabelecimento_desc,
                        est.situacao_especial,
                        CASE 
                            WHEN est.data_situacao_especial IS NULL THEN NULL::text
                            ELSE COALESCE(
                                NULLIF(TO_CHAR(est.data_situacao_especial, 'DD/MM/YYYY'), ''),
                                est.data_situacao_especial::text
                            )
                        END AS data_situacao_especial,
                        est.cidade_exterior,
                        est.ddd_fax,
                        est.fax,
                        emp.razao_social,
                        emp.capital_social,
                        emp.porte,
                        CASE 
                            WHEN emp.porte = '00' THEN 'Não informado'
                            WHEN emp.porte = '01' THEN 'Micro empresa'
                            WHEN emp.porte = '03' THEN 'Empresa de pequeno porte'
                            WHEN emp.porte = '05' THEN 'Demais'
                            ELSE NULL
                        END AS porte_desc,
                        emp.natureza_juridica AS natureza_juridica_cod,
                        nat.descricao AS natureza_juridica_desc,
                        emp.ente_federativo,
                        emp.qualificacao_do_responsavel AS qualif_resp_empresa_cod,
                        qual_resp.descricao AS qualif_resp_empresa_desc,
                        simp.opcao_simples,
                        CASE 
                            WHEN simp.data_opcao_simples IS NULL THEN NULL::text
                            WHEN simp.data_opcao_simples::text = '' OR simp.data_opcao_simples::text LIKE '%%BC%%' THEN NULL::text
                            ELSE COALESCE(
                                NULLIF(TO_CHAR(simp.data_opcao_simples, 'DD/MM/YYYY'), ''),
                                simp.data_opcao_simples::text
                            )
                        END AS data_opcao_simples,
                        CASE 
                            WHEN simp.data_exclusao_simples IS NULL THEN NULL::text
                            WHEN simp.data_exclusao_simples::text = '' OR simp.data_exclusao_simples::text LIKE '%%BC%%' THEN NULL::text
                            ELSE COALESCE(
                                NULLIF(TO_CHAR(simp.data_exclusao_simples, 'DD/MM/YYYY'), ''),
                                simp.data_exclusao_simples::text
                            )
                        END AS data_exclusao_simples,
                        simp.opcao_mei,
                        CASE 
                            WHEN simp.data_opcao_mei IS NULL THEN NULL::text
                            WHEN simp.data_opcao_mei::text = '' OR simp.data_opcao_mei::text LIKE '%%BC%%' THEN NULL::text
                            ELSE COALESCE(
                                NULLIF(TO_CHAR(simp.data_opcao_mei, 'DD/MM/YYYY'), ''),
                                simp.data_opcao_mei::text
                            )
                        END AS data_opcao_mei,
                        CASE 
                            WHEN simp.data_exclusao_mei IS NULL THEN NULL::text
                            WHEN simp.data_exclusao_mei::text = '' OR simp.data_exclusao_mei::text LIKE '%%BC%%' THEN NULL::text
                            ELSE COALESCE(
                                NULLIF(TO_CHAR(simp.data_exclusao_mei, 'DD/MM/YYYY'), ''),
                                simp.data_exclusao_mei::text
                            )
                        END AS data_exclusao_mei,
                        COALESCE(
                            json_agg(
                                jsonb_build_object(
                                    'identificador_socio', soc.identificador_socio,
                                    'nome_socio', soc.nome_socio,
                                    'cnpj_cpf_socio', soc.cnpj_cpf_socio,
                                    'faixa_etaria', soc.faixa_etaria,
                                    'faixa_etaria_desc', 
                                    CASE 
                                        WHEN soc.faixa_etaria = '1' THEN 'Entre 0 a 12 anos'
                                        WHEN soc.faixa_etaria = '2' THEN 'Entre 13 a 20 anos'
                                        WHEN soc.faixa_etaria = '3' THEN 'Entre 21 a 30 anos'
                                        WHEN soc.faixa_etaria = '4' THEN 'Entre 31 a 40 anos'
                                        WHEN soc.faixa_etaria = '5' THEN 'Entre 41 a 50 anos'
                                        WHEN soc.faixa_etaria = '6' THEN 'Entre 51 a 60 anos'
                                        WHEN soc.faixa_etaria = '7' THEN 'Entre 61 a 70 anos'
                                        WHEN soc.faixa_etaria = '8' THEN 'Entre 71 a 80 anos'
                                        WHEN soc.faixa_etaria = '9' THEN 'Maior de 80 anos'
                                        WHEN soc.faixa_etaria = '0' THEN 'Não informada'
                                        ELSE NULL
                                    END,
                                    'data_entrada_sociedade', 
                                    CASE 
                                        WHEN soc.data_entrada_sociedade IS NULL THEN NULL
                                        WHEN soc.data_entrada_sociedade::text LIKE '%%BC%%' THEN NULL
                                        WHEN soc.data_entrada_sociedade::text = '' THEN NULL
                                        ELSE COALESCE(
                                            NULLIF(TO_CHAR(soc.data_entrada_sociedade, 'DD/MM/YYYY'), ''),
                                            soc.data_entrada_sociedade::text
                                        )
                                    END,
                                    'qualif_socio_cod', soc.qualificacao_socio,
                                    'qualif_socio_desc', qual_soc.descricao,
                                    'pais_socio_cod', soc.pais,
                                    'pais_socio_desc', pais_soc.descricao,
                                    'representante_legal', soc.representante_legal,
                                    'nome_representante', soc.nome_representante,
                                    'qualif_rep_legal_cod', soc.qualificacao_representante,
                                    'qualif_rep_legal_desc', qual_rep.descricao
                                )
                            ) FILTER (WHERE soc.cnpj_basico IS NOT NULL),
                            '[]'::json
                        ) AS socios,
                        COUNT(soc.cnpj_basico) FILTER (WHERE soc.cnpj_basico IS NOT NULL) AS qtd_socios
                    FROM estabelecimentos est
                    LEFT JOIN empresas emp ON est.cnpj_basico = emp.cnpj_basico
                    LEFT JOIN simples simp ON est.cnpj_basico = simp.cnpj_basico
                    LEFT JOIN socios soc ON est.cnpj_basico = soc.cnpj_basico
                    LEFT JOIN municipios mun ON est.municipio = mun.codigo
                    LEFT JOIN cnaes cnae ON est.cnae_fiscal = cnae.codigo
                    LEFT JOIN motivos mot ON est.motivo_situacao = mot.codigo
                    LEFT JOIN naturezas nat ON emp.natureza_juridica = nat.codigo
                    LEFT JOIN paises pais_est ON est.pais = pais_est.codigo
                    LEFT JOIN qualificacoes qual_resp ON emp.qualificacao_do_responsavel = qual_resp.codigo
                    LEFT JOIN paises pais_soc ON soc.pais = pais_soc.codigo
                    LEFT JOIN qualificacoes qual_soc ON soc.qualificacao_socio = qual_soc.codigo
                    LEFT JOIN qualificacoes qual_rep ON soc.qualificacao_representante = qual_rep.codigo
                    """ + where_clause + """
                    GROUP BY
                        est.cnpj, est.matriz_filial, est.nome_fantasia, est.situacao_cadastral,
                        mot.descricao, est.data_situacao, est.data_inicio, est.cnae_fiscal,
                        cnae.descricao, est.cnae_fiscal_secundaria, est.tipo_logradouro,
                        est.logradouro, est.numero, est.complemento, est.bairro, est.cep,
                        est.uf, est.municipio, mun.descricao, est.ddd_1, est.telefone_1,
                        est.ddd_2, est.telefone_2, est.email, est.pais, pais_est.descricao,
                        est.situacao_especial, est.data_situacao_especial, est.cidade_exterior,
                        est.ddd_fax, est.fax,
                        emp.razao_social, emp.capital_social, emp.porte,
                        CASE 
                            WHEN emp.porte = '00' THEN 'Não informado'
                            WHEN emp.porte = '01' THEN 'Micro empresa'
                            WHEN emp.porte = '03' THEN 'Empresa de pequeno porte'
                            WHEN emp.porte = '05' THEN 'Demais'
                            ELSE NULL
                        END,
                        emp.natureza_juridica,
                        nat.descricao, emp.ente_federativo, emp.qualificacao_do_responsavel,
                        qual_resp.descricao,
                        simp.opcao_simples, simp.data_opcao_simples, simp.data_exclusao_simples,
                        simp.opcao_mei, simp.data_opcao_mei, simp.data_exclusao_mei
                    HAVING 1=1
                """
                
                # Adicionar filtros de quantidade de sócios no HAVING
                if filters.get('qtd_socios_min'):
                    try:
                        qtd_min = int(filters['qtd_socios_min'])
                        query = query.replace("HAVING 1=1", f"HAVING COUNT(soc.cnpj_basico) FILTER (WHERE soc.cnpj_basico IS NOT NULL) >= {qtd_min}")
                    except (ValueError, TypeError):
                        pass
                
                if filters.get('qtd_socios_max'):
                    try:
                        qtd_max = int(filters['qtd_socios_max'])
                        having_clause = query.split("HAVING")[1] if "HAVING" in query else "1=1"
                        if ">=" in having_clause:
                            query = query.replace(having_clause, f"{having_clause} AND COUNT(soc.cnpj_basico) FILTER (WHERE soc.cnpj_basico IS NOT NULL) <= {qtd_max}")
                        else:
                            query = query.replace("HAVING 1=1", f"HAVING COUNT(soc.cnpj_basico) FILTER (WHERE soc.cnpj_basico IS NOT NULL) <= {qtd_max}")
                    except (ValueError, TypeError):
                        pass
                
                query += " ORDER BY est.cnpj"
                
                # Contar total de registros
                # Se houver filtro de quantidade de sócios, usar subquery
                if filters.get('qtd_socios_min') or filters.get('qtd_socios_max'):
                    count_query = f"""
                        SELECT COUNT(DISTINCT est.cnpj)
                        FROM estabelecimentos est
                        LEFT JOIN empresas emp ON est.cnpj_basico = emp.cnpj_basico
                        LEFT JOIN simples simp ON est.cnpj_basico = simp.cnpj_basico
                        LEFT JOIN socios soc ON est.cnpj_basico = soc.cnpj_basico
                        {where_clause}
                        GROUP BY est.cnpj
                    """
                    having_parts = []
                    if filters.get('qtd_socios_min'):
                        try:
                            qtd_min = int(filters['qtd_socios_min'])
                            having_parts.append(f"COUNT(soc.cnpj_basico) FILTER (WHERE soc.cnpj_basico IS NOT NULL) >= {qtd_min}")
                        except (ValueError, TypeError):
                            pass
                    if filters.get('qtd_socios_max'):
                        try:
                            qtd_max = int(filters['qtd_socios_max'])
                            having_parts.append(f"COUNT(soc.cnpj_basico) FILTER (WHERE soc.cnpj_basico IS NOT NULL) <= {qtd_max}")
                        except (ValueError, TypeError):
                            pass
                    if having_parts:
                        count_query += " HAVING " + " AND ".join(having_parts)
                    count_query = f"SELECT COUNT(*) FROM ({count_query}) AS subquery"
                else:
                    count_query = f"""
                        SELECT COUNT(DISTINCT est.cnpj)
                        FROM estabelecimentos est
                        LEFT JOIN empresas emp ON est.cnpj_basico = emp.cnpj_basico
                        LEFT JOIN simples simp ON est.cnpj_basico = simp.cnpj_basico
                        {where_clause}
                    """
                
                cursor.execute(count_query, params)
                total_count = cursor.fetchone()[0]
                
                # Calcular offset para paginação
                offset = (page - 1) * page_size
                
                # Adicionar LIMIT e OFFSET para paginação
                query += f" LIMIT {page_size} OFFSET {offset}"
                
                cursor.execute(query, params)
                
                rows = cursor.fetchall()
                
                # Processar cada resultado
                columns = [col[0] for col in cursor.description]
                empresas_list = []
                
                for row in rows:
                    data = dict(zip(columns, row))
                    empresa_data = self._processar_dados_empresa(data)
                    empresas_list.append(empresa_data)
                
                # Calcular informações de paginação
                total_pages = (total_count + page_size - 1) // page_size if total_count > 0 else 0
                
                # Preparar filtros aplicados (incluindo municipio se fornecido)
                filters_applied = {k: v for k, v in filters.items() if v}
                
                # Se municipio foi fornecido, incluir na resposta
                if filters.get('municipio'):
                    filters_applied['municipio'] = filters['municipio']
                
                return Response({
                    'count': len(empresas_list),
                    'total_count': total_count,
                    'page': page,
                    'page_size': page_size,
                    'total_pages': total_pages,
                    'filters': filters_applied,
                    'municipio': filters.get('municipio') if filters.get('municipio') else None,
                    'results': empresas_list
                })
                
        except Exception as e:
            import traceback
            error_msg = str(e)
            print(f"Erro ao buscar empresas: {error_msg}")
            traceback.print_exc()
            
            # Mensagens de erro mais específicas
            if "syntax error" in error_msg.lower() or "sql" in error_msg.lower():
                from rest_framework.exceptions import APIException
                raise APIException(f"Erro na consulta SQL. Verifique os parâmetros fornecidos. Detalhes: {error_msg}")
            elif "not found" in error_msg.lower() or "does not exist" in error_msg.lower():
                from rest_framework.exceptions import NotFound
                raise NotFound("Nenhuma empresa encontrada com os filtros fornecidos")
            else:
                from rest_framework.exceptions import APIException
                raise APIException(f"Erro ao buscar empresas. Verifique os parâmetros e tente novamente. Detalhes: {error_msg}")
    
    def _buscar_por_cnae(self, cnae, page=1, page_size=1000, cnae_sec=False):
        """
        Busca empresas por CNAE com paginação.
        :param cnae: Código CNAE (7 dígitos)
        :param page: Número da página (padrão: 1)
        :param page_size: Tamanho da página (padrão: 1000)
        :param cnae_sec: Se True, busca também em CNAEs secundários (padrão: False)
        """
        # Limpar e validar CNAE (deve ter 7 dígitos)
        cnae = cnae.strip()
        if len(cnae) != 7 or not cnae.isdigit():
            from rest_framework.exceptions import ValidationError
            raise ValidationError("CNAE deve ter 7 dígitos")
        
        try:
            with connection.cursor() as cursor:
                # Query para buscar empresas com o CNAE (principal ou secundário)
                query = """
                    SELECT
                        est.cnpj,
                        est.matriz_filial,
                        est.nome_fantasia,
                        est.situacao_cadastral,
                        mot.descricao AS situacao_motivo_desc,
                        CASE 
                            WHEN est.data_situacao IS NULL THEN NULL::text
                            ELSE COALESCE(
                                NULLIF(TO_CHAR(est.data_situacao, 'DD/MM/YYYY'), ''),
                                est.data_situacao::text
                            )
                        END AS data_situacao,
                        CASE 
                            WHEN est.data_inicio IS NULL THEN NULL::text
                            ELSE COALESCE(
                                NULLIF(TO_CHAR(est.data_inicio, 'DD/MM/YYYY'), ''),
                                est.data_inicio::text
                            )
                        END AS data_abertura,
                        est.cnae_fiscal,
                        cnae.descricao AS cnae_principal_desc,
                        est.cnae_fiscal_secundaria,
                        est.tipo_logradouro,
                        est.logradouro,
                        est.numero,
                        est.complemento,
                        est.bairro,
                        est.cep,
                        est.uf,
                        est.municipio AS municipio_codigo,
                        mun.descricao AS municipio_desc,
                        est.ddd_1, est.telefone_1,
                        est.ddd_2, est.telefone_2,
                        est.email,
                        est.pais AS pais_estabelecimento_cod,
                        pais_est.descricao AS pais_estabelecimento_desc,
                        est.situacao_especial,
                        CASE 
                            WHEN est.data_situacao_especial IS NULL THEN NULL::text
                            ELSE COALESCE(
                                NULLIF(TO_CHAR(est.data_situacao_especial, 'DD/MM/YYYY'), ''),
                                est.data_situacao_especial::text
                            )
                        END AS data_situacao_especial,
                        est.cidade_exterior,
                        est.ddd_fax,
                        est.fax,
                        emp.razao_social,
                        emp.capital_social,
                        emp.porte,
                        CASE 
                            WHEN emp.porte = '00' THEN 'Não informado'
                            WHEN emp.porte = '01' THEN 'Micro empresa'
                            WHEN emp.porte = '03' THEN 'Empresa de pequeno porte'
                            WHEN emp.porte = '05' THEN 'Demais'
                            ELSE NULL
                        END AS porte_desc,
                        emp.natureza_juridica AS natureza_juridica_cod,
                        nat.descricao AS natureza_juridica_desc,
                        emp.ente_federativo,
                        emp.qualificacao_do_responsavel AS qualif_resp_empresa_cod,
                        qual_resp.descricao AS qualif_resp_empresa_desc,
                        simp.opcao_simples,
                        CASE 
                            WHEN simp.data_opcao_simples IS NULL THEN NULL::text
                            WHEN simp.data_opcao_simples::text = '' OR simp.data_opcao_simples::text LIKE '%%BC%%' THEN NULL::text
                            ELSE COALESCE(
                                NULLIF(TO_CHAR(simp.data_opcao_simples, 'DD/MM/YYYY'), ''),
                                simp.data_opcao_simples::text
                            )
                        END AS data_opcao_simples,
                        CASE 
                            WHEN simp.data_exclusao_simples IS NULL THEN NULL::text
                            WHEN simp.data_exclusao_simples::text = '' OR simp.data_exclusao_simples::text LIKE '%%BC%%' THEN NULL::text
                            ELSE COALESCE(
                                NULLIF(TO_CHAR(simp.data_exclusao_simples, 'DD/MM/YYYY'), ''),
                                simp.data_exclusao_simples::text
                            )
                        END AS data_exclusao_simples,
                        simp.opcao_mei,
                        CASE 
                            WHEN simp.data_opcao_mei IS NULL THEN NULL::text
                            WHEN simp.data_opcao_mei::text = '' OR simp.data_opcao_mei::text LIKE '%%BC%%' THEN NULL::text
                            ELSE COALESCE(
                                NULLIF(TO_CHAR(simp.data_opcao_mei, 'DD/MM/YYYY'), ''),
                                simp.data_opcao_mei::text
                            )
                        END AS data_opcao_mei,
                        CASE 
                            WHEN simp.data_exclusao_mei IS NULL THEN NULL::text
                            WHEN simp.data_exclusao_mei::text = '' OR simp.data_exclusao_mei::text LIKE '%%BC%%' THEN NULL::text
                            ELSE COALESCE(
                                NULLIF(TO_CHAR(simp.data_exclusao_mei, 'DD/MM/YYYY'), ''),
                                simp.data_exclusao_mei::text
                            )
                        END AS data_exclusao_mei,
                        COALESCE(
                            json_agg(
                                jsonb_build_object(
                                    'identificador_socio', soc.identificador_socio,
                                    'nome_socio', soc.nome_socio,
                                    'cnpj_cpf_socio', soc.cnpj_cpf_socio,
                                    'faixa_etaria', soc.faixa_etaria,
                                    'faixa_etaria_desc', 
                                    CASE 
                                        WHEN soc.faixa_etaria = '1' THEN 'Entre 0 a 12 anos'
                                        WHEN soc.faixa_etaria = '2' THEN 'Entre 13 a 20 anos'
                                        WHEN soc.faixa_etaria = '3' THEN 'Entre 21 a 30 anos'
                                        WHEN soc.faixa_etaria = '4' THEN 'Entre 31 a 40 anos'
                                        WHEN soc.faixa_etaria = '5' THEN 'Entre 41 a 50 anos'
                                        WHEN soc.faixa_etaria = '6' THEN 'Entre 51 a 60 anos'
                                        WHEN soc.faixa_etaria = '7' THEN 'Entre 61 a 70 anos'
                                        WHEN soc.faixa_etaria = '8' THEN 'Entre 71 a 80 anos'
                                        WHEN soc.faixa_etaria = '9' THEN 'Maior de 80 anos'
                                        WHEN soc.faixa_etaria = '0' THEN 'Não informada'
                                        ELSE NULL
                                    END,
                                    'data_entrada_sociedade', 
                                    CASE 
                                        WHEN soc.data_entrada_sociedade IS NULL THEN NULL
                                        WHEN soc.data_entrada_sociedade::text LIKE '%%BC%%' THEN NULL
                                        WHEN soc.data_entrada_sociedade::text = '' THEN NULL
                                        ELSE COALESCE(
                                            NULLIF(TO_CHAR(soc.data_entrada_sociedade, 'DD/MM/YYYY'), ''),
                                            soc.data_entrada_sociedade::text
                                        )
                                    END,
                                    'qualif_socio_cod', soc.qualificacao_socio,
                                    'qualif_socio_desc', qual_soc.descricao,
                                    'pais_socio_cod', soc.pais,
                                    'pais_socio_desc', pais_soc.descricao,
                                    'representante_legal', soc.representante_legal,
                                    'nome_representante', soc.nome_representante,
                                    'qualif_rep_legal_cod', soc.qualificacao_representante,
                                    'qualif_rep_legal_desc', qual_rep.descricao
                                )
                            ) FILTER (WHERE soc.cnpj_basico IS NOT NULL),
                            '[]'::json
                        ) AS socios
                    FROM estabelecimentos est
                    LEFT JOIN empresas emp ON est.cnpj_basico = emp.cnpj_basico
                    LEFT JOIN simples simp ON est.cnpj_basico = simp.cnpj_basico
                    LEFT JOIN socios soc ON est.cnpj_basico = soc.cnpj_basico
                    LEFT JOIN municipios mun ON est.municipio = mun.codigo
                    LEFT JOIN cnaes cnae ON est.cnae_fiscal = cnae.codigo
                    LEFT JOIN motivos mot ON est.motivo_situacao = mot.codigo
                    LEFT JOIN naturezas nat ON emp.natureza_juridica = nat.codigo
                    LEFT JOIN paises pais_est ON est.pais = pais_est.codigo
                    LEFT JOIN qualificacoes qual_resp ON emp.qualificacao_do_responsavel = qual_resp.codigo
                    LEFT JOIN paises pais_soc ON soc.pais = pais_soc.codigo
                    LEFT JOIN qualificacoes qual_soc ON soc.qualificacao_socio = qual_soc.codigo
                    LEFT JOIN qualificacoes qual_rep ON soc.qualificacao_representante = qual_rep.codigo
                    WHERE est.cnae_fiscal = %s
                    GROUP BY
                        est.cnpj, est.matriz_filial, est.nome_fantasia, est.situacao_cadastral,
                        mot.descricao, est.data_situacao, est.data_inicio, est.cnae_fiscal,
                        cnae.descricao, est.cnae_fiscal_secundaria, est.tipo_logradouro,
                        est.logradouro, est.numero, est.complemento, est.bairro, est.cep,
                        est.uf, est.municipio, mun.descricao, est.ddd_1, est.telefone_1,
                        est.ddd_2, est.telefone_2, est.email, est.pais, pais_est.descricao,
                        est.situacao_especial, est.data_situacao_especial, est.cidade_exterior,
                        est.ddd_fax, est.fax,
                        emp.razao_social, emp.capital_social, emp.porte,
                        CASE 
                            WHEN emp.porte = '00' THEN 'Não informado'
                            WHEN emp.porte = '01' THEN 'Micro empresa'
                            WHEN emp.porte = '03' THEN 'Empresa de pequeno porte'
                            WHEN emp.porte = '05' THEN 'Demais'
                            ELSE NULL
                        END,
                        emp.natureza_juridica,
                        nat.descricao, emp.ente_federativo, emp.qualificacao_do_responsavel,
                        qual_resp.descricao,
                        simp.opcao_simples, simp.data_opcao_simples, simp.data_exclusao_simples,
                        simp.opcao_mei, simp.data_opcao_mei, simp.data_exclusao_mei
                    ORDER BY est.cnpj
                """
                
                # Se cnae_sec for True, adicionar condições para buscar também em CNAEs secundários
                if cnae_sec:
                    query = query.replace(
                        "WHERE est.cnae_fiscal = %s",
                        """WHERE (
                            est.cnae_fiscal = %s 
                            OR est.cnae_fiscal_secundaria = %s
                            OR est.cnae_fiscal_secundaria LIKE %s
                            OR est.cnae_fiscal_secundaria LIKE %s
                            OR est.cnae_fiscal_secundaria LIKE %s
                        )"""
                    )
                    # Parâmetros para a query: 5 valores para os 5 placeholders
                    params = [cnae, cnae, f'{cnae},%', f'%,{cnae},%', f'%,{cnae}']
                else:
                    # Apenas CNAE principal
                    params = [cnae]
                
                # Contar total de registros (antes de adicionar LIMIT/OFFSET)
                if cnae_sec:
                    count_query = """
                        SELECT COUNT(DISTINCT est.cnpj)
                        FROM estabelecimentos est
                        WHERE (
                            est.cnae_fiscal = %s 
                            OR est.cnae_fiscal_secundaria = %s
                            OR est.cnae_fiscal_secundaria LIKE %s
                            OR est.cnae_fiscal_secundaria LIKE %s
                            OR est.cnae_fiscal_secundaria LIKE %s
                        )
                    """
                    count_params = [cnae, cnae, f'{cnae},%', f'%,{cnae},%', f'%,{cnae}']
                else:
                    count_query = """
                        SELECT COUNT(DISTINCT est.cnpj)
                        FROM estabelecimentos est
                        WHERE est.cnae_fiscal = %s
                    """
                    count_params = [cnae]
                
                cursor.execute(count_query, count_params)
                total_count = cursor.fetchone()[0]
                
                # Calcular offset para paginação
                offset = (page - 1) * page_size
                
                # Adicionar LIMIT e OFFSET para paginação
                query += f" LIMIT {page_size} OFFSET {offset}"
                
                cursor.execute(query, params)
                
                rows = cursor.fetchall()
                
                # Processar cada resultado
                columns = [col[0] for col in cursor.description]
                empresas_list = []
                
                for row in rows:
                    data = dict(zip(columns, row))
                    empresa_data = self._processar_dados_empresa(data)
                    empresas_list.append(empresa_data)
                
                # Calcular informações de paginação
                total_pages = (total_count + page_size - 1) // page_size if total_count > 0 else 0
                
                return Response({
                    'count': len(empresas_list),
                    'total_count': total_count,
                    'page': page,
                    'page_size': page_size,
                    'total_pages': total_pages,
                    'cnae': cnae,
                    'cnae_sec': cnae_sec,
                    'results': empresas_list
                })
                
        except Exception as e:
            import traceback
            print(f"Erro ao buscar empresas por CNAE: {e}")
            traceback.print_exc()
            from rest_framework.exceptions import APIException
            raise APIException(f"Erro ao buscar empresas por CNAE: {str(e)}")

    def get_queryset(self):
        # Annotate descriptions manually to avoid FK issues with dirty data
        municipio_desc = Subquery(
            Municipios.objects.filter(codigo=OuterRef('municipio')).values('descricao')[:1]
        )
        cnae_desc = Subquery(
            Cnaes.objects.filter(codigo=OuterRef('cnae_fiscal')).values('descricao')[:1]
        )
        motivo_desc = Subquery(
            Motivos.objects.filter(codigo=OuterRef('motivo_situacao')).values('descricao')[:1]
        )
        pais_desc = Subquery(
            Paises.objects.filter(codigo=OuterRef('pais')).values('descricao')[:1]
        )

        queryset = Estabelecimentos.objects.all().select_related(
            'cnpj_basico' # This FK is safe (Empresas)
        ).annotate(
            municipio_desc=municipio_desc,
            cnae_principal_desc=cnae_desc,
            situacao_motivo_desc=motivo_desc,
            pais_desc=pais_desc
        )

        return queryset

    def retrieve(self, request, *args, **kwargs):
        """Endpoint desabilitado. Use /api/companies/cnpj/{cnpj}/ para buscar por CNPJ"""
        from rest_framework.exceptions import MethodNotAllowed
        raise MethodNotAllowed("GET", detail="Use /api/companies/cnpj/{cnpj}/ para buscar por CNPJ")
    
    def _processar_dados_empresa(self, data):
        """Função auxiliar para processar dados de uma empresa e retornar estrutura completa"""
        import json
        
        # Converter JSON de sócios e reorganizar estrutura
        socios_raw = []
        if data.get('socios'):
            if isinstance(data['socios'], str):
                socios_raw = json.loads(data['socios'])
            elif isinstance(data['socios'], list):
                socios_raw = data['socios']
            elif data['socios'] is None:
                socios_raw = []
        else:
            socios_raw = []
        
        # Reorganizar estrutura dos sócios: códigos e descrições como subchaves
        socios_organizados = []
        for socio in socios_raw:
            socio_org = {
                'identificacao': {
                    'identificador_socio': socio.get('identificador_socio'),
                    'nome_socio': socio.get('nome_socio'),
                    'cnpj_cpf_socio': socio.get('cnpj_cpf_socio')
                },
                'faixa_etaria': {
                    'codigo': socio.get('faixa_etaria'),
                    'descricao': socio.get('faixa_etaria_desc')
                },
                'data_entrada_sociedade': socio.get('data_entrada_sociedade'),
                'qualificacao_socio': {
                    'codigo': socio.get('qualif_socio_cod'),
                    'descricao': socio.get('qualif_socio_desc')
                },
                'pais': {
                    'codigo': socio.get('pais_socio_cod'),
                    'descricao': socio.get('pais_socio_desc')
                },
                'representante_legal': {
                    'representante_legal': socio.get('representante_legal'),
                    'nome_representante': socio.get('nome_representante'),
                    'qualificacao_representante': {
                        'codigo': socio.get('qualif_rep_legal_cod'),
                        'descricao': socio.get('qualif_rep_legal_desc')
                    }
                }
            }
            socios_organizados.append(socio_org)
        
        data['socios'] = socios_organizados
        
        # Processar CNAEs secundários (pode ser string com múltiplos códigos separados por vírgula)
        cnaes_secundarios_list = []
        cnae_secundaria_str = data.get('cnae_fiscal_secundaria', '')
        if cnae_secundaria_str and cnae_secundaria_str.strip():
            # CNAEs secundários estão separados por vírgula
            cnae_codes = [c.strip() for c in cnae_secundaria_str.split(',') if c.strip() and len(c.strip()) == 7]
            # Buscar descrições para cada CNAE secundário
            if cnae_codes:
                with connection.cursor() as cnae_cursor:
                    placeholders = ','.join(['%s'] * len(cnae_codes))
                    cnae_cursor.execute(f"""
                        SELECT codigo, descricao 
                        FROM cnaes 
                        WHERE codigo IN ({placeholders})
                    """, cnae_codes)
                    cnae_desc_map = {row[0]: row[1] for row in cnae_cursor.fetchall()}
                    
                    for code in cnae_codes:
                        cnaes_secundarios_list.append({
                            'codigo': code,
                            'descricao': cnae_desc_map.get(code)
                        })
        data['cnaes_secundarios'] = cnaes_secundarios_list
        
        # Construir resposta estruturada
        response_data = {
            'estabelecimento': {
                'identificacao': {
                    'cnpj': data.get('cnpj'),
                    'matriz_filial': data.get('matriz_filial'),
                    'nome_fantasia': data.get('nome_fantasia')
                },
                'situacao': {
                    'situacao_cadastral': data.get('situacao_cadastral'),
                    'situacao_motivo_desc': data.get('situacao_motivo_desc'),
                    'data_situacao': data.get('data_situacao'),
                    'data_abertura': data.get('data_abertura'),
                    'situacao_especial': data.get('situacao_especial'),
                    'data_situacao_especial': data.get('data_situacao_especial')
                },
                'cnae': {
                    'principal': {
                        'codigo': data.get('cnae_fiscal'),
                        'descricao': data.get('cnae_principal_desc')
                    },
                    'secundarios': data.get('cnaes_secundarios', [])
                },
                'endereco': {
                    'tipo_logradouro': data.get('tipo_logradouro'),
                    'logradouro': data.get('logradouro'),
                    'numero': data.get('numero'),
                    'complemento': data.get('complemento'),
                    'bairro': data.get('bairro'),
                    'cep': data.get('cep'),
                    'uf': data.get('uf'),
                    'municipio': data.get('municipio_codigo'),
                    'municipio_desc': data.get('municipio_desc'),
                    'cidade_exterior': data.get('cidade_exterior'),
                    'pais': data.get('pais_estabelecimento_cod'),
                    'pais_desc': data.get('pais_estabelecimento_desc')
                },
                'contato': {
                    'ddd_1': data.get('ddd_1'),
                    'telefone_1': data.get('telefone_1'),
                    'ddd_2': data.get('ddd_2'),
                    'telefone_2': data.get('telefone_2'),
                    'ddd_fax': data.get('ddd_fax'),
                    'fax': data.get('fax'),
                    'email': data.get('email')
                }
            },
            'empresa': {
                'identificacao': {
                    'razao_social': data.get('razao_social')
                },
                'natureza_juridica': {
                    'codigo': data.get('natureza_juridica_cod'),
                    'descricao': data.get('natureza_juridica_desc')
                },
                'qualificacao': {
                    'codigo': data.get('qualif_resp_empresa_cod'),
                    'descricao': data.get('qualif_resp_empresa_desc')
                },
                'capital': {
                    'capital_social': data.get('capital_social')
                },
                'porte': {
                    'codigo': data.get('porte'),
                    'descricao': data.get('porte_desc')
                },
                'ente_federativo': data.get('ente_federativo'),
                'simples': {
                    'simples': {
                        'opcao_simples': data.get('opcao_simples'),
                        'data_opcao_simples': data.get('data_opcao_simples'),
                        'data_exclusao_simples': data.get('data_exclusao_simples')
                    },
                    'mei': {
                        'opcao_mei': data.get('opcao_mei'),
                        'data_opcao_mei': data.get('data_opcao_mei'),
                        'data_exclusao_mei': data.get('data_exclusao_mei')
                    }
                } if data.get('opcao_simples') or data.get('opcao_mei') else None,
                'socios': data.get('socios', [])
            }
        }
        
        return response_data
    
    def get_serializer_class(self):
        if self.action == 'retrieve':
            return CompanyDetailSerializer
        return EstabelecimentosSerializer

class CnaesViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = Cnaes.objects.all()
    serializer_class = CnaesSerializer
    filter_backends = [filters.SearchFilter]
    search_fields = ['codigo', 'descricao']

class MunicipiosViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = Municipios.objects.all()
    serializer_class = MunicipiosSerializer
    filter_backends = [filters.SearchFilter]
    search_fields = ['codigo', 'descricao']
