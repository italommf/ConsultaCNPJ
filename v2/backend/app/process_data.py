"""Função para processar dados da empresa na estrutura de resposta da API"""
from typing import Dict, List, Any, Optional
from .utils import to_str, format_date, format_capital_social
from .schemas import (
    CompanyDetailResponse,
    EstabelecimentoV1,
    IdentificacaoEstabelecimento,
    SituacaoEstabelecimento,
    CnaeInfo,
    CnaePrincipal,
    CnaeSecundario,
    EnderecoEstabelecimento,
    ContatoEstabelecimento,
    EmpresaV1,
    IdentificacaoEmpresa,
    NaturezaJuridica,
    Qualificacao,
    Capital,
    Porte,
    SimplesNacional,
    SimplesInfo,
    MeiInfo,
    SocioV1,
    IdentificacaoSocio,
    FaixaEtaria,
    QualificacaoSocio,
    PaisSocio,
    RepresentanteLegal,
    QualificacaoRepresentante
)


def get_faixa_etaria_desc(codigo: Optional[str]) -> Optional[str]:
    """Retorna descrição da faixa etária"""
    faixas = {
        '0': 'Não informada',
        '1': 'Entre 0 a 12 anos',
        '2': 'Entre 13 a 20 anos',
        '3': 'Entre 21 a 30 anos',
        '4': 'Entre 31 a 40 anos',
        '5': 'Entre 41 a 50 anos',
        '6': 'Entre 51 a 60 anos',
        '7': 'Entre 61 a 70 anos',
        '8': 'Entre 71 a 80 anos',
        '9': 'Maior de 80 anos'
    }
    return faixas.get(to_str(codigo))


def get_porte_desc(codigo: Optional[str]) -> Optional[str]:
    """Retorna descrição do porte"""
    portes = {
        '00': 'Não informado',
        '01': 'Micro empresa',
        '03': 'Empresa de pequeno porte',
        '05': 'Demais'
    }
    return portes.get(to_str(codigo))


def processar_dados_empresa(
    data: Dict[str, Any]
) -> CompanyDetailResponse:
    """
    Processa dados brutos da query e reorganiza na estrutura de resposta da API.
    Transforma dados do ClickHouse em estrutura JSON aninhada com todas as descrições.
    """
    # Processar sócios
    socios_raw = []
    if data.get('socios'):
        if isinstance(data['socios'], str):
            try:
                import json
                socios_raw = json.loads(data['socios'])
            except:
                socios_raw = []
        elif isinstance(data['socios'], list):
            socios_raw = data['socios']
    
    socios_organizados = []
    for socio in socios_raw:
        socio_org = SocioV1(
            identificacao=IdentificacaoSocio(
                identificador_socio=to_str(socio.get('identificador_socio')),
                nome_socio=to_str(socio.get('nome_socio')),
                cnpj_cpf_socio=to_str(socio.get('cnpj_cpf_socio'))
            ),
            faixa_etaria=FaixaEtaria(
                codigo=to_str(socio.get('faixa_etaria')),
                descricao=get_faixa_etaria_desc(socio.get('faixa_etaria'))
            ),
            data_entrada_sociedade=format_date(to_str(socio.get('data_entrada_sociedade'))),
            qualificacao_socio=QualificacaoSocio(
                codigo=to_str(socio.get('qualif_socio_cod')),
                descricao=to_str(socio.get('qualif_socio_desc'))
            ),
            pais=PaisSocio(
                codigo=to_str(socio.get('pais_socio_cod')),
                descricao=to_str(socio.get('pais_socio_desc'))
            ),
            representante_legal=RepresentanteLegal(
                representante_legal=to_str(socio.get('representante_legal')),
                nome_representante=to_str(socio.get('nome_representante')),
                qualificacao_representante=QualificacaoRepresentante(
                    codigo=to_str(socio.get('qualif_rep_legal_cod')),
                    descricao=to_str(socio.get('qualif_rep_legal_desc'))
                )
            )
        )
        socios_organizados.append(socio_org)
    
    # Processar CNAEs secundários (já vêm processados do endpoint)
    cnaes_secundarios_list = []
    if data.get('cnaes_secundarios'):
        for cnae_sec in data['cnaes_secundarios']:
            cnaes_secundarios_list.append(CnaeSecundario(
                codigo=to_str(cnae_sec.get('codigo')),
                descricao=to_str(cnae_sec.get('descricao'))
            ))
    
    # Montar estrutura do estabelecimento
    estabelecimento = EstabelecimentoV1(
        identificacao=IdentificacaoEstabelecimento(
            cnpj=to_str(data.get('cnpj')),
            matriz_filial=to_str(data.get('matriz_filial')),
            nome_fantasia=to_str(data.get('nome_fantasia'))
        ),
        situacao=SituacaoEstabelecimento(
            situacao_cadastral=to_str(data.get('situacao_cadastral')),
            situacao_motivo_desc=to_str(data.get('situacao_motivo_desc')),
            data_situacao=format_date(to_str(data.get('data_situacao'))),
            data_abertura=format_date(to_str(data.get('data_abertura'))),
            situacao_especial=to_str(data.get('situacao_especial')),
            data_situacao_especial=format_date(to_str(data.get('data_situacao_especial')))
        ),
        cnae=CnaeInfo(
            principal=CnaePrincipal(
                codigo=to_str(data.get('cnae_fiscal')),
                descricao=to_str(data.get('cnae_principal_desc'))
            ),
            secundarios=cnaes_secundarios_list
        ),
        endereco=EnderecoEstabelecimento(
            tipo_logradouro=to_str(data.get('tipo_logradouro')),
            logradouro=to_str(data.get('logradouro')),
            numero=to_str(data.get('numero')),
            complemento=to_str(data.get('complemento')),
            bairro=to_str(data.get('bairro')),
            cep=to_str(data.get('cep')),
            uf=to_str(data.get('uf')),
            municipio=to_str(data.get('municipio_codigo')),
            municipio_desc=to_str(data.get('municipio_desc')),
            cidade_exterior=to_str(data.get('cidade_exterior')),
            pais=to_str(data.get('pais_estabelecimento_cod')),
            pais_desc=to_str(data.get('pais_estabelecimento_desc'))
        ),
        contato=ContatoEstabelecimento(
            ddd_1=to_str(data.get('ddd_1')),
            telefone_1=to_str(data.get('telefone_1')),
            ddd_2=to_str(data.get('ddd_2')),
            telefone_2=to_str(data.get('telefone_2')),
            ddd_fax=to_str(data.get('ddd_fax')),
            fax=to_str(data.get('fax')),
            email=to_str(data.get('email'))
        )
    )
    
    # Montar estrutura da empresa
    simples_nacional = None
    if data.get('opcao_simples') or data.get('opcao_mei'):
        simples_nacional = SimplesNacional(
            simples=SimplesInfo(
                opcao_simples=to_str(data.get('opcao_simples')),
                data_opcao_simples=format_date(to_str(data.get('data_opcao_simples'))),
                data_exclusao_simples=format_date(to_str(data.get('data_exclusao_simples')))
            ) if data.get('opcao_simples') else None,
            mei=MeiInfo(
                opcao_mei=to_str(data.get('opcao_mei')),
                data_opcao_mei=format_date(to_str(data.get('data_opcao_mei'))),
                data_exclusao_mei=format_date(to_str(data.get('data_exclusao_mei')))
            ) if data.get('opcao_mei') else None
        )
    
    empresa = EmpresaV1(
        identificacao=IdentificacaoEmpresa(
            razao_social=to_str(data.get('razao_social'))
        ),
        natureza_juridica=NaturezaJuridica(
            codigo=to_str(data.get('natureza_juridica_cod')),
            descricao=to_str(data.get('natureza_juridica_desc'))
        ),
        qualificacao={
            'codigo': to_str(data.get('qualif_resp_empresa_cod')),
            'descricao': to_str(data.get('qualif_resp_empresa_desc'))
        },
        capital=Capital(
            capital_social=format_capital_social(data.get('capital_social'))
        ),
        porte=Porte(
            codigo=to_str(data.get('porte')),
            descricao=get_porte_desc(data.get('porte'))
        ),
        ente_federativo=to_str(data.get('ente_federativo')),
        simples=simples_nacional,
        socios=socios_organizados
    )
    
    return CompanyDetailResponse(
        estabelecimento=estabelecimento,
        empresa=empresa
    )

