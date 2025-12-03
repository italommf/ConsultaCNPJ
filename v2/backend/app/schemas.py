"""Schemas Pydantic para validação e serialização"""
from pydantic import BaseModel, Field, validator
from typing import Optional, List
from datetime import date


# =================================================================================
# Schemas de Autenticação
# =================================================================================
class Token(BaseModel):
    access_token: str
    token_type: str = "bearer"


class TokenData(BaseModel):
    username: Optional[str] = None


# =================================================================================
# Schemas de Empresas
# =================================================================================
class EmpresaBase(BaseModel):
    cnpj_basico: str
    razao_social: Optional[str] = None
    natureza_juridica: Optional[str] = None
    qualificacao_do_responsavel: Optional[str] = None
    capital_social: Optional[float] = None  # Convertido de UInt64 (centavos) para reais
    porte: Optional[str] = None
    ente_federativo: Optional[str] = None


class Empresa(EmpresaBase):
    class Config:
        from_attributes = True


# =================================================================================
# Schemas de Estabelecimentos
# =================================================================================
class EstabelecimentoBase(BaseModel):
    cnpj: str
    cnpj_basico: str
    cnpj_ordem: Optional[str] = None
    cnpj_dv: Optional[str] = None
    matriz_filial: Optional[str] = None
    nome_fantasia: Optional[str] = None
    situacao_cadastral: Optional[str] = None
    data_situacao: Optional[str] = None  # Formatado como DD/MM/YYYY
    motivo_situacao: Optional[str] = None
    cidade_exterior: Optional[str] = None
    pais: Optional[str] = None
    data_inicio: Optional[str] = None  # Formatado como DD/MM/YYYY
    cnae_fiscal: Optional[str] = None
    cnae_fiscal_secundaria: Optional[str] = None
    tipo_logradouro: Optional[str] = None
    logradouro: Optional[str] = None
    numero: Optional[str] = None
    complemento: Optional[str] = None
    bairro: Optional[str] = None
    cep: Optional[str] = None
    uf: Optional[str] = None
    municipio: Optional[str] = None
    ddd_1: Optional[str] = None
    telefone_1: Optional[str] = None
    ddd_2: Optional[str] = None
    telefone_2: Optional[str] = None
    ddd_fax: Optional[str] = None
    fax: Optional[str] = None
    email: Optional[str] = None
    situacao_especial: Optional[str] = None
    data_situacao_especial: Optional[str] = None  # Formatado como DD/MM/YYYY


class Estabelecimento(EstabelecimentoBase):
    class Config:
        from_attributes = True


# =================================================================================
# Schemas de Sócios
# =================================================================================
class SocioBase(BaseModel):
    identificador_socio: Optional[str] = None
    nome_socio: Optional[str] = None
    cnpj_cpf_socio: Optional[str] = None
    qualificacao_socio: Optional[str] = None
    data_entrada_sociedade: Optional[str] = None  # Formatado como DD/MM/YYYY
    pais: Optional[str] = None
    representante_legal: Optional[str] = None
    nome_representante: Optional[str] = None
    qualificacao_representante: Optional[str] = None
    faixa_etaria: Optional[str] = None


class Socio(SocioBase):
    class Config:
        from_attributes = True


# =================================================================================
# Schemas de Simples
# =================================================================================
class SimplesBase(BaseModel):
    opcao_simples: Optional[str] = None
    data_opcao_simples: Optional[str] = None  # Formatado como DD/MM/YYYY
    data_exclusao_simples: Optional[str] = None  # Formatado como DD/MM/YYYY
    opcao_mei: Optional[str] = None
    data_opcao_mei: Optional[str] = None  # Formatado como DD/MM/YYYY
    data_exclusao_mei: Optional[str] = None  # Formatado como DD/MM/YYYY


class Simples(SimplesBase):
    class Config:
        from_attributes = True


# =================================================================================
# Schemas para estrutura compatível com v1
# =================================================================================
class IdentificacaoEstabelecimento(BaseModel):
    cnpj: Optional[str] = None
    matriz_filial: Optional[str] = None
    nome_fantasia: Optional[str] = None


class SituacaoEstabelecimento(BaseModel):
    situacao_cadastral: Optional[str] = None
    situacao_motivo_desc: Optional[str] = None
    data_situacao: Optional[str] = None
    data_abertura: Optional[str] = None
    situacao_especial: Optional[str] = None
    data_situacao_especial: Optional[str] = None


class CnaePrincipal(BaseModel):
    codigo: Optional[str] = None
    descricao: Optional[str] = None


class CnaeSecundario(BaseModel):
    codigo: Optional[str] = None
    descricao: Optional[str] = None


class CnaeInfo(BaseModel):
    principal: Optional[CnaePrincipal] = None
    secundarios: List[CnaeSecundario] = []


class EnderecoEstabelecimento(BaseModel):
    tipo_logradouro: Optional[str] = None
    logradouro: Optional[str] = None
    numero: Optional[str] = None
    complemento: Optional[str] = None
    bairro: Optional[str] = None
    cep: Optional[str] = None
    uf: Optional[str] = None
    municipio: Optional[str] = None
    municipio_desc: Optional[str] = None
    cidade_exterior: Optional[str] = None
    pais: Optional[str] = None
    pais_desc: Optional[str] = None


class ContatoEstabelecimento(BaseModel):
    ddd_1: Optional[str] = None
    telefone_1: Optional[str] = None
    ddd_2: Optional[str] = None
    telefone_2: Optional[str] = None
    ddd_fax: Optional[str] = None
    fax: Optional[str] = None
    email: Optional[str] = None


class EstabelecimentoV1(BaseModel):
    identificacao: IdentificacaoEstabelecimento
    situacao: SituacaoEstabelecimento
    cnae: CnaeInfo
    endereco: EnderecoEstabelecimento
    contato: ContatoEstabelecimento


class IdentificacaoEmpresa(BaseModel):
    razao_social: Optional[str] = None


class NaturezaJuridica(BaseModel):
    codigo: Optional[str] = None
    descricao: Optional[str] = None


class Qualificacao(BaseModel):
    codigo: Optional[str] = None
    descricao: Optional[str] = None


class Capital(BaseModel):
    capital_social: Optional[float] = None


class Porte(BaseModel):
    codigo: Optional[str] = None
    descricao: Optional[str] = None


class SimplesInfo(BaseModel):
    opcao_simples: Optional[str] = None
    data_opcao_simples: Optional[str] = None
    data_exclusao_simples: Optional[str] = None


class MeiInfo(BaseModel):
    opcao_mei: Optional[str] = None
    data_opcao_mei: Optional[str] = None
    data_exclusao_mei: Optional[str] = None


class SimplesNacional(BaseModel):
    simples: Optional[SimplesInfo] = None
    mei: Optional[MeiInfo] = None


class IdentificacaoSocio(BaseModel):
    identificador_socio: Optional[str] = None
    nome_socio: Optional[str] = None
    cnpj_cpf_socio: Optional[str] = None


class FaixaEtaria(BaseModel):
    codigo: Optional[str] = None
    descricao: Optional[str] = None


class QualificacaoSocio(BaseModel):
    codigo: Optional[str] = None
    descricao: Optional[str] = None


class PaisSocio(BaseModel):
    codigo: Optional[str] = None
    descricao: Optional[str] = None


class QualificacaoRepresentante(BaseModel):
    codigo: Optional[str] = None
    descricao: Optional[str] = None


class RepresentanteLegal(BaseModel):
    representante_legal: Optional[str] = None
    nome_representante: Optional[str] = None
    qualificacao_representante: Optional[QualificacaoRepresentante] = None


class SocioV1(BaseModel):
    identificacao: IdentificacaoSocio
    faixa_etaria: FaixaEtaria
    data_entrada_sociedade: Optional[str] = None
    qualificacao_socio: QualificacaoSocio
    pais: PaisSocio
    representante_legal: RepresentanteLegal


class EmpresaV1(BaseModel):
    identificacao: IdentificacaoEmpresa
    natureza_juridica: NaturezaJuridica
    qualificacao: Qualificacao
    capital: Capital
    porte: Porte
    ente_federativo: Optional[str] = None
    simples: Optional[SimplesNacional] = None
    socios: List[SocioV1] = []


class CompanyDetailResponse(BaseModel):
    estabelecimento: EstabelecimentoV1
    empresa: EmpresaV1


# =================================================================================
# Schemas de Busca
# =================================================================================
class SearchRequest(BaseModel):
    q: Optional[str] = Field(None, description="Busca textual em nome_fantasia ou razao_social")
    cnpj: Optional[str] = Field(None, description="CNPJ completo (14 dígitos)")
    cnpj_basico: Optional[str] = Field(None, description="CNPJ básico (8 dígitos)")
    uf: Optional[str] = Field(None, description="UF (2 letras)")
    municipio: Optional[str] = Field(None, description="Código do município (4 dígitos)")
    cnae_fiscal: Optional[str] = Field(None, description="CNAE fiscal principal (7 dígitos)")
    cnae_secundario: Optional[str] = Field(None, description="CNAE secundário (7 dígitos)")
    situacao_cadastral: Optional[str] = Field(None, description="Situação cadastral")
    matriz_filial: Optional[str] = Field(None, description="1=Matriz, 2=Filial")
    natureza_juridica: Optional[str] = Field(None, description="Código natureza jurídica")
    porte: Optional[str] = Field(None, description="Porte da empresa")
    capital_social_min: Optional[float] = Field(None, description="Capital social mínimo")
    capital_social_max: Optional[float] = Field(None, description="Capital social máximo")
    opcao_simples: Optional[str] = Field(None, description="S ou N")
    opcao_mei: Optional[str] = Field(None, description="S ou N")
    page: int = Field(1, ge=1, description="Número da página")
    page_size: int = Field(100, ge=1, le=1000, description="Tamanho da página (máx 1000)")


class SearchResponse(BaseModel):
    total: int
    page: int
    page_size: int
    total_pages: int
    results: List[Estabelecimento]


# =================================================================================
# Schemas de Tabelas de Domínio
# =================================================================================
class CnaeBase(BaseModel):
    codigo: str
    descricao: Optional[str] = None


class Cnae(CnaeBase):
    class Config:
        from_attributes = True


class MunicipioBase(BaseModel):
    codigo: str
    descricao: Optional[str] = None


class Municipio(MunicipioBase):
    class Config:
        from_attributes = True


class MotivoBase(BaseModel):
    codigo: str
    descricao: Optional[str] = None


class Motivo(MotivoBase):
    class Config:
        from_attributes = True


class NaturezaBase(BaseModel):
    codigo: str
    descricao: Optional[str] = None


class Natureza(NaturezaBase):
    class Config:
        from_attributes = True


class PaisBase(BaseModel):
    codigo: str
    descricao: Optional[str] = None


class Pais(PaisBase):
    class Config:
        from_attributes = True

