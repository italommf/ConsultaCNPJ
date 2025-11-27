from rest_framework import serializers
from django.db.models import Subquery, OuterRef
from .models import Empresas, Estabelecimentos, Cnaes, Municipios, Naturezas, Motivos, Paises, Qualificacoes, Simples, Socios

class CnaesSerializer(serializers.ModelSerializer):
    class Meta:
        model = Cnaes
        fields = '__all__'

class MunicipiosSerializer(serializers.ModelSerializer):
    class Meta:
        model = Municipios
        fields = '__all__'

class NaturezasSerializer(serializers.ModelSerializer):
    class Meta:
        model = Naturezas
        fields = '__all__'

class MotivosSerializer(serializers.ModelSerializer):
    class Meta:
        model = Motivos
        fields = '__all__'

class PaisesSerializer(serializers.ModelSerializer):
    class Meta:
        model = Paises
        fields = '__all__'

class QualificacoesSerializer(serializers.ModelSerializer):
    class Meta:
        model = Qualificacoes
        fields = '__all__'

class SimplesSerializer(serializers.ModelSerializer):
    # Dados agrupados por contexto
    simples = serializers.SerializerMethodField()
    mei = serializers.SerializerMethodField()

    class Meta:
        model = Simples
        fields = ['simples', 'mei']

    def get_simples(self, obj):
        """Dados de opção pelo Simples"""
        return {
            'opcao_simples': obj.opcao_simples,
            'data_opcao_simples': obj.data_opcao_simples,
            'data_exclusao_simples': obj.data_exclusao_simples
        }

    def get_mei(self, obj):
        """Dados de opção pelo MEI"""
        return {
            'opcao_mei': obj.opcao_mei,
            'data_opcao_mei': obj.data_opcao_mei,
            'data_exclusao_mei': obj.data_exclusao_mei
        }

class SociosSerializer(serializers.ModelSerializer):
    # Descriptions will be accessed via getattr in SerializerMethodField
    
    # Dados agrupados por contexto
    identificacao = serializers.SerializerMethodField()
    qualificacao = serializers.SerializerMethodField()
    representante = serializers.SerializerMethodField()
    localizacao = serializers.SerializerMethodField()

    class Meta:
        model = Socios
        fields = ['identificacao', 'qualificacao', 'representante', 'localizacao', 'data_entrada_sociedade', 'faixa_etaria']

    def get_identificacao(self, obj):
        """Dados de identificação do sócio"""
        return {
            'identificador_socio': obj.identificador_socio,
            'nome_socio': obj.nome_socio,
            'cnpj_cpf_socio': obj.cnpj_cpf_socio
        }

    def get_qualificacao(self, obj):
        """Dados de qualificação do sócio"""
        return {
            'codigo': obj.qualificacao_socio,
            'descricao': getattr(obj, 'qualificacao_socio_desc', None)
        }

    def get_representante(self, obj):
        """Dados do representante legal"""
        return {
            'representante_legal': obj.representante_legal,
            'nome_representante': obj.nome_representante,
            'qualificacao_representante': {
                'codigo': obj.qualificacao_representante,
                'descricao': getattr(obj, 'qualificacao_representante_desc', None)
            }
        }

    def get_localizacao(self, obj):
        """Dados de localização do sócio"""
        return {
            'pais': obj.pais,
            'pais_desc': getattr(obj, 'pais_socio_desc', None)
        }

class EmpresasSerializer(serializers.ModelSerializer):
    # Descriptions will be accessed via getattr in SerializerMethodField
    simples = serializers.SerializerMethodField()
    socios = serializers.SerializerMethodField()
    
    # Dados agrupados por contexto
    identificacao = serializers.SerializerMethodField()
    natureza_juridica = serializers.SerializerMethodField()
    qualificacao = serializers.SerializerMethodField()
    capital = serializers.SerializerMethodField()

    class Meta:
        model = Empresas
        fields = ['identificacao', 'natureza_juridica', 'qualificacao', 'capital', 'porte', 'ente_federativo', 'simples', 'socios']
    
    def get_simples(self, obj):
        """Acessa simples através do cache de prefetch ou relacionamento"""
        try:
            # Tentar acessar através do cache de prefetch primeiro
            if hasattr(obj, '_prefetched_objects_cache') and 'simples' in obj._prefetched_objects_cache:
                simples_obj = obj._prefetched_objects_cache['simples']
                if simples_obj is None:
                    return None
                return SimplesSerializer(simples_obj).data
            # Se não estiver no cache, tentar acessar diretamente (pode causar query)
            if hasattr(obj, 'simples') and obj.simples is not None:
                return SimplesSerializer(obj.simples).data
        except Exception:
            # Se houver qualquer erro, retornar None
            return None
        return None
    
    def get_socios(self, obj):
        """Acessa socios através do cache de prefetch ou relacionamento"""
        try:
            # Tentar acessar através do cache de prefetch primeiro
            if hasattr(obj, '_prefetched_objects_cache') and 'socios' in obj._prefetched_objects_cache:
                socios_objs = obj._prefetched_objects_cache['socios']
                if socios_objs and len(socios_objs) > 0:
                    return SociosSerializer(socios_objs, many=True).data
            # Se não estiver no cache, tentar acessar diretamente (pode causar query)
            if hasattr(obj, 'socios'):
                try:
                    socios_queryset = obj.socios.all()
                    if socios_queryset.exists():
                        return SociosSerializer(socios_queryset, many=True).data
                except Exception:
                    pass
        except Exception as e:
            # Se houver qualquer erro, retornar lista vazia
            import traceback
            print(f"Erro em get_socios: {e}")
            traceback.print_exc()
            return []
        return []

    def get_identificacao(self, obj):
        """Dados de identificação da empresa"""
        return {
            'cnpj_basico': obj.cnpj_basico,
            'razao_social': obj.razao_social
        }

    def get_natureza_juridica(self, obj):
        """Dados de natureza jurídica"""
        return {
            'codigo': obj.natureza_juridica,
            'descricao': getattr(obj, 'natureza_juridica_desc', None)
        }

    def get_qualificacao(self, obj):
        """Dados de qualificação do responsável"""
        return {
            'codigo': obj.qualificacao_do_responsavel,
            'descricao': getattr(obj, 'qualificacao_responsavel_desc', None)
        }

    def get_capital(self, obj):
        """Dados de capital social"""
        return {
            'capital_social': obj.capital_social
        }

class EstabelecimentosSerializer(serializers.ModelSerializer):
    # Annotated fields will be accessed via getattr in SerializerMethodField
    razao_social = serializers.CharField(source='cnpj_basico.razao_social', read_only=True)
    
    # Dados agrupados por contexto
    endereco = serializers.SerializerMethodField()
    contato = serializers.SerializerMethodField()
    cnae = serializers.SerializerMethodField()
    situacao = serializers.SerializerMethodField()
    identificacao = serializers.SerializerMethodField()

    class Meta:
        model = Estabelecimentos
        fields = [
            'cnpj', 'cnpj_basico', 'identificacao', 'nome_fantasia', 'razao_social',
            'situacao', 'cnae', 'endereco', 'contato', 'data_inicio'
        ]

    def get_identificacao(self, obj):
        """Dados de identificação do estabelecimento"""
        return {
            'cnpj': obj.cnpj,
            'cnpj_basico': obj.cnpj_basico.cnpj_basico if obj.cnpj_basico else None,
            'cnpj_ordem': obj.cnpj_ordem,
            'cnpj_dv': obj.cnpj_dv,
            'matriz_filial': obj.matriz_filial,
            'nome_fantasia': obj.nome_fantasia,
            'razao_social': obj.razao_social if hasattr(obj, 'razao_social') else (obj.cnpj_basico.razao_social if obj.cnpj_basico else None)
        }

    def get_situacao(self, obj):
        """Dados de situação cadastral"""
        return {
            'situacao_cadastral': obj.situacao_cadastral,
            'data_situacao': obj.data_situacao,
            'motivo_situacao': obj.motivo_situacao,
            'motivo_situacao_desc': getattr(obj, 'situacao_motivo_desc', None),
            'situacao_especial': obj.situacao_especial,
            'data_situacao_especial': obj.data_situacao_especial
        }

    def get_cnae(self, obj):
        """Dados de CNAE"""
        return {
            'cnae_fiscal': obj.cnae_fiscal,
            'cnae_fiscal_desc': getattr(obj, 'cnae_principal_desc', None),
            'cnae_fiscal_secundaria': obj.cnae_fiscal_secundaria
        }

    def get_endereco(self, obj):
        """Dados de endereço"""
        return {
            'tipo_logradouro': obj.tipo_logradouro,
            'logradouro': obj.logradouro,
            'numero': obj.numero,
            'complemento': obj.complemento,
            'bairro': obj.bairro,
            'cep': obj.cep,
            'uf': obj.uf,
            'municipio': obj.municipio,
            'municipio_desc': getattr(obj, 'municipio_desc', None),
            'cidade_exterior': obj.cidade_exterior,
            'pais': obj.pais,
            'pais_desc': getattr(obj, 'pais_desc', None)
        }

    def get_contato(self, obj):
        """Dados de contato"""
        return {
            'ddd_1': obj.ddd_1,
            'telefone_1': obj.telefone_1,
            'ddd_2': obj.ddd_2,
            'telefone_2': obj.telefone_2,
            'ddd_fax': obj.ddd_fax,
            'fax': obj.fax,
            'email': obj.email
        }

class CompanyDetailSerializer(serializers.ModelSerializer):
    # Full detail view - retorna todos os dados da empresa organizados por contexto
    empresa = EmpresasSerializer(source='cnpj_basico', read_only=True)
    estabelecimento = serializers.SerializerMethodField()

    class Meta:
        model = Estabelecimentos
        fields = ['empresa', 'estabelecimento']

    def get_estabelecimento(self, obj):
        """Dados do estabelecimento atual organizados por contexto"""
        return {
            'identificacao': {
                'cnpj': obj.cnpj,
                'cnpj_basico': obj.cnpj_basico.cnpj_basico if obj.cnpj_basico else None,
                'cnpj_ordem': obj.cnpj_ordem,
                'cnpj_dv': obj.cnpj_dv,
                'matriz_filial': obj.matriz_filial,
                'nome_fantasia': obj.nome_fantasia
            },
            'situacao': {
                'situacao_cadastral': obj.situacao_cadastral,
                'data_situacao': obj.data_situacao,
                'motivo_situacao': obj.motivo_situacao,
                'motivo_situacao_desc': getattr(obj, 'situacao_motivo_desc', None),
                'situacao_especial': obj.situacao_especial,
                'data_situacao_especial': obj.data_situacao_especial
            },
            'cnae': {
                'cnae_fiscal': obj.cnae_fiscal,
                'cnae_fiscal_desc': getattr(obj, 'cnae_principal_desc', None),
                'cnae_fiscal_secundaria': obj.cnae_fiscal_secundaria
            },
            'endereco': {
                'tipo_logradouro': obj.tipo_logradouro,
                'logradouro': obj.logradouro,
                'numero': obj.numero,
                'complemento': obj.complemento,
                'bairro': obj.bairro,
                'cep': obj.cep,
                'uf': obj.uf,
                'municipio': obj.municipio,
                'municipio_desc': getattr(obj, 'municipio_desc', None),
                'cidade_exterior': obj.cidade_exterior,
                'pais': obj.pais,
                'pais_desc': getattr(obj, 'pais_desc', None)
            },
            'contato': {
                'ddd_1': obj.ddd_1,
                'telefone_1': obj.telefone_1,
                'ddd_2': obj.ddd_2,
                'telefone_2': obj.telefone_2,
                'ddd_fax': obj.ddd_fax,
                'fax': obj.fax,
                'email': obj.email
            },
            'data_inicio': obj.data_inicio
        }
