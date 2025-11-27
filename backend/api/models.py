from django.db import models

class Cnaes(models.Model):
    codigo = models.CharField(primary_key=True, max_length=7)
    descricao = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'cnaes'

class Naturezas(models.Model):
    codigo = models.CharField(primary_key=True, max_length=4)
    descricao = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'naturezas'

class Qualificacoes(models.Model):
    codigo = models.CharField(primary_key=True, max_length=2)
    descricao = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'qualificacoes'

class Motivos(models.Model):
    codigo = models.CharField(primary_key=True, max_length=2)
    descricao = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'motivos'

class Municipios(models.Model):
    codigo = models.CharField(primary_key=True, max_length=4)
    descricao = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'municipios'

class Paises(models.Model):
    codigo = models.CharField(primary_key=True, max_length=3)
    descricao = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'paises'

class Empresas(models.Model):
    cnpj_basico = models.CharField(primary_key=True, max_length=8)
    razao_social = models.TextField(blank=True, null=True)
    natureza_juridica = models.CharField(max_length=4, blank=True, null=True)
    qualificacao_do_responsavel = models.CharField(max_length=2, blank=True, null=True)
    capital_social = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)
    porte = models.CharField(max_length=2, blank=True, null=True)
    ente_federativo = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'empresas'

class Simples(models.Model):
    cnpj_basico = models.OneToOneField(Empresas, models.DO_NOTHING, db_column='cnpj_basico', primary_key=True)
    opcao_simples = models.CharField(max_length=1, blank=True, null=True)
    data_opcao_simples = models.DateField(blank=True, null=True)
    data_exclusao_simples = models.DateField(blank=True, null=True)
    opcao_mei = models.CharField(max_length=1, blank=True, null=True)
    data_opcao_mei = models.DateField(blank=True, null=True)
    data_exclusao_mei = models.DateField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'simples'

class Estabelecimentos(models.Model):
    cnpj = models.CharField(primary_key=True, max_length=14) 
    cnpj_basico = models.ForeignKey(Empresas, models.DO_NOTHING, db_column='cnpj_basico', blank=True, null=True, related_name='estabelecimentos')
    cnpj_ordem = models.CharField(max_length=4, blank=True, null=True)
    cnpj_dv = models.CharField(max_length=2, blank=True, null=True)
    matriz_filial = models.CharField(max_length=1, blank=True, null=True)
    nome_fantasia = models.TextField(blank=True, null=True)
    situacao_cadastral = models.CharField(max_length=2, blank=True, null=True)
    data_situacao = models.DateField(blank=True, null=True)
    motivo_situacao = models.CharField(max_length=2, blank=True, null=True)
    cidade_exterior = models.TextField(blank=True, null=True)
    pais = models.CharField(max_length=3, blank=True, null=True)
    data_inicio = models.DateField(blank=True, null=True)
    cnae_fiscal = models.CharField(max_length=7, blank=True, null=True)
    cnae_fiscal_secundaria = models.TextField(blank=True, null=True)
    tipo_logradouro = models.TextField(blank=True, null=True)
    logradouro = models.TextField(blank=True, null=True)
    numero = models.TextField(blank=True, null=True)
    complemento = models.TextField(blank=True, null=True)
    bairro = models.TextField(blank=True, null=True)
    cep = models.CharField(max_length=8, blank=True, null=True)
    uf = models.CharField(max_length=2, blank=True, null=True)
    municipio = models.CharField(max_length=4, blank=True, null=True)
    ddd_1 = models.TextField(blank=True, null=True)
    telefone_1 = models.TextField(blank=True, null=True)
    ddd_2 = models.TextField(blank=True, null=True)
    telefone_2 = models.TextField(blank=True, null=True)
    ddd_fax = models.TextField(blank=True, null=True)
    fax = models.TextField(blank=True, null=True)
    email = models.TextField(blank=True, null=True)
    situacao_especial = models.TextField(blank=True, null=True)
    data_situacao_especial = models.DateField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'estabelecimentos'

class Socios(models.Model):
    cnpj_basico = models.ForeignKey(Empresas, models.DO_NOTHING, db_column='cnpj_basico', blank=True, null=True, related_name='socios')
    identificador_socio = models.CharField(max_length=1, blank=True, null=True)
    nome_socio = models.TextField(blank=True, null=True)
    cnpj_cpf_socio = models.TextField(blank=True, null=False, primary_key=True) # FAKE PK for Django, null=False required
    qualificacao_socio = models.CharField(max_length=2, blank=True, null=True)
    data_entrada_sociedade = models.DateField(blank=True, null=True)
    pais = models.CharField(max_length=3, blank=True, null=True)
    representante_legal = models.TextField(blank=True, null=True)
    nome_representante = models.TextField(blank=True, null=True)
    qualificacao_representante = models.CharField(max_length=2, blank=True, null=True)
    faixa_etaria = models.CharField(max_length=1, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'socios'
