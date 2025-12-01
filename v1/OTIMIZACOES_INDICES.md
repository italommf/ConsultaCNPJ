# Otimizações de Índices e Performance - Banco CNPJ

## Resumo das Otimizações Implementadas

### 1. Índices Otimizados (Apenas os Essenciais)

#### Estabelecimentos
- ✅ `idx_estab_cnpj_unique` (UNIQUE) - Busca principal por CNPJ
- ✅ `idx_estab_cnpj_basico` - JOINs com empresas, simples, socios
- ✅ `idx_estab_cnae` - Filtro por CNAE principal
- ✅ `idx_estab_uf` - Filtro por UF
- ✅ `idx_estab_municipio` - Filtro por município
- ✅ `idx_estab_situacao` - Filtro por situação cadastral
- ✅ `idx_estab_matriz_filial` - Filtro matriz/filial
- ✅ `idx_estab_uf_municipio` (composto) - Filtro UF + Município (muito usado juntos)
- ✅ `idx_estab_fantasia_trgm` (GIN) - Busca fuzzy em nome_fantasia

#### Empresas
- ✅ `idx_empresas_natureza` - Filtro por natureza jurídica
- ✅ `idx_empresas_porte` - Filtro por porte
- ✅ `idx_empresas_capital` - Filtro por capital social (range queries)
- ✅ `idx_empresas_razao_trgm` (GIN) - Busca fuzzy em razao_social

#### Sócios
- ✅ `idx_socios_cnpj_basico` - JOIN com estabelecimentos

#### Simples
- ✅ `idx_simples_opcao` - Filtro opção simples
- ✅ `idx_simples_mei` - Filtro opção MEI

### 2. Índices Removidos (Desnecessários)

❌ **Removidos para economizar espaço:**
- `idx_estab_uf_cnae` (composto) - Redundante, temos índices separados
- `idx_estab_situacao_uf` (composto) - Redundante, temos índices separados
- `idx_empresas_natureza_porte` (composto) - Redundante, temos índices separados
- `idx_socios_nome_trgm` - Não usado na API
- `idx_socios_cnpj_cpf` - Não usado na API

### 3. Otimizações de Espaço

#### Tipos de Dados Otimizados
- ✅ `char(n)` para campos fixos (CNPJ, CEP, UF, etc.)
- ✅ `varchar(n)` para campos de texto com limite
- ✅ `date` para campos de data (em vez de text)
- ✅ `numeric` para valores monetários (em vez de text)
- ✅ `text` apenas para campos que podem ser muito grandes

#### Compressão TOAST
- ✅ Configurado `STORAGE EXTENDED` para colunas grandes
- ✅ Compressão automática do PostgreSQL para campos grandes

#### Fillfactor
- ✅ `fillfactor = 100` para tabelas read-only (sem espaço extra)

#### VACUUM FULL
- ✅ Executado em todas as tabelas após importação
- ✅ Reduz significativamente o tamanho físico do banco

### 4. Otimizações de Performance

#### Autovacuum Configurado
- ✅ Ajustado para tabelas grandes (estabelecimentos, socios)
- ✅ `autovacuum_vacuum_scale_factor = 0.05` (mais agressivo)
- ✅ `autovacuum_analyze_scale_factor = 0.02` (atualiza estatísticas mais frequentemente)

#### ANALYZE
- ✅ Executado após criação de índices
- ✅ Atualiza estatísticas do planner para queries otimizadas

### 5. Extensões PostgreSQL

- ✅ `pg_trgm` - Para busca fuzzy em texto (nome_fantasia, razao_social)

## Resultados Esperados

### Redução de Tamanho
- **Antes**: ~80GB
- **Depois**: ~30-40GB (estimativa)
- **Economia**: ~50-60% de redução

### Melhoria de Performance
- ✅ Consultas por CNPJ: **Instantâneas** (índice UNIQUE)
- ✅ Filtros por UF/Município: **Muito rápidas** (índices + composto)
- ✅ Filtros por CNAE: **Muito rápidas** (índice)
- ✅ Busca fuzzy: **Rápida** (GIN com pg_trgm)
- ✅ JOINs: **Otimizados** (índices em chaves estrangeiras)

## Manutenção

### Após Importação de Novos Dados
Execute periodicamente:
```sql
ANALYZE;
VACUUM;
```

### Monitoramento
Verifique tamanho dos índices:
```sql
SELECT 
    tablename,
    pg_size_pretty(pg_total_relation_size('public.'||tablename)) as total_size,
    pg_size_pretty(pg_relation_size('public.'||tablename)) as table_size,
    pg_size_pretty(pg_total_relation_size('public.'||tablename) - pg_relation_size('public.'||tablename)) as index_size
FROM pg_tables
WHERE schemaname = 'public'
ORDER BY pg_total_relation_size('public.'||tablename) DESC;
```

## Notas Importantes

1. **Índices compostos**: Criados apenas quando realmente necessários (UF+Município)
2. **Índices GIN**: Apenas para busca fuzzy (nome_fantasia, razao_social)
3. **Tabelas de domínio**: Não precisam de índices adicionais (já têm PRIMARY KEY)
4. **Fillfactor 100**: Ideal para dados read-only, economiza espaço
5. **Autovacuum**: Configurado para manter tabelas grandes sempre otimizadas


