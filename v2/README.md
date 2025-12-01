# CNPJ Search API v2 - FastAPI + ClickHouse

API otimizada para busca de dados CNPJ usando **FastAPI** e **ClickHouse**.

## Características

- ✅ **Ultra otimizado**: Tipos de dados otimizados (FixedString, LowCardinality, UInt64)
- ✅ **Compressão ZSTD**: Redução de 50-70% no tamanho do banco
- ✅ **Particionamento**: Por mês de data_inicio para queries rápidas
- ✅ **Ordenação otimizada**: ORDER BY (uf, municipio, cnae_fiscal, cnpj)
- ✅ **Autenticação JWT**: Proteção por token
- ✅ **Endpoints indexados**: Todas as buscas usam índices otimizados
- ✅ **Performance**: Queries em milissegundos

## Estrutura do Projeto

```
v2/
├─ backend/              # FastAPI
│  ├─ app/
│  │  ├─ main.py        # App principal
│  │  ├─ routes/        # Endpoints
│  │  └─ ...
│  └─ Dockerfile
├─ clickhouse/           # Schema SQL
│  └─ schema.sql
├─ importacao/           # Scripts de importação
│  ├─ main.py
│  └─ import_csv.py
├─ docker-compose.yml
└─ README.md
```

## Instalação e Uso

### 1. Usando Docker (Recomendado)

```bash
# Subir serviços
docker-compose up -d

# Criar schema no ClickHouse
docker exec -it clickhouse-cnpj clickhouse-client --query="$(cat clickhouse/schema.sql)"

# Importar dados
cd importacao
python main.py
```

### 2. Instalação Manual

```bash
# Instalar ClickHouse
# Ubuntu/Debian:
sudo apt-get install clickhouse-server clickhouse-client

# Iniciar ClickHouse
sudo service clickhouse-server start

# Criar schema
clickhouse-client < clickhouse/schema.sql

# Instalar dependências Python
cd backend
pip install -r requirements.txt

# Configurar variáveis de ambiente
cp .env.example .env
# Editar .env com suas configurações

# Importar dados
cd ../importacao
python main.py

# Rodar API
cd ../backend
uvicorn app.main:app --reload
```

## Autenticação

### Obter Token

```bash
curl -X POST "http://localhost:8000/auth/token" \
  -u admin:secret \
  -H "Content-Type: application/json"
```

Resposta:
```json
{
  "access_token": "eyJ...",
  "token_type": "bearer"
}
```

### Usar Token

```bash
curl -X GET "http://localhost:8000/api/companies/cnpj/12345678000199" \
  -H "Authorization: Bearer eyJ..."
```

## Endpoints

### Autenticação
- `POST /auth/token` - Obter token JWT
- `GET /auth/me` - Informações do usuário autenticado

### Empresas
- `GET /companies/cnpj/{cnpj}` - Buscar por CNPJ completo
- `GET /companies/search` - Busca com filtros (todos indexados)

### CNAEs
- `GET /cnaes/` - Listar CNAEs
- `GET /cnaes/{codigo}` - Buscar CNAE por código

### Municípios
- `GET /municipios/` - Listar municípios
- `GET /municipios/{codigo}` - Buscar município por código

## Filtros de Busca (todos indexados)

- `q` - Busca textual (nome_fantasia ou CNPJ)
- `cnpj` - CNPJ completo (14 dígitos)
- `uf` - UF (2 letras)
- `municipio` - Código do município (4 dígitos)
- `cnae_fiscal` - CNAE fiscal (7 dígitos)
- `situacao_cadastral` - Situação cadastral
- `matriz_filial` - 1=Matriz, 2=Filial
- `page` - Número da página (padrão: 1)
- `page_size` - Tamanho da página (padrão: 100, máx: 1000)

## Exemplo de Uso

```bash
# Buscar por CNPJ
curl -X GET "http://localhost:8000/companies/cnpj/12345678000199" \
  -H "Authorization: Bearer TOKEN"

# Buscar por UF e município
curl -X GET "http://localhost:8000/companies/search?uf=SP&municipio=355030&page=1&page_size=100" \
  -H "Authorization: Bearer TOKEN"

# Buscar por CNAE
curl -X GET "http://localhost:8000/companies/search?cnae_fiscal=6201501" \
  -H "Authorization: Bearer TOKEN"
```

## Otimizações Implementadas

1. **Tipos Otimizados**:
   - `FixedString(N)` para códigos fixos
   - `LowCardinality` para campos com poucos valores únicos
   - `UInt64` para capital_social (em centavos)

2. **Ordenação**:
   - `ORDER BY (uf, municipio, cnae_fiscal, cnpj)` em estabelecimentos

3. **Particionamento**:
   - `PARTITION BY toYYYYMM(data_inicio)` para compressão e queries rápidas

4. **Compressão**:
   - `ZSTD(3)` em todas as tabelas

5. **Index Granularity**:
   - `8192` para balance entre performance e espaço

## Performance Esperada

- **Tamanho do banco**: 25-40 GB (vs 80 GB no PostgreSQL)
- **Queries por CNPJ**: 1-2 ms
- **Queries com filtros indexados**: 5-50 ms
- **Importação**: 3-8 milhões de registros/segundo

## Documentação da API

Acesse `http://localhost:8000/docs` para documentação interativa (Swagger).

## Troubleshooting

### ClickHouse não conecta
- Verificar se o serviço está rodando: `docker ps`
- Verificar logs: `docker logs clickhouse-cnpj`

### Erro de importação
- Verificar se os arquivos CSV estão em `v1/data/`
- Verificar encoding dos arquivos (deve ser UTF-8)
- Verificar logs de erro

### Erro de autenticação
- Verificar se o token está sendo enviado no header
- Verificar se o token não expirou (24h por padrão)




