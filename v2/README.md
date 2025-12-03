# CNPJ Search API v2 - FastAPI + ClickHouse

API otimizada para busca de dados CNPJ usando **FastAPI** e **ClickHouse**.

> **Importante:** a versão recomendada do projeto é **v2** (esta pasta).  
> A v1 (PostgreSQL + Django) permanece apenas para referência/legado.

## Características

- ✅ **Ultra otimizado**: Tipos de dados otimizados (FixedString, LowCardinality, UInt64)
- ✅ **Compressão ZSTD**: Redução de 50-70% no tamanho do banco
- ✅ **Particionamento**: Por mês de `data_inicio` para queries rápidas
- ✅ **Ordenação otimizada**: `ORDER BY (uf, municipio, cnae_fiscal, cnpj)`
- ✅ **Autenticação JWT**: Proteção por token (`/auth/token`)
- ✅ **Consultas em tempo real**: Todas as buscas leem direto do ClickHouse
- ✅ **Endpoints indexados**: Todas as buscas usam índices otimizados
- ✅ **Performance**: Queries em milissegundos

## Estrutura do Projeto

```text
v2/
├─ backend/              # FastAPI (API)
│  ├─ app/
│  │  ├─ main.py        # App principal FastAPI
│  │  ├─ routes/        # Endpoints (companies, cnaes, municipios, auth)
│  │  ├─ schemas.py     # Schemas Pydantic
│  │  ├─ clickhouse_client.py  # Conexão com ClickHouse
│  │  └─ ...
│  └─ Dockerfile
├─ clickhouse/           # Configs e schema SQL
│  ├─ config.xml
│  ├─ users.xml
│  └─ schema.sql
├─ importacao/           # Pipeline de importação para ClickHouse
│  ├─ main.py            # Orquestra a importação completa
│  ├─ process.py         # Fluxo de etapas (download, unzip, import)
│  ├─ functions/import_csv.py   # Importadores com Polars
│  └─ utilities/         # Utilitários (clickhouse, downloader, csv_stats, etc.)
├─ downloads/            # ZIPs baixados da Receita (entrada bruta)
├─ data/                 # CSVs descompactados (entrada para importação)
├─ docker-compose.yml
└─ README.md
```

## Instalação e Uso

### 1. Usando Docker (Recomendado)

```bash
# Na pasta v2/
docker-compose up -d

# Criar schema no ClickHouse
docker exec -it clickhouse-cnpj \
  clickhouse-client --query="$(cat /clickhouse/schema.sql)"

# Importar dados (se você optar por rodar a importação dentro de um container, adapte o caminho)
cd importacao
python main.py
```

### 2. Instalação Manual (sem Docker)

```bash
# Instalar ClickHouse (Ubuntu/Debian, via repositório oficial - ver docs oficiais)
sudo apt-get install clickhouse-server clickhouse-client

# Iniciar ClickHouse
sudo service clickhouse-server start

# Criar banco e schema
clickhouse-client < clickhouse/schema.sql

# Criar e ativar venv (opcional, mas recomendado)
cd backend
python -m venv .venv
.venv\Scripts\activate  # Windows
# ou
source .venv/bin/activate  # Linux

# Instalar dependências da API
pip install -r requirements.txt

# Configurar variáveis de ambiente
cp .env.example .env
# Edite .env com host/porta/usuário/senha do ClickHouse

# Importar dados (no host Windows, usando o importador otimizado)
cd ../importacao
python main.py

# Subir a API
cd ../backend
uvicorn app.main:app --reload
```

## Autenticação

### Obter Token

```bash
curl -X POST "http://localhost:8000/auth/token" \
  -u admin:secret
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
curl -X GET "http://localhost:8000/companies/cnpj/12345678000199" \
  -H "Authorization: Bearer eyJ..."
```

## Endpoints

### Autenticação
- `POST /auth/token` – Obter token JWT (HTTP Basic: `admin` / `secret`)
- `GET /auth/me` – Informações do usuário autenticado

### Empresas / Estabelecimentos
- `GET /companies/cnpj/{cnpj}` – Empresa completa por CNPJ  
  (estabelecimento + empresa + simples + sócios, em tempo real)
- `GET /companies/search` – Busca geral de estabelecimentos com múltiplos filtros
- `GET /companies/cnae/{cnae}` – Busca estabelecimentos por CNAE (principal e/ou secundário)

### CNAEs
- `GET /cnaes/` – Listar CNAEs (com busca textual via `q`)
- `GET /cnaes/{codigo}` – Buscar CNAE por código

### Municípios
- `GET /municipios/` – Listar municípios (com busca textual via `q`)
- `GET /municipios/{codigo}` – Buscar município por código

## Filtros de Busca (todos indexados)

### `GET /companies/search`

- `q` – Busca textual (`nome_fantasia` ou `cnpj`)
- `cnpj` – CNPJ completo (14 dígitos)
- `uf` – UF (2 letras)
- `municipio` – Código do município (4 dígitos)
- `cnae_fiscal` – CNAE fiscal principal (7 dígitos)
- `situacao_cadastral` – Situação cadastral
- `matriz_filial` – `1` = Matriz, `2` = Filial
- `page` – Número da página (padrão: 1)
- `page_size` – Tamanho da página (padrão: 100, máx: 1000)

### `GET /companies/cnae/{cnae}`

- `cnae` (path) – CNAE principal (7 dígitos)
- `cnae_sec` (query, opcional) – se `true`, inclui CNAEs secundários (`cnae_fiscal_secundaria`)
- `page`, `page_size` – paginação

## Exemplos de Uso

```bash
# Buscar empresa completa por CNPJ
curl -H "Authorization: Bearer TOKEN" \
  "http://localhost:8000/companies/cnpj/12345678000199"

# Buscar estabelecimentos por UF e município
curl -H "Authorization: Bearer TOKEN" \
  "http://localhost:8000/companies/search?uf=SP&municipio=3550&page=1&page_size=100"

# Buscar estabelecimentos por CNAE (incluindo secundários)
curl -H "Authorization: Bearer TOKEN" \
  "http://localhost:8000/companies/cnae/6201501?cnae_sec=true"
```

## Otimizações Implementadas

1. **Tipos Otimizados**:
   - `FixedString(N)` para códigos fixos (CNPJ, CEP, CNAE, UF etc.)
   - `LowCardinality` para campos com poucos valores únicos (UF, matriz_filial, porte, etc.)
   - `UInt64` para `capital_social` (em centavos)

2. **Ordenação**:
   - `ORDER BY (uf, municipio, cnae_fiscal, cnpj)` em `estabelecimentos`

3. **Particionamento**:
   - `PARTITION BY toYYYYMM(data_inicio)` para compressão e queries por data mais rápidas

4. **Compressão**:
   - `ZSTD(3)` em todas as tabelas

5. **Index Granularity**:
   - `8192` para balancear performance e espaço

## Performance Esperada

- **Tamanho do banco**: ~25–40 GB (vs ~80 GB no PostgreSQL da v1)
- **Queries por CNPJ**: 1–2 ms
- **Queries com filtros indexados**: 5–50 ms
- **Importação**: milhões de registros/segundo com Polars em batches

## Documentação da API

- `http://localhost:8000/docs` – documentação interativa (Swagger)
- `http://localhost:8000/redoc` – documentação em formato Redoc
- Arquivo detalhado desta versão: [`API_DOCUMENTATION.md`](./API_DOCUMENTATION.md)

## Troubleshooting

### ClickHouse não conecta
- Verificar se o serviço está rodando: `sudo service clickhouse-server status` ou `docker ps`
- Verificar logs: `sudo journalctl -u clickhouse-server` ou `docker logs clickhouse-cnpj`

### Erro de importação
- Verificar se os arquivos CSV estão em `v2/importacao/data/` nas pastas corretas (`empresas`, `estabelecimentos`, `socios`, `simples`, `dominio`)
- Verificar encoding dos arquivos (import usamos `encoding="utf8-lossy"` para tratar caracteres inválidos)
- Verificar logs do script `v2/importacao/main.py`

### Erro de autenticação
- Verificar se o token está sendo enviado no header `Authorization: Bearer <token>`
- Verificar se o token não expirou


