# Documentação da API - CNPJ Search v2 (FastAPI + ClickHouse)

## Visão Geral

- **Versão atual recomendada**: v2 (FastAPI + ClickHouse)
- **Banco de dados**: ClickHouse (schema em `clickhouse/schema.sql`)
- **Autenticação**: JWT obtido via HTTP Basic (`admin / secret` por padrão)
- **Formato de respostas**: JSON

### Base URL (desenvolvimento)

```text
http://localhost:8000
```

---

## Autenticação

Todas as rotas (exceto `/` e `/health`) exigem um **Bearer Token** JWT no header `Authorization`.

### 1. Obter Token

**Endpoint**

```text
POST /auth/token
```

**Autenticação**

- HTTP Basic Auth
  - `username`: `admin`
  - `password`: `secret`

**Exemplo (curl)**

```bash
curl -X POST "http://localhost:8000/auth/token" \
  -u admin:secret
```

**Resposta (200)**

```json
{
  "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "token_type": "bearer"
}
```

### 2. Ver Usuário Atual

**Endpoint**

```text
GET /auth/me
```

**Headers**

```text
Authorization: Bearer <access_token>
```

**Resposta (200)**

```json
{
  "username": "admin",
  "exp": 1735689600
}
```

---

## Endpoints de Empresas (`/companies`)

### 1. Buscar Empresa Completa por CNPJ

Retorna **todos os dados** da empresa: estabelecimento, empresa, simples e sócios, em tempo real.

**Endpoint**

```text
GET /companies/cnpj/{cnpj}
```

**Parâmetros de Path**

- `cnpj` (obrigatório): CNPJ completo (14 dígitos, com ou sem formatação).

**Exemplos**

```text
GET /companies/cnpj/12345678000199
GET /companies/cnpj/12.345.678/0001-99
```

**Headers**

```text
Authorization: Bearer <access_token>
```

**Resposta (200) – Estrutura completa**

```json
{
  "estabelecimento": {
    "identificacao": {
      "cnpj": "12345678000199",
      "matriz_filial": "1",
      "nome_fantasia": "EMPRESA EXEMPLO LTDA"
    },
    "situacao": {
      "situacao_cadastral": "2",
      "situacao_motivo_desc": "SEM MOTIVO",
      "data_situacao": "01/01/2020",
      "data_abertura": "01/01/2020",
      "situacao_especial": null,
      "data_situacao_especial": null
    },
    "cnae": {
      "principal": {
        "codigo": "6201501",
        "descricao": "Desenvolvimento de programas de computador sob encomenda"
      },
      "secundarios": [
        {
          "codigo": "6202300",
          "descricao": "Desenvolvimento e licenciamento de programas customizáveis"
        }
      ]
    },
    "endereco": {
      "tipo_logradouro": "RUA",
      "logradouro": "EXEMPLO",
      "numero": "123",
      "complemento": "SALA 10",
      "bairro": "CENTRO",
      "cep": "01234567",
      "uf": "SP",
      "municipio": "3550308",
      "municipio_desc": "SAO PAULO",
      "cidade_exterior": null,
      "pais": "105",
      "pais_desc": "BRASIL"
    },
    "contato": {
      "ddd_1": "11",
      "telefone_1": "12345678",
      "ddd_2": null,
      "telefone_2": null,
      "ddd_fax": null,
      "fax": null,
      "email": "contato@exemplo.com.br"
    }
  },
  "empresa": {
    "identificacao": {
      "razao_social": "EMPRESA EXEMPLO LTDA"
    },
    "natureza_juridica": {
      "codigo": "2062",
      "descricao": "Sociedade Empresária Limitada"
    },
    "qualificacao": {
      "codigo": "05",
      "descricao": "Administrador"
    },
    "capital": {
      "capital_social": 100000.0
    },
    "porte": {
      "codigo": "03",
      "descricao": "Empresa de pequeno porte"
    },
    "ente_federativo": null,
    "simples": {
      "simples": {
        "opcao_simples": "S",
        "data_opcao_simples": "01/01/2020",
        "data_exclusao_simples": null
      },
      "mei": {
        "opcao_mei": "N",
        "data_opcao_mei": null,
        "data_exclusao_mei": null
      }
    },
    "socios": [
      {
        "identificacao": {
          "identificador_socio": "1",
          "nome_socio": "JOAO SILVA",
          "cnpj_cpf_socio": "***456789**"
        },
        "faixa_etaria": {
          "codigo": "5",
          "descricao": "Entre 41 a 50 anos"
        },
        "data_entrada_sociedade": "01/01/2020",
        "qualificacao_socio": {
          "codigo": "05",
          "descricao": "Administrador"
        },
        "pais": {
          "codigo": "105",
          "descricao": "BRASIL"
        },
        "representante_legal": {
          "representante_legal": "N",
          "nome_representante": null,
          "qualificacao_representante": {
            "codigo": "00",
            "descricao": "Não informada"
          }
        }
      }
    ]
  }
}
```

**Erros comuns**

- `400`: CNPJ inválido (menos de 14 dígitos).
- `404`: CNPJ não encontrado.

---

### 2. Busca Geral de Estabelecimentos com Filtros

Busca estabelecimentos com múltiplos filtros opcionais (todos em tempo real).

**Endpoint**

```text
GET /companies/search
```

**Parâmetros de Query**

- Paginação:
  - `page` (opcional, padrão: `1`)
  - `page_size` (opcional, padrão: `100`, máx: `1000`)
- Filtros:
  - `q`: busca textual em `nome_fantasia` ou `cnpj`
  - `cnpj`: CNPJ completo (14 dígitos)
  - `uf`: UF (2 letras, ex: `SP`)
  - `municipio`: código do município (4 dígitos)
  - `cnae_fiscal`: CNAE principal (7 dígitos)
  - `situacao_cadastral`: situação cadastral (`2`=Ativa, etc.)
  - `matriz_filial`: `1`=Matriz, `2`=Filial

**Exemplos**

```text
GET /companies/search?uf=SP&municipio=3550&page=1&page_size=100
GET /companies/search?cnae_fiscal=6201501&situacao_cadastral=2
GET /companies/search?q=padaria
```

**Resposta (200) – Esquema**

```json
{
  "total": 12345,
  "page": 1,
  "page_size": 100,
  "total_pages": 124,
  "results": [
    {
      "cnpj": "12345678000199",
      "cnpj_basico": "12345678",
      "cnpj_ordem": "0001",
      "cnpj_dv": "99",
      "matriz_filial": "1",
      "nome_fantasia": "EMPRESA EXEMPLO LTDA",
      "situacao_cadastral": "2",
      "data_situacao": "01/01/2020",
      "motivo_situacao": "00",
      "cidade_exterior": null,
      "pais": "105",
      "data_inicio": "01/01/2020",
      "cnae_fiscal": "6201501",
      "cnae_fiscal_secundaria": "6202300,6203100",
      "tipo_logradouro": "RUA",
      "logradouro": "EXEMPLO",
      "numero": "123",
      "complemento": "SALA 10",
      "bairro": "CENTRO",
      "cep": "01234567",
      "uf": "SP",
      "municipio": "3550308",
      "ddd_1": "11",
      "telefone_1": "12345678",
      "ddd_2": null,
      "telefone_2": null,
      "ddd_fax": null,
      "fax": null,
      "email": "contato@exemplo.com.br",
      "situacao_especial": null,
      "data_situacao_especial": null
    }
  ]
}
```

---

### 3. Buscar Estabelecimentos por CNAE

Busca estabelecimentos que possuem um CNAE específico (principal ou secundário).

**Endpoint**

```text
GET /companies/cnae/{cnae}
```

**Parâmetros de Path**

- `cnae` (obrigatório): código CNAE (7 dígitos, ex: `6201501`).

**Parâmetros de Query**

- `page` (opcional): número da página (padrão: 1).
- `page_size` (opcional): tamanho da página (padrão: 100, máx: 1000).
- `cnae_sec` (opcional): se `true`, busca também em `cnae_fiscal_secundaria`.

**Exemplos**

```text
GET /companies/cnae/6201501
GET /companies/cnae/6201501?cnae_sec=true&page=1&page_size=200
```

**Resposta (200)**  
Mesma estrutura do `GET /companies/search` (lista de `Estabelecimento`).

---

## Endpoints de CNAEs (`/cnaes`)

### 1. Listar CNAEs

```text
GET /cnaes
```

**Parâmetros de Query**

- `q` (opcional): busca textual na descrição (`LIKE '%q%'`).
- `page` (opcional): número da página (padrão: 1).
- `page_size` (opcional): tamanho da página (padrão: 100, máx: 1000).

**Resposta (200)**

```json
[
  { "codigo": "6201501", "descricao": "Desenvolvimento de programas de computador sob encomenda" },
  { "codigo": "6202300", "descricao": "Desenvolvimento e licenciamento de programas customizáveis" }
]
```

### 2. Buscar CNAE por Código

```text
GET /cnaes/{codigo}
```

- `codigo`: CNAE com até 7 dígitos (será truncado para 7).

**Resposta (200)**

```json
{
  "codigo": "6201501",
  "descricao": "Desenvolvimento de programas de computador sob encomenda"
}
```

---

## Endpoints de Municípios (`/municipios`)

### 1. Listar Municípios

```text
GET /municipios
```

**Parâmetros de Query**

- `q` (opcional): busca textual na descrição (`LIKE '%q%'`).
- `page` (opcional): número da página (padrão: 1).
- `page_size` (opcional): tamanho da página (padrão: 100, máx: 1000).

**Resposta (200)**

```json
[
  { "codigo": "3550308", "descricao": "SAO PAULO" },
  { "codigo": "3304557", "descricao": "RIO DE JANEIRO" }
]
```

### 2. Buscar Município por Código

```text
GET /municipios/{codigo}
```

- `codigo`: código do município com até 4 dígitos (será truncado para 4).

**Resposta (200)**

```json
{
  "codigo": "3550308",
  "descricao": "SAO PAULO"
}
```

---

## Códigos de Status HTTP

- `200 OK` – requisição bem-sucedida.
- `400 Bad Request` – parâmetros inválidos (ex.: CNPJ ou CNAE incorretos).
- `401 Unauthorized` – token ausente ou inválido.
- `404 Not Found` – recurso não encontrado (ex.: CNPJ/CNAE/município inexistente).
- `500 Internal Server Error` – erro interno inesperado.

---

## Exemplos de Uso (curl)

```bash
# Obter token
TOKEN=$(curl -s -X POST "http://localhost:8000/auth/token" -u admin:secret | jq -r .access_token)

# Buscar empresa por CNPJ
curl -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8000/companies/cnpj/12345678000199"

# Buscar estabelecimentos por UF e município
curl -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8000/companies/search?uf=SP&municipio=3550&page=1&page_size=100"

# Buscar estabelecimentos por CNAE (principal)
curl -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8000/companies/cnae/6201501"

# Buscar CNAEs
curl -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8000/cnaes?q=desenvolvimento"
```

---

## Boas Práticas

1. Sempre usar **paginação** (`page` / `page_size`) para buscas amplas (CNAE, UF, etc.).
2. Sempre enviar o header `Authorization: Bearer <access_token>`.
3. Preferir filtros indexados: `cnpj`, `uf`, `municipio`, `cnae_fiscal`, `situacao_cadastral`, `matriz_filial`.
4. Para integrações, use a documentação interativa em `http://localhost:8000/docs` (Swagger) ou `http://localhost:8000/redoc`.


