# Documentação Completa da API - Sistema de Consulta CNPJ

## Base URL
```
https://consultacnpj.italommf.com.br/api
```

## Autenticação
A API requer autenticação via Token. Inclua o token no header:
```
Authorization: Token seu_token_aqui
```

**Importante:** Deixe um espaço entre "Token" e o valor do token.

---

## Endpoints Disponíveis

### 1. Buscar Empresa por CNPJ

Retorna todos os dados completos de uma empresa específica pelo CNPJ.

**Endpoint:** `GET /companies/cnpj/{cnpj}/`

**Parâmetros:**
- `cnpj` (path, obrigatório): CNPJ da empresa (14 dígitos, com ou sem formatação)

**Exemplos de Requisição:**
```
GET /api/companies/cnpj/12345678000190/
GET /api/companies/cnpj/12.345.678/0001-90/
GET /api/companies/cnpj/12345678000190
```

**Exemplo no Postman:**
- URL: `https://consultacnpj.italommf.com.br/api/companies/cnpj/12345678000190/`
- Method: `GET`
- Headers:
  - `Authorization`: `Token seu_token_aqui`

**Resposta de Sucesso (200):**
```json
{
  "estabelecimento": {
    "identificacao": {
      "cnpj": "12345678000190",
      "matriz_filial": "1",
      "nome_fantasia": "Empresa Exemplo LTDA"
    },
    "situacao": {
      "situacao_cadastral": "2",
      "situacao_motivo_desc": "Ativa",
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
          "descricao": "Desenvolvimento e licenciamento de programas de computador customizáveis"
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
      "capital_social": "100000.00"
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
          "nome_socio": "JOÃO SILVA",
          "cnpj_cpf_socio": "12345678901"
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
          "representante_legal": "S",
          "nome_representante": "MARIA SILVA",
          "qualificacao_representante": {
            "codigo": "10",
            "descricao": "Diretor"
          }
        }
      }
    ]
  }
}
```

**Resposta de Erro (404):**
```json
{
  "detail": "CNPJ não encontrado"
}
```

---

### 2. Busca Geral de Empresas (com Filtros)

Busca empresas com múltiplos filtros opcionais. Todos os filtros podem ser combinados.

**Endpoint:** `GET /companies/search/`

**Parâmetros de Paginação:**
- `page` (query, opcional): Número da página (padrão: 1)
- `page_size` (query, opcional): Tamanho da página (padrão: 1000, máximo recomendado: 1000)

**Filtros Disponíveis:**

#### CNAE
- `cnae_principal` (query, opcional): CNAE principal (7 dígitos, ex: `6201501`)
- `cnae_secundario` (query, opcional): CNAE secundário (7 dígitos, ex: `6202300`)

#### Localização
- `uf` (query, opcional): Estado (sigla de 2 letras, ex: `SP`, `RJ`, `MG`)
- `municipio` (query, opcional): Código do município (4 dígitos, ex: `3550308` para São Paulo)

#### Capital Social
- `capital_social_min` (query, opcional): Capital social mínimo (número decimal, ex: `10000`)
- `capital_social_max` (query, opcional): Capital social máximo (número decimal, ex: `1000000`)

#### Sócios
- `qtd_socios_min` (query, opcional): Quantidade mínima de sócios (número inteiro, ex: `2`)
- `qtd_socios_max` (query, opcional): Quantidade máxima de sócios (número inteiro, ex: `10`)

#### Situação e Tipo
- `situacao_cadastral` (query, opcional): Situação cadastral
  - `"2"`: Ativa
  - `"3"`: Suspensa
  - `"4"`: Inapta
  - `"8"`: Baixada
- `matriz_filial` (query, opcional): Tipo de estabelecimento
  - `"1"`: Matriz
  - `"2"`: Filial

#### Porte da Empresa
- `porte` (query, opcional): Porte da empresa
  - `"00"`: Não informado
  - `"01"`: Micro empresa
  - `"03"`: Empresa de pequeno porte
  - `"05"`: Demais

#### Natureza Jurídica
- `natureza_juridica` (query, opcional): Código da natureza jurídica (4 dígitos, ex: `2062` para Sociedade Empresária Limitada)

#### Simples Nacional e MEI
- `simples` (query, opcional): Opção pelo Simples Nacional
  - `"S"`: Sim
  - `"N"`: Não
- `mei` (query, opcional): Opção pelo MEI
  - `"S"`: Sim
  - `"N"`: Não

**Exemplo 1: Buscar por CNAE Principal**
```
GET /api/companies/search/?cnae_principal=6201501
```

**Exemplo 2: Buscar por Estado e Capital Social**
```
GET /api/companies/search/?uf=SP&capital_social_min=10000&capital_social_max=100000
```

**Exemplo 3: Buscar Empresas com Múltiplos Sócios**
```
GET /api/companies/search/?qtd_socios_min=2&qtd_socios_max=5
```

**Exemplo 4: Busca Complexa com Múltiplos Filtros**
```
GET /api/companies/search/?cnae_principal=6201501&uf=SP&matriz_filial=1&simples=S&porte=03&capital_social_min=50000&page=1&page_size=500
```

**Exemplo 5: Buscar por CNAE Secundário**
```
GET /api/companies/search/?cnae_secundario=6202300
```

**Exemplo 6: Buscar por Município**
```
GET /api/companies/search/?municipio=3550308
```

**Exemplo 7: Buscar Empresas Ativas em SP com Simples Nacional**
```
GET /api/companies/search/?uf=SP&situacao_cadastral=2&simples=S
```

**Exemplo 8: Buscar Micro Empresas com MEI**
```
GET /api/companies/search/?porte=01&mei=S
```

**Resposta de Sucesso (200):**
```json
{
  "count": 100,
  "total_count": 500,
  "page": 1,
  "page_size": 1000,
  "total_pages": 1,
  "filters": {
    "cnae_principal": "6201501",
    "uf": "SP",
    "capital_social_min": "50000"
  },
  "results": [
    {
      "estabelecimento": { ... },
      "empresa": { ... }
    },
    ...
  ]
}
```

**Campos da Resposta:**
- `count`: Quantidade de resultados na página atual
- `total_count`: Total de resultados encontrados (considerando todos os filtros)
- `page`: Página atual
- `page_size`: Tamanho da página
- `total_pages`: Total de páginas
- `filters`: Objeto com os filtros aplicados
- `results`: Array com os resultados (mesma estrutura do endpoint de busca por CNPJ)

---

### 3. Buscar Empresas por CNAE

Busca empresas que possuem um CNAE específico (principal ou secundário).

**Endpoint:** `GET /companies/cnae/{cnae}/`

**Parâmetros:**
- `cnae` (path, obrigatório): Código CNAE (7 dígitos, ex: `6201501`)

**Query Parameters:**
- `page` (opcional): Número da página (padrão: 1)
- `page_size` (opcional): Tamanho da página (padrão: 1000)
- `cnae_sec` (opcional): Se `true`, busca também em CNAEs secundários (padrão: `false`)
  - Valores aceitos: `true`, `1`, `yes`, `on` (case-insensitive)

**Exemplo 1: Buscar por CNAE Principal**
```
GET /api/companies/cnae/6201501/
```

**Exemplo 2: Buscar por CNAE (incluindo secundários)**
```
GET /api/companies/cnae/6201501/?cnae_sec=true
```

**Exemplo 3: Com Paginação**
```
GET /api/companies/cnae/6201501/?page=1&page_size=100
```

**Resposta de Sucesso (200):**
```json
{
  "count": 50,
  "total_count": 150,
  "page": 1,
  "page_size": 1000,
  "total_pages": 1,
  "cnae": "6201501",
  "cnae_sec": false,
  "results": [
    {
      "estabelecimento": { ... },
      "empresa": { ... }
    },
    ...
  ]
}
```

---

### 4. Listar CNAEs

Lista todos os CNAEs disponíveis com busca opcional.

**Endpoint:** `GET /cnaes/`

**Query Parameters:**
- `search` (opcional): Busca por código ou descrição (ex: `6201501` ou `desenvolvimento`)
- `page` (opcional): Número da página
- `page_size` (opcional): Tamanho da página

**Exemplo 1: Listar Todos**
```
GET /api/cnaes/
```

**Exemplo 2: Buscar CNAE**
```
GET /api/cnaes/?search=6201501
GET /api/cnaes/?search=desenvolvimento
```

**Resposta de Sucesso (200):**
```json
{
  "count": 10,
  "next": "http://localhost:8000/api/cnaes/?page=2",
  "previous": null,
  "results": [
    {
      "codigo": "6201501",
      "descricao": "Desenvolvimento de programas de computador sob encomenda"
    },
    ...
  ]
}
```

---

### 5. Listar Municípios

Lista todos os municípios disponíveis com busca opcional.

**Endpoint:** `GET /municipios/`

**Query Parameters:**
- `search` (opcional): Busca por código ou descrição (ex: `3550308` ou `são paulo`)
- `page` (opcional): Número da página
- `page_size` (opcional): Tamanho da página

**Exemplo 1: Listar Todos**
```
GET /api/municipios/
```

**Exemplo 2: Buscar Município**
```
GET /api/municipios/?search=3550308
GET /api/municipios/?search=são paulo
```

**Resposta de Sucesso (200):**
```json
{
  "count": 10,
  "next": "http://localhost:8000/api/municipios/?page=2",
  "previous": null,
  "results": [
    {
      "codigo": "3550308",
      "descricao": "SAO PAULO"
    },
    ...
  ]
}
```

---

## Códigos de Status HTTP

- `200 OK`: Requisição bem-sucedida
- `400 Bad Request`: Parâmetros inválidos
- `401 Unauthorized`: Token de autenticação inválido ou ausente
- `404 Not Found`: Recurso não encontrado (ex: CNPJ não existe)
- `405 Method Not Allowed`: Método HTTP não permitido
- `500 Internal Server Error`: Erro interno do servidor

---

## Exemplos de Uso no Postman

### Configurar Variáveis de Ambiente

1. Crie um ambiente no Postman com:
   - `base_url`: `https://consultacnpj.italommf.com.br/api`
   - `token`: `seu_token_aqui`

2. Use as variáveis nas requisições:
   - URL: `{{base_url}}/companies/cnpj/12345678000190/`
   - Header: `Authorization: Token {{token}}`

### Exemplos de Requisições

#### 1. Buscar por CNPJ
```
GET {{base_url}}/companies/cnpj/12345678000190/
Authorization: Token {{token}}
```

#### 2. Buscar por CNAE Principal
```
GET {{base_url}}/companies/search/?cnae_principal=6201501
Authorization: Token {{token}}
```

#### 3. Buscar com Múltiplos Filtros
```
GET {{base_url}}/companies/search/?uf=SP&capital_social_min=10000&capital_social_max=100000&simples=S&page=1&page_size=100
Authorization: Token {{token}}
```

#### 4. Buscar Empresas por CNAE (incluindo secundários)
```
GET {{base_url}}/companies/cnae/6201501/?cnae_sec=true&page=1&page_size=500
Authorization: Token {{token}}
```

---

## Exemplos de Uso em Python

```python
import requests

BASE_URL = "https://consultacnpj.italommf.com.br/api"
TOKEN = "seu_token_aqui"

headers = {
    "Authorization": f"Token {TOKEN}"
}

# 1. Buscar por CNPJ
def buscar_por_cnpj(cnpj):
    url = f"{BASE_URL}/companies/cnpj/{cnpj}/"
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()

# 2. Buscar empresas com filtros
def buscar_empresas(**filtros):
    url = f"{BASE_URL}/companies/search/"
    params = {k: v for k, v in filtros.items() if v is not None}
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    return response.json()

# 3. Buscar todas as páginas
def buscar_todas_empresas(**filtros):
    todas_empresas = []
    page = 1
    
    while True:
        resultado = buscar_empresas(page=page, page_size=1000, **filtros)
        todas_empresas.extend(resultado['results'])
        
        if page >= resultado['total_pages']:
            break
        
        page += 1
    
    return todas_empresas

# Exemplos de uso
empresa = buscar_por_cnpj("12345678000190")
print(empresa['empresa']['identificacao']['razao_social'])

# Buscar empresas em SP com Simples Nacional
resultado = buscar_empresas(uf="SP", simples="S", page_size=100)
print(f"Encontradas {resultado['total_count']} empresas")

# Buscar todas as empresas de um CNAE
todas = buscar_todas_empresas(cnae_principal="6201501")
print(f"Total: {len(todas)} empresas")
```

---

## Tabela de Referência: Códigos Comuns

### Situação Cadastral
- `"2"`: Ativa
- `"3"`: Suspensa
- `"4"`: Inapta
- `"8"`: Baixada

### Matriz/Filial
- `"1"`: Matriz
- `"2"`: Filial

### Porte
- `"00"`: Não informado
- `"01"`: Micro empresa
- `"03"`: Empresa de pequeno porte
- `"05"`: Demais

### Simples/MEI
- `"S"`: Sim
- `"N"`: Não

---

## Limites e Boas Práticas

1. **Paginação**: Sempre use paginação para grandes volumes de dados. O padrão é 1000 por página.
2. **Filtros Específicos**: Use filtros específicos para reduzir o volume de dados retornados.
3. **Rate Limiting**: Respeite os limites de requisições por segundo.
4. **Cache**: Considere cachear resultados quando apropriado.
5. **Tratamento de Erros**: Sempre trate erros HTTP adequadamente.

---

## Tratamento de Erros

### Python
```python
import requests

try:
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()  # Lança exceção para status 4xx/5xx
    data = response.json()
except requests.exceptions.HTTPError as e:
    if e.response.status_code == 404:
        print("CNPJ não encontrado")
    elif e.response.status_code == 401:
        print("Token inválido")
    else:
        print(f"Erro HTTP {e.response.status_code}: {e.response.text}")
except requests.exceptions.RequestException as e:
    print(f"Erro na requisição: {e}")
```

---

## Suporte

Para dúvidas ou problemas, consulte a documentação do Django REST Framework ou entre em contato com a equipe de desenvolvimento.
