# Documentação da API - Sistema de Consulta CNPJ

## Base URL
```
http://localhost:8000/api
```

## Autenticação
A API requer autenticação via Token. Inclua o token no header:
```
Authorization: Token seu_token_aqui
```

---

## Endpoints Disponíveis

### 1. Buscar Empresa por CNPJ

Retorna todos os dados completos de uma empresa específica pelo CNPJ.

**Endpoint:** `GET /companies/cnpj/{cnpj}/`

**Parâmetros:**
- `cnpj` (path): CNPJ da empresa (14 dígitos, com ou sem formatação)

**Exemplo de Requisição:**
```
GET /api/companies/cnpj/12345678000190/
GET /api/companies/cnpj/12.345.678/0001-90/
```

#### Python (requests)
```python
import requests

url = "http://localhost:8000/api/companies/cnpj/12345678000190/"
headers = {
    "Authorization": "Token seu_token_aqui"
}

response = requests.get(url, headers=headers)
data = response.json()

print(data)
```

#### Postman
1. Método: `GET`
2. URL: `http://localhost:8000/api/companies/cnpj/12345678000190/`
3. Headers:
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

### 2. Busca Geral de Empresas

Busca empresas com múltiplos filtros opcionais. Todos os filtros são opcionais e podem ser combinados.

**Endpoint:** `GET /companies/search/`

**Parâmetros de Paginação:**
- `page` (query, opcional): Número da página (padrão: 1)
- `page_size` (query, opcional): Tamanho da página (padrão: 1000)

**Filtros Disponíveis:**

#### CNAE
- `cnae_principal` (query, opcional): CNAE principal (7 dígitos)
- `cnae_secundario` (query, opcional): CNAE secundário (7 dígitos)

#### Localização
- `uf` (query, opcional): Estado (sigla de 2 letras, ex: SP, RJ, MG)
- `municipio` (query, opcional): Código do município

#### Capital Social
- `capital_social_min` (query, opcional): Capital social mínimo (número decimal)
- `capital_social_max` (query, opcional): Capital social máximo (número decimal)

#### Sócios
- `qtd_socios_min` (query, opcional): Quantidade mínima de sócios (número inteiro)
- `qtd_socios_max` (query, opcional): Quantidade máxima de sócios (número inteiro)

#### Outros Filtros
- `situacao_cadastral` (query, opcional): Situação cadastral (ex: "2" para Ativa)
- `matriz_filial` (query, opcional): 1=Matriz, 2=Filial
- `porte` (query, opcional): Porte da empresa
  - `00`: Não informado
  - `01`: Micro empresa
  - `03`: Empresa de pequeno porte
  - `05`: Demais
- `natureza_juridica` (query, opcional): Código da natureza jurídica
- `simples` (query, opcional): Opção pelo Simples Nacional (`S` ou `N`)
- `mei` (query, opcional): Opção pelo MEI (`S` ou `N`)

#### Exemplo 1: Buscar por CNAE Principal
```
GET /api/companies/search/?cnae_principal=6201501
```

**Python:**
```python
import requests

url = "http://localhost:8000/api/companies/search/"
headers = {
    "Authorization": "Token seu_token_aqui"
}
params = {
    "cnae_principal": "6201501"
}

response = requests.get(url, headers=headers, params=params)
data = response.json()

print(f"Total encontrado: {data['total_count']}")
print(f"Página {data['page']} de {data['total_pages']}")
for empresa in data['results']:
    print(f"- {empresa['empresa']['identificacao']['razao_social']}")
```

**Postman:**
1. Método: `GET`
2. URL: `http://localhost:8000/api/companies/search/`
3. Params (Query Params):
   - `cnae_principal`: `6201501`
4. Headers:
   - `Authorization`: `Token seu_token_aqui`

#### Exemplo 2: Buscar por Estado e Capital Social
```
GET /api/companies/search/?uf=SP&capital_social_min=10000&capital_social_max=100000
```

**Python:**
```python
import requests

url = "http://localhost:8000/api/companies/search/"
headers = {
    "Authorization": "Token seu_token_aqui"
}
params = {
    "uf": "SP",
    "capital_social_min": 10000,
    "capital_social_max": 100000,
    "page": 1,
    "page_size": 100
}

response = requests.get(url, headers=headers, params=params)
data = response.json()

print(f"Total: {data['total_count']} empresas")
print(f"Resultados na página: {data['count']}")
```

**Postman:**
1. Método: `GET`
2. URL: `http://localhost:8000/api/companies/search/`
3. Params:
   - `uf`: `SP`
   - `capital_social_min`: `10000`
   - `capital_social_max`: `100000`
   - `page`: `1`
   - `page_size`: `100`

#### Exemplo 3: Buscar Empresas com Múltiplos Sócios
```
GET /api/companies/search/?qtd_socios_min=2&qtd_socios_max=5
```

**Python:**
```python
import requests

url = "http://localhost:8000/api/companies/search/"
headers = {
    "Authorization": "Token seu_token_aqui"
}
params = {
    "qtd_socios_min": 2,
    "qtd_socios_max": 5
}

response = requests.get(url, headers=headers, params=params)
data = response.json()

for empresa in data['results']:
    qtd_socios = len(empresa['empresa']['socios'])
    print(f"{empresa['empresa']['identificacao']['razao_social']}: {qtd_socios} sócios")
```

**Postman:**
1. Método: `GET`
2. URL: `http://localhost:8000/api/companies/search/`
3. Params:
   - `qtd_socios_min`: `2`
   - `qtd_socios_max`: `5`

#### Exemplo 4: Busca Complexa com Múltiplos Filtros
```
GET /api/companies/search/?cnae_principal=6201501&uf=SP&matriz_filial=1&simples=S&porte=03&capital_social_min=50000
```

**Python:**
```python
import requests

url = "http://localhost:8000/api/companies/search/"
headers = {
    "Authorization": "Token seu_token_aqui"
}
params = {
    "cnae_principal": "6201501",
    "uf": "SP",
    "matriz_filial": "1",  # Apenas matrizes
    "simples": "S",  # Com Simples Nacional
    "porte": "03",  # Pequeno porte
    "capital_social_min": 50000,
    "page": 1,
    "page_size": 500
}

response = requests.get(url, headers=headers, params=params)
data = response.json()

print(f"Filtros aplicados: {data['filters']}")
print(f"Total encontrado: {data['total_count']}")
print(f"Página {data['page']} de {data['total_pages']}")
```

**Postman:**
1. Método: `GET`
2. URL: `http://localhost:8000/api/companies/search/`
3. Params:
   - `cnae_principal`: `6201501`
   - `uf`: `SP`
   - `matriz_filial`: `1`
   - `simples`: `S`
   - `porte`: `03`
   - `capital_social_min`: `50000`
   - `page`: `1`
   - `page_size`: `500`

#### Exemplo 5: Buscar por CNAE Secundário
```
GET /api/companies/search/?cnae_secundario=6202300
```

**Python:**
```python
import requests

url = "http://localhost:8000/api/companies/search/"
headers = {
    "Authorization": "Token seu_token_aqui"
}
params = {
    "cnae_secundario": "6202300"
}

response = requests.get(url, headers=headers, params=params)
data = response.json()

print(f"Empresas com CNAE secundário 6202300: {data['total_count']}")
```

**Postman:**
1. Método: `GET`
2. URL: `http://localhost:8000/api/companies/search/`
3. Params:
   - `cnae_secundario`: `6202300`

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

## Códigos de Status HTTP

- `200 OK`: Requisição bem-sucedida
- `400 Bad Request`: Parâmetros inválidos
- `401 Unauthorized`: Token de autenticação inválido ou ausente
- `404 Not Found`: Recurso não encontrado (ex: CNPJ não existe)
- `500 Internal Server Error`: Erro interno do servidor

---

## Exemplos de Uso Avançado

### Python - Classe Helper

```python
import requests
from typing import Dict, List, Optional

class CNPJAPI:
    def __init__(self, base_url: str, token: str):
        self.base_url = base_url.rstrip('/')
        self.headers = {
            "Authorization": f"Token {token}"
        }
    
    def buscar_por_cnpj(self, cnpj: str) -> Dict:
        """Busca empresa completa por CNPJ"""
        url = f"{self.base_url}/companies/cnpj/{cnpj}/"
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        return response.json()
    
    def buscar_empresas(
        self,
        cnae_principal: Optional[str] = None,
        cnae_secundario: Optional[str] = None,
        uf: Optional[str] = None,
        municipio: Optional[str] = None,
        capital_social_min: Optional[float] = None,
        capital_social_max: Optional[float] = None,
        qtd_socios_min: Optional[int] = None,
        qtd_socios_max: Optional[int] = None,
        situacao_cadastral: Optional[str] = None,
        matriz_filial: Optional[str] = None,
        porte: Optional[str] = None,
        natureza_juridica: Optional[str] = None,
        simples: Optional[str] = None,
        mei: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000
    ) -> Dict:
        """Busca empresas com filtros"""
        url = f"{self.base_url}/companies/search/"
        params = {
            "page": page,
            "page_size": page_size
        }
        
        # Adicionar apenas filtros não nulos
        if cnae_principal:
            params["cnae_principal"] = cnae_principal
        if cnae_secundario:
            params["cnae_secundario"] = cnae_secundario
        if uf:
            params["uf"] = uf
        if municipio:
            params["municipio"] = municipio
        if capital_social_min:
            params["capital_social_min"] = capital_social_min
        if capital_social_max:
            params["capital_social_max"] = capital_social_max
        if qtd_socios_min:
            params["qtd_socios_min"] = qtd_socios_min
        if qtd_socios_max:
            params["qtd_socios_max"] = qtd_socios_max
        if situacao_cadastral:
            params["situacao_cadastral"] = situacao_cadastral
        if matriz_filial:
            params["matriz_filial"] = matriz_filial
        if porte:
            params["porte"] = porte
        if natureza_juridica:
            params["natureza_juridica"] = natureza_juridica
        if simples:
            params["simples"] = simples
        if mei:
            params["mei"] = mei
        
        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        return response.json()
    
    def buscar_todas_paginas(
        self,
        **filtros
    ) -> List[Dict]:
        """Busca todas as páginas de resultados"""
        todas_empresas = []
        page = 1
        
        while True:
            resultado = self.buscar_empresas(page=page, **filtros)
            todas_empresas.extend(resultado['results'])
            
            if page >= resultado['total_pages']:
                break
            
            page += 1
        
        return todas_empresas

# Exemplo de uso
api = CNPJAPI("http://localhost:8000/api", "seu_token_aqui")

# Buscar por CNPJ
empresa = api.buscar_por_cnpj("12345678000190")
print(empresa['empresa']['identificacao']['razao_social'])

# Buscar empresas em SP com Simples Nacional
resultado = api.buscar_empresas(uf="SP", simples="S", page_size=100)
print(f"Encontradas {resultado['total_count']} empresas")

# Buscar todas as empresas de um CNAE (todas as páginas)
todas = api.buscar_todas_paginas(cnae_principal="6201501")
print(f"Total: {len(todas)} empresas")
```

---

## Postman Collection

### Importar Collection

1. Abra o Postman
2. Clique em "Import"
3. Cole o JSON abaixo ou crie manualmente as requisições

### Variáveis de Ambiente

Crie um ambiente no Postman com as seguintes variáveis:
- `base_url`: `http://localhost:8000/api`
- `token`: `seu_token_aqui`

### Exemplos de Requisições no Postman

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
GET {{base_url}}/companies/search/?uf=SP&capital_social_min=10000&capital_social_max=100000&simples=S
Authorization: Token {{token}}
```

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

## Limites e Boas Práticas

1. **Paginação**: Sempre use paginação para grandes volumes de dados
2. **Rate Limiting**: Respeite os limites de requisições por segundo
3. **Cache**: Considere cachear resultados quando apropriado
4. **Filtros**: Use filtros específicos para reduzir o volume de dados retornados
5. **Tratamento de Erros**: Sempre trate erros HTTP adequadamente

---

## Suporte

Para dúvidas ou problemas, consulte a documentação do Django REST Framework ou entre em contato com a equipe de desenvolvimento.

