# ConsultaCNPJ

Projeto para consulta e análise da base pública de CNPJ.

## Versões do Projeto

- **v2 (recomendado)** – FastAPI + ClickHouse  
  - API de alta performance para consulta em tempo real.
  - Diretório: `v2/`
  - README da v2: `v2/README.md`
  - Documentação detalhada da API v2: `v2/API_DOCUMENTATION.md`

- **v1 (legado)** – Django REST Framework + PostgreSQL  
  - Mantido apenas para referência e compatibilidade.
  - Diretório: `v1/`
  - Documentação da API v1: `v1/API_DOCUMENTATION.md`

## Por onde começar?

1. Leia `v2/README.md` para ver instalação, importação de dados e como subir a API nova.
2. Execute o script de importação em `v2/importacao/process.py` para importar os dados da Receita Federal.
3. Depois de subir a API v2, acesse:
   - `http://localhost:8000/docs` (Swagger)
   - `http://localhost:8000/redoc` (Redoc)
4. Para detalhes de cada endpoint, consulte `v2/API_DOCUMENTATION.md`.

## Notas Importantes

- **Encoding**: Os scripts de importação foram corrigidos para preservar acentos portugueses (tentam UTF-8 primeiro, depois latin-1).
- **Verificação**: Use `v2/importacao/verificar_encoding.py` para verificar encoding e acentos nos dados antes e depois da importação.


