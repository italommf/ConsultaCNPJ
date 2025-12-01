# API de Consulta CNPJ

Sistema de consulta de dados de empresas brasileiras através do CNPJ, utilizando dados da Receita Federal.

## 📋 Características

- Consulta de empresas por CNPJ
- Busca avançada com filtros
- API RESTful com Django REST Framework
- Autenticação via Token
- Documentação completa da API

## 🚀 Tecnologias

- **Backend**: Django 5.2.8
- **API**: Django REST Framework 3.15.2
- **Banco de Dados**: PostgreSQL
- **Servidor**: Gunicorn + Nginx
- **Autenticação**: Token Authentication

## 📦 Instalação

### Pré-requisitos

- Python 3.8+
- PostgreSQL 12+
- pip

### Passos

1. Clone o repositório:
```bash
git clone https://github.com/italommf/ConsultaCNPJ.git
cd ConsultaCNPJ
```

2. Crie um ambiente virtual:
```bash
cd backend
python -m venv venv
source venv/bin/activate  # Linux/Mac
# ou
venv\Scripts\activate  # Windows
```

3. Instale as dependências:
```bash
pip install -r requirements.txt
```

4. Configure as variáveis de ambiente:
```bash
cp env.example .env
# Edite o arquivo .env com suas configurações
```

5. Configure o banco de dados:
```bash
# Crie o banco de dados PostgreSQL
# Configure as credenciais no arquivo .env
```

6. Execute as migrações:
```bash
python manage.py migrate
```

7. Crie um superusuário:
```bash
python manage.py createsuperuser
```

8. Importe os dados (se necessário):
```bash
# Execute os scripts de importação na pasta scripts/
```

9. Execute o servidor de desenvolvimento:
```bash
python manage.py runserver
```

## 📚 Documentação da API

Consulte o arquivo [API_DOCUMENTATION.md](API_DOCUMENTATION.md) para ver todos os endpoints disponíveis.

## 🚀 Deploy

Para instruções de deploy na Hostinger, consulte:
- [README_DEPLOY.md](deploy/README_DEPLOY.md) - Guia completo de deploy
- [CHECKLIST_DEPLOY.md](deploy/CHECKLIST_DEPLOY.md) - Checklist de verificação

## 🔐 Variáveis de Ambiente

Veja o arquivo `backend/env.example` para todas as variáveis necessárias.

### Principais variáveis:

- `SECRET_KEY`: Chave secreta do Django
- `DEBUG`: Modo debug (False em produção)
- `DJANGO_ENV`: Ambiente (development/production)
- `DB_NAME`: Nome do banco de dados
- `DB_USER`: Usuário do PostgreSQL
- `DB_PASSWORD`: Senha do PostgreSQL
- `ALLOWED_HOSTS`: Hosts permitidos

## 📝 Estrutura do Projeto

```
ConsultaCNPJ/
├── backend/              # Aplicação Django
│   ├── api/             # App da API
│   ├── DadosPJ/         # Configurações do projeto
│   ├── manage.py
│   └── requirements.txt
├── deploy/              # Arquivos de configuração de deploy
│   ├── gunicorn_config.py
│   ├── nginx_consultacnpj.conf
│   ├── cnpj-api.service
│   └── README_DEPLOY.md
├── scripts/             # Scripts de importação de dados
└── API_DOCUMENTATION.md # Documentação da API
```

## 🔒 Segurança

- Autenticação obrigatória via Token
- CORS configurado para produção
- Headers de segurança habilitados
- SSL/HTTPS em produção

## 📄 Licença

Este projeto é privado e de propriedade de Italo MMF.

## 👤 Autor

Italo MMF

## 📞 Suporte

Para suporte, entre em contato através do repositório.

