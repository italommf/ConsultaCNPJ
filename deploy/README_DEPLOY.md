# Guia de Deploy - API CNPJ

## Pré-requisitos

- VPS KVM2 na Hostinger
- Acesso SSH root ou sudo
- Domínio `consultacnpj.italommf.com.br` apontando para o IP da VPS

## Passo 1: Preparação do Servidor

```bash
# Atualizar sistema
sudo apt update && sudo apt upgrade -y

# Instalar dependências
sudo apt install -y python3 python3-pip python3-venv postgresql postgresql-contrib nginx git curl
```

## Passo 2: Configurar PostgreSQL

```bash
# Acessar PostgreSQL
sudo -u postgres psql

# Criar banco e usuário
CREATE DATABASE cnpjdb;
CREATE USER cnpj_user WITH PASSWORD 'SUA_SENHA_FORTE_AQUI';
ALTER ROLE cnpj_user SET client_encoding TO 'utf8';
ALTER ROLE cnpj_user SET default_transaction_isolation TO 'read committed';
ALTER ROLE cnpj_user SET timezone TO 'UTC';
GRANT ALL PRIVILEGES ON DATABASE cnpjdb TO cnpj_user;
\q
```

## Passo 3: Preparar Diretórios

```bash
# Criar diretório do projeto
sudo mkdir -p /var/www/cnpj_api
sudo chown $USER:$USER /var/www/cnpj_api

# Criar diretórios de log
sudo mkdir -p /var/log/gunicorn
sudo mkdir -p /var/run/gunicorn
sudo mkdir -p /var/log/django
sudo chown www-data:www-data /var/log/gunicorn
sudo chown www-data:www-data /var/run/gunicorn
sudo chown www-data:www-data /var/log/django
```

## Passo 4: Fazer Upload do Código

```bash
# Opção 1: Via Git
cd /var/www/cnpj_api
git clone SEU_REPOSITORIO.git backend

# Opção 2: Via SCP (do seu computador local)
# scp -r backend/ usuario@servidor:/var/www/cnpj_api/
```

## Passo 5: Configurar Ambiente Virtual

```bash
cd /var/www/cnpj_api/backend
python3 -m venv venv
source venv/bin/activate

# Instalar dependências
pip install --upgrade pip
pip install -r requirements.txt
```

## Passo 6: Configurar Variáveis de Ambiente

```bash
cd /var/www/cnpj_api/backend
cp env.example .env
nano .env
```

Edite o arquivo `.env` com suas configurações:
- `SECRET_KEY`: Gere uma nova com `python manage.py shell` → `from django.core.management.utils import get_random_secret_key; print(get_random_secret_key())`
- `DJANGO_ENV`: `production` (importante para usar as configurações de produção)
- `DB_PASSWORD`: A senha do PostgreSQL que você criou
- `ALLOWED_HOSTS`: `consultacnpj.italommf.com.br`

## Passo 7: Importar Dados do Banco

```bash
# Opção 1: Se os dados estão em CSV na VPS
cd /var/www/cnpj_api
source backend/venv/bin/activate
cd scripts
python main.py

# Opção 2: Restaurar dump do banco local
# No servidor local:
pg_dump -U usuario -h localhost cnpjdb > backup.sql

# Na VPS:
psql -U cnpj_user -d cnpjdb < backup.sql
```

## Passo 8: Configurar Django

```bash
cd /var/www/cnpj_api/backend
source venv/bin/activate

# Coletar arquivos estáticos
python manage.py collectstatic --noinput

# Criar superusuário (se necessário)
python manage.py createsuperuser
```

## Passo 9: Configurar Gunicorn

```bash
# Copiar arquivo de serviço
sudo cp /var/www/cnpj_api/deploy/cnpj-api.service /etc/systemd/system/
sudo cp /var/www/cnpj_api/deploy/gunicorn_config.py /var/www/cnpj_api/deploy/

# Ajustar permissões
sudo chown www-data:www-data /var/www/cnpj_api/backend
sudo chmod +x /var/www/cnpj_api/backend/venv/bin/gunicorn

# Ativar serviço
sudo systemctl daemon-reload
sudo systemctl start cnpj-api
sudo systemctl enable cnpj-api
sudo systemctl status cnpj-api
```

## Passo 10: Configurar Nginx

```bash
# Copiar configuração
sudo cp /var/www/cnpj_api/deploy/nginx_consultacnpj.conf /etc/nginx/sites-available/consultacnpj

# Ativar site
sudo ln -s /etc/nginx/sites-available/consultacnpj /etc/nginx/sites-enabled/

# Remover site padrão (se existir)
sudo rm /etc/nginx/sites-enabled/default

# Testar configuração
sudo nginx -t

# Reiniciar Nginx
sudo systemctl restart nginx
```

## Passo 11: Configurar SSL (HTTPS)

```bash
# Instalar Certbot
sudo apt install certbot python3-certbot-nginx

# Obter certificado SSL
sudo certbot --nginx -d consultacnpj.italommf.com.br

# O Certbot atualizará automaticamente o Nginx
# Renovação automática já está configurada
```

Após configurar SSL, atualize o `.env`:
```
SECURE_SSL_REDIRECT=True
```

E reinicie o serviço:
```bash
sudo systemctl restart cnpj-api
```

## Passo 12: Configurar Firewall

```bash
sudo ufw allow 22/tcp
sudo ufw allow 80/tcp
sudo ufw allow 443/tcp
sudo ufw enable
```

## Comandos Úteis

```bash
# Ver logs do Gunicorn
sudo journalctl -u cnpj-api -f
sudo tail -f /var/log/gunicorn/error.log

# Reiniciar API
sudo systemctl restart cnpj-api

# Reiniciar Nginx
sudo systemctl restart nginx

# Ver status dos serviços
sudo systemctl status cnpj-api
sudo systemctl status nginx
sudo systemctl status postgresql
```

## URLs Finais

- API: `https://consultacnpj.italommf.com.br/api/`
- Admin: `https://consultacnpj.italommf.com.br/admin/`
- Endpoints:
  - `https://consultacnpj.italommf.com.br/api/companies/cnpj/{cnpj}/`
  - `https://consultacnpj.italommf.com.br/api/companies/search/`

## Troubleshooting

### Erro 502 Bad Gateway
- Verificar se Gunicorn está rodando: `sudo systemctl status cnpj-api`
- Verificar logs: `sudo journalctl -u cnpj-api -n 50`

### Erro de permissão
- Verificar permissões: `sudo chown -R www-data:www-data /var/www/cnpj_api/backend`
- Verificar logs do Nginx: `sudo tail -f /var/log/nginx/error.log`

### CORS bloqueando requisições
- Verificar `.env`: `CORS_ALLOW_ALL_ORIGINS=True` (temporário) ou configurar `CORS_ALLOWED_ORIGINS`
- Reiniciar API: `sudo systemctl restart cnpj-api`

