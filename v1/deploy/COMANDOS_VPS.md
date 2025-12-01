# Comandos para Executar na VPS - Deploy API CNPJ

## Passo 1: Atualizar Sistema e Instalar Dependências

```bash
sudo apt update && sudo apt upgrade -y
sudo apt install -y python3 python3-pip python3-venv postgresql postgresql-contrib nginx git curl
```

## Passo 2: Configurar PostgreSQL

```bash
sudo -u postgres psql
```

No prompt do PostgreSQL, execute:
```sql
CREATE DATABASE cnpjdb;
CREATE USER cnpj_user WITH PASSWORD 'SUA_SENHA_FORTE_AQUI';
ALTER ROLE cnpj_user SET client_encoding TO 'utf8';
ALTER ROLE cnpj_user SET default_transaction_isolation TO 'read committed';
ALTER ROLE cnpj_user SET timezone TO 'UTC';
GRANT ALL PRIVILEGES ON DATABASE cnpjdb TO cnpj_user;
\q
```

## Passo 3: Criar Diretórios

```bash
sudo mkdir -p /var/www/cnpj_api
sudo mkdir -p /var/log/gunicorn
sudo mkdir -p /var/run/gunicorn
sudo mkdir -p /var/log/django
sudo chown $USER:$USER /var/www/cnpj_api
sudo chown www-data:www-data /var/log/gunicorn
sudo chown www-data:www-data /var/run/gunicorn
sudo chown www-data:www-data /var/log/django
```

## Passo 4: Clonar Repositório

```bash
cd /var/www/cnpj_api
git clone https://github.com/italommf/ConsultaCNPJ.git .
```

## Passo 5: Configurar Ambiente Virtual

```bash
cd /var/www/cnpj_api/backend
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

## Passo 6: Configurar Variáveis de Ambiente

```bash
cd /var/www/cnpj_api/backend
cp env.example .env
nano .env
```

Edite o arquivo `.env` com:
```
SECRET_KEY=<gerar_comando_abaixo>
DEBUG=False
DJANGO_ENV=production
ALLOWED_HOSTS=consultacnpj.italommf.com.br

DB_NAME=cnpjdb
DB_USER=cnpj_user
DB_PASSWORD=SUA_SENHA_FORTE_AQUI
DB_HOST=localhost
DB_PORT=5432

CORS_ALLOW_ALL_ORIGINS=False
CORS_ALLOWED_ORIGINS=https://consultacnpj.italommf.com.br
CSRF_TRUSTED_ORIGINS=https://consultacnpj.italommf.com.br
SECURE_SSL_REDIRECT=True
```

## Passo 7: Gerar SECRET_KEY

```bash
cd /var/www/cnpj_api/backend
source venv/bin/activate
python manage.py shell
```

No shell do Django:
```python
from django.core.management.utils import get_random_secret_key
print(get_random_secret_key())
```
Copie o valor e cole no arquivo `.env` na variável `SECRET_KEY`.

## Passo 8: Executar Migrações

```bash
cd /var/www/cnpj_api/backend
source venv/bin/activate
python manage.py migrate
```

## Passo 9: Coletar Arquivos Estáticos

```bash
cd /var/www/cnpj_api/backend
source venv/bin/activate
python manage.py collectstatic --noinput
```

## Passo 10: Criar Superusuário (Opcional)

```bash
cd /var/www/cnpj_api/backend
source venv/bin/activate
python manage.py createsuperuser
```

## Passo 11: Configurar Gunicorn

```bash
sudo cp /var/www/cnpj_api/deploy/cnpj-api.service /etc/systemd/system/
sudo chown www-data:www-data -R /var/www/cnpj_api/backend
sudo systemctl daemon-reload
sudo systemctl start cnpj-api
sudo systemctl enable cnpj-api
sudo systemctl status cnpj-api
```

## Passo 12: Configurar Nginx

```bash
sudo cp /var/www/cnpj_api/deploy/nginx_consultacnpj.conf /etc/nginx/sites-available/consultacnpj
sudo ln -s /etc/nginx/sites-available/consultacnpj /etc/nginx/sites-enabled/
sudo rm -f /etc/nginx/sites-enabled/default
sudo nginx -t
sudo systemctl restart nginx
```

## Passo 13: Configurar SSL (HTTPS)

```bash
sudo apt install certbot python3-certbot-nginx
sudo certbot --nginx -d consultacnpj.italommf.com.br
```

## Passo 14: Configurar Firewall

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
sudo tail -f /var/log/django/cnpj_api.log

# Reiniciar serviços
sudo systemctl restart cnpj-api
sudo systemctl restart nginx

# Ver status
sudo systemctl status cnpj-api
sudo systemctl status nginx
sudo systemctl status postgresql

# Testar API
curl http://localhost:8000/api/
```

## Importar Dados do Banco (Se necessário)

Se você tem um dump do banco local:

```bash
# No servidor local (se tiver acesso)
pg_dump -U usuario -h localhost cnpjdb > backup.sql

# Na VPS
psql -U cnpj_user -d cnpjdb < backup.sql
```

Ou execute os scripts de importação:
```bash
cd /var/www/cnpj_api/scripts
source ../backend/venv/bin/activate
python main.py
```

