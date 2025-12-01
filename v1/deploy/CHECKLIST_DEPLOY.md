# Checklist de Deploy - API CNPJ na Hostinger

## ✅ Verificações Pré-Deploy

### 1. Arquivos de Configuração
- [x] `requirements.txt` existe e está atualizado
- [x] `env.example` existe com todas as variáveis necessárias
- [x] Configurações de produção em `settings/production.py`
- [x] Arquivo `.gitignore` configurado (não commitar `.env`)

### 2. Configurações de Servidor
- [x] `gunicorn_config.py` configurado
- [x] `cnpj-api.service` (systemd) configurado
- [x] `nginx_consultacnpj.conf` configurado
- [x] `README_DEPLOY.md` com instruções completas

### 3. Segurança
- [x] `DEBUG=False` em produção
- [x] `SECRET_KEY` será gerada no servidor
- [x] `ALLOWED_HOSTS` configurado
- [x] SSL/HTTPS configurado (via Certbot)
- [x] CORS configurado para produção
- [x] Headers de segurança configurados

### 4. Banco de Dados
- [ ] Backup do banco local criado
- [ ] Scripts de importação testados
- [ ] Credenciais do PostgreSQL preparadas

### 5. Variáveis de Ambiente Necessárias
Verificar se todas estas variáveis estarão no `.env` do servidor:

```
SECRET_KEY=<gerar no servidor>
DEBUG=False
DJANGO_ENV=production
ALLOWED_HOSTS=consultacnpj.italommf.com.br

DB_NAME=cnpjdb
DB_USER=cnpj_user
DB_PASSWORD=<senha forte>
DB_HOST=localhost
DB_PORT=5432

CORS_ALLOW_ALL_ORIGINS=False
CORS_ALLOWED_ORIGINS=https://consultacnpj.italommf.com.br
CSRF_TRUSTED_ORIGINS=https://consultacnpj.italommf.com.br
SECURE_SSL_REDIRECT=True
```

## ⚠️ Ações Necessárias no Servidor

### 1. Criar Diretórios de Log
```bash
sudo mkdir -p /var/log/django
sudo chown www-data:www-data /var/log/django
```

### 2. Verificar Permissões
```bash
sudo chown -R www-data:www-data /var/www/cnpj_api/backend
sudo chmod -R 755 /var/www/cnpj_api/backend
```

### 3. Gerar SECRET_KEY
```bash
cd /var/www/cnpj_api/backend
source venv/bin/activate
python manage.py shell
# No shell do Django:
from django.core.management.utils import get_random_secret_key
print(get_random_secret_key())
# Copiar o valor e colar no .env
```

### 4. Coletar Arquivos Estáticos
```bash
cd /var/www/cnpj_api/backend
source venv/bin/activate
python manage.py collectstatic --noinput
```

### 5. Executar Migrações
```bash
python manage.py migrate
```

## 📋 Checklist de Deploy

### Fase 1: Preparação
- [ ] Servidor VPS configurado
- [ ] Domínio apontando para o IP
- [ ] Acesso SSH configurado
- [ ] PostgreSQL instalado e configurado
- [ ] Python 3 e pip instalados

### Fase 2: Instalação
- [ ] Código enviado para `/var/www/cnpj_api/`
- [ ] Ambiente virtual criado
- [ ] Dependências instaladas (`pip install -r requirements.txt`)
- [ ] Arquivo `.env` criado e configurado
- [ ] Diretórios de log criados

### Fase 3: Banco de Dados
- [ ] Banco de dados criado
- [ ] Usuário PostgreSQL criado
- [ ] Dados importados ou dump restaurado
- [ ] Migrações executadas
- [ ] Superusuário criado (se necessário)

### Fase 4: Django
- [ ] `collectstatic` executado
- [ ] Permissões configuradas
- [ ] Testes básicos executados

### Fase 5: Gunicorn
- [ ] Arquivo de serviço copiado
- [ ] Serviço iniciado e habilitado
- [ ] Status verificado (`systemctl status cnpj-api`)
- [ ] Logs verificados

### Fase 6: Nginx
- [ ] Configuração copiada
- [ ] Site habilitado
- [ ] Configuração testada (`nginx -t`)
- [ ] Nginx reiniciado

### Fase 7: SSL
- [ ] Certbot instalado
- [ ] Certificado SSL obtido
- [ ] Nginx atualizado automaticamente
- [ ] HTTPS funcionando

### Fase 8: Testes
- [ ] API acessível em `https://consultacnpj.italommf.com.br/api/`
- [ ] Admin acessível em `https://consultacnpj.italommf.com.br/admin/`
- [ ] Endpoints testados
- [ ] CORS funcionando
- [ ] Logs sendo gerados corretamente

## 🔍 Comandos de Verificação

```bash
# Status dos serviços
sudo systemctl status cnpj-api
sudo systemctl status nginx
sudo systemctl status postgresql

# Logs
sudo journalctl -u cnpj-api -f
sudo tail -f /var/log/django/cnpj_api.log
sudo tail -f /var/log/nginx/error.log

# Testar API
curl https://consultacnpj.italommf.com.br/api/
```

## ⚠️ Problemas Conhecidos e Soluções

### Erro 502 Bad Gateway
- Verificar se Gunicorn está rodando
- Verificar logs do Gunicorn
- Verificar se a porta 8000 está acessível

### Erro de Permissão
- Verificar ownership: `sudo chown -R www-data:www-data /var/www/cnpj_api/backend`
- Verificar permissões dos diretórios de log

### CORS bloqueando
- Verificar variável `CORS_ALLOWED_ORIGINS` no `.env`
- Reiniciar serviço: `sudo systemctl restart cnpj-api`

### Erro de Database
- Verificar credenciais no `.env`
- Verificar se PostgreSQL está rodando
- Verificar se o banco existe e tem dados

## 📝 Notas Importantes

1. **Nunca commitar o arquivo `.env`** - ele contém informações sensíveis
2. **Sempre usar `DJANGO_ENV=production`** no servidor
3. **Backup regular do banco de dados** é essencial
4. **Monitorar logs** regularmente para detectar problemas
5. **Atualizar dependências** periodicamente para segurança

