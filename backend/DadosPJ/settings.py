"""
Django settings - seleciona automaticamente o ambiente baseado na variável DJANGO_ENV
ou usa development como padrão.

Para usar:
- Desenvolvimento: export DJANGO_ENV=development (ou não definir, é o padrão)
- Produção: export DJANGO_ENV=production
"""

import os
from decouple import config

# Determina qual ambiente usar baseado na variável de ambiente
# Padrão: development
ENVIRONMENT = config('DJANGO_ENV', default='development')

if ENVIRONMENT == 'production':
    from .settings.production import *
else:
    from .settings.development import *
