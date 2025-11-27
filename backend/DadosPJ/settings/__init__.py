"""
Settings package - importa as configurações baseadas no ambiente
"""

import os
from decouple import config

# Determina qual ambiente usar baseado na variável de ambiente
# Padrão: development
ENVIRONMENT = config('DJANGO_ENV', default='development')

if ENVIRONMENT == 'production':
    from .production import *
else:
    from .development import *


