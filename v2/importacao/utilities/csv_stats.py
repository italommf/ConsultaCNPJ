"""Funções para análise e contagem de arquivos CSV"""
import logging
from pathlib import Path
from typing import Dict

from utilities.utils import contar_linhas_csv, encontrar_arquivos_csv, validar_arquivo

logger = logging.getLogger(__name__)


def contar_linhas_arquivos(data_dir: Path) -> Dict[str, dict]:
    """Conta linhas válidas e problemáticas de todos os arquivos CSV"""
    logger.info("Contando linhas dos arquivos CSV...")
    contagens = {
        "empresas": {"validas": 0, "problematicas": 0, "arquivos": []},
        "estabelecimentos": {"validas": 0, "problematicas": 0, "arquivos": []},
        "socios": {"validas": 0, "problematicas": 0, "arquivos": []},
        "simples": {"validas": 0, "problematicas": 0, "arquivos": []},
        "cnaes": {"validas": 0, "problematicas": 0, "arquivos": []},
        "motivos": {"validas": 0, "problematicas": 0, "arquivos": []},
        "municipios": {"validas": 0, "problematicas": 0, "arquivos": []},
        "naturezas": {"validas": 0, "problematicas": 0, "arquivos": []},
        "paises": {"validas": 0, "problematicas": 0, "arquivos": []},
        "qualificacoes": {"validas": 0, "problematicas": 0, "arquivos": []},
    }

    mapeamento = {
        "EMPRE": ("empresas", 7),
        "ESTABELE": ("estabelecimentos", 29),
        "SOCIO": ("socios", 11),
        "SIMPLES": ("simples", 7),
        "CNAE": ("cnaes", 2),
        "MOTI": ("motivos", 2),
        "MUNIC": ("municipios", 2),
        "NATJU": ("naturezas", 2),
        "PAIS": ("paises", 2),
        "QUALS": ("qualificacoes", 2),
    }

    for padrao, (tabela, num_cols) in mapeamento.items():
        arquivos = encontrar_arquivos_csv(data_dir, padrao)
        for arquivo in arquivos:
            if not validar_arquivo(arquivo):
                continue
            validas, problematicas = contar_linhas_csv(arquivo, num_cols)
            contagens[tabela]["validas"] += validas
            contagens[tabela]["problematicas"] += problematicas
            contagens[tabela]["arquivos"].append(
                {"nome": arquivo.name, "validas": validas, "problematicas": problematicas}
            )
            logger.info(
                "  %s: %s válidas, %s problemáticas",
                arquivo.name,
                f"{validas:,}",
                f"{problematicas:,}",
            )

    return contagens




