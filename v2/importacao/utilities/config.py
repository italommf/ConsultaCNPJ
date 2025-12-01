"""Funções auxiliares para configuração e diretórios"""
import os
import sys
from pathlib import Path
from typing import Tuple


def garantir_encoding_windows():
    """Garante encoding UTF-8 no Windows"""
    if sys.platform == "win32":
        try:
            if sys.stdout.encoding != "utf-8":
                sys.stdout.reconfigure(encoding="utf-8")
            if sys.stderr.encoding != "utf-8":
                sys.stderr.reconfigure(encoding="utf-8")
        except Exception:
            pass


def resolver_diretorios(base_dir: Path) -> Tuple[Path, Path]:
    """Resolve diretórios de dados e downloads dentro da pasta importacao"""
    # base_dir deve ser a pasta importacao
    importacao_dir = base_dir.resolve()
    
    # Obter caminhos das variáveis de ambiente ou usar padrão
    data_path = os.getenv("DATA_DIR", "data")
    downloads_path = os.getenv("DOWNLOADS_DIR", "downloads")
    
    # Resolver caminhos
    data_dir = _resolver_caminho(data_path, importacao_dir)
    downloads_dir = _resolver_caminho(downloads_path, importacao_dir)

    data_dir.mkdir(parents=True, exist_ok=True)
    downloads_dir.mkdir(parents=True, exist_ok=True)
    return data_dir.resolve(), downloads_dir.resolve()


def _resolver_caminho(path_str: str, base_dir: Path) -> Path:
    """Resolve um caminho relativo ou absoluto baseado no diretório base"""
    path = Path(path_str)
    
    # Se for caminho absoluto, usar diretamente
    if path.is_absolute():
        return path
    
    # Se começar com ../, subir um nível do base_dir
    if path_str.startswith("../"):
        return (base_dir.parent / path_str[3:]).resolve()
    
    # Caso contrário, caminho relativo ao base_dir
    return (base_dir / path_str).resolve()

