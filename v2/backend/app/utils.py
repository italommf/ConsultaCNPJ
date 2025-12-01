"""Utilitários para a API"""
from typing import Any, Optional


def to_str(value: Any) -> Optional[str]:
    """
    Converte valor do ClickHouse para string.
    ClickHouse pode retornar bytes ou strings dependendo da configuração.
    """
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.decode('utf-8', errors='replace')
    return str(value)


def format_date(date_str: Optional[str]) -> Optional[str]:
    """Formata data do ClickHouse (YYYY-MM-DD) para DD/MM/YYYY"""
    if not date_str or date_str == "0000-00-00" or date_str == "" or date_str == "1970-01-01":
        return None
    try:
        parts = date_str.split("-")
        if len(parts) == 3:
            return f"{parts[2]}/{parts[1]}/{parts[0]}"
    except:
        pass
    return None


def format_capital_social(cents: Optional[int]) -> Optional[float]:
    """Converte capital_social de centavos (UInt64) para reais (float)"""
    if cents is None or cents == 0:
        return None
    return cents / 100.0




