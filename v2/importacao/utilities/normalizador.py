"""Funções de normalização de dados para importação"""
from typing import Optional
from datetime import datetime


def normalizar_cnpj(cnpj_basico: str, cnpj_ordem: str, cnpj_dv: str) -> str:
    """
    Gera CNPJ completo (14 dígitos) a partir dos componentes.
    Preenche com zeros à esquerda se necessário.
    """
    cnpj_basico = str(cnpj_basico).strip().zfill(8)
    cnpj_ordem = str(cnpj_ordem).strip().zfill(4)
    cnpj_dv = str(cnpj_dv).strip().zfill(2)
    return f"{cnpj_basico}{cnpj_ordem}{cnpj_dv}"


def normalizar_data(data_str: Optional[str]) -> Optional[str]:
    """
    Normaliza data de YYYYMMDD para YYYY-MM-DD (formato Date do ClickHouse).
    Retorna None se data inválida.
    """
    if not data_str or data_str.strip() == "" or data_str == "00000000":
        return None
    
    data_str = data_str.strip()
    
    # Se já está no formato YYYY-MM-DD, retornar
    if len(data_str) == 10 and data_str[4] == "-" and data_str[7] == "-":
        try:
            datetime.strptime(data_str, "%Y-%m-%d")
            return data_str
        except:
            return None
    
    # Formato YYYYMMDD
    if len(data_str) == 8 and data_str.isdigit():
        try:
            year = int(data_str[0:4])
            month = int(data_str[4:6])
            day = int(data_str[6:8])
            
            # Validar range
            if year < 1900 or year > 2100:
                return None
            if month < 1 or month > 12:
                return None
            if day < 1 or day > 31:
                return None
            
            # Validar data completa
            try:
                datetime(year, month, day)
                return f"{year:04d}-{month:02d}-{day:02d}"
            except ValueError:
                return None
        except (ValueError, IndexError):
            return None
    
    return None


def normalizar_capital_social(valor: Optional[str]) -> Optional[int]:
    """
    Converte capital_social de string para UInt64 (centavos).
    Exemplo: "1000.50" -> 100050
    """
    if not valor or valor.strip() == "":
        return None
    
    try:
        # Remover espaços e caracteres não numéricos exceto ponto
        valor_clean = valor.strip().replace(",", ".")
        
        # Tentar converter para float
        valor_float = float(valor_clean)
        
        # Converter para centavos (UInt64)
        centavos = int(valor_float * 100)
        
        # Validar range (máximo ~9.22 quintilhões de centavos)
        if centavos < 0:
            return None
        
        return centavos
    except (ValueError, OverflowError):
        return None


def limpar_string(valor: Optional[str], max_length: Optional[int] = None) -> Optional[str]:
    """
    Limpa string: remove espaços, caracteres nulos, etc.
    Opcionalmente trunca para max_length.
    """
    if not valor:
        return None
    
    # Remover bytes nulos
    valor = valor.replace("\x00", "")
    
    # Strip
    valor = valor.strip()
    
    if valor == "":
        return None
    
    # Truncar se necessário
    if max_length and len(valor) > max_length:
        valor = valor[:max_length]
    
    return valor


def normalizar_codigo(valor: Optional[str], tamanho: int) -> Optional[str]:
    """
    Normaliza código (char fixo): preenche com zeros à esquerda ou trunca.
    """
    if not valor:
        return None
    
    valor = str(valor).strip()
    
    if valor == "":
        return None
    
    # Preencher com zeros à esquerda
    valor = valor.zfill(tamanho)
    
    # Truncar se necessário
    if len(valor) > tamanho:
        valor = valor[:tamanho]
    
    return valor




