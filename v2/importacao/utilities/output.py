"""Funções para formatação de saída"""
import time
from typing import Dict, List

from clickhouse_driver import Client

from utilities.clickhouse import obter_estatisticas, obter_tamanho_banco


def print_header(text: str):
    """Imprime cabeçalho formatado"""
    print("\n" + "=" * 80)
    print(f"  {text}")
    print("=" * 80)


def print_step(current: int, total: int, text: str):
    """Imprime passo formatado"""
    print(f"\n[{current}/{total}] {text}")
    print("-" * 80)


def imprimir_resumo_contagens(contagens_csv: Dict[str, dict]) -> None:
    """Imprime resumo de contagens de linhas"""
    print("\nResumo de linhas por tabela:")
    for tabela, dados in contagens_csv.items():
        if dados["validas"] > 0:
            print(
                f"  {tabela:20s} | Válidas: {dados['validas']:>15,} | "
                f"Problemáticas: {dados['problematicas']:>10,}"
            )


def imprimir_estatisticas_finais(client: Client, database: str, inicio: float) -> None:
    """Imprime estatísticas finais do processamento"""
    print("\n" + "=" * 80)
    print("ESTATÍSTICAS FINAIS")
    print("=" * 80)

    tabelas = [
        "empresas",
        "estabelecimentos",
        "socios",
        "simples",
        "cnaes",
        "motivos",
        "municipios",
        "naturezas",
        "paises",
        "qualificacoes",
    ]
    stats = obter_estatisticas(client, tabelas)
    print("\nRegistros no banco de dados:")
    for tabela, count in stats.items():
        print(f"  {tabela:20s}: {count:>15,}")

    tamanho = obter_tamanho_banco(client, database)
    if tamanho:
        print(f"\nTamanho total do banco: {tamanho}")

    tempo_total = time.time() - inicio
    horas = int(tempo_total // 3600)
    minutos = int((tempo_total % 3600) // 60)
    segundos = int(tempo_total % 60)
    print(f"\nTempo total de processamento: {horas:02d}:{minutos:02d}:{segundos:02d}")
    print("\n" + "=" * 80)
    print("✓ PROCESSO FINALIZADO COM SUCESSO!")
    print("=" * 80)




