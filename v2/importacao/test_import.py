"""Script de teste para importação de estabelecimentos"""
from import_csv import ClickHouseImporter
from clickhouse_driver import Client
from pathlib import Path

# Conectar ao ClickHouse
c = Client(host='localhost', port=9000, user='default', database='cnpj')
imp = ClickHouseImporter(c, batch_size=100)

# Encontrar arquivo de teste
arquivos = list(Path('../data/estabelecimentos').glob('*.ESTABELE*'))
if arquivos:
    print(f"Testando importação de {arquivos[0].name}...")
    try:
        imp.importar_estabelecimentos(arquivos[0])
        print("✓ Sucesso!")
    except Exception as e:
        print(f"✗ Erro: {e}")
        import traceback
        traceback.print_exc()
else:
    print("Nenhum arquivo encontrado")






