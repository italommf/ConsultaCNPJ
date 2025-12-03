"""Script para verificar encoding e acentos nos dados"""
import sys
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import chardet

# Adicionar path
BASE_DIR = Path(__file__).parent
sys.path.insert(0, str(BASE_DIR))

try:
    import polars as pl
    POLARS_AVAILABLE = True
except ImportError:
    POLARS_AVAILABLE = False
    print("⚠ Polars não está instalado. Instale com: pip install polars")

try:
    from clickhouse_driver import Client
    from dotenv import load_dotenv
    import os
    CLICKHOUSE_AVAILABLE = True
except ImportError:
    CLICKHOUSE_AVAILABLE = False
    print("⚠ ClickHouse driver não está instalado")

# Caracteres com acento para verificar
ACENTOS = 'áéíóúâêôãõçÁÉÍÓÚÂÊÔÃÕÇ'
CARACTERES_PROBLEMATICOS = ['', '\ufffd', '']


def detectar_encoding_arquivo(arquivo: Path) -> Dict[str, any]:
    """Detecta o encoding de um arquivo usando chardet"""
    try:
        with open(arquivo, 'rb') as f:
            raw_data = f.read(10000)  # Ler primeiros 10KB
            resultado = chardet.detect(raw_data)
            return resultado
    except Exception as e:
        return {'encoding': None, 'confidence': 0, 'error': str(e)}


def verificar_acentos_em_string(texto: str) -> Dict[str, any]:
    """Verifica se uma string tem acentos e caracteres problemáticos"""
    if not texto:
        return {
            'tem_acentos': False,
            'tem_problematicos': False,
            'acentos_encontrados': [],
            'problematicos_encontrados': []
        }
    
    acentos_encontrados = [c for c in texto if c in ACENTOS]
    problematicos_encontrados = [c for c in texto if c in CARACTERES_PROBLEMATICOS]
    
    return {
        'tem_acentos': len(acentos_encontrados) > 0,
        'tem_problematicos': len(problematicos_encontrados) > 0,
        'acentos_encontrados': list(set(acentos_encontrados)),
        'problematicos_encontrados': list(set(problematicos_encontrados)),
        'texto': texto[:100]  # Primeiros 100 caracteres
    }


def testar_encoding_polars(arquivo: Path, encodings: List[str] = None) -> Dict[str, any]:
    """Testa diferentes encodings com Polars e verifica acentos"""
    if not POLARS_AVAILABLE:
        return {'erro': 'Polars não disponível'}
    
    if encodings is None:
        encodings = ['utf-8', 'latin-1', 'cp1252', 'utf8-lossy']
    
    resultados = {}
    
    for encoding in encodings:
        try:
            df = pl.read_csv(
                arquivo,
                separator=';',
                has_header=False,
                infer_schema_length=0,
                ignore_errors=True,
                encoding=encoding,
            )
            
            # Pegar primeira linha com descrição (geralmente coluna 1)
            if len(df.columns) >= 2:
                primeira_linha = df.row(0)
                if len(primeira_linha) >= 2:
                    descricao = str(primeira_linha[1])
                    verificacao = verificar_acentos_em_string(descricao)
                    
                    resultados[encoding] = {
                        'sucesso': True,
                        'linhas': len(df),
                        'colunas': len(df.columns),
                        'verificacao': verificacao,
                        'primeira_descricao': descricao[:200]
                    }
                else:
                    resultados[encoding] = {
                        'sucesso': True,
                        'linhas': len(df),
                        'colunas': len(df.columns),
                        'erro': 'Sem coluna de descrição'
                    }
            else:
                resultados[encoding] = {
                    'sucesso': True,
                    'linhas': len(df),
                    'colunas': len(df.columns),
                    'erro': 'Poucas colunas'
                }
        except Exception as e:
            resultados[encoding] = {
                'sucesso': False,
                'erro': str(e)
            }
    
    return resultados


def verificar_dados_clickhouse(tabela: str, client: Client, limite: int = 10) -> Dict[str, any]:
    """Verifica dados no ClickHouse para ver se têm acentos"""
    try:
        # Buscar algumas linhas com descrições
        query = f"""
            SELECT * 
            FROM {tabela} 
            LIMIT {limite}
        """
        rows = client.execute(query)
        
        resultados = []
        for row in rows:
            # Assumir que a última coluna ou segunda coluna é a descrição
            if len(row) >= 2:
                descricao = str(row[1]) if len(row) > 1 else str(row[-1])
                verificacao = verificar_acentos_em_string(descricao)
                resultados.append({
                    'dados': row[:3],  # Primeiras 3 colunas
                    'descricao': descricao[:200],
                    'verificacao': verificacao
                })
        
        return {
            'sucesso': True,
            'total_linhas': len(rows),
            'resultados': resultados
        }
    except Exception as e:
        return {
            'sucesso': False,
            'erro': str(e)
        }


def conectar_clickhouse() -> Optional[Client]:
    """Conecta ao ClickHouse usando variáveis de ambiente"""
    if not CLICKHOUSE_AVAILABLE:
        return None
    
    try:
        # Tentar carregar .env do diretório atual ou do backend
        env_file = BASE_DIR / '.env'
        if not env_file.exists():
            env_file = BASE_DIR.parent / 'backend' / '.env'
        
        if env_file.exists():
            load_dotenv(env_file)
        
        client = Client(
            host=os.getenv('CLICKHOUSE_HOST', 'localhost'),
            port=int(os.getenv('CLICKHOUSE_PORT', 9000)),
            user=os.getenv('CLICKHOUSE_USER', 'default'),
            password=os.getenv('CLICKHOUSE_PASSWORD', ''),
            database=os.getenv('CLICKHOUSE_DATABASE', 'cnpj')
        )
        return client
    except Exception as e:
        print(f"⚠ Erro ao conectar ao ClickHouse: {e}")
        return None


def main():
    """Função principal"""
    print("="*80)
    print("VERIFICAÇÃO DE ENCODING E ACENTOS NOS DADOS")
    print("="*80)
    
    # 1. Verificar arquivos CSV originais
    print("\n" + "="*80)
    print("1. VERIFICANDO ARQUIVOS CSV ORIGINAIS")
    print("="*80)
    
    data_dir = BASE_DIR / "data" / "dominio"
    arquivos_teste = [
        data_dir / "F.K03200$Z.D51108.CNAECSV",
        data_dir / "F.K03200$Z.D51108.QUALSCSV",
        data_dir / "F.K03200$Z.D51108.NATJUCSV",
    ]
    
    for arquivo in arquivos_teste:
        if not arquivo.exists():
            print(f"\n⚠ Arquivo não encontrado: {arquivo.name}")
            continue
        
        print(f"\n{'─'*80}")
        print(f"Arquivo: {arquivo.name}")
        print(f"{'─'*80}")
        
        # Detectar encoding
        deteccao = detectar_encoding_arquivo(arquivo)
        print(f"\n📊 Detecção automática (chardet):")
        print(f"   Encoding: {deteccao.get('encoding', 'N/A')}")
        print(f"   Confiança: {deteccao.get('confidence', 0):.2%}")
        
        # Testar com Polars
        if POLARS_AVAILABLE:
            print(f"\n📊 Teste com Polars:")
            resultados_polars = testar_encoding_polars(arquivo)
            
            for encoding, resultado in resultados_polars.items():
                print(f"\n   Encoding: {encoding}")
                if resultado.get('sucesso'):
                    print(f"   ✓ Sucesso - Linhas: {resultado.get('linhas', 0):,}")
                    if 'verificacao' in resultado:
                        verif = resultado['verificacao']
                        print(f"   Acentos: {'✓ SIM' if verif['tem_acentos'] else '✗ NÃO'}")
                        if verif['tem_acentos']:
                            print(f"   Acentos encontrados: {', '.join(verif['acentos_encontrados'])}")
                        print(f"   Problemáticos: {'⚠ SIM' if verif['tem_problematicos'] else '✓ NÃO'}")
                        if verif['tem_problematicos']:
                            print(f"   Caracteres problemáticos: {verif['problematicos_encontrados']}")
                        print(f"   Exemplo: {verif['texto'][:100]}")
                else:
                    print(f"   ✗ Erro: {resultado.get('erro', 'Desconhecido')}")
    
    # 2. Verificar dados no ClickHouse
    print("\n" + "="*80)
    print("2. VERIFICANDO DADOS NO CLICKHOUSE")
    print("="*80)
    
    client = conectar_clickhouse()
    if client:
        tabelas_teste = ['cnaes', 'qualificacoes', 'naturezas']
        
        for tabela in tabelas_teste:
            print(f"\n{'─'*80}")
            print(f"Tabela: {tabela}")
            print(f"{'─'*80}")
            
            resultado = verificar_dados_clickhouse(tabela, client, limite=5)
            
            if resultado.get('sucesso'):
                print(f"✓ Total de linhas verificadas: {resultado['total_linhas']}")
                
                for i, item in enumerate(resultado['resultados'], 1):
                    print(f"\n   Linha {i}:")
                    verif = item['verificacao']
                    print(f"   Acentos: {'✓ SIM' if verif['tem_acentos'] else '✗ NÃO'}")
                    if verif['tem_acentos']:
                        print(f"   Acentos encontrados: {', '.join(verif['acentos_encontrados'])}")
                    print(f"   Problemáticos: {'⚠ SIM' if verif['tem_problematicos'] else '✓ NÃO'}")
                    if verif['tem_problematicos']:
                        print(f"   Caracteres problemáticos: {verif['problematicos_encontrados']}")
                    print(f"   Descrição: {item['descricao'][:100]}")
            else:
                print(f"✗ Erro: {resultado.get('erro', 'Desconhecido')}")
    else:
        print("\n⚠ Não foi possível conectar ao ClickHouse")
        print("   Configure as variáveis de ambiente ou crie um arquivo .env")
    
    # 3. Resumo e recomendações
    print("\n" + "="*80)
    print("3. RESUMO E RECOMENDAÇÕES")
    print("="*80)
    print("\n✓ Verificação concluída!")
    print("\n📝 Próximos passos:")
    print("   1. Se os arquivos CSV têm acentos mas o ClickHouse não:")
    print("      → O problema está na importação. Verifique os scripts de importação.")
    print("   2. Se os arquivos CSV não têm acentos:")
    print("      → O problema está na origem dos dados (arquivos da Receita Federal).")
    print("   3. Se o encoding detectado não for UTF-8 ou latin-1:")
    print("      → Ajuste a função ler_csv_com_encoding() para usar o encoding correto.")


if __name__ == "__main__":
    try:
        import chardet
    except ImportError:
        print("⚠ chardet não está instalado. Instale com: pip install chardet")
        print("   Continuando sem detecção automática de encoding...")
    
    main()

