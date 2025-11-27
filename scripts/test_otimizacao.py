"""
Script de teste para validar otimizações antes de implementar
Testa criação de tabelas otimizadas e conversão de dados
"""
import psycopg2
import sys
import os
from pathlib import Path

# Configurações
BASE_DIR = Path(os.environ.get('CNPJ_BASE_DIR', "/var/www/cnpj_api"))
DB_HOST = os.environ.get('DB_HOST', "localhost")
DB_PORT = os.environ.get('DB_PORT', "5432")
DB_USER = os.environ.get('DB_USER', "cnpj_user")
DB_NAME = os.environ.get('DB_NAME', "cnpjdb")
DB_PASSWORD = os.environ.get('DB_PASSWORD', 'Hefesto@45')

def get_connection():
    return psycopg2.connect(
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME
    )

def test_table_creation():
    """Testa criação de tabela otimizada"""
    print("="*80)
    print("TESTE: Criação de Tabela Otimizada")
    print("="*80)
    
    conn = get_connection()
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    
    # Criar tabela de teste
    cur.execute("DROP TABLE IF EXISTS test_otimizado;")
    cur.execute("""
        CREATE TABLE test_otimizado (
            id serial PRIMARY KEY,
            nome_fantasia varchar(200),
            logradouro varchar(200),
            bairro varchar(100),
            email varchar(150),
            ddd char(2),
            telefone varchar(15),
            data_teste date
        );
    """)
    
    # Inserir dados de teste
    cur.execute("""
        INSERT INTO test_otimizado (nome_fantasia, logradouro, bairro, email, ddd, telefone, data_teste)
        VALUES 
            ('EMPRESA TESTE LTDA', 'RUA TESTE', 'CENTRO', 'teste@teste.com', '11', '12345678', '2020-01-01'),
            ('OUTRA EMPRESA', 'AV TESTE', 'JARDIM', 'outro@teste.com', '21', '98765432', '2021-05-15');
    """)
    
    # Verificar tamanho
    cur.execute("""
        SELECT pg_size_pretty(pg_total_relation_size('test_otimizado')) as tamanho;
    """)
    tamanho = cur.fetchone()[0]
    print(f"Tamanho da tabela otimizada: {tamanho}")
    
    # Testar formatação de data
    cur.execute("""
        SELECT 
            data_teste,
            TO_CHAR(data_teste, 'DD/MM/YYYY') as data_formatada
        FROM test_otimizado
        LIMIT 1;
    """)
    resultado = cur.fetchone()
    print(f"Data original: {resultado[0]}")
    print(f"Data formatada: {resultado[1]}")
    
    # Limpar
    cur.execute("DROP TABLE test_otimizado;")
    
    cur.close()
    conn.close()
    
    print("\n✓ Teste de criação de tabela otimizada: OK")
    return True

def test_data_conversion():
    """Testa conversão de dados TEXT para tipos otimizados"""
    print("\n" + "="*80)
    print("TESTE: Conversão de Dados")
    print("="*80)
    
    conn = get_connection()
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    
    # Criar tabela com TEXT
    cur.execute("DROP TABLE IF EXISTS test_conversao;")
    cur.execute("""
        CREATE TABLE test_conversao (
            id serial PRIMARY KEY,
            nome text,
            data_text text,
            telefone text
        );
    """)
    
    # Inserir dados
    cur.execute("""
        INSERT INTO test_conversao (nome, data_text, telefone)
        VALUES 
            ('EMPRESA TESTE', '01/01/2020', '11987654321'),
            ('OUTRA EMPRESA', '15/05/2021', '21987654321');
    """)
    
    # Converter tipos
    print("Convertendo nome text -> varchar(200)...")
    cur.execute("""
        ALTER TABLE test_conversao 
        ALTER COLUMN nome TYPE varchar(200) 
        USING LEFT(TRIM(nome), 200);
    """)
    
    print("Convertendo telefone text -> varchar(15)...")
    cur.execute("""
        ALTER TABLE test_conversao 
        ALTER COLUMN telefone TYPE varchar(15) 
        USING LEFT(TRIM(telefone), 15);
    """)
    
    print("Convertendo data_text text -> date...")
    cur.execute("""
        ALTER TABLE test_conversao 
        ALTER COLUMN data_text TYPE date 
        USING CASE 
            WHEN data_text IS NULL OR TRIM(data_text) = '' THEN NULL
            WHEN data_text ~ '^\\d{2}/\\d{2}/\\d{4}$' THEN 
                TO_DATE(data_text, 'DD/MM/YYYY')
            ELSE NULL
        END;
    """)
    
    # Verificar tamanho antes e depois
    cur.execute("SELECT pg_total_relation_size('test_conversao');")
    tamanho_depois = cur.fetchone()[0]
    print(f"Tamanho após conversão: {pg_size_pretty(tamanho_depois)}")
    
    # Testar formatação
    cur.execute("""
        SELECT 
            nome,
            data_text,
            TO_CHAR(data_text, 'DD/MM/YYYY') as data_formatada,
            telefone
        FROM test_conversao;
    """)
    
    for row in cur.fetchall():
        print(f"  Nome: {row[0]}, Data: {row[2]}, Telefone: {row[3]}")
    
    # Limpar
    cur.execute("DROP TABLE test_conversao;")
    
    cur.close()
    conn.close()
    
    print("\n✓ Teste de conversão de dados: OK")
    return True

def test_index_performance():
    """Testa performance de índices"""
    print("\n" + "="*80)
    print("TESTE: Performance de Índices")
    print("="*80)
    
    conn = get_connection()
    cur = conn.cursor()
    
    # Verificar se há dados
    cur.execute("SELECT COUNT(*) FROM estabelecimentos LIMIT 1;")
    count = cur.fetchone()[0]
    
    if count == 0:
        print("⚠ Banco vazio, pulando teste de performance")
        cur.close()
        conn.close()
        return True
    
    # Testar busca sem índice (se não existir)
    import time
    
    print("Testando busca por CNPJ...")
    start = time.time()
    cur.execute("SELECT COUNT(*) FROM estabelecimentos WHERE cnpj = '00000000000000';")
    elapsed = time.time() - start
    print(f"  Tempo: {elapsed*1000:.2f}ms")
    
    print("Testando busca por UF...")
    start = time.time()
    cur.execute("SELECT COUNT(*) FROM estabelecimentos WHERE uf = 'SP' LIMIT 1000;")
    elapsed = time.time() - start
    print(f"  Tempo: {elapsed*1000:.2f}ms")
    
    # Verificar índices existentes
    cur.execute("""
        SELECT indexname, indexdef 
        FROM pg_indexes 
        WHERE tablename = 'estabelecimentos' 
        LIMIT 5;
    """)
    
    print("\nÍndices existentes em estabelecimentos:")
    for row in cur.fetchall():
        print(f"  - {row[0]}")
    
    cur.close()
    conn.close()
    
    print("\n✓ Teste de performance: OK")
    return True

def main():
    print("\n" + "="*80)
    print("TESTES DE OTIMIZAÇÃO - BANCO CNPJ")
    print("="*80)
    
    try:
        test_table_creation()
        test_data_conversion()
        test_index_performance()
        
        print("\n" + "="*80)
        print("✓ TODOS OS TESTES PASSARAM!")
        print("="*80)
        print("\nPróximos passos:")
        print("1. Implementar otimizações no page.py")
        print("2. Testar importação completa")
        print("3. Validar API")
        
    except Exception as e:
        print(f"\n✗ ERRO NOS TESTES: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()

