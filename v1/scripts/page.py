import os
import sys
import csv
import time
import zipfile
import shutil
import psycopg2
import concurrent.futures
import getpass
from pathlib import Path
import tempfile
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from datetime import datetime

# =================================================================================
# CONFIGURAÇÕES GERAIS
# =================================================================================
# BASE_DIR pode ser sobrescrito por variável de ambiente ou usa padrão
# Padrão para VPS, mas pode ser sobrescrito por variável de ambiente
BASE_DIR = Path(os.environ.get('CNPJ_BASE_DIR', "/var/www/cnpj_api"))
DOWNLOADS_DIR = BASE_DIR / "downloads"
DATA_DIR = BASE_DIR / "data"

DB_HOST = os.environ.get('DB_HOST', "localhost")
DB_PORT = os.environ.get('DB_PORT', "5432")
DB_USER = os.environ.get('DB_USER', "cnpj_user")
DB_NAME = os.environ.get('DB_NAME', "cnpjdb")

# Mapeamento de arquivos para tabelas e colunas
# Nota: Usamos tipos TEXT para todas as colunas inicialmente para garantir a importação.
# A conversão para tipos reais (Numeric, Date) é feita na etapa de Pós-Processamento.
FILES_TABLES_MAP = {
    "Empresas": {
        "table": "empresas",
        "columns": ["cnpj_basico", "razao_social", "natureza_juridica", "qualificacao_do_responsavel", "capital_social", "porte", "ente_federativo"]
    },
    "Estabelecimentos": {
        "table": "estabelecimentos",
        "columns": [
            "cnpj_basico", "cnpj_ordem", "cnpj_dv", "matriz_filial", "nome_fantasia", "situacao_cadastral", "data_situacao", 
            "motivo_situacao", "cidade_exterior", "pais", "data_inicio", "cnae_fiscal", "cnae_fiscal_secundaria", 
            "tipo_logradouro", "logradouro", "numero", "complemento", "bairro", "cep", "uf", "municipio", 
            "ddd_1", "telefone_1", "ddd_2", "telefone_2", "ddd_fax", "fax", "email", "situacao_especial", "data_situacao_especial"
        ]
    },
    "Socios": {
        "table": "socios",
        "columns": [
            "cnpj_basico", "identificador_socio", "nome_socio", "cnpj_cpf_socio", "qualificacao_socio", "data_entrada_sociedade", 
            "pais", "representante_legal", "nome_representante", "qualificacao_representante", "faixa_etaria"
        ]
    },
    "Simples": {
        "table": "simples",
        "columns": ["cnpj_basico", "opcao_simples", "data_opcao_simples", "data_exclusao_simples", "opcao_mei", "data_opcao_mei", "data_exclusao_mei"]
    },
    "Cnaes": {"table": "cnaes", "columns": ["codigo", "descricao"]},
    "Motivos": {"table": "motivos", "columns": ["codigo", "descricao"]},
    "Municipios": {"table": "municipios", "columns": ["codigo", "descricao"]},
    "Naturezas": {"table": "naturezas", "columns": ["codigo", "descricao"]},
    "Paises": {"table": "paises", "columns": ["codigo", "descricao"]},
    "Qualificacoes": {"table": "qualificacoes", "columns": ["codigo", "descricao"]}
}

# Mapeamento Tabela -> Identificador no Nome do Arquivo (Para Verificação)
TABLE_MATCHERS = {
    "empresas": ["EMPRE", "EMPRESAS"],
    "estabelecimentos": ["ESTABELE", "ESTABELECIMENTOS"],
    "socios": ["SOCIO", "SOCIOS"],
    "simples": ["SIMPLES"],
    "cnaes": ["CNAE"],
    "motivos": ["MOTI"],
    "municipios": ["MUNIC"],
    "naturezas": ["NATJU"],
    "paises": ["PAIS"],
    "qualificacoes": ["QUALS"]
}

# =================================================================================
# CLASSES UTILITÁRIAS
# =================================================================================
class NullByteStripper:
    """Remove bytes nulos (0x00) de arquivos em tempo de leitura."""
    def __init__(self, f):
        self.f = f
    def read(self, size=-1):
        data = self.f.read(size)
        return data.replace('\x00', '')
    def readline(self, size=-1):
        data = self.f.readline(size)
        return data.replace('\x00', '')
    def __iter__(self):
        return self
    def __next__(self):
        data = next(self.f)
        return data.replace('\x00', '')


def normalizar_data(data_str):
    """
    Normaliza uma data do formato YYYYMMDD para DD/MM/YYYY.
    Retorna string vazia (que será NULL) se a data for inválida.
    
    Datas inválidas:
    - Vazias ou só espaços
    - Formato 00000000
    - Ano < 1900 ou > 2100
    - Datas antes de Cristo (contém 'BC')
    """
    if not data_str or not data_str.strip():
        return ""  # Será NULL no banco
    
    data_str = data_str.strip()
    
    # Verificar se contém 'BC' (antes de Cristo)
    if 'BC' in data_str.upper() or data_str.upper().endswith(' BC'):
        return ""  # Será NULL no banco
    
    # Verificar se é formato YYYYMMDD (8 dígitos)
    if len(data_str) == 8 and data_str.isdigit():
        year = int(data_str[:4])
        month = int(data_str[4:6])
        day = int(data_str[6:8])
        
        # Validar ano
        if year == 0 or year < 1900 or year > 2100:
            return ""  # Será NULL no banco
        
        # Validar mês
        if month < 1 or month > 12:
            return ""  # Será NULL no banco
        
        # Validar dia
        if day < 1 or day > 31:
            return ""  # Será NULL no banco
        
        # Tentar validar data completa (ex: 31/02 não existe)
        try:
            datetime(int(year), int(month), int(day))
        except ValueError:
            return ""  # Data inválida, será NULL no banco
        
        # Converter para DD/MM/YYYY
        return f"{day:02d}/{month:02d}/{year}"
    
    # Se não for formato esperado, retornar vazio
    return ""


def criar_csv_temporario_normalizado(filepath, table_info):
    """
    Gera um arquivo temporário normalizado com:
    - Campos vazios convertidos para string vazia (será NULL no banco)
    - Datas convertidas de YYYYMMDD para DD/MM/YYYY ou vazio se inválidas
    
    :param filepath: Caminho do arquivo CSV original
    :param table_info: Dicionário com informações da tabela (table, columns)
    :return: Caminho do arquivo temporário normalizado
    """
    temp_file = tempfile.NamedTemporaryFile(delete=False, mode='w', encoding='utf-8', newline='')
    
    # Mapear índices das colunas de data
    columns = table_info["columns"]
    date_columns = {
        "estabelecimentos": ["data_situacao", "data_inicio", "data_situacao_especial"],
        "socios": ["data_entrada_sociedade"],
        "simples": ["data_opcao_simples", "data_exclusao_simples", "data_opcao_mei", "data_exclusao_mei"]
    }
    
    table_name = table_info["table"]
    date_column_indices = []
    if table_name in date_columns:
        for date_col in date_columns[table_name]:
            if date_col in columns:
                date_column_indices.append(columns.index(date_col))
    
    try:
        writer = csv.writer(temp_file, delimiter=';', quotechar='"', lineterminator='\n')
        with open(filepath, 'r', encoding='utf-8', errors='replace', newline='') as src:
            reader = csv.reader(NullByteStripper(src), delimiter=';', quotechar='"')
            for row in reader:
                sanitized_row = []
                for idx, value in enumerate(row):
                    # Se for coluna de data, normalizar
                    if idx in date_column_indices:
                        normalized_date = normalizar_data(value)
                        sanitized_row.append(normalized_date)
                    else:
                        # Tratar campos vazios ou só espaços
                        if not value or value.strip() == "":
                            sanitized_row.append("")
                        else:
                            sanitized_row.append(value)
                writer.writerow(sanitized_row)
    finally:
        temp_file.close()
    return temp_file.name



# =================================================================================
# FUNÇÕES DE BANCO DE DADOS
# =================================================================================
def get_db_connection(password, dbname=None):
    """Cria uma conexão com o banco de dados."""
    return psycopg2.connect(
        user=DB_USER,
        password=password,
        host=DB_HOST,
        port=DB_PORT,
        dbname=dbname or DB_NAME
    )

def criar_banco_se_nao_existir(password):
    """Cria o banco de dados 'cnpjdb' se ele não existir."""
    print("\n[1/6] Verificando Banco de Dados...")
    try:
        conn = get_db_connection(password, dbname='postgres')
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        
        cur.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{DB_NAME}'")
        exists = cur.fetchone()
        
        if not exists:
            print(f"Criando banco de dados '{DB_NAME}'...")
            cur.execute(f"CREATE DATABASE {DB_NAME}")
            print("Banco criado com sucesso.")
        else:
            print(f"Banco de dados '{DB_NAME}' já existe.")
            
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Erro crítico ao criar banco: {e}")
        sys.exit(1)

def recriar_tabelas(password):
    """Limpa (DROP) e recria todas as tabelas com schema permissivo (TEXT)."""
    print("\n[2/6] Recriando Tabelas (Schema Inicial)...")
    try:
        conn = get_db_connection(password)
        cur = conn.cursor()
        
        # Drop tables
        tabelas = ["empresas", "estabelecimentos", "socios", "simples", "cnaes", "motivos", "municipios", "naturezas", "paises", "qualificacoes"]
        for t in tabelas:
            cur.execute(f"DROP TABLE IF EXISTS {t} CASCADE;")
            
        # Create tables (OTIMIZADAS desde o início para reduzir tamanho e melhorar performance)
        print("Criando tabelas otimizadas...")
        
        cur.execute("""
            CREATE TABLE empresas (
                cnpj_basico char(8) PRIMARY KEY,
                razao_social varchar(200),
                natureza_juridica char(4),
                qualificacao_do_responsavel char(2),
                capital_social numeric(20,2),
                porte char(2),
                ente_federativo varchar(100)
            );
        """)
        
        cur.execute("""
            CREATE TABLE estabelecimentos (
                cnpj_basico char(8),
                cnpj_ordem char(4),
                cnpj_dv char(2),
                matriz_filial char(1),
                nome_fantasia varchar(200),
                situacao_cadastral char(2),
                data_situacao date,
                motivo_situacao char(2),
                cidade_exterior varchar(100),
                pais char(3),
                data_inicio date,
                cnae_fiscal char(7),
                cnae_fiscal_secundaria text,
                tipo_logradouro varchar(50),
                logradouro varchar(200),
                numero varchar(20),
                complemento varchar(100),
                bairro varchar(100),
                cep char(8),
                uf char(2),
                municipio char(4),
                ddd_1 char(2),
                telefone_1 varchar(15),
                ddd_2 char(2),
                telefone_2 varchar(15),
                ddd_fax char(2),
                fax varchar(15),
                email varchar(150),
                situacao_especial varchar(100),
                data_situacao_especial date
            );
        """)
        
        # Coluna gerada CNPJ completo
        cur.execute("""
            ALTER TABLE estabelecimentos 
            ADD COLUMN cnpj char(14) 
            GENERATED ALWAYS AS (cnpj_basico || cnpj_ordem || cnpj_dv) STORED;
        """)

        cur.execute("""
            CREATE TABLE socios (
                cnpj_basico char(8),
                identificador_socio char(1),
                nome_socio varchar(200),
                cnpj_cpf_socio varchar(20),
                qualificacao_socio char(2),
                data_entrada_sociedade date,
                pais char(3),
                representante_legal char(1),
                nome_representante varchar(200),
                qualificacao_representante char(2),
                faixa_etaria char(1)
            );
        """)
        
        cur.execute("""
            CREATE TABLE simples (
                cnpj_basico char(8) PRIMARY KEY,
                opcao_simples char(1),
                data_opcao_simples date,
                data_exclusao_simples date,
                opcao_mei char(1),
                data_opcao_mei date,
                data_exclusao_mei date
            );
        """)
        
        # Tabelas de domínio (otimizadas)
        cur.execute("CREATE TABLE cnaes (codigo char(7) PRIMARY KEY, descricao varchar(300));")
        cur.execute("CREATE TABLE motivos (codigo char(2) PRIMARY KEY, descricao varchar(200));")
        cur.execute("CREATE TABLE municipios (codigo char(4) PRIMARY KEY, descricao varchar(100));")
        cur.execute("CREATE TABLE naturezas (codigo char(4) PRIMARY KEY, descricao varchar(200));")
        cur.execute("CREATE TABLE paises (codigo char(3) PRIMARY KEY, descricao varchar(100));")
        cur.execute("CREATE TABLE qualificacoes (codigo char(2) PRIMARY KEY, descricao varchar(200));")

        conn.commit()
    # Criar índices básicos imediatamente (antes da importação para melhor performance)
    print("Criando índices básicos...")
    try:
        cur.execute("CREATE INDEX IF NOT EXISTS idx_estab_cnpj_basico_temp ON estabelecimentos (cnpj_basico);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_socios_cnpj_basico_temp ON socios (cnpj_basico);")
    except Exception as e:
        print(f"  Aviso ao criar índices básicos: {e}")
    
    conn.commit()
    # Criar índices básicos imediatamente (antes da importação para melhor performance)
    print("Criando índices básicos iniciais...")
    try:
        cur.execute("CREATE INDEX IF NOT EXISTS idx_estab_cnpj_basico_temp ON estabelecimentos (cnpj_basico);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_socios_cnpj_basico_temp ON socios (cnpj_basico);")
    except Exception as e:
        print(f"  Aviso ao criar índices básicos: {e}")
    
    conn.commit()
    cur.close()
    conn.close()
    print("Tabelas recriadas com sucesso.")
    except Exception as e:
        print(f"Erro ao recriar tabelas: {e}")
        sys.exit(1)

# =================================================================================
# FUNÇÕES DE ARQUIVO E IMPORTAÇÃO
# =================================================================================
def get_target_folder(filename):
    """Define a pasta de destino baseada no nome do arquivo."""
    filename = filename.upper()
    if "EMPRE" in filename: return DATA_DIR / "empresas"
    if "ESTABELE" in filename: return DATA_DIR / "estabelecimentos"
    if "SOCIO" in filename: return DATA_DIR / "socios"
    if "SIMPLES" in filename: return DATA_DIR / "simples"
    return DATA_DIR / "dominio"

CNPJ_BASE_URL = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/"

def download_file(url, dest_folder):
    """Downloads a file from a URL to a destination folder with a progress indicator."""
    local_filename = dest_folder / url.split('/')[-1]
    
    if local_filename.exists():
        print(f"Arquivo {local_filename.name} já existe. Pulando.")
        return

    print(f"Baixando {url}...")
    try:
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            total_length = r.headers.get('content-length')

            with open(local_filename, 'wb') as f:
                if total_length is None: 
                    f.write(r.content)
                else:
                    dl = 0
                    total_length = int(total_length)
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk: 
                            dl += len(chunk)
                            f.write(chunk)
                            done = int(50 * dl / total_length)
                            percent = 100 * dl / total_length
                            sys.stdout.write(f"\r[{'=' * done}{' ' * (50-done)}] {percent:.2f}% | {dl/1024/1024:.2f} MB")
                            sys.stdout.flush()
        print() 
    except Exception as e:
        print(f"\nErro ao baixar {url}: {e}")

def baixar_arquivos_mes_atual():
    """Baixa os arquivos do mês atual da Receita Federal."""
    print("\n[0/6] Baixando Arquivos do Mês Atual...")
    
    target_date = datetime.now().strftime("%Y-%m")
    target_url = urljoin(CNPJ_BASE_URL, f"{target_date}/")
    
    print(f"Acessando {target_url}...")
    
    try:
        response = requests.get(target_url)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Erro ao acessar URL: {e}")
        print("Verifique se a data está correta e se a página existe.")
        return

    soup = BeautifulSoup(response.text, 'html.parser')
    
    links = [urljoin(target_url, a['href']) for a in soup.find_all('a', href=True) if a['href'].lower().endswith('.zip')]
    
    if not links:
        print("Nenhum arquivo .zip encontrado nesta localização.")
        return

    print(f"Encontrados {len(links)} arquivos.")
    
    DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Salvando arquivos em {DOWNLOADS_DIR}")

    total_links = len(links)
    start_time = time.time()
    
    for idx, link in enumerate(links, start=1):
        download_file(link, DOWNLOADS_DIR)

    elapsed = time.time() - start_time
    print(f"Downloads concluídos em {elapsed:.2f}s.")

def descompactar_arquivos():
    """Descompacta todos os ZIPs da pasta downloads para a estrutura organizada."""
    print("\n[3/6] Descompactando Arquivos...")
    
    # Limpeza opcional: se quiser garantir zero lixo, descomente abaixo
    # if DATA_DIR.exists(): shutil.rmtree(DATA_DIR)
    
    zip_files = list(DOWNLOADS_DIR.glob("*.zip"))
    if not zip_files:
        print("Nenhum arquivo ZIP encontrado.")
        return

    def unzip_task(zip_path):
        try:
            target = get_target_folder(zip_path.name)
            target.mkdir(parents=True, exist_ok=True)
            with zipfile.ZipFile(zip_path, 'r') as z:
                z.extractall(target)
            return True
        except Exception as e:
            print(f"Erro em {zip_path.name}: {e}")
            return False

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(unzip_task, zip_files))
    
    print(f"Descompactação concluída. {sum(results)} arquivos processados.")

def importar_arquivo_individual(password, filepath, table_info, normalize_empty=False):
    """Importa um único arquivo CSV para o banco."""
    table_name = table_info["table"]
    columns = table_info["columns"]
    
    start_time = time.time()
    temp_path = None
    
    try:
        conn = get_db_connection(password)
        cur = conn.cursor()
        cols_str = ", ".join(columns)
        
        # Sempre normalizar para garantir conversão correta de datas
        temp_path = criar_csv_temporario_normalizado(filepath, table_info)
        source_path = temp_path
        
        # Mapear colunas de data para conversão durante importação
        date_columns_map = {
            "estabelecimentos": ["data_situacao", "data_inicio", "data_situacao_especial"],
            "socios": ["data_entrada_sociedade"],
            "simples": ["data_opcao_simples", "data_exclusao_simples", "data_opcao_mei", "data_exclusao_mei"]
        }
        
        date_cols = date_columns_map.get(table_name, [])
        
        # Se a tabela tem colunas de data e elas são do tipo DATE, usar tabela temporária
        if date_cols:
            # Verificar se alguma coluna de data é do tipo DATE
            cur.execute("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_schema = 'public' 
                AND table_name = %s 
                AND column_name = ANY(%s)
            """, (table_name, date_cols))
            
            date_cols_in_db = {row[0]: row[1] for row in cur.fetchall()}
            has_date_type = any(dt == 'date' for dt in date_cols_in_db.values())
            
            if has_date_type:
                # Usar tabela temporária para conversão DD/MM/YYYY -> DATE
                temp_table = f"{table_name}_temp_import"
                temp_cols_def = ", ".join([f"{col} text" for col in columns])
                cur.execute(f"CREATE TEMP TABLE {temp_table} ({temp_cols_def});")
                
                # Importar para tabela temporária
                with open(source_path, 'r', encoding='utf-8', errors='replace') as f:
                    clean_f = NullByteStripper(f)
                    sql = f"COPY {temp_table} ({cols_str}) FROM STDIN WITH (FORMAT csv, DELIMITER ';', NULL '', QUOTE '\"', ENCODING 'UTF8')"
                    cur.copy_expert(sql, clean_f)
                
                # Converter e inserir na tabela final
                insert_cols = []
                for col in columns:
                    if col in date_cols_in_db and date_cols_in_db[col] == 'date':
                        insert_cols.append(f"""
                            CASE 
                                WHEN {temp_table}.{col} IS NULL OR TRIM({temp_table}.{col}) = '' THEN NULL
                                WHEN {temp_table}.{col} ~ '^\\d{{2}}/\\d{{2}}/\\d{{4}}$' THEN 
                                    TO_DATE({temp_table}.{col}, 'DD/MM/YYYY')
                                ELSE NULL
                            END AS {col}
                        """)
                    else:
                        insert_cols.append(f"{temp_table}.{col}")
                
                insert_cols_str = ", ".join(insert_cols)
                cur.execute(f"INSERT INTO {table_name} ({cols_str}) SELECT {insert_cols_str} FROM {temp_table};")
                cur.execute(f"DROP TABLE {temp_table};")
            else:
                # Importar diretamente (datas ainda são TEXT)
                with open(source_path, 'r', encoding='utf-8', errors='replace') as f:
                    clean_f = NullByteStripper(f)
                    sql = f"COPY {table_name} ({cols_str}) FROM STDIN WITH (FORMAT csv, DELIMITER ';', NULL '', QUOTE '\"', ENCODING 'UTF8')"
                    cur.copy_expert(sql, clean_f)
        else:
            # Sem colunas de data, importar diretamente
            with open(source_path, 'r', encoding='utf-8', errors='replace') as f:
                clean_f = NullByteStripper(f)
                sql = f"COPY {table_name} ({cols_str}) FROM STDIN WITH (FORMAT csv, DELIMITER ';', NULL '', QUOTE '\"', ENCODING 'UTF8')"
                cur.copy_expert(sql, clean_f)
            
        conn.commit()
        cur.close()
        conn.close()
        
        elapsed = time.time() - start_time
        print(f"  -> Importado: {filepath.name} (tempo: {elapsed:.2f}s)")
        return True
    except Exception as e:
        elapsed = time.time() - start_time
        print(f"  -> ERRO ao importar {filepath.name} (tempo: {elapsed:.2f}s): {e}")
        return False
    finally:
        if temp_path:
            try:
                os.remove(temp_path)
            except OSError:
                pass

def executar_importacao(password, tables_filter=None, normalize_empty=True):
    """
    Gerencia a importação de todos os arquivos CSV.
    Normaliza datas de YYYYMMDD para DD/MM/YYYY antes da inserção.
    :param tables_filter: Lista de nomes de tabelas (str) para importar. Se None, importa tudo.
    :param normalize_empty: Se True, normaliza campos vazios e datas antes do COPY para gerar NULL no banco.
    """
    print("\n[4/6] Importando Dados para o PostgreSQL...")
    print("  Normalizando datas de YYYYMMDD para DD/MM/YYYY antes da inserção...")
    if tables_filter:
        print(f"  Modo de Correção: Importando apenas tabelas {tables_filter}")
    
    start_time = time.time()
    
    files = list(DATA_DIR.rglob("*"))
    files = [f for f in files if f.is_file() and not f.name.startswith('.')]
    files.sort(key=lambda x: x.stat().st_size) # Menores primeiro para feedback rápido
    
    import_targets = []
    
    for filepath in files:
        filename = filepath.name.upper()
        table_info = None
        
        # Matcher
        if "EMPRE" in filename and "CSV" in filename: table_info = FILES_TABLES_MAP["Empresas"]
        elif "ESTABELE" in filename: table_info = FILES_TABLES_MAP["Estabelecimentos"]
        elif "SOCIO" in filename: table_info = FILES_TABLES_MAP["Socios"]
        elif "SIMPLES" in filename: table_info = FILES_TABLES_MAP["Simples"]
        elif "CNAE" in filename: table_info = FILES_TABLES_MAP["Cnaes"]
        elif "MOTI" in filename: table_info = FILES_TABLES_MAP["Motivos"]
        elif "MUNIC" in filename: table_info = FILES_TABLES_MAP["Municipios"]
        elif "NATJU" in filename: table_info = FILES_TABLES_MAP["Naturezas"]
        elif "PAIS" in filename: table_info = FILES_TABLES_MAP["Paises"]
        elif "QUALS" in filename: table_info = FILES_TABLES_MAP["Qualificacoes"]
        
        if table_info:
            if tables_filter and table_info["table"] not in tables_filter:
                continue
            import_targets.append((filepath, table_info))
        else:
            # Só logar ignorados se não estivermos filtrando (para não poluir o log de correção)
            if not tables_filter:
                print(f"  -> Ignorado (não mapeado): {filename}")

    if not import_targets:
        print("Nenhum arquivo para importar.")
        return

    total_targets = len(import_targets)
    print(f"  Total de arquivos para importar: {total_targets}\n")
    
    for idx, (filepath, table_info) in enumerate(import_targets, start=1):
        importar_arquivo_individual(password, filepath, table_info, normalize_empty=normalize_empty)
    
    elapsed = time.time() - start_time
    print(f"\n  Importação concluída em {elapsed:.2f}s")

# =================================================================================
# VERIFICAÇÃO E PÓS-PROCESSAMENTO
# =================================================================================
def count_lines_in_file(filepath):
    """Conta registros lógicos respeitando aspas do CSV."""
    count = 0
    try:
        with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
            # Configuração padrão da Receita: separador ';' e quote '"'
            reader = csv.reader(f, delimiter=';', quotechar='"')
            for _ in reader:
                count += 1
        return count
    except Exception as e:
        print(f"Erro ao ler {filepath.name}: {e}")
        return 0

def get_files_for_table(table_name):
    """Retorna lista de arquivos CSV que correspondem a uma tabela."""
    matchers = TABLE_MATCHERS.get(table_name, [])
    found_files = []
    
    # Busca recursiva em todas as subpastas de data/
    for f in DATA_DIR.rglob("*"):
        if f.is_file() and not f.name.startswith('.'):
            fname_upper = f.name.upper()
            for m in matchers:
                if m in fname_upper:
                    found_files.append(f)
                    break
    return found_files

def verificar_importacao(password):
    """
    Verifica se a contagem de linhas bate com o banco.
    Retorna uma lista de nomes de tabelas que apresentaram divergência.
    """
    print("\n[5/6] Verificando Integridade da Importação...")
    
    conn = get_db_connection(password)
    cur = conn.cursor()

    print(f"{'TABELA':<20} | {'ARQUIVOS':<10} | {'LINHAS CSV':<15} | {'LINHAS BANCO':<15} | {'DIFERENÇA':<10} | {'STATUS':<10}")
    print("-" * 90)
    
    divergentes = []
    
    # Verificar todas as tabelas mapeadas
    tabelas_verificar = ["empresas", "estabelecimentos", "socios", "simples", "cnaes", "motivos", "municipios", "naturezas", "paises", "qualificacoes"]
    
    for table in tabelas_verificar:
        files = get_files_for_table(table)
        
        # Contar linhas CSV (Paralelo)
        total_csv_lines = 0
        if files:
            with concurrent.futures.ThreadPoolExecutor() as executor:
                results = list(executor.map(count_lines_in_file, files))
            total_csv_lines = sum(results)
            
        # Contar linhas Banco
        try:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            db_count = cur.fetchone()[0]
        except Exception as e:
            db_count = -1
            print(f"Erro ao contar tabela {table}: {e}")
            
        diff = total_csv_lines - db_count
        
        # Status
        if db_count == -1: 
            status = "ERRO DB"
            divergentes.append(table)
        elif diff == 0: 
            status = "OK"
        else: 
            status = "DIVERGENTE"
            divergentes.append(table)
        
        print(f"{table:<20} | {len(files):<10} | {total_csv_lines:<15} | {db_count:<15} | {diff:<10} | {status:<10}")
        
    conn.close()
    return divergentes

def verificar_e_corrigir_importacao(password, normalize_empty=True):
    """
    Executa a verificação e tenta corrigir divergências automaticamente.
    Usa contagem lógica de registros para evitar falsos positivos com quebras de linha.
    """
    max_retries = 3
    for attempt in range(max_retries):
        divergentes = verificar_importacao(password)
        
        if not divergentes:
            print("\nVerificação concluída: Tudo OK.")
            return True
            
        print(f"\n[ALERTA] Divergências detectadas em: {divergentes}")
        print(f"Iniciando tentativa de correção automática ({attempt+1}/{max_retries})...")
        
        # Truncar tabelas divergentes
        conn = get_db_connection(password)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        for table in divergentes:
            print(f"  -> Limpando tabela {table} para reimportação...")
            cur.execute(f"TRUNCATE TABLE {table};")
        cur.close()
        conn.close()
        
        # Reimportar apenas as divergentes (sempre normalizar para datas)
        executar_importacao(password, tables_filter=divergentes, normalize_empty=True)
    
    print("\n[ERRO] Não foi possível corrigir todas as divergências após várias tentativas.")
    return False

def get_column_type(cur, table, column):
    """Retorna o tipo de dado atual de uma coluna no schema public."""
    cur.execute("SELECT data_type FROM information_schema.columns WHERE table_schema = 'public' AND table_name = %s AND column_name = %s", (table, column))
    res = cur.fetchone()
    return res[0] if res else None

def converter_e_indexar(password):
    """Converte tipos de dados e cria índices (com verificação de estado)."""
    print("\n[6/6] Convertendo Tipos e Criando Índices...")
    print("  Convertendo tipos de dados e criando índices para otimização de consultas...")
    
    conn = get_db_connection(password)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    
    # Lista de Conversões: (Tabela, Coluna, Tipo Alvo, Comando SQL)
    # NOTA: As tabelas já são criadas otimizadas, mas esta função serve para converter
    # tabelas antigas ou fazer ajustes finais
    conversions = [
        # Capital social (se ainda for text)
        ("empresas", "capital_social", "numeric", 
         "ALTER TABLE empresas ALTER COLUMN capital_social TYPE numeric USING (REPLACE(NULLIF(TRIM(capital_social::text), ''), ',', '.')::numeric);"),
        
        # Datas em estabelecimentos (se ainda forem text)
        ("estabelecimentos", "data_situacao", "date",
         "ALTER TABLE estabelecimentos ALTER COLUMN data_situacao TYPE date USING CASE WHEN data_situacao IS NULL OR TRIM(data_situacao) = '' THEN NULL WHEN data_situacao ~ '^\\d{2}/\\d{2}/\\d{4}$' THEN TO_DATE(data_situacao, 'DD/MM/YYYY') ELSE NULL END;"),
        ("estabelecimentos", "data_inicio", "date",
         "ALTER TABLE estabelecimentos ALTER COLUMN data_inicio TYPE date USING CASE WHEN data_inicio IS NULL OR TRIM(data_inicio) = '' THEN NULL WHEN data_inicio ~ '^\\d{2}/\\d{2}/\\d{4}$' THEN TO_DATE(data_inicio, 'DD/MM/YYYY') ELSE NULL END;"),
        ("estabelecimentos", "data_situacao_especial", "date",
         "ALTER TABLE estabelecimentos ALTER COLUMN data_situacao_especial TYPE date USING CASE WHEN data_situacao_especial IS NULL OR TRIM(data_situacao_especial) = '' THEN NULL WHEN data_situacao_especial ~ '^\\d{2}/\\d{2}/\\d{4}$' THEN TO_DATE(data_situacao_especial, 'DD/MM/YYYY') ELSE NULL END;"),
        
        # Datas em socios
        ("socios", "data_entrada_sociedade", "date",
         "ALTER TABLE socios ALTER COLUMN data_entrada_sociedade TYPE date USING CASE WHEN data_entrada_sociedade IS NULL OR TRIM(data_entrada_sociedade) = '' THEN NULL WHEN data_entrada_sociedade ~ '^\\d{2}/\\d{2}/\\d{4}$' THEN TO_DATE(data_entrada_sociedade, 'DD/MM/YYYY') ELSE NULL END;"),
        
        # Datas em simples
        ("simples", "data_opcao_simples", "date",
         "ALTER TABLE simples ALTER COLUMN data_opcao_simples TYPE date USING CASE WHEN data_opcao_simples IS NULL OR TRIM(data_opcao_simples) = '' THEN NULL WHEN data_opcao_simples ~ '^\\d{2}/\\d{2}/\\d{4}$' THEN TO_DATE(data_opcao_simples, 'DD/MM/YYYY') ELSE NULL END;"),
        ("simples", "data_exclusao_simples", "date",
         "ALTER TABLE simples ALTER COLUMN data_exclusao_simples TYPE date USING CASE WHEN data_exclusao_simples IS NULL OR TRIM(data_exclusao_simples) = '' THEN NULL WHEN data_exclusao_simples ~ '^\\d{2}/\\d{2}/\\d{4}$' THEN TO_DATE(data_exclusao_simples, 'DD/MM/YYYY') ELSE NULL END;"),
        ("simples", "data_opcao_mei", "date",
         "ALTER TABLE simples ALTER COLUMN data_opcao_mei TYPE date USING CASE WHEN data_opcao_mei IS NULL OR TRIM(data_opcao_mei) = '' THEN NULL WHEN data_opcao_mei ~ '^\\d{2}/\\d{2}/\\d{4}$' THEN TO_DATE(data_opcao_mei, 'DD/MM/YYYY') ELSE NULL END;"),
        ("simples", "data_exclusao_mei", "date",
         "ALTER TABLE simples ALTER COLUMN data_exclusao_mei TYPE date USING CASE WHEN data_exclusao_mei IS NULL OR TRIM(data_exclusao_mei) = '' THEN NULL WHEN data_exclusao_mei ~ '^\\d{2}/\\d{2}/\\d{4}$' THEN TO_DATE(data_exclusao_mei, 'DD/MM/YYYY') ELSE NULL END;"),
    ]
    
    print("  --- Verificando e Convertendo Tipos ---")
    for table, col, target_type, cmd in conversions:
        try:
            current_type = get_column_type(cur, table, col)
            if current_type and current_type != target_type:
                print(f"    -> Convertendo {table}.{col} (de {current_type} para {target_type})...", end=" ", flush=True)
                try:
                    start = time.time()
                    cur.execute(cmd)
                    elapsed = time.time() - start
                    print(f"Concluído em {elapsed:.2f}s")
                except Exception as e:
                    print(f"ERRO: {e}")
            else:
                if current_type:
                    print(f"    -> {table}.{col} já é do tipo {target_type}.")
        except Exception as e:
            # Coluna pode não existir ou tabela pode não existir ainda
            pass

    # Índices (OTIMIZADOS para performance máxima)
    print("\n  --- Criando Índices Otimizados (se não existirem) ---")
    index_commands = [
        # Extensões
        ("Extensão pg_trgm", "CREATE EXTENSION IF NOT EXISTS pg_trgm;"),
        
        # Índices principais estabelecimentos (mais usados)
        ("Índice UNIQUE CNPJ estabelecimentos", "CREATE UNIQUE INDEX IF NOT EXISTS idx_estab_cnpj_unique ON estabelecimentos (cnpj);"),
        ("Índice CNPJ básico estabelecimentos", "CREATE INDEX IF NOT EXISTS idx_estab_cnpj_basico ON estabelecimentos (cnpj_basico);"),
        
        # Índices compostos para buscas comuns (melhoram muito a performance)
        ("Índice composto UF + Município", "CREATE INDEX IF NOT EXISTS idx_estab_uf_municipio ON estabelecimentos (uf, municipio);"),
        ("Índice composto UF + CNAE", "CREATE INDEX IF NOT EXISTS idx_estab_uf_cnae ON estabelecimentos (uf, cnae_fiscal);"),
        ("Índice composto Situação + UF", "CREATE INDEX IF NOT EXISTS idx_estab_situacao_uf ON estabelecimentos (situacao_cadastral, uf);"),
        
        # Índices simples
        ("Índice UF estabelecimentos", "CREATE INDEX IF NOT EXISTS idx_estab_uf ON estabelecimentos (uf);"),
        ("Índice município estabelecimentos", "CREATE INDEX IF NOT EXISTS idx_estab_municipio ON estabelecimentos (municipio);"),
        ("Índice CNAE fiscal estabelecimentos", "CREATE INDEX IF NOT EXISTS idx_estab_cnae_fiscal ON estabelecimentos (cnae_fiscal);"),
        ("Índice situação cadastral", "CREATE INDEX IF NOT EXISTS idx_estab_situacao ON estabelecimentos (situacao_cadastral);"),
        ("Índice matriz/filial", "CREATE INDEX IF NOT EXISTS idx_estab_matriz_filial ON estabelecimentos (matriz_filial);"),
        
        # Índices de texto (GIN para busca rápida)
        ("Índice nome fantasia (trgm)", "CREATE INDEX IF NOT EXISTS idx_estab_fantasia_trgm ON estabelecimentos USING GIN (nome_fantasia gin_trgm_ops);"),
        
        # Índices empresas
        ("Índice razão social empresas (trgm)", "CREATE INDEX IF NOT EXISTS idx_empresas_razao_trgm ON empresas USING GIN (razao_social gin_trgm_ops);"),
        ("Índice natureza jurídica empresas", "CREATE INDEX IF NOT EXISTS idx_empresas_natureza ON empresas (natureza_juridica);"),
        ("Índice porte empresas", "CREATE INDEX IF NOT EXISTS idx_empresas_porte ON empresas (porte);"),
        ("Índice capital social empresas", "CREATE INDEX IF NOT EXISTS idx_empresas_capital ON empresas (capital_social);"),
        
        # Índices compostos empresas
        ("Índice composto Natureza + Porte", "CREATE INDEX IF NOT EXISTS idx_empresas_natureza_porte ON empresas (natureza_juridica, porte);"),
        
        # Índices sócios
        ("Índice nome sócio (trgm)", "CREATE INDEX IF NOT EXISTS idx_socios_nome_trgm ON socios USING GIN (nome_socio gin_trgm_ops);"),
        ("Índice CNPJ/CPF sócio", "CREATE INDEX IF NOT EXISTS idx_socios_cnpj_cpf ON socios (cnpj_cpf_socio);"),
        ("Índice CNPJ básico sócios", "CREATE INDEX IF NOT EXISTS idx_socios_cnpj_basico ON socios (cnpj_basico);"),
        
        # Índices simples
        ("Índice opção simples", "CREATE INDEX IF NOT EXISTS idx_simples_opcao ON simples (opcao_simples);"),
        ("Índice opção MEI", "CREATE INDEX IF NOT EXISTS idx_simples_mei ON simples (opcao_mei);"),
    ]
    
    for desc, cmd in index_commands:
        print(f"    -> Criando {desc}...", end=" ", flush=True)
        try:
            start = time.time()
            cur.execute(cmd)
            elapsed = time.time() - start
            print(f"Concluído em {elapsed:.2f}s")
        except Exception as e:
            print(f"ERRO: {e}")
    
    # Otimizações finais
    print("\n  --- Otimizações Finais ---")
    
    # ANALYZE para atualizar estatísticas
    print("    -> Executando ANALYZE nas tabelas...", end=" ", flush=True)
    try:
        start = time.time()
        tabelas = ["empresas", "estabelecimentos", "socios", "simples", "cnaes", "motivos", "municipios", "naturezas", "paises", "qualificacoes"]
        for tabela in tabelas:
            cur.execute(f"ANALYZE {tabela};")
        elapsed = time.time() - start
        print(f"Concluído em {elapsed:.2f}s")
    except Exception as e:
        print(f"ERRO: {e}")
    
    # VACUUM (sem FULL para não bloquear) para limpar espaço morto
    print("    -> Executando VACUUM nas tabelas...", end=" ", flush=True)
    try:
        start = time.time()
        for tabela in tabelas:
            cur.execute(f"VACUUM {tabela};")
        elapsed = time.time() - start
        print(f"Concluído em {elapsed:.2f}s")
    except Exception as e:
        print(f"ERRO: {e}")
    
    # Verificar tamanho final
    print("\n  --- Tamanho do Banco ---")
    try:
        cur.execute("SELECT pg_size_pretty(pg_database_size(current_database()));")
        tamanho = cur.fetchone()[0]
        print(f"    Tamanho total do banco: {tamanho}")
        
        cur.execute("""
            SELECT 
                tablename,
                pg_size_pretty(pg_total_relation_size('public.'||tablename)) as size
            FROM pg_tables
            WHERE schemaname = 'public'
            ORDER BY pg_total_relation_size('public.'||tablename) DESC
            LIMIT 5;
        """)
        print("    Top 5 tabelas por tamanho:")
        for row in cur.fetchall():
            print(f"      {row[0]}: {row[1]}")
    except Exception as e:
        print(f"    Erro ao verificar tamanho: {e}")
            
    cur.close()
    conn.close()
    print("\nConversão e indexação concluídas.")
