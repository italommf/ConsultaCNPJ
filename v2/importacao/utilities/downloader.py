"""Módulo para download e descompactação de arquivos da Receita Federal"""
import os
import sys
import time
import zipfile
import shutil
import concurrent.futures
from pathlib import Path
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

# URL base dos dados da Receita Federal
CNPJ_BASE_URL = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/"


def get_target_folder(filename: str, data_dir: Path) -> Path:
    """Determina a pasta de destino baseado no nome do arquivo."""
    filename_upper = filename.upper()
    if "EMPRE" in filename_upper:
        return data_dir / "empresas"
    if "ESTABELE" in filename_upper:
        return data_dir / "estabelecimentos"
    if "SOCIO" in filename_upper:
        return data_dir / "socios"
    if "SIMPLES" in filename_upper:
        return data_dir / "simples"
    return data_dir / "dominio"


def download_file(url: str, dest_folder: Path):
    """Baixa um arquivo de uma URL para uma pasta de destino com indicador de progresso."""
    local_filename = dest_folder / url.split('/')[-1]
    
    if local_filename.exists():
        logger.info(f"Arquivo {local_filename.name} já existe. Pulando.")
        return True

    logger.info(f"Baixando {url}...")
    try:
        with requests.get(url, stream=True, timeout=300) as r:
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
        print()  # Nova linha após a barra de progresso
        return True
    except Exception as e:
        logger.error(f"Erro ao baixar {url}: {e}")
        return False


def baixar_arquivos_mes_atual(downloads_dir: Path):
    """Baixa os arquivos do mês atual da Receita Federal."""
    logger.info("Baixando arquivos do mês atual da Receita Federal...")
    
    target_date = datetime.now().strftime("%Y-%m")
    target_url = urljoin(CNPJ_BASE_URL, f"{target_date}/")
    
    logger.info(f"Acessando {target_url}...")
    
    try:
        response = requests.get(target_url, timeout=30)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.error(f"Erro ao acessar URL: {e}")
        logger.error("Verifique se a data está correta e se a página existe.")
        return False

    soup = BeautifulSoup(response.text, 'html.parser')
    
    links = [urljoin(target_url, a['href']) for a in soup.find_all('a', href=True) if a['href'].lower().endswith('.zip')]
    
    if not links:
        logger.warning("Nenhum arquivo .zip encontrado nesta localização.")
        return False

    logger.info(f"Encontrados {len(links)} arquivos.")
    
    downloads_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Salvando arquivos em {downloads_dir}")

    start_time = time.time()
    success_count = 0
    
    for idx, link in enumerate(links, start=1):
        logger.info(f"[{idx}/{len(links)}] Processando...")
        if download_file(link, downloads_dir):
            success_count += 1

    elapsed = time.time() - start_time
    logger.info(f"Downloads concluídos em {elapsed:.2f}s. {success_count}/{len(links)} arquivos baixados.")
    
    return success_count > 0


def descompactar_arquivos(downloads_dir: Path, data_dir: Path):
    """Descompacta todos os ZIPs da pasta downloads para a estrutura organizada.
    Baseado na função do v1/scripts/page.py que funcionava bem."""
    logger.info("Descompactando arquivos...")
    
    zip_files = list(downloads_dir.glob("*.zip"))
    if not zip_files:
        logger.warning("Nenhum arquivo ZIP encontrado.")
        return False

    def unzip_task(zip_path: Path):
        try:
            target = get_target_folder(zip_path.name, data_dir)
            target.mkdir(parents=True, exist_ok=True)
            with zipfile.ZipFile(zip_path, 'r') as z:
                z.extractall(target)
            return True
        except Exception as e:
            logger.error(f"Erro ao descompactar {zip_path.name}: {e}")
            return False

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(unzip_task, zip_files))
    
    success_count = sum(results)
    logger.info(f"Descompactação concluída. {success_count} arquivos processados.")
    
    return success_count > 0

