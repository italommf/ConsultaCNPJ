import sys
import getpass
import os
from pathlib import Path
import page

# Garantir encoding UTF-8
if sys.platform == 'win32':
    try:
        if sys.stdout.encoding != 'utf-8':
            sys.stdout.reconfigure(encoding='utf-8')
        if sys.stderr.encoding != 'utf-8':
            sys.stderr.reconfigure(encoding='utf-8')
    except:
        pass


def main():
    
    print("="*80)
    print("PROCESSAMENTO COMPLETO DE DADOS CNPJ - POSTGRESQL")
    print("="*80)
    print("  ✓ Tabelas criadas com tipos otimizados (char, varchar, date, numeric)")
    print("  ✓ Normalizando datas de YYYYMMDD para DD/MM/YYYY antes da inserção")
    print("  ✓ Datas inválidas serão convertidas para NULL")
    print("  ✓ Valores truncados automaticamente para respeitar limites das colunas")
    print("="*80)
    
    # Mostrar informações do sistema e diretórios
    print(f"\nSistema Operacional: {sys.platform}")
    print(f"Diretório Base: {page.BASE_DIR}")
    print(f"Diretório Downloads: {page.DOWNLOADS_DIR}")
    print(f"Diretório Data: {page.DATA_DIR}")
    print("="*80)
    
    normalizar_vazios = True
    
    if len(sys.argv) > 1:
        password = sys.argv[1]
        # Garantir que password seja string Unicode
        if isinstance(password, bytes):
            password = password.decode('utf-8', errors='replace')
    else:
        password = getpass.getpass("Digite a senha do usuário postgres: ")
        # Garantir que password seja string Unicode
        if isinstance(password, bytes):
            password = password.decode('utf-8', errors='replace')
    
    # Garantir que password seja sempre string
    password = str(password)
    
    # Perguntar se deseja limpar o banco antes de começar
    print("\n" + "="*80)
    resposta = input("Deseja limpar o banco de dados antes de importar? (s/N): ").strip().lower()
    if resposta in ('s', 'sim', 'y', 'yes'):
        page.limpar_banco_dados(password)
        print("="*80)
        
    # 0. Baixar Arquivos (Mês Atual)
    page.baixar_arquivos_mes_atual()

    # 1. Criar Banco
    page.criar_banco_se_nao_existir(password)
    
    # 2. Descompactar
    page.descompactar_arquivos()
    
    # 3. Recriar Tabelas
    page.recriar_tabelas(password)
    
    # 4. Importar (Inicial) - sempre normalizar para compatibilidade com tipos otimizados
    page.executar_importacao(password, normalize_empty=True)
    
    # 5. Verificar e Corrigir (Loop) - sempre normalizar
    page.verificar_e_corrigir_importacao(password, normalize_empty=True)
    
    # 6. Converter e Indexar
    page.converter_e_indexar(password)
    
    print("\n" + "="*80)
    print("PROCESSO FINALIZADO COM SUCESSO!")
    print("="*80)

if __name__ == "__main__":
    main()
