import sys
import getpass
import page


def main():
    
    print("="*80)
    print("PROCESSAMENTO COMPLETO DE DADOS CNPJ - POSTGRESQL")
    print("="*80)
    print("  Normalizando datas de YYYYMMDD para DD/MM/YYYY antes da inserção")
    print("  Datas inválidas serão convertidas para NULL")
    print("="*80)
    normalizar_vazios = True
    
    # if len(sys.argv) > 1:
    #     password = sys.argv[1]
    # else:
    #     password = getpass.getpass("Digite a senha do usuário postgres: ")

    password = 'Italommf@45'
        
    # 0. Baixar Arquivos (Mês Atual)
    # page.baixar_arquivos_mes_atual()

    # # 1. Criar Banco
    # page.criar_banco_se_nao_existir(password)
    
    # # 2. Descompactar
    # page.descompactar_arquivos()
    
    # # 3. Recriar Tabelas
    # page.recriar_tabelas(password)
    
    # # 4. Importar (Inicial)
    # page.executar_importacao(password, normalize_empty=normalizar_vazios)
    
    # # 5. Verificar e Corrigir (Loop)
    # page.verificar_e_corrigir_importacao(password, normalize_empty=normalizar_vazios)
    
    # 6. Converter e Indexar
    page.converter_e_indexar(password)
    
    print("\n" + "="*80)
    print("PROCESSO FINALIZADO COM SUCESSO!")
    print("="*80)

if __name__ == "__main__":
    main()
