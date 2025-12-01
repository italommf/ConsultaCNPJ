#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Script para limpar completamente o banco de dados CNPJ."""

import sys
import getpass
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
    print("LIMPEZA COMPLETA DO BANCO DE DADOS CNPJ")
    print("="*80)
    print("  ⚠ ATENÇÃO: Todas as tabelas de dados serão removidas!")
    print("  ⚠ Tabelas do Django (auth, sessions, etc) NÃO serão removidas")
    print("="*80)
    
    # Obter senha
    if len(sys.argv) > 1:
        password = sys.argv[1]
    else:
        password = getpass.getpass("Digite a senha do usuário postgres: ")
    
    password = str(password)
    
    # Confirmar
    resposta = input("\nTem certeza que deseja limpar o banco? (digite 'SIM' para confirmar): ")
    if resposta != 'SIM':
        print("Operação cancelada.")
        return
    
    # Limpar banco
    sucesso = page.limpar_banco_dados(password)
    
    if sucesso:
        print("\n" + "="*80)
        print("✓ Banco limpo com sucesso!")
        print("  Agora você pode executar main.py para recriar as tabelas e importar os dados.")
        print("="*80)
    else:
        print("\n" + "="*80)
        print("✗ Erro ao limpar o banco. Verifique os erros acima.")
        print("="*80)
        sys.exit(1)

if __name__ == "__main__":
    main()

