"""
Script para testar compatibilidade da API após otimizações
Testa se as respostas da API estão corretas com os novos tipos de dados
"""
import requests
import json

BASE_URL = "https://consultacnpj.italommf.com.br/api"
TOKEN = "66b53c968c85a8f516aa44fe0f8fa3dfe11b104f"

headers = {
    "Authorization": f"Token {TOKEN}"
}

def test_buscar_cnpj(cnpj):
    """Testa busca por CNPJ"""
    print(f"\nTestando busca por CNPJ: {cnpj}")
    url = f"{BASE_URL}/companies/cnpj/{cnpj}/"
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        print(f"  Status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            
            # Verificar estrutura
            assert 'estabelecimento' in data, "Resposta deve ter 'estabelecimento'"
            assert 'empresa' in data, "Resposta deve ter 'empresa'"
            
            # Verificar formato de datas (devem estar em DD/MM/YYYY)
            estab = data['estabelecimento']
            if 'situacao' in estab:
                data_situacao = estab['situacao'].get('data_situacao')
                if data_situacao:
                    print(f"  Data situação: {data_situacao}")
                    # Verificar formato (deve ser DD/MM/YYYY ou None)
                    if data_situacao and '/' in data_situacao:
                        parts = data_situacao.split('/')
                        assert len(parts) == 3, f"Data deve ter formato DD/MM/YYYY, recebido: {data_situacao}"
                        print(f"  ✓ Formato de data correto")
            
            print(f"  ✓ Estrutura da resposta OK")
            return True
        else:
            print(f"  ✗ Erro: {response.text}")
            return False
            
    except Exception as e:
        print(f"  ✗ Erro na requisição: {e}")
        return False

def test_busca_geral():
    """Testa busca geral com filtros"""
    print(f"\nTestando busca geral...")
    url = f"{BASE_URL}/companies/search/"
    
    params = {
        "uf": "SP",
        "page": 1,
        "page_size": 10
    }
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        print(f"  Status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            
            # Verificar estrutura
            assert 'results' in data, "Resposta deve ter 'results'"
            assert 'total_count' in data, "Resposta deve ter 'total_count'"
            
            print(f"  Total encontrado: {data.get('total_count', 0)}")
            print(f"  Resultados na página: {len(data.get('results', []))}")
            
            if data.get('results'):
                # Verificar primeiro resultado
                primeiro = data['results'][0]
                assert 'estabelecimento' in primeiro, "Resultado deve ter 'estabelecimento'"
                assert 'empresa' in primeiro, "Resultado deve ter 'empresa'"
                print(f"  ✓ Estrutura da resposta OK")
            
            return True
        else:
            print(f"  ✗ Erro: {response.text}")
            return False
            
    except Exception as e:
        print(f"  ✗ Erro na requisição: {e}")
        return False

def test_endpoints_auxiliares():
    """Testa endpoints auxiliares"""
    print(f"\nTestando endpoints auxiliares...")
    
    endpoints = [
        "/cnaes/",
        "/municipios/"
    ]
    
    for endpoint in endpoints:
        url = f"{BASE_URL}{endpoint}"
        try:
            response = requests.get(url, headers=headers, params={"page_size": 5}, timeout=10)
            print(f"  {endpoint}: {response.status_code}")
            if response.status_code == 200:
                data = response.json()
                assert 'results' in data, f"{endpoint} deve retornar 'results'"
                print(f"    ✓ OK ({len(data.get('results', []))} resultados)")
            else:
                print(f"    ✗ Erro: {response.text[:100]}")
        except Exception as e:
            print(f"    ✗ Erro: {e}")

def main():
    print("="*80)
    print("TESTE DE COMPATIBILIDADE DA API APÓS OTIMIZAÇÕES")
    print("="*80)
    
    # Teste 1: Busca por CNPJ (se houver dados)
    # test_buscar_cnpj("12345678000190")  # Descomente quando houver dados
    
    # Teste 2: Busca geral
    test_busca_geral()
    
    # Teste 3: Endpoints auxiliares
    test_endpoints_auxiliares()
    
    print("\n" + "="*80)
    print("TESTES CONCLUÍDOS")
    print("="*80)

if __name__ == "__main__":
    main()


