import requests
import json

token = "296a89c9c8c15ce57e9c94bf95fb03137290b8c1474a252f3b4557a8d43f0a48ce0767e170"
empresa_id = "1"
url = "https://apigw.pactosolucoes.com.br/EndpointControl/show"

headers = {
    "Authorization": f"Bearer {token}",
    "empresaId": empresa_id,
    "accept": "*/*"
}

try:
    print("--- SOLICITANDO MAPA DA API ---")
    response = requests.get(url, headers=headers, timeout=30)
    
    if response.status_code == 200:
        mapa = response.json()
        
        # O retorno costuma ser uma lista de dicionários com 'path', 'method', etc.
        # Vamos procurar o que nos interessa:
        palavras_chave = ['matricula', 'venda', 'contrato', 'financeiro', 'bi']
        
        print("\n[ ENCONTRADOS ]")
        rotas_encontradas = []
        
        # Ajuste a lógica abaixo conforme a estrutura do JSON que vier
        # Geralmente é uma lista simples ou um objeto com chaves por módulo
        for rota in mapa:
            # Converte a rota (seja string ou dict) para texto para busca
            rota_str = str(rota).lower()
            if any(p in rota_str for p in palavras_chave):
                print(f"📍 {rota}")
                rotas_encontradas.append(rota)
        
        if not rotas_encontradas:
            print("Nenhuma rota de venda/matrícula encontrada no mapa.")
            # Se não encontrar, imprima as 5 primeiras rotas para vermos a estrutura
            print("\nExemplo de estrutura do mapa:", json.dumps(mapa[:5], indent=2))
            
    else:
        print(f"Erro {response.status_code}: {response.text}")

except Exception as e:
    print(f"Falha na conexão: {e}")