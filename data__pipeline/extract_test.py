# Imports do projeto
import requests
from dotenv import load_dotenv
import os
import json
import sys  # Importado para usar sys.exit em caso de erro grave

# Carregando o .env
load_dotenv()

# --- Constantes ---
# URL base da API
URL_BASE = "https://apigw.pactosolucoes.com.br"

# Token e ID da empresa que estão no .env
TOKEN = os.getenv("TOKEN")
EMPRESA_ID = os.getenv("EMPRESA_ID")

def fazer_requisicao_get(endpoint, headers, params=None):
    """
    Função genérica para realizar requisições GET para a API Pacto.
    Já inclui tratamento de erros robusto.
    """
    url_completa = f"{URL_BASE}{endpoint}"
    
    try:
        # Adicionado um timeout (10 segundos) como boa prática
        response = requests.get(url_completa, headers=headers, params=params, timeout=10)
        
        # Lança uma exceção para erros HTTP (códigos 4xx e 5xx)
        response.raise_for_status() 
        
        # Se a resposta for bem-sucedida, retorna o JSON
        return response.json()

    # --- Tratamento de Erros Específicos ---
    except requests.exceptions.HTTPError as errh:
        # Erros de API (401, 404, 500, etc.)
        print(f"Erro HTTP: {errh.response.status_code} - {errh.response.text}")
    except requests.exceptions.ConnectionError as errc:
        # Erros de conexão (DNS, rede)
        print(f"Erro de Conexão: {errc}")
    except requests.exceptions.Timeout as errt:
        # A requisição demorou demais
        print(f"Erro de Timeout: {errt}")
    except requests.exceptions.RequestException as err:
        # Erro genérico da biblioteca requests
        print(f"Erro Inesperado na Requisição: {err}")
    except json.JSONDecodeError:
        # A API retornou algo que não é JSON (ex: um HTML de erro)
        print("Erro: A resposta da API não é um JSON válido.")
        
    return None # Retorna None se qualquer erro ocorrer

# --- Funções de Negócio ---

def buscar_empresa(headers, empresa_id):
    """
    Função específica de negócio para buscar UMA empresa.
    Ela usa a função genérica 'fazer_requisicao_get'.
    """
    print(f"Buscando dados da empresa ID: {empresa_id}...")
    endpoint = f"/v1/empresa/{empresa_id}"
    return fazer_requisicao_get(endpoint, headers=headers)

# --- Execução Principal ---

def main():
    """
    Função principal que orquestra a execução do script.
    """
    # Validação crucial: verifica se as variáveis de ambiente foram carregadas
    if not TOKEN or not EMPRESA_ID:
        print("Erro Crítico: TOKEN ou EMPRESA_ID não encontrados no arquivo .env.")
        print("Por favor, verifique seu arquivo .env e tente novamente.")
        sys.exit(1) # Termina o script indicando um erro

    # Monta o cabeçalho padrão para todas as requisições
    headers = {
        "Authorization": f"Bearer {TOKEN}",
        "Content-Type": "application/json",
        "empresaId": EMPRESA_ID
    }

    # Chama a função de negócio
    dados_empresa = buscar_empresa(headers, EMPRESA_ID)
    
    # A lógica de "o que fazer com os dados" fica aqui,
    # separada da lógica de "como buscar os dados".
    if dados_empresa:
        print("\nConsulta realizada com sucesso!")
        # Usar json.dumps para imprimir formatado (pretty-print)
        print(json.dumps(dados_empresa, indent=2, ensure_ascii=False))
    else:
        print("\nNão foi possível obter os dados da empresa.")

# Este bloco garante que a função 'main()' só execute
# quando você roda o script diretamente (ex: python seu_script.py)
if __name__ == "__main__":
    main()