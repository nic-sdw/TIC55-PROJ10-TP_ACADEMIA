# Imports do projeto
import requests
from dotenv import load_dotenv
import os
import json
import datetime
import pandas as pd

# Carregando o .env
load_dotenv()

# URL base que está no site da pacto
urlBase = "https://apigw.pactosolucoes.com.br"

# Token e ID da empreas que estão no .env
token = os.getenv("TOKEN")
empresa_id = os.getenv("EMPRESA_ID")

# Função para buscar a empresa especifíca dentro do sistema da pacto
# Dá para fazer uma que busca todas, mas para o propósito que queremos não faz sentido

def getEmpresa():

  # Endpoint da API
  url_empresas = f"{urlBase}/v1/empresa/{empresa_id}"
  # Header da pacto
  headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json",
    "empresaId": empresa_id
  }

  # Aqui é realizada a requisição do tipo GET para a API
  response = requests.get(url_empresas, headers=headers)
  
  # Se o status da requisição for 200(sucesso), ele retorna os dados em um json
  if response.status_code == 200:
    data = response.json()
    print("Consulta funcionou!")
    print(json.dumps(data, indent=2, ensure_ascii=False))
    return data
  
  # Se não ela específica o erro...
  else:
    print(f"Erro {response.status_code}: {response.text}")
    return None
    
getEmpresa()  
  

  
