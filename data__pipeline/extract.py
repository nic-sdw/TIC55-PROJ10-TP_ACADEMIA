# Imports principais do projeto
import requests
from dotenv import load_dotenv
import os
import json
import pandas as pd

# Carregando variáveis do .env (token, empresa_ID, etc)
load_dotenv()

# Pegando o token e o ID da empresa do arquivo .env
token = os.getenv("TOKEN")
empresa_id = os.getenv("EMPRESA_ID")

# URL base usada em todos os endpoints da Pacto
urlBase = "https://apigw.pactosolucoes.com.br"

# Esses headers são obrigatórios em TODAS as requisições da API da Pacto
headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json",
    "empresaId": empresa_id
}


# Função para buscar os dados da empresa cadastrada no sistema
# Não faz sentido criar uma função para listar todas as empresas,
# porque só queremos consultar a empresa atual mesmo.
def getEmpresa():
    url_empresas = f"{urlBase}/v1/empresa/{empresa_id}"

    response = requests.get(url_empresas, headers=headers)

    if response.status_code == 200:
        data = response.json()
        print("A consulta deu certo!")
        print(json.dumps(data, indent=2, ensure_ascii=False))
        return data
    else:
        print(f"Erro {response.status_code}: {response.text}")
        return None



# Função genérica para consultar qualquer endpoint de agendamentos da Pacto.
# Isso evita repetição de código, já que 'executados' e 'faltaram' seguem o mesmo padrão.
def getAgendamentos(path, professor_id=1, page=1, size=10, sort="nome,asc"):

    url = f"{urlBase}{path}"

    # Parâmetros aceitos pelo endpoint (todos opcionais, exceto empresaId que vai no header)
    params = {
        "professorId": professor_id,
        "page": page,
        "size": size,
        "sort": sort,
        "filters": json.dumps({"professorId": professor_id})
    }

    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        print("A requisição funcionou!")
        dados = response.json()
        print(json.dumps(dados, indent=2, ensure_ascii=False))
        return dados
    else:
        print(f"Algo deu errado na consulta ({path})... {response.status_code}, {response.text}")
        return None



# Função que busca os agendamentos EXECUTADOS
def getAgendamentosExecutados(professor_id=1, page=1, size=10, sort="nome,asc"):
    return getAgendamentos(
        path="/psec/treino-bi/agendamento-executaram",
        professor_id=professor_id,
        page=page,
        size=size,
        sort=sort
    )


# Função que busca os agendamentos que FALTARAM
def getAgendamentosFaltaram(professor_id=1, page=1, size=10, sort="nome,asc"):
    return getAgendamentos(
        path="/psec/treino-bi/agendamento-faltaram",
        professor_id=professor_id,
        page=page,
        size=size,
        sort=sort
    )

getAgendamentosFaltaram()