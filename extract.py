# Imports principais do projeto
import requests
from dotenv import load_dotenv
import os
import json

# Carregando variáveis do .env
load_dotenv()
token = os.getenv("TOKEN")
empresa_id = os.getenv("EMPRESA_ID")
urlBase = "https://apigw.pactosolucoes.com.br"

headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json",
    "empresaId": empresa_id
}

# --- Funções de Consulta API ---

def getAgendamentos(path, professor_id=1, page=0, size=100, sort="nome,asc"):
    url = f"{urlBase}{path}"
    params = {
        "professorId": professor_id,
        "page": page,
        "size": size,
        "sort": sort,
        "filters": json.dumps({"professorId": professor_id})
    }
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Erro na consulta ({path}): {response.status_code}")
        return None

def getAgendamentosExecutados(professor_id=1, page=0):
    return getAgendamentos("/psec/treino-bi/agendamento-executaram", professor_id, page)

def getDadosPaginados(api_agendamentos, professor_id=1):
    pagina_atual = 0
    while True:
        print(f"Buscando página {pagina_atual}...")
        dados = api_agendamentos(professor_id=professor_id, page=pagina_atual)

        if dados is None:
            break

        content = dados.get('content', [])
        for agendamento in content:
            yield agendamento

        if not content: # Se a lista vier vazia, acabou a paginação
            break
            
        pagina_atual += 1

def getAgendamentosFiltrados():
    eventos_filtros = ["Aula Experimental", "Primeiro Treino sem A.E", "Primeiro Treino com A.E"]
    
    # Coleta todos os dados usando o gerador
    agendamentos = list(getDadosPaginados(getAgendamentosExecutados))
    
    # Filtra apenas os eventos desejados
    agendamentos_filtrados = [
        a for a in agendamentos if a.get('evento') in eventos_filtros
    ]
    
    print(f"Coleta finalizada! Total bruto coletado: {len(agendamentos_filtrados)}")
    return agendamentos_filtrados