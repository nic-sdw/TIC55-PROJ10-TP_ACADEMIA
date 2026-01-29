# Imports principais do projeto
import requests
import json
import gspread
import pandas as pd
from pathlib import Path


from . import config

# Função para buscar os dados da empresa cadastrada no sistema
# Não faz sentido criar uma função para listar todas as empresas,
# porque só queremos consultar a empresa atual mesmo.
def getEmpresa():
    url_empresas = f"{config.URL_BASE}/v1/empresa/{config.EMPRESA_ID}"

    response = requests.get(url_empresas, headers=config.HEADERS)

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
def getAgendamentos(path, professor_id=1, page=0, size=100, sort="nome,asc"):

    url = f"{config.URL_BASE}{path}"

    # Parâmetros aceitos pelo endpoint (todos opcionais, exceto empresaId que vai no header)
    params = {
        "professorId": professor_id,
        "page": page,
        "size": size,
        "sort": sort,
        "filters": json.dumps({"professorId": professor_id})
    }

    response = requests.get(url, headers=config.HEADERS, params=params)

    if response.status_code == 200:
        print("A requisição funcionou!")
        dados = response.json()
        # print(json.dumps(dados, indent=2, ensure_ascii=False))
        return dados
    else:
        print(f"Algo deu errado na consulta ({path})... {response.status_code}, {response.text}")
        return None



# Função que busca os agendamentos EXECUTADOS
def getAgendamentosExecutados(professor_id=1, page=0, size=100, sort="nome,asc"):
    return getAgendamentos(
        path="/psec/treino-bi/agendamento-executaram",
        professor_id=professor_id,
        page=page,
        size=size,
        sort=sort
    )


# Função que busca os agendamentos que FALTARAM
def getAgendamentosFaltaram(professor_id=1, page=0, size=100, sort="nome,asc"):
    return getAgendamentos(
        path="/psec/treino-bi/agendamento-faltaram",
        professor_id=professor_id,
        page=page,
        size=size,
        sort=sort
    )
# Função geradora que busca dados de uma função de API paginada.
# api_agendamentos: A função que busca os dados (ex: getAgendamentosExecutados).
def getDadosPaginados(api_agendamentos, professor_id=1):
    
    pagina_atual = 0
    
    while True:
        print(f"Buscando página {pagina_atual}...")
        dados = api_agendamentos(professor_id=professor_id, page=pagina_atual)

        if dados is None:
            print("Erro na conexão ou resposta inesperada da API PACTO")
            break

        content = dados.get('content', [])

        for agendamento in content:
            yield agendamento

        # A coleta para quando a API não retorna mais conteúdo na lista 'content'.
        if not content:
            print("Fim da coleta de dados na API.")
            break
            
        pagina_atual += 1
#Função pra pegar os agendamentos filtrados por eventos         
def getAgendamentosFiltrados():
    eventos_filtros = ["Aula Experimental", "Primeiro Treino sem A.E", "Primeiro Treino com A.E"]

    agendamentos_filtrados = [ 
                          agendamento for agendamento in getDadosPaginados(getAgendamentosExecutados)
                          if agendamento.get('evento') in eventos_filtros
                          ]
    print(f"Coleta finalizada! Total bruto coletado: {len(agendamentos_filtrados)}")
    return agendamentos_filtrados

#Função pra conectar com a planilha de mkt
#Retorna um DF bruto, lógica de limpeza será feita no transform
def get_leads():

    print(" Conectando a planilha de mkt...")
    
    caminho_credenciais = Path(__file__).parent.parent / config.GOOGLE_JSON_FILE
    
    try:
        gc = gspread.service_account(filename=str(caminho_credenciais))
        sh = gc.open_by_key(config.GOOGLE_SHEETS_MKT)
        worksheet = sh.worksheet("Diária")
        
        # Leitura bruta (Lista de Listas)
        rows = worksheet.get_all_values()
        
        if not rows:
            print(" Erro: Planilha vazia.")
            return pd.DataFrame()

        # Tratamento de Cabeçalhos Duplicados (ex: 'Origem' e 'Origem')
        headers = rows[0]
        new_headers = []
        seen_headers = {}
        
        for h in headers:
            if h in seen_headers:
                seen_headers[h] += 1
                new_headers.append(f"{h}_{seen_headers[h]}")
            else:
                seen_headers[h] = 1
                new_headers.append(h)
        
        # Retorna o DataFrame Bruto
        return pd.DataFrame(rows[1:], columns=new_headers)

    except Exception as e:
        print(f" Erro ao ler planilha: {e}")
        return pd.DataFrame()