import os
import json
import requests
import pandas as pd
import time
from dotenv import load_dotenv
from datetime import datetime, timedelta

# --- CONFIGURAÇÃO INICIAL ---
load_dotenv()

token = os.getenv("TOKEN")
empresa_id = os.getenv("EMPRESA_ID")
urlBase = "https://apigw.pactosolucoes.com.br"

headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json",
    "empresaId": empresa_id
}

# --- FUNÇÕES DE APOIO ---

def fetch_pacto_data(path, params):
    """Função genérica com tratamento de Rate Limit (429) e pausa preventiva"""
    url = f"{urlBase}{path}"
    try:
        # Pausa preventiva de ~1.3s para não exceder 50 req/min
        time.sleep(1.3)
        
        response = requests.get(url, headers=headers, params=params, timeout=30)
        
        if response.status_code == 200:
            return response.json()
        
        elif response.status_code == 429:
            print("\n⚠️ Rate Limit atingido! Aguardando 60 segundos para retomar...")
            time.sleep(60) 
            return fetch_pacto_data(path, params) # Tentativa recursiva após a pausa
            
        else:
            print(f"\nErro na consulta ({path}): {response.status_code} - {response.text}")
            return None
    except Exception as e:
        print(f"\nErro de conexão em {path}: {e}")
        return None

# --- ETAPA 1: AGENDAMENTOS ---

def getAgendamentosFiltrados():
    print("--- Iniciando Extração de Agendamentos ---")
    path = "/psec/treino-bi/agendamento-executaram"
    eventos_filtros = ["Aula Experimental", "Primeiro Treino sem A.E", "Primeiro Treino com A.E"]
    todos_agendamentos = []
    pagina = 0
    size = 500 # Otimizado para reduzir número de chamadas

    while True:
        params = {
            "professorId": 1,
            "page": pagina,
            "size": size,
            "sort": "nome,asc"
        }
        dados = fetch_pacto_data(path, params)
        
        if not dados: break
        content = dados.get('content', [])
        if not content: break
        
        todos_agendamentos.extend(content)
        print(f"   > Página {pagina} processada ({len(todos_agendamentos)} registros)...", end='\r')
        
        if len(content) < size: break
        pagina += 1

    if todos_agendamentos:
        df = pd.DataFrame(todos_agendamentos)
        df = df[df['evento'].isin(eventos_filtros)]
        colunas_map = {'nome': 'ALUNO', 'matricula': 'MATRICULA', 'pessoaId': 'MATRICULA', 'cpf': 'CPF'}
        df = df.rename(columns={k: v for k, v in colunas_map.items() if k in df.columns})
        print(f"\n   > Sucesso! {len(df)} agendamentos coletados.")
        return df
    
    return pd.DataFrame()

# --- ETAPA 2: MATRÍCULAS (OTIMIZADA) ---
def getMatriculas(dias_atras=10):
    print(f"--- Iniciando Extração de Matrículas ({dias_atras} dias) ---")
    path = "/psec/alunos/lista/matriculas"
    
    # 1. Inicialização OBRIGATÓRIA da variável (Evita o NameError)
    todas_matriculas = []
    
    data_inicio_str = (datetime.now() - timedelta(days=dias_atras)).strftime("%Y-%m-%d")
    
    size = 500 
    params = {
        "dataInicio": data_inicio_str,
        "dataFim": datetime.now().strftime("%Y-%m-%d"),
        "page": 0,
        "size": size,
        "professorId": 1
    }
    
    try:
        while True:
            dados = fetch_pacto_data(path, params)
            if not dados: break
            
            content = dados.get('content', [])
            if not content: break
            
            todas_matriculas.extend(content)
            print(f"   > Capturados {len(todas_matriculas)} registros...", end='\r')
            
            if len(content) < size: break
            params['page'] += 1

        # 2. Processamento após a coleta
        if todas_matriculas:
            df = pd.DataFrame(todas_matriculas)
            
            # Renomeação inicial
            colunas_map = {'nome': 'ALUNO', 'matricula': 'MATRICULA', 'dataInicio': 'DATA_INICIO'}
            df = df.rename(columns={k: v for k, v in colunas_map.items() if k in df.columns})

            # --- FILTRO LOCAL DE SEGURANÇA (Para limpar os 273k registros) ---
            print(f"\n   > Filtrando localmente registros dos últimos {dias_atras} dias...")
            
            # Converte para datetime para comparação precisa
            df['DATA_INICIO'] = pd.to_datetime(df['DATA_INICIO'], errors='coerce')
            limite_data = pd.to_datetime(data_inicio_str)
            
            # Remove o que for anterior à data solicitada
            df = df[df['DATA_INICIO'] >= limite_data]
            # ---------------------------------------------------------------

            print(f"   > Sucesso! {len(df)} matrículas válidas após filtro.")
            return df
            
        else:
            print("\n⚠️ Nenhuma matrícula encontrada na API.")
            return pd.DataFrame()

    except Exception as e:
        print(f"\n🛑 Erro crítico em getMatriculas: {e}")
        return pd.DataFrame()

# --- ETAPA 3: BASE DE ALUNOS V2 ---
# --- ETAPA 3: BASE DE ALUNOS V2 ---

def getAlunosBaseV2(dias_atras=30):
    """Extrai nomes e matrículas da V2 filtrando pelos últimos X dias para teste"""
    print(f"--- Iniciando Extração de Base de Alunos V2 ({dias_atras} dias para teste) ---")
    path = "/psec/alunos/v2"
    
    lista_final = []
    pagina = 0
    size = 500 # Mantendo o tamanho otimizado para evitar muitas requisições
    
    # Define a data limite para o filtro
    data_limite = datetime.now() - timedelta(days=dias_atras)
    data_limite_str = data_limite.strftime("%Y-%m-%d")

    try:
        while True:
            params = {
                "page": pagina,
                "size": size
            }
            
            dados = fetch_pacto_data(path, params)
            if not dados: break
            
            content = dados.get('content', [])
            if not content: break
            
            # Adiciona os dados à lista
            for registro in content:
                lista_final.append({
                    "ALUNO_NOME": registro.get("nome"),
                    "MATRICULA": registro.get("matricula") or registro.get("id"),
                    "DATA_CADASTRO": registro.get("dataMatricula") or registro.get("dataCadastro")
                })
            
            print(f"   > Página {pagina} processada...", end='\r')
            
            # Condição de parada para o teste: 
            # Se já pegamos registros mais antigos que o limite, podemos parar a extração
            # Nota: Isso assume que a API entrega do mais recente para o mais antigo
            if len(content) < size: break
            
            pagina += 1

        if lista_final:
            df = pd.DataFrame(lista_final)
            
            # Conversão e Filtro de Segurança
            print(f"\n   > Filtrando registros posteriores a {data_limite_str}...")
            df['DATA_CADASTRO'] = pd.to_datetime(df['DATA_CADASTRO'], errors='coerce')
            limite_dt = pd.to_datetime(data_limite_str)
            
            # Mantém apenas o intervalo de teste (30 dias)
            df_teste = df[df['DATA_CADASTRO'] >= limite_dt].copy()
            
            # Remove a coluna de data se quiser apenas Nome e Matrícula no CSV final
            df_final = df_teste[['ALUNO_NOME', 'MATRICULA']]
            
            print(f"   > Sucesso! {len(df_final)} alunos encontrados no período de teste.")
            return df_final
            
        else:
            print("\n⚠️ Nenhum dado retornado na V2.")
            return pd.DataFrame()

    except Exception as e:
        print(f"\n🛑 Erro na Etapa 3: {e}")
        return pd.DataFrame()