# extract.py
import requests
import json
import gspread
import pandas as pd
import time
from pathlib import Path
from datetime import datetime

import config 

# =============================================================================
# 1. FUNÇÕES AUXILIARES
# =============================================================================

def getDadosPaginados(api_agendamentos, professor_id=1):
    pagina_atual = 0
    while True:
        print(f"   --> Buscando página {pagina_atual}...")
        dados = api_agendamentos(professor_id=professor_id, page=pagina_atual)

        if dados is None:
            print("   ❌ Erro na API ou Conexão Interrompida.")
            break

        content = dados.get('content', [])
        for item in content:
            yield item

        if not content:
            break  
        pagina_atual += 1
        time.sleep(1.5) # Respeitando limite da API

# =============================================================================
# 2. AGENDAMENTOS (PACTO)
# =============================================================================

def getAgendamentos(path, professor_id=1, page=0, size=100, sort="nome,asc"):
    url = f"{config.URL_BASE}{path}"
    params = {
        "professorId": professor_id, "page": page, "size": size, "sort": sort,
        "filters": json.dumps({"professorId": professor_id})
    }
    try:
        response = requests.get(url, headers=config.HEADERS, params=params)
        if response.status_code == 200: return response.json()
        elif response.status_code == 429:
            time.sleep(60) # Espera 1 min se bloquear
            return getAgendamentos(path, professor_id, page, size, sort) # Tenta de novo
        else: return None
    except: return None

def getAgendamentosExecutados(professor_id=1, page=0, size=100, sort="nome,asc"):
    return getAgendamentos("/psec/treino-bi/agendamento-executaram", professor_id, page, size, sort)

def getAgendamentosFiltrados():
    print("--- Extraindo Agendamentos REAIS (Executados) ---")
    eventos_filtros = ["Aula Experimental", "Primeiro Treino sem A.E", "Primeiro Treino com A.E"]
    
    agendamentos = [ 
        a for a in getDadosPaginados(getAgendamentosExecutados)
        if a.get('evento') in eventos_filtros
    ]
    print(f"✅ Total recuperado da Pacto: {len(agendamentos)}")
    return agendamentos

# =============================================================================
# 3. LEADS (GOOGLE SHEETS)
# =============================================================================

def get_leads():
    print("--- Tentando conectar na Planilha de Marketing ---")
    
    caminho_pai = Path(__file__).parent.parent / config.GOOGLE_JSON_FILE
    caminho_atual = Path(__file__).parent / config.GOOGLE_JSON_FILE
    caminho_final = caminho_pai if caminho_pai.exists() else (caminho_atual if caminho_atual.exists() else None)
    
    if not caminho_final:
        print(f"⚠️ ARQUIVO '{config.GOOGLE_JSON_FILE}' NÃO ENCONTRADO.")
        print("   -> O sistema vai rodar SEM cruzar com o Marketing.")
        return pd.DataFrame() # Retorna vazio, sem inventar dados

    try:
        gc = gspread.service_account(filename=str(caminho_final))
        sh = gc.open_by_key(config.GOOGLE_SHEETS_MKT)
        rows = sh.worksheet("Diária").get_all_values()
        
        if not rows: return pd.DataFrame()

        # Tratamento de cabeçalhos duplicados
        headers = rows[0]
        new_headers = []
        seen = {}
        for h in headers:
            if h in seen: seen[h] += 1; new_headers.append(f"{h}_{seen[h]}")
            else: seen[h] = 1; new_headers.append(h)
            
        print(f"✅ Leads carregados do Google: {len(rows)-1}")
        return pd.DataFrame(rows[1:], columns=new_headers)

    except Exception as e:
        print(f"❌ Erro ao ler planilha do Google: {e}")
        return pd.DataFrame()

# =============================================================================
# 4. ALUNOS (PACTO API V2)
# =============================================================================

def getAlunosV2(professor_id=None, page=0, size=100, sort="nome,asc"):
    url = f"{config.URL_BASE}/psec/alunos/v2"
    params = {"page": page, "size": size, "sort": sort}
    
    for _ in range(3): # 3 Tentativas
        try:
            r = requests.get(url, headers=config.HEADERS, params=params)
            if r.status_code == 200: return r.json()
            elif r.status_code == 429: 
                print("⏳ API Ocupada. Aguardando..."); time.sleep(61)
            else: return None
        except: time.sleep(5)
    return None

def get_alunos_raw():
    print("--- Extraindo Base Completa de Alunos (Isso demora) ---")
    dados = list(getDadosPaginados(getAlunosV2, professor_id=None))
    df = pd.DataFrame(dados)
    print(f"✅ Total de Alunos Ativos/Inativos: {len(df)}")
    return df