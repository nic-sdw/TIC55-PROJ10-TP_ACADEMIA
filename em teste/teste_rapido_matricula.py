import os
import requests
import pandas as pd
import time
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.getenv("TOKEN")
EMPRESA_ID = os.getenv("EMPRESA_ID")
URL_BASE = "https://apigw.pactosolucoes.com.br"

headers = {
    "Authorization": f"Bearer {TOKEN}",
    "empresaId": EMPRESA_ID,
    "Content-Type": "application/json"
}

def fetch_pacto_data(path, params):
    """Handler com proteção contra Rate Limit"""
    url = f"{URL_BASE}{path}"
    time.sleep(1.3) # Respeita o limite de 50 req/min
    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 429:
            print("\n⚠️ Rate Limit! Aguardando 60s...")
            time.sleep(60)
            return fetch_pacto_data(path, params)
        return None
    except Exception as e:
        print(f"Erro na conexão: {e}")
        return None

def extrair_base_completa_v2():
    print("--- INICIANDO EXTRAÇÃO COMPLETA V2 (MATRÍCULA + ALUNO + CONTRATO) ---")
    
    path = "/psec/alunos/v2"
    pagina = 0
    tamanho_pagina = 100
    lista_acumulada = []

    while True:
        params = {"page": pagina, "size": tamanho_pagina}
        dados = fetch_pacto_data(path, params)
        
        if not dados:
            break
            
        content = dados.get('content', [])
        if not content:
            break
            
        lista_acumulada.extend(content)
        print(f"   > Página {pagina} processada. Total acumulado: {len(lista_acumulada)}", end='\r')
        
        # Se a página atual veio com menos registros que o tamanho definido, chegamos ao fim
        if len(content) < tamanho_pagina:
            break
            
        pagina += 1

    if lista_acumulada:
        df_bruto = pd.DataFrame(lista_acumulada)
        
        # 1. MAPEAMENTO COM AS COLUNAS ZW IDENTIFICADAS
        colunas_alvo = {
            'matriculaZW': 'MATRICULA',
            'nome': 'ALUNO',
            'contratoZW': 'CONTRATO_ID',
            'situacaoContratoZW': 'STATUS_CONTRATO',
            'dataMatriculaZW': 'DATA_MATRICULA'
        }
        
        df_processado = df_bruto.rename(columns=colunas_alvo)
        
        # 2. ORGANIZAÇÃO LADO A LADO
        colunas_finais = ['MATRICULA', 'ALUNO', 'CONTRATO_ID', 'STATUS_CONTRATO', 'DATA_MATRICULA']
        existentes = [c for c in colunas_finais if c in df_processado.columns]
        
        df_final = df_processado[existentes].copy()

        # 3. EXPORTAÇÃO
        data_string = datetime.now().strftime("%Y%m%d_%H%M")
        nome_arquivo = f"base_alunos_contratos_{data_string}.csv"
        df_final.to_csv(nome_arquivo, index=False, sep=';', encoding='utf-8-sig')
        
        print(f"\n\n✅ SUCESSO! Base total exportada: {len(df_final)} registros.")
        print(f"📂 Arquivo gerado: {nome_arquivo}")
    else:
        print("\n⚠️ Nenhum dado foi extraído.")

if __name__ == "__main__":
    extrair_base_completa_v2()