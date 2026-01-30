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
    url = f"{URL_BASE}{path}"
    time.sleep(1.3) # Proteção essencial contra erro 429
    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 429:
            print("\n⚠️ Limite atingido! Pausando por 60s...")
            time.sleep(60)
            return fetch_pacto_data(path, params)
        return None
    except Exception as e:
        print(f"Erro: {e}")
        return None

def extrair_todos_alunos_v2():
    print("--- 🚀 INICIANDO EXTRAÇÃO COMPLETA (BASE TOTAL V2) ---")
    
    path = "/psec/alunos/v2"
    pagina = 0
    tamanho_requisicao = 100 # Quantos alunos pedir por vez
    lista_final = []

    while True:
        params = {"page": pagina, "size": tamanho_requisicao}
        dados = fetch_pacto_data(path, params)
        
        if not dados: break
        
        content = dados.get('content', [])
        if not content: break # Sai do loop se a página estiver vazia
        
        lista_final.extend(content)
        print(f"   > Página {pagina} processada. Acumulado: {len(lista_final)} registros...", end='\r')
        
        # Condição de parada: se vieram menos do que o tamanho pedido, a base acabou
        if len(content) < tamanho_requisicao:
            break
            
        pagina += 1

    if lista_final:
        df_bruto = pd.DataFrame(lista_final)
        
        # 1. MAPEAMENTO DAS COLUNAS REAIS (ZW)
        colunas_map = {
            'matriculaZW': 'MATRICULA',
            'nome': 'ALUNO',
            'contratoZW': 'CONTRATO_ID',
            'situacaoContratoZW': 'STATUS',
            'dataMatriculaZW': 'DATA_MATRICULA'
        }
        
        df_processado = df_bruto.rename(columns=colunas_map)

        # 2. FILTRAGEM E ORGANIZAÇÃO
        # Selecionamos apenas as colunas que você quer ver lado a lado
        colunas_finais = ['MATRICULA', 'ALUNO', 'CONTRATO_ID', 'STATUS', 'DATA_MATRICULA']
        existentes = [c for c in colunas_finais if c in df_processado.columns]
        
        df_resultado = df_processado[existentes].copy()

        # 3. EXPORTAÇÃO FINAL
        nome_arquivo = "base_completa_alunos_contratos.csv"
        df_resultado.to_csv(nome_arquivo, index=False, sep=';', encoding='utf-8-sig')
        
        print(f"\n\n✅ SUCESSO! Base total extraída: {len(df_resultado)} registros.")
        print(f"📂 Arquivo gerado: {nome_arquivo}")
    else:
        print("\n⚠️ Nenhum dado foi encontrado.")

if __name__ == "__main__":
    extrair_todos_alunos_v2()