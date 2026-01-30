import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv

# --- 1. CONFIGURAÇÃO DE AMBIENTE ---
load_dotenv()

TOKEN = os.getenv("TOKEN")
EMPRESA_ID = os.getenv("EMPRESA_ID")
URL_BASE = "https://apigw.pactosolucoes.com.br"

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json",
    "empresaId": EMPRESA_ID
}

def extrair_contratos_periodo(dias=30):
    print(f"--- INICIANDO EXTRAÇÃO DE CONTRATOS (ÚLTIMOS {dias} DIAS) ---")
    
    # Endpoint exato do seu mapeamento JSON
    path = "/psec/alunos/lista/matriculas"
    url = f"{URL_BASE}{path}"
    
    # Cálculo das datas
    data_inicio = (datetime.now() - timedelta(days=dias)).strftime("%Y-%m-%d")
    data_fim = datetime.now().strftime("%Y-%m-%d")
    
    pagina = 0
    tamanho_pagina = 100
    todos_contratos = []

    try:
        while True:
            # Parâmetros baseados na assinatura do método Java no seu JSON
            params = {
                "dataInicio": data_inicio,
                "dataFim": data_fim,
                "page": pagina,
                "size": tamanho_pagina,
                "professorId": 1 # Valor padrão comum para BI da Pacto
            }
            
            print(f" > Capturando página {pagina}...")
            response = requests.get(url, headers=HEADERS, params=params, timeout=30)
            
            if response.status_code == 200:
                dados = response.json()
                content = dados.get('content', [])
                
                if not content:
                    print(" > Fim dos registros encontrados.")
                    break
                
                todos_contratos.extend(content)
                print(f"   - {len(content)} registros adicionados.")
                
                # Se a página veio incompleta, é a última
                if len(content) < tamanho_pagina:
                    break
                
                pagina += 1
            else:
                print(f"❌ Erro {response.status_code}: {response.text}")
                break

        if todos_contratos:
            df = pd.DataFrame(todos_contratos)
            
            # Mapeamento de colunas para o CSV final
            colunas_finais = {
                'nome': 'ALUNO',
                'matricula': 'MATRICULA',
                'dataInicio': 'DATA_INICIO',
                'dataVencimento': 'DATA_VENCIMENTO',
                'descricaoPlano': 'PLANO',
                'valorTotal': 'VALOR_TOTAL',
                'situacao': 'STATUS'
            }
            
            # Filtra apenas as colunas que existem no retorno da API
            existentes = {k: v for k, v in colunas_finais.items() if k in df.columns}
            df = df[list(existentes.keys())].rename(columns=existentes)
            
            # Salva o arquivo CSV
            nome_arquivo = "contratos_30_dias.csv"
            df.to_csv(nome_arquivo, index=False, sep=';', encoding='utf-8-sig')
            
            print(f"\n✅ SUCESSO! {len(df)} contratos extraídos.")
            print(f"📂 Arquivo gerado: {nome_arquivo}")
        else:
            print("⚠️ Nenhum contrato encontrado nos últimos 30 dias.")

    except Exception as e:
        print(f"🛑 Erro crítico: {e}")

if __name__ == "__main__":
    extrair_contratos_periodo(30)