# load.py
import gspread
import pandas as pd
import requests
import json
from datetime import datetime
import os

# Importação direta
import config 

# =============================================================================
# 1. CONEXÃO COM GOOGLE SHEETS
# =============================================================================
def connect_google_sheets():
    """
    Estabelece a conexão com o Google Sheets usando as credenciais.
    """
    try:
        # Verifica se o arquivo existe antes de tentar conectar
        if not os.path.exists(str(config.GOOGLE_CREDENTIALS_PATH)):
            return None
            
        return gspread.service_account(filename=str(config.GOOGLE_CREDENTIALS_PATH))
    except Exception as e:
        print(f"❌ Erro na autenticação do Google Sheets: {e}")
        return None

# =============================================================================
# 2. SALVAR NO BANCO DE DADOS (COM FALLBACK LOCAL)
# =============================================================================
def save_in_database(df, nome_da_aba="Historico"):
    """
    Salva o DataFrame no Google Sheets. 
    SE FALHAR (sem internet ou sem credencial), salva em EXCEL localmente.
    """
    if df is None or df.empty:
        print(f"⚠️ O DataFrame para '{nome_da_aba}' está vazio. Nada salvo.")
        return

    print(f"💾 Salvando {len(df)} linhas em '{nome_da_aba}'...")
    
    # --- TENTATIVA 1: GOOGLE SHEETS ---
    salvo_no_google = False
    client = connect_google_sheets()
    
    if client:
        try:
            sheet = client.open_by_key(config.TP_ACADEMIA_DB_ID)
            
            try:
                worksheet = sheet.worksheet(nome_da_aba)
            except gspread.WorksheetNotFound:
                worksheet = sheet.add_worksheet(title=nome_da_aba, rows=1000, cols=20)
            
            # Tratamento para JSON (NaN -> '')
            df_limpo = df.fillna('').astype(str)
            dados = [df_limpo.columns.values.tolist()] + df_limpo.values.tolist()
            
            worksheet.clear()
            worksheet.update(range_name='A1', values=dados)
            print(f"✅ SUCESSO! Dados salvos na nuvem (Google Sheets).")
            salvo_no_google = True
            
        except Exception as e:
            print(f"❌ Falha ao gravar no Google: {e}")
    else:
        print("⚠️ Conexão com Google Sheets não disponível (Arquivo .json ausente ou erro).")

    # --- TENTATIVA 2: LOCAL (SE O GOOGLE FALHAR) ---
    if not salvo_no_google:
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M")
            nome_arquivo = f"BACKUP_{nome_da_aba}_{timestamp}.xlsx"
            
            df.to_excel(nome_arquivo, index=False)
            print(f"✅ SALVO LOCALMENTE: Os dados estão seguros no arquivo '{nome_arquivo}'")
        except Exception as e:
            print(f"❌ Erro crítico: Não foi possível salvar nem localmente: {e}")

# =============================================================================
# 3. ENVIAR PARA BACKEND (WEBHOOK / API)
# =============================================================================
def enviar_conversoes_backend(df_final, url_backend, token_backend=None):
    """
    Envia vendas para API externa.
    Prioriza 'STATUS_FINAL' == 'VENDA NOVA' se existir (para evitar alunos antigos).
    """
    print("\n🚀 INICIANDO ENVIO PARA O BACKEND...")
    
    if df_final.empty:
        print("⚠️ Nada para enviar.")
        return

    # 1. Filtra as vendas corretas
    vendas = pd.DataFrame()
    
    # Lógica inteligente: Se tiver validação de data, usa ela (Mais seguro)
    if 'STATUS_FINAL' in df_final.columns:
        vendas = df_final[df_final['STATUS_FINAL'] == 'VENDA NOVA'].copy()
    
    # Se não tiver validação de data, usa o Fuzzy Match puro
    elif 'SITUACAO_CONVERSAO' in df_final.columns:
        vendas = df_final[df_final['SITUACAO_CONVERSAO'] == 'CONVERTIDO'].copy()
        
    elif 'matriculaZW' in df_final.columns:
        vendas = df_final[df_final['matriculaZW'].notna()].copy()

    if vendas.empty:
        print("ℹ️ Nenhuma venda nova qualificada para envio.")
        return

    print(f"📤 Preparando para enviar {len(vendas)} conversões...")

    # 2. Configura Headers
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    if token_backend:
        headers["Authorization"] = f"Bearer {token_backend}"

    sucessos = 0
    falhas = 0

    # 3. Envia um por um
    for index, row in vendas.iterrows():
        try:
            # Payload padrão
            payload = {
                "nome_lead": row.get('ALUNO', ''),
                "matricula": row.get('matriculaZW', ''),
                "plano": row.get('planoZW', ''),
                "origem": row.get('ORIGEM', 'Desconhecido'),
                "vendedora": row.get('VENDEDORA', ''),
                "data_matricula": str(row.get('dataMatriculaZW', '')),
                "data_lead": str(row.get('DATA_LEAD', '')),
                "score_match": int(row.get('match_score', 0)),
                "status": row.get('STATUS_FINAL', 'CONVERTIDO'),
                "data_processamento": datetime.now().isoformat()
            }

            # POST
            response = requests.post(url_backend, json=payload, headers=headers)

            if response.status_code in [200, 201]:
                print(f"   ✅ Enviado: {payload['nome_lead']}")
                sucessos += 1
            else:
                print(f"   ❌ Erro API ({response.status_code}): {payload['nome_lead']}")
                falhas += 1

        except Exception as e:
            print(f"   ❌ Erro de conexão: {e}")
            falhas += 1

    print(f"🏁 ENVIO CONCLUÍDO. Sucessos: {sucessos} | Falhas: {falhas}")