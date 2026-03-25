import gspread
import pandas as pd
from . import config

def connect_google_sheets():
    try:
        return gspread.service_account(filename=str(config.GOOGLE_CREDENTIALS_PATH))
    except Exception as e:
        print(f"ERRO: Falha na autenticação do Google: {e}")
        return None
  
def save_in_database(df, nome_da_aba="Historico"):
  if df is None or df.empty:
    print("O df chegou vazio, nada será enviado ao banco de dados.")
    return
  print(f"Conectando ao 'GS', para salvar {len(df)} linhas...")
  
  client = connect_google_sheets()
  
  if not client:
    return
  
  try:
    
    sheet = client.open_by_key(config.TP_ACADEMIA_DB_ID)
    
    try:
        worksheet = sheet.worksheet(nome_da_aba)
        valores = worksheet.get_all_values()
        if valores:
            df_existente = pd.DataFrame(valores[1:], columns=valores[0])
        else:
            df_existente = pd.DataFrame()
    except gspread.WorksheetNotFound:
      print(f"Aba '{nome_da_aba}', não encontrada... Criando uma nova!")
      worksheet = sheet.add_worksheet(title=nome_da_aba, rows=1000, cols=20)
      df_existente = pd.DataFrame()
      
    df_limpo = df.fillna('')
    df_limpo = df_limpo.astype(str)
    
    if not df_existente.empty:
        df_existente = df_existente.reindex(columns=df_limpo.columns).fillna('').astype(str)
        df_combinado = pd.concat([df_existente, df_limpo], ignore_index=True)
        
        if nome_da_aba == 'VENDAS_MKT':
          df_combinado = df_combinado.drop_duplicates(subset=['ALUNO'], keep='last')
        else:
          df_combinado = df_combinado.drop_duplicates(keep='last')
    else:
        df_combinado = df_limpo
        
    dados_para_enviar = [df_combinado.columns.values.tolist()] + df_combinado.values.tolist()
            
    # ALTERAÇÃO 3: Limpa a planilha de qualquer lixo antes de enviar
    worksheet.clear()
    worksheet.update(range_name='A1', values=dados_para_enviar)
      
  except Exception as e:
    print(f"ERRO ao salvar a planilha: {e}")
    
