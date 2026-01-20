import gspread
from google.oauth2.service_account import Credentials
import pandas as pd

def salvar_no_google_sheets(df, nome_planilha, nome_aba="Página1"):
    """
    Recebe um DataFrame e o envia para uma aba específica do Google Sheets.
    """
    print(f"Conectando ao Google Sheets: {nome_planilha}...")
    
    # 1. Autenticação usando o arquivo credentials.json
    escopos = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    
    try:
        creds = Credentials.from_service_account_file("credentials.json", scopes=escopos)
        client = gspread.authorize(creds)
        
        # 2. Abre a planilha (pelo nome ou pela chave/ID)
        spreadsheet = client.open(nome_planilha)
        
        # 3. Seleciona a aba (Worksheet)
        try:
            sheet = spreadsheet.worksheet(nome_aba)
        except:
            # Se a aba não existir, cria uma nova
            sheet = spreadsheet.add_worksheet(title=nome_aba, rows=1000, cols=20)
            
        # 4. Limpa o conteúdo antigo para não duplicar
        sheet.clear()
        
        # 5. Prepara os dados para envio (Headers + Linhas)
        # O gspread espera uma lista de listas, não um DataFrame direto
        dados_lista = [df.columns.values.tolist()] + df.values.tolist()
        
        # 6. Envia os dados
        sheet.update(dados_lista)
        
        print("Sucesso! Dados atualizados no Google Sheets.")
        
    except FileNotFoundError:
        print("ERRO: O arquivo 'credentials.json' não foi encontrado na pasta.")
    except Exception as e:
        print(f"ERRO ao salvar no Google Sheets: {e}")