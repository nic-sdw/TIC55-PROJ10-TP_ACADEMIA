# config.py
import os
from pathlib import Path
from dotenv import load_dotenv

# 1. Carrega variáveis de ambiente
# O load_dotenv procura um arquivo .env na raiz do projeto
load_dotenv()

# 2. Definição de Caminhos (Paths)
# __file__ é este arquivo (config.py). 
# .parent é a pasta 'data__pipeline'.
# .parent.parent é a pasta 'TP10' (Raiz do projeto).
BASE_DIR = Path(__file__).resolve().parent
ROOT_DIR = BASE_DIR.parent

# Nome do arquivo JSON
_json_file_name = os.getenv("GOOGLE_JSON_FILE", "service_account.json")

# Lógica de Busca do JSON:
# Tenta achar na raiz (TP10) primeiro. Se não achar, tenta na pasta local (data__pipeline).
path_raiz = ROOT_DIR / _json_file_name
path_local = BASE_DIR / _json_file_name

if path_raiz.exists():
    GOOGLE_CREDENTIALS_PATH = path_raiz
elif path_local.exists():
    GOOGLE_CREDENTIALS_PATH = path_local
else:
    # Se não achar em lugar nenhum, define um padrão na raiz para o erro aparecer depois
    GOOGLE_CREDENTIALS_PATH = path_raiz

# 3. Configurações da API PACTO
TOKEN = os.getenv("TOKEN")
EMPRESA_ID = os.getenv("EMPRESA_ID")
URL_BASE = "https://apigw.pactosolucoes.com.br"

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json",
    "Accept": "application/json",
    "EmpresaId": str(EMPRESA_ID) # Garante que seja string
}

# 4. Configurações do Google Sheets
TP_ACADEMIA_DB_ID = os.getenv("TP_ACADEMIA_DB_ID")
GOOGLE_SHEETS_MKT = os.getenv("GOOGLE_SHEETS_MKT")