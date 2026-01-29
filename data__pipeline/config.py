import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Variáveis de ambiente
TOKEN = os.getenv("TOKEN")
EMPRESA_ID = os.getenv("EMPRESA_ID")
URL_BASE = "https://apigw.pactosolucoes.com.br"

# Headers padrão da pacto
HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json",
    "empresaId": EMPRESA_ID
}

TP_ACADEMIA_DB_ID = os.getenv("TP_ACADEMIA_DB_ID")
GOOGLE_JSON_FILE = os.getenv("GOOGLE_JSON_FILE", "service_account.json")
GOOGLE_SHEETS_MKT = os.getenv("GOOGLE_SHEETS_MKT")

# Pega a raiz do projeto dinamicamente
ROOT_DIR = Path(__file__).resolve().parent.parent

# Caminho para a credencial do Google
_json_file_name = os.getenv("GOOGLE_JSON_FILE", "service_account.json")
GOOGLE_CREDENTIALS_PATH = ROOT_DIR / _json_file_name