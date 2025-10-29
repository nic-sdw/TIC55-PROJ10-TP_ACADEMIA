import requests as rq
from dotenv import load_dotenv
import os
import json
import datetime

load_dotenv()

urlBase = "https://apigw.pactosolucoes.com.br"

def getApi(token):

  url_empresas = f"{urlBase}/v1/empresa/ativas"
  headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
  }
