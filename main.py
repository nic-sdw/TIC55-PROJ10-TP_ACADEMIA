import os
from dotenv import load_dotenv

from data__pipeline import extract
from data__pipeline import transform
from data__pipeline import load

def run():
  
  load_dotenv()
  # Aqui eu crio uma variável que extrai e filtra os dados no extract.py
  dados = extract.getAgendamentosFiltrados() 
  # Passo os 'dados' por parâmetro e executo a função para tratar os dados e jogo para a variável 'df_filtrado'
  df_filtrado = transform.getAgendamentosLimpos(dados)
  
  print(df_filtrado)
  
  # Completo o ETL carregando os dados no banco de dados (planilha Google)
  load.save_in_database(df_filtrado, nome_da_aba="HISTORICO")
  
if __name__ == "__main__":
  run() 