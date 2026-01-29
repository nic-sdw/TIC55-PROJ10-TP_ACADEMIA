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
  
  
  # Completo o ETL carregando os dados no banco de dados (planilha Google)
  load.save_in_database(df_filtrado, nome_da_aba="HISTORICO")
  
  
  # --- 2. MARKETING (Leads) ---
  print("\n--- 2. MARKETING (Leads) ---")
    
  # Extract: Lê bruto da planilha (Nome corrigido para bater com o extract.py)
  df_mkt_bruto = extract.get_leads()
    
  # Transform: Limpa regex, filtra mês e vendedora
  df_mkt = transform.process_leads_marketing(df_mkt_bruto)

  if not df_mkt.empty:
      print(f"   Sucesso! {len(df_mkt)} leads processados e limpos.")
      print(df_mkt)
  else:
      print("   Aviso: Nenhum lead encontrado na planilha para este mês.")


  # --- 3. CRUZAMENTO (NOVO) ---
  # Aqui usamos a função consolidar_dados que criamos no transform.py
  print("\n--- 3. CONSOLIDAÇÃO ---")
  
  if not df_filtrado.empty:
      # Cruza Pacto (df_filtrado) com Marketing (df_mkt)
      df_final = transform.consolidar_dados(df_filtrado, df_mkt)
      print("------------------------------------------------------------")
      print(df_final)
      # Salva o relatório final
      load.save_in_database(df_final, nome_da_aba="RELATORIO_FINAL")
  else:
      print("   Sem dados da Pacto para gerar relatório.")

if __name__ == "__main__":
  run()