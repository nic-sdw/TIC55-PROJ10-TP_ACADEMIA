from transform import getAgendamentosLimpos
import pandas as pd
import os

def main():
    print("--- INICIANDO TESTE LOCAL ---")
    print("1. Conectando na API da Pacto e baixando dados...")
    
    # Chama a função de transformação (que usa a extração internamente)
    try:
        df_final = getAgendamentosLimpos()
    except Exception as e:
        print(f"ERRO CRÍTICO: Não foi possível baixar os dados. Verifique o .env. Detalhe: {e}")
        return

    # Verifica se veio algo
    if df_final is not None and not df_final.empty:
        qtd = len(df_final)
        print(f"\n2. SUCESSO! {qtd} agendamentos baixados e processados.")
        
        # Mostra as primeiras 3 linhas para você conferir na tela se separou os nomes
        print("\n--- Amostra dos Dados (Primeiras 3 linhas) ---")
        colunas_confere = ['ALUNO_PRINCIPAL', 'ALUNO_ACOMPANHANTE', 'QTD_PESSOAS']
        # Verifica se as colunas existem antes de imprimir (segurança)
        cols_existentes = [c for c in colunas_confere if c in df_final.columns]
        print(df_final[cols_existentes].head(3))
        
        # Salva o arquivo
        nome_arquivo = "teste_agendamentos.csv"
        print(f"\n3. Salvando arquivo '{nome_arquivo}'...")
        df_final.to_csv(nome_arquivo, index=False, encoding='utf-8-sig', sep=';')
        
        print(f"CONCLUÍDO: Abra o arquivo '{nome_arquivo}' na pasta do projeto.")
        
    else:
        print("\nAVISO: O script rodou, mas a lista de agendamentos veio vazia.")
        print("Dica: Verifique se há agendamentos nos filtros (Aula Experimental, etc) na data de hoje/ontem.")

if __name__ == "__main__":
    main()