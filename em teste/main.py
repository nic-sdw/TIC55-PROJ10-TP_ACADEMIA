import pandas as pd
import os
from transform import getAgendamentosLimpos, getMatriculasLimpas 

def gerar_log_auditoria(df_agendamentos, df_matriculas, vendas_convertidas):
    """Gera um resumo técnico de inconsistências e leads perdidos"""
    print("\n[LOG] Gerando Auditoria de Dados...")
    
    # 1. Identificar quem agendou mas não foi encontrado nas matrículas (Leads Perdidos)
    # Usamos o 'ALUNO' como chave se a 'MATRICULA' for instável
    col_chave = 'MATRICULA' if 'MATRICULA' in df_agendamentos.columns else 'ALUNO'
    
    leads_perdidos = df_agendamentos[~df_agendamentos[col_chave].isin(df_matriculas[col_chave])]
    
    # 2. Identificar registros sem Identificador Único (Matrícula)
    registros_sem_id = df_agendamentos[df_agendamentos['MATRICULA'].isna() | (df_agendamentos['MATRICULA'] == "")]

    # 3. Criar Resumo
    resumo = {
        "Data Processamento": pd.Timestamp.now().strftime("%d/%m/%Y %H:%M"),
        "Total Agendamentos": len(df_agendamentos),
        "Total Matrículas (60d)": len(df_matriculas),
        "Conversões Identificadas": len(vendas_convertidas),
        "Leads sem Matrícula (Inconsistentes)": len(registros_sem_id),
        "Leads não Convertidos (Perdidos)": len(leads_perdidos)
    }
    
    # Salva o Log em TXT para leitura rápida
    with open("log_auditoria.txt", "w", encoding="utf-8") as f:
        f.write("=== RELATÓRIO DE AUDITORIA DE DADOS ===\n")
        for chave, valor in resumo.items():
            f.write(f"{chave}: {valor}\n")
            print(f"   > {chave}: {valor}")
    
    # Salva a lista de Leads Perdidos para o time comercial agir
    if not leads_perdidos.empty:
        leads_perdidos.to_csv("leads_para_recuperacao.csv", index=False, sep=";", encoding="utf-8-sig")
        print(f"   > Lista de recuperação salva em: leads_para_recuperacao.csv")

def main():
    print("--- INICIANDO ROTINA GERAL (AGENDAMENTOS + VENDAS) ---")

    df_agendamentos = pd.DataFrame()
    df_matriculas = pd.DataFrame()

    # 1. PROCESSO DE AGENDAMENTOS
    try:
        df_agendamentos = getAgendamentosLimpos()
        if not df_agendamentos.empty:
            df_agendamentos.to_csv("resultado_agendamentos.csv", index=False, encoding='utf-8-sig', sep=';')
    except Exception as e:
        print(f"   > ERRO em Agendamentos: {e}")

    # 2. PROCESSO DE MATRÍCULAS (VENDAS)
    try:
        df_matriculas = getMatriculasLimpas(dias=60)
        if not df_matriculas.empty:
            df_matriculas.to_csv("resultado_matriculas.csv", index=False, encoding='utf-8-sig', sep=';')
    except Exception as e:
        print(f"   > ERRO em Matrículas: {e}")

    # 3. ANÁLISE DE CONVERSÃO E AUDITORIA
    if not df_agendamentos.empty and not df_matriculas.empty:
        print("\n[3/3] Calculando Taxa de Conversão...")
        
        # Lógica de cruzamento (Fallback para ALUNO se MATRICULA falhar)
        if 'MATRICULA' in df_agendamentos.columns and 'MATRICULA' in df_matriculas.columns:
            vendas_convertidas = df_agendamentos[df_agendamentos['MATRICULA'].isin(df_matriculas['MATRICULA'])]
        else:
            vendas_convertidas = df_agendamentos[df_agendamentos['ALUNO'].isin(df_matriculas['ALUNO'])]
        
        # Executa a Auditoria
        gerar_log_auditoria(df_agendamentos, df_matriculas, vendas_convertidas)
        
        if not vendas_convertidas.empty:
            vendas_convertidas.to_csv("relatorio_conversao_final.csv", index=False, encoding='utf-8-sig', sep=';')
    else:
        print("\n[AVISO] Dados insuficientes para auditoria completa.")

    print("\n--- FIM DO PROCESSO ---")

if __name__ == "__main__":
    main()