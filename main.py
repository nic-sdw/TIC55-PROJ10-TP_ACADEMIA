import os
import pandas as pd
from dotenv import load_dotenv

# Importa os módulos do pipeline
from data__pipeline import extract
from data__pipeline import transform
from data__pipeline import load

# --- CONFIGURAÇÃO DO BACKEND ---
URL_WEBHOOK_BACKEND = os.getenv("URL_WEBHOOK_BACKEND")

def run():
    # Carrega variáveis de ambiente
    load_dotenv()
    
    print("###########################################################")
    print("### INICIANDO PIPELINE DE DADOS (ETL) ###")
    print("###########################################################")

    # =========================================================================
    # PREPARAÇÃO: DADOS COMUNS (LEADS)
    # =========================================================================
    print("\n>>> 0. PREPARAÇÃO: BAIXANDO LEADS (USADO NAS DUAS ETAPAS)")
    # Precisamos dos leads tanto para os Agendamentos quanto para as Vendas
    df_mkt_bruto = extract.get_leads()
    df_leads_tratado = transform.process_leads_marketing(df_mkt_bruto)
    
    if df_leads_tratado.empty:
        print("   ⚠️ Aviso: Nenhum lead encontrado. O cruzamento será limitado.")

    # =========================================================================
    # PARTE 1: OPERACIONAL (AGENDAMENTOS COM ORIGEM)
    # =========================================================================
    print("\n>>> 1. OPERACIONAL: AGENDAMENTOS E ORIGEM")
    
    # 1. Extract (Agendamentos Executados)
    dados_agendamentos = extract.getAgendamentosFiltrados() 
    
    # 2. Transform (Limpeza)
    df_agendamentos_limpo = transform.getAgendamentosLimpos(dados_agendamentos)
    
    # 3. Transform (Cruzamento com Leads) - NOVIDADE!
    # Agora sabemos se quem agendou veio do Instagram
    if not df_agendamentos_limpo.empty and not df_leads_tratado.empty:
        df_agendamentos_final = transform.cruzar_agendamentos_com_leads(
            df_agendamentos_limpo, 
            df_leads_tratado, 
            threshold=85
        )
    else:
        df_agendamentos_final = df_agendamentos_limpo

    # 4. Load (Salva Histórico)
    if not df_agendamentos_final.empty:
        load.save_in_database(df_agendamentos_final, nome_da_aba="HISTORICO_TREINOS")
    else:
        print("   Aviso: Nenhum agendamento para salvar.")

    # =========================================================================
    # PARTE 2: COMERCIAL (VENDAS REAIS)
    # =========================================================================
    print("\n>>> 2. COMERCIAL: VENDAS E VALIDAÇÃO DE DATA")
    
    if df_leads_tratado.empty:
        print("   ❌ Sem leads para processar vendas.")
    
    else:
        # 1. Extract (Alunos API)
        print("   [A] Buscando Base de Alunos na API...")
        df_alunos_api = extract.get_alunos_raw()

        if df_alunos_api.empty:
            print("   ❌ Erro: API de alunos vazia ou falha na conexão.")
        
        else:
            # 2. Transform (Fuzzy Match - Quem é quem?)
            print("   [B] Cruzando Nomes (Fuzzy Match)...")
            df_fuzzy = transform.cruzar_vendas_fuzzy(
                df_leads_tratado, 
                df_alunos_api, 
                threshold=85
            )

            # 3. Transform (Validação de Datas - É venda nova?) - NOVIDADE!
            # Separa "Venda Nova" de "Aluno Antigo"
            df_relatorio_final = transform.cruzar_com_matriculas(df_fuzzy)

            # 4. Load (Salva Relatório Completo)
            print("   [C] Salvando Relatório de Vendas...")
            load.save_in_database(df_relatorio_final, nome_da_aba="RELATORIO_VENDAS")

            # 5. Load (Webhook - Apenas Vendas Novas)
            if URL_WEBHOOK_BACKEND:
                print("   [D] Verificando envios para o Backend...")
                
                # Filtra apenas o que é VENDA NOVA confirmada
                # (Ignora 'Aluno Já Ativo' e 'Não Encontrado')
                vendas_para_envio = df_relatorio_final[
                    df_relatorio_final['STATUS_FINAL'] == 'VENDA NOVA'
                ]
                
                if not vendas_para_envio.empty:
                    load.enviar_conversoes_backend(vendas_para_envio, URL_WEBHOOK_BACKEND)
                else:
                    print("   ℹ️ Nenhuma venda nova para enviar hoje.")
            else:
                print("   ℹ️ Webhook não configurado.")

    print("\n###########################################################")
    print("### PIPELINE FINALIZADO COM SUCESSO ###")
    print("###########################################################")

if __name__ == "__main__":
    run()