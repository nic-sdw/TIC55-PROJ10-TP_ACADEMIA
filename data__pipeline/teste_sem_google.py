import pandas as pd
import random
import os
import time
from datetime import datetime

# Importa seus módulos
import extract
import transform

# ==============================================================================
# CONFIGURAÇÃO: TESTE RÁPIDO (AMOSTRA RECENTE)
# ==============================================================================

def get_alunos_amostra_recente(paginas=5):
    """
    Função exclusiva de teste:
    Baixa apenas as últimas 'paginas' de alunos ordenados pela data de matrícula.
    Simula pegar apenas os últimos 2 meses de alunos novos.
    """
    print(f"   ⏳ Baixando amostra dos últimos alunos matriculados (Limitado a {paginas} páginas)...")
    lista_alunos = []
    
    for pag in range(paginas):
        print(f"      -> Baixando página {pag} (Ordenado por Data Recente)...")
        # 'dataMatriculaZW,desc' traz os mais novos primeiro
        dados = extract.getAlunosV2(page=pag, size=100, sort="dataMatriculaZW,desc")
        
        if dados and 'content' in dados:
            lista_alunos.extend(dados['content'])
        else:
            print("      ⚠️ Fim dos dados ou erro na página.")
            break
        
        time.sleep(1) # Pausa leve para não travar
        
    return pd.DataFrame(lista_alunos)

def executar_teste_offline():
    print("\n###########################################################")
    print("### TESTE OFFLINE RÁPIDO (AMOSTRA DE 2 MESES) ###")
    print("###########################################################")

    # -------------------------------------------------------------------------
    # PASSO 1: BAIXAR AMOSTRA DE ALUNOS REAIS
    # -------------------------------------------------------------------------
    print("\n>>> 1. CONECTANDO NA PACTO (DADOS REAIS - AMOSTRA)...")
    try:
        # EM VEZ DE BAIXAR TUDO, BAIXAMOS SÓ UMA PARTE:
        # df_alunos_api = extract.get_alunos_raw() <--- LINHA ANTIGA COMENTADA
        
        # CHAMA A FUNÇÃO NOVA DE AMOSTRA (5 Páginas = ~500 alunos mais recentes)
        df_alunos_api = get_alunos_amostra_recente(paginas=5)
        
        if df_alunos_api.empty:
            print("❌ Erro: API retornou vazio. Verifique Token ou Conexão.")
            return
        
        print(f"✅ Sucesso! Baixamos {len(df_alunos_api)} alunos recentes para o teste.")

    except Exception as e:
        print(f"❌ Erro crítico na API: {e}")
        return

    # -------------------------------------------------------------------------
    # PASSO 2: CRIAR LEADS "FAKE" BASEADOS NESSA AMOSTRA
    # -------------------------------------------------------------------------
    print("\n>>> 2. GERANDO LEADS SIMULADOS COM DADOS REAIS...")
    
    col_nome = 'nome' if 'nome' in df_alunos_api.columns else 'Nome'
    
    # Pega 3 nomes dessa lista de alunos novos
    qtd_amostra = min(3, len(df_alunos_api))
    amostra = df_alunos_api.sample(qtd_amostra)
    nomes_reais = amostra[col_nome].tolist()

    leads_simulados = []
    
    print("   📋 Simulando leads com nomes recentes da base:")
    
    for nome in nomes_reais:
        # Simula erro de digitação (Primeiro e Ultimo nome apenas)
        partes = nome.split()
        if len(partes) > 1:
            nome_lead = f"{partes[0]} {partes[-1]}".title() # Ex: JOAO DA SILVA -> Joao Silva
        else:
            nome_lead = nome.title()
            
        print(f"      - Aluno Real: {nome}  -->  Lead: {nome_lead}")
        
        leads_simulados.append({
            'ALUNO': nome_lead, 
            'ORIGEM': 'Instagram (Teste Recente)',
            'ORIGEM_2': 'Direct',
            'DATA_LEAD': '01/01/2025',
            'VENDEDORA': 'Teste',
            'MES_REFERENCIA': 'Janeiro'
        })
    
    # Adiciona um falso para teste negativo
    leads_simulados.append({
        'ALUNO': 'Usuario Inexistente Teste',
        'ORIGEM': 'Google', 'ORIGEM_2': '-',
        'DATA_LEAD': '01/01/2025', 'VENDEDORA': 'Teste', 'MES_REFERENCIA': 'Janeiro'
    })

    df_leads_fake = pd.DataFrame(leads_simulados)

    # -------------------------------------------------------------------------
    # PASSO 3: RODAR O FUZZY MATCH
    # -------------------------------------------------------------------------
    print("\n>>> 3. CRUZANDO DADOS (FUZZY MATCH)...")
    
    # Cruza os leads simulados com a base reduzida que baixamos
    df_resultado = transform.cruzar_vendas_fuzzy(df_leads_fake, df_alunos_api, threshold=85)

    # -------------------------------------------------------------------------
    # PASSO 4: VALIDAÇÃO DE DATA (NOVO!)
    # Testar também a função que vê se é aluno novo ou antigo
    # -------------------------------------------------------------------------
    print("\n>>> 4. VALIDANDO DATAS (ALUNO NOVO vs ANTIGO)...")
    if not df_resultado.empty:
        df_final = transform.cruzar_com_matriculas(df_resultado)
    else:
        df_final = df_resultado

    # -------------------------------------------------------------------------
    # PASSO 5: SALVAR CSV
    # -------------------------------------------------------------------------
    print("\n>>> 5. SALVANDO RESULTADO...")
    
    cols_view = ['ALUNO', 'match_nome_api', 'match_score', 'STATUS_FINAL', 'DATA_MATRICULA_FMT']
    cols_existentes = [c for c in cols_view if c in df_final.columns]
    
    print("\n--- PRÉVIA ---")
    print(df_final[cols_existentes].to_string(index=False))
    
    df_final.to_csv("resultado_teste_rapido.csv", sep=';', encoding='utf-8-sig', index=False)
    print("\n✅ Teste Concluído! Arquivo: resultado_teste_rapido.csv")

if __name__ == "__main__":
    executar_teste_offline()