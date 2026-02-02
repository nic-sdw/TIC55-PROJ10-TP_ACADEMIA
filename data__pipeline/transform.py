import pandas as pd
import re
from datetime import datetime
from rapidfuzz import process, fuzz, utils 

# funçao para remover duplicatas e formata os dados.
# Retorna um DataFrame do pandas com os dados limpos e formatados
def getAgendamentosLimpos(data):
    
    if not data:
        print("Zero dados retornados do extract.py")
        return pd.DataFrame()
    
    agendamentos_df = pd.DataFrame(data)

    # Converter a coluna 'inicio' para o formato datetime
    agendamentos_df['inicio'] = pd.to_datetime(agendamentos_df['inicio'])

    colunas_planilha = ['matricula', 'nomeAluno', 'evento', 'Data', 'Hora']

    agendamentos_df = (
        agendamentos_df
        .sort_values(by='inicio', ascending=True)
        .drop_duplicates(subset=['matricula', 'evento'], keep='last')
        .assign(
            Data = lambda df: df['inicio'].dt.strftime('%d/%m/%Y'),
            Hora = lambda df: df['inicio'].dt.strftime('%H:%M')
        )
    )
        
    colunas_padrao = {
        "matricula": "MATRICULA",
        "nomeAluno": "ALUNO",
        "evento": "TIPO DE TREINO",
        "Data": "DATA",
        "Hora": "HORA"
    }
    
    agendamentos_df = agendamentos_df.rename(columns=colunas_padrao)
    
    agendamentos_df["ATENDENTE"] = agendamentos_df["HORA"].apply(
        lambda h: "ATENDENTE 1" if int(h[:2]) < 12 else "ATENDENTE 2"
    )
    
    colunas_ordenadas = [
        "MATRICULA",
        "ALUNO",
        "TIPO DE TREINO",
        "ATENDENTE",
        "DATA",
        "HORA"
    ]

    print(f"Processamento concluído. Total de agendamentos válidos: {len(agendamentos_df)}")
    print(agendamentos_df[colunas_ordenadas])
    return agendamentos_df[colunas_ordenadas]

#Poderia ser uma lista 
def _get_nome_mes(mes_numero):
    meses = [None, 'Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho',
             'Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro']
    return meses[mes_numero] if 1 <= mes_numero <= 12 else None

# função Auxiliar para Fuzzy Matching
def obter_nomes_cruzados(nome_mkt, lista_alunos_pacto, corte=85):
    if not nome_mkt or not lista_alunos_pacto:
        return nome_mkt
        
    # token_set_ratio para lidar com nomes abreviados ou nomes do meio extras
    resultado = process.extractOne(
        nome_mkt, 
        lista_alunos_pacto, 
        scorer=fuzz.token_set_ratio, 
        processor=utils.default_process,
        score_cutoff=corte
    )
    
    if resultado:
        nome_encontrado, score, _ = resultado
        if nome_encontrado.upper() != nome_mkt.upper():
            print(f"   Fuzzy matching: '{nome_mkt}' -> '{nome_encontrado}' ({score:.1f}%)")
        return nome_encontrado
            
    return nome_mkt

#Regex, filtra mês atual e identifica vendedora.
#Recebe o DF bruto do Extract
def process_leads_marketing(df_bruto):

    if df_bruto.empty:
        return pd.DataFrame()

    print(" Processando leads...")
    
    hoje = datetime.now()
    nome_mes_atual = _get_nome_mes(hoje.month)

    if 'Mês' not in df_bruto.columns:
        print("Mes nao encontrado na planilha, enviando df vazio")
        return pd.DataFrame()

    # sem  o filtro de mês para ler o histórico completo
    # filtro_mes = df_bruto['Mês'].astype(str).str.strip().str.upper()
    # df_filtrado = df_bruto[filtro_mes == nome_mes_atual.upper()].copy()
    df_filtrado = df_bruto.copy()
    
    leads_limpos = []

    for _, row in df_filtrado.iterrows():
        origem = row.get('Origem', 'Desconhecido')
        origem_2 = row.get('Origem_2', 'Desconhecido')
        
        data_lead = str(row.get('Data', '')).strip()
        
        # Pega o mês da própria linha para referência 
        col_mes = next((c for c in df_bruto.columns if 'Mês' in c or 'Mes' in c), None)
        mes_ref = str(row.get(col_mes, '')).strip() if col_mes else nome_mes_atual

        mapa_vendedoras = [
            (str(row.get('Nomes agendados (Daniela Dalla)', '')), 'Daniela Dalla'),
            (str(row.get('Nomes agendados (Daniela Teixeira)', '')), 'Daniela Teixeira')
        ]
        
        for nomes_sujos, nome_vendedora in mapa_vendedoras:
            for nome_sujo in nomes_sujos.split('\n'):
                # Regex para limpar datas junto com o nome
                nome_limpo = re.sub(r'[0-9].*', '', nome_sujo) # Remove números/datas
                nome_limpo = re.sub(r'[-/].*', '', nome_limpo).strip().upper() # Remove traços
                
                if nome_limpo and len(nome_limpo) > 2 and nome_limpo not in ['0', '-', 'NAN', 'NONE']:
                    leads_limpos.append({
                        'ALUNO': nome_limpo,
                        'ORIGEM': origem,
                        'ORIGEM_2': origem_2,
                        'DATA': data_lead,
                        'VENDEDORA': nome_vendedora,
                        'MES_REFERENCIA': mes_ref
                    })
    
    return pd.DataFrame(leads_limpos)

# Recebe os dados limpos da Pacto e do Marketing e realiza o cruzamento.
# Retorna o DataFrame final pronto para salvar.
def consolidar_dados(df_pacto, df_mkt):

    print(" Cruzando dados (Pacto + Marketing)...")

    if df_pacto.empty:
        return pd.DataFrame()

    # Se não tiver dados do Mkt, retorna a Pacto com colunas de origem vazias
    if df_mkt.empty:
        df_pacto['ORIGEM'] = 'Orgânico/Outros'
        df_pacto['VENDEDORA'] = 'Recepção/Sistema'
        return df_pacto

    # Cria Chaves de Busca (Padronização)
    # .copy() para não alterar o dataframe original fora da função
    df_pacto_copy = df_pacto.copy()
    df_mkt_copy = df_mkt.copy()

    # Fuzzy Match antes de criar a chave
    if 'ALUNO' in df_pacto_copy.columns:
        lista_alunos = df_pacto_copy['ALUNO'].dropna().unique().tolist()
        print(f"   Aplicando correção Fuzzy em {len(df_mkt_copy)} leads...")
        df_mkt_copy['ALUNO'] = df_mkt_copy['ALUNO'].apply(
            lambda x: obter_nomes_cruzados(x, lista_alunos, corte=85)
        )

    df_pacto_copy['CHAVE_TEMP'] = df_pacto_copy['ALUNO'].astype(str).str.strip().str.upper()
    df_mkt_copy['CHAVE_TEMP'] = df_mkt_copy['ALUNO'].astype(str).str.strip().str.upper()

    #  Deduplicação de segurança no Marketing (pega o último registro do lead)
    df_mkt_copy = df_mkt_copy.drop_duplicates(subset=['CHAVE_TEMP'], keep='last')

    # MERGE (Left Join)
    # Mantemos a Pacto (Left) e trazemos as colunas do Mkt
    df_final = pd.merge(
        df_pacto_copy,
        df_mkt_copy[['CHAVE_TEMP', 'ORIGEM', 'VENDEDORA']], 
        on='CHAVE_TEMP',
        how='left'
    )

    # Tratamento de quem não teve match 
    df_final['ORIGEM'] = df_final['ORIGEM'].fillna('Orgânico/Outros')
    df_final['VENDEDORA'] = df_final['VENDEDORA'].fillna('Recepção/Sistema')

    # Limpeza da chave auxiliar
    df_final = df_final.drop(columns=['CHAVE_TEMP'])

    matches = len(df_final[df_final['ORIGEM'] != 'Orgânico/Outros'])
    print(f"   [Transform] Cruzamento finalizado. {matches} atribuições encontradas.")
    
    return df_final