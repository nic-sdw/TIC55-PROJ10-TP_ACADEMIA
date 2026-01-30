# transform.py
import pandas as pd
import re
from datetime import datetime
from unidecode import unidecode
from thefuzz import process, fuzz

# =============================================================================
# 1. HELPERS (Normalização e Utilitários)
# =============================================================================

def _get_nome_mes(mes_numero):
    meses = [None, 'Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho',
             'Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro']
    return meses[mes_numero] if 1 <= mes_numero <= 12 else None

def normalizar_texto(texto):
    """Remove acentos, espaços extras e coloca em minúsculo para comparação."""
    if not isinstance(texto, str):
        return ""
    # unidecode transforma 'João' em 'Joao', lower() em 'joao', strip() tira espaços
    return unidecode(texto).lower().strip()

# =============================================================================
# 2. LIMPEZA DE AGENDAMENTOS (TREINOS)
# =============================================================================

def getAgendamentosLimpos(data):
    if not data:
        print("Zero dados retornados do extract.py")
        return pd.DataFrame()
    
    agendamentos_df = pd.DataFrame(data)

    # Converter a coluna 'inicio' para o formato datetime
    if 'inicio' in agendamentos_df.columns:
        agendamentos_df['inicio'] = pd.to_datetime(agendamentos_df['inicio'])

        agendamentos_df = (
            agendamentos_df
            .sort_values(by='inicio', ascending=True)
            .drop_duplicates(subset=['matricula', 'evento'], keep='last')
            .assign(
                Data = lambda df: df['inicio'].dt.strftime('%d/%m/%Y'),
                Hora = lambda df: df['inicio'].dt.strftime('%H:%M')
            )
        )
        
        # Lógica de Atendente baseada no horário
        agendamentos_df["ATENDENTE"] = agendamentos_df["Hora"].apply(
            lambda h: "ATENDENTE 1" if int(h[:2]) < 12 else "ATENDENTE 2"
        )
    else:
        # Fallback caso a coluna não exista
        agendamentos_df['Data'] = ""
        agendamentos_df['Hora'] = ""
        agendamentos_df["ATENDENTE"] = ""

    colunas_padrao = {
        "matricula": "MATRICULA",
        "nomeAluno": "ALUNO",
        "evento": "TIPO DE TREINO",
        "Data": "DATA",
        "Hora": "HORA"
    }
    
    # Renomeia apenas as colunas que existem
    agendamentos_df = agendamentos_df.rename(columns=colunas_padrao)
    
    # Seleciona colunas finais, garantindo que existam
    cols_finais = ["MATRICULA", "ALUNO", "TIPO DE TREINO", "ATENDENTE", "DATA", "HORA"]
    for col in cols_finais:
        if col not in agendamentos_df.columns:
            agendamentos_df[col] = None

    print(f"Processamento concluído. Total de agendamentos válidos: {len(agendamentos_df)}")
    return agendamentos_df[cols_finais]

# =============================================================================
# 3. PROCESSAMENTO DE LEADS (MARKETING)
# =============================================================================

def process_leads_marketing(df_bruto):
    """
    Limpa a planilha de MKT, filtra pelo mês atual e extrai nomes das vendedoras.
    """
    if df_bruto.empty:
        return pd.DataFrame()

    print(" Processando leads...")
    
    hoje = datetime.now()
    nome_mes_atual = _get_nome_mes(hoje.month)

    # Tenta achar a coluna de mês (pode ser 'Mês' ou 'Mes')
    col_mes = next((c for c in df_bruto.columns if 'Mês' in c or 'Mes' in c), None)
    
    if not col_mes:
        print("Coluna de Mês não encontrada na planilha.")
        return pd.DataFrame()

    # Filtro de Mês
    filtro_mes = df_bruto[col_mes].astype(str).str.strip().str.upper()
    
    # NOTA: Para testes com meses anteriores, você pode comentar a linha abaixo
    df_filtrado = df_bruto[filtro_mes == nome_mes_atual.upper()].copy()
    # df_filtrado = df_bruto.copy() # <--- Use essa se quiser ignorar o mês
    
    leads_limpos = []

    for _, row in df_filtrado.iterrows():
        origem = row.get('Origem', 'Desconhecido')
        origem_2 = row.get('Origem_2', 'Desconhecido')
        data_lead = str(row.get('Data', '')).strip()
        
        # Mapeamento das colunas de vendedoras
        mapa_vendedoras = [
            (str(row.get('Nomes agendados (Daniela Dalla)', '')), 'Daniela Dalla'),
            (str(row.get('Nomes agendados (Daniela Teixeira)', '')), 'Daniela Teixeira')
        ]
        
        for nomes_sujos, nome_vendedora in mapa_vendedoras:
            # Quebra linhas caso tenha mais de um nome na mesma célula
            for nome_sujo in nomes_sujos.split('\n'):
                # Regex para limpar datas e traços (Ex: "Joao - 10/10" -> "Joao")
                nome_limpo = re.sub(r'\s*-\s*\d{2}/\d{2}.*', '', nome_sujo).strip()
                
                # Validação básica de nome (ignora vazios ou '-')
                if nome_limpo and len(nome_limpo) > 2 and nome_limpo not in ['0', '-']:
                    leads_limpos.append({
                        'ALUNO': nome_limpo.upper(), 
                        'ORIGEM': origem,
                        'ORIGEM_2': origem_2,
                        'DATA_LEAD': data_lead,
                        'VENDEDORA': nome_vendedora,
                        'MES_REFERENCIA': nome_mes_atual
                    })
    
    df_leads = pd.DataFrame(leads_limpos)
    print(f" Leads processados: {len(df_leads)}")
    return df_leads

# =============================================================================
# 4. CRUZAMENTO INTELIGENTE (FUZZY MATCH)
# =============================================================================

def cruzar_vendas_fuzzy(df_leads, df_alunos, threshold=85):
    """
    Cruza a planilha de Leads com a API de Alunos usando lógica Fuzzy.
    """
    print(f" Iniciando Cruzamento Fuzzy (Corte: {threshold})...")

    # --- BLINDAGEM CONTRA ERROS ---
    # Se não houver leads, retorna DF vazio MAS com as colunas certas para não quebrar o main.py
    if df_leads.empty:
        print("⚠️ Sem leads para cruzar. Retornando estrutura vazia.")
        return pd.DataFrame(columns=['ALUNO', 'match_score', 'match_nome_api', 'matriculaZW', 'SITUACAO_CONVERSAO'])

    if df_alunos.empty:
        print("⚠️ Sem dados de alunos da API.")
        df_leads['match_score'] = 0
        df_leads['SITUACAO_CONVERSAO'] = 'Não Encontrado'
        return df_leads
    
    # 1. Normalização
    df_leads['nome_norm'] = df_leads['ALUNO'].apply(normalizar_texto)
    
    col_nome_api = 'nome' if 'nome' in df_alunos.columns else 'Nome'
    df_alunos['nome_norm'] = df_alunos[col_nome_api].apply(normalizar_texto)

    # 2. Lista de nomes únicos da API
    nomes_api = df_alunos['nome_norm'].dropna().unique().tolist()
    
    print(" Calculando similaridade entre nomes...")

    # 3. Busca Fuzzy
    def buscar_match(nome_lead):
        if not nome_lead: return None, 0
        # token_sort_ratio: Ignora ordem ("Silva Joao" == "Joao Silva")
        match, score = process.extractOne(nome_lead, nomes_api, scorer=fuzz.token_sort_ratio)
        if score >= threshold:
            return match, score
        return None, 0

    matches = df_leads['nome_norm'].apply(buscar_match)
    
    df_leads['match_nome_api'] = [m[0] for m in matches]
    df_leads['match_score'] = [m[1] for m in matches]

    # 4. Merge Final
    df_alunos_unique = df_alunos.drop_duplicates(subset=['nome_norm'])
    
    cols_possiveis = ['nome_norm', 'matriculaZW', 'contratoZW', 'dataMatriculaZW', 'situacaoAluno', 'planoZW']
    cols_existentes = [c for c in cols_possiveis if c in df_alunos.columns]

    df_final = pd.merge(
        df_leads,
        df_alunos_unique[cols_existentes],
        left_on='match_nome_api',
        right_on='nome_norm',
        how='left',
        suffixes=('', '_pacto')
    )

    # 5. Classificação
    df_final['SITUACAO_CONVERSAO'] = df_final['match_score'].apply(
        lambda x: 'CONVERTIDO' if x >= threshold else 'NÃO ENCONTRADO'
    )

    # 6. Limpeza
    cols_drop = ['nome_norm', 'nome_norm_pacto', 'match_nome_api']
    df_final.drop(columns=[c for c in cols_drop if c in df_final.columns], inplace=True, errors='ignore')

    # Stats
    total_convertidos = len(df_final[df_final['SITUACAO_CONVERSAO'] == 'CONVERTIDO'])
    print(f" [Transform] Cruzamento finalizado.")
    print(f" Leads Totais: {len(df_leads)}")
    print(f" Vendas Confirmadas (Matches): {total_convertidos}")

    return df_final