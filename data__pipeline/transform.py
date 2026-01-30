import pandas as pd
from extract import getAgendamentosFiltrados

def getAgendamentosLimpos() -> pd.DataFrame:
    print("Iniciando transformação dos dados...")
    dados_brutos = getAgendamentosFiltrados()

    # 1. Tratamento de Segurança (Caso a API não retorne nada)
    if not dados_brutos:
        print("Nenhum dado encontrado. Retornando DataFrame vazio.")
        return pd.DataFrame(columns=[
            "MATRICULA", "ALUNO", "ALUNO_PRINCIPAL", "ALUNO_ACOMPANHANTE",
            "QTD_PESSOAS", "TIPO DE TREINO", "ATENDENTE", "DATA", "HORA"
        ])

    agendamentos_df = pd.DataFrame(dados_brutos)

    # Converter a coluna 'inicio' para o formato datetime
    agendamentos_df['inicio'] = pd.to_datetime(agendamentos_df['inicio'])

    # Pipeline de limpeza básico (Remove duplicatas e cria colunas de data/hora)
    agendamentos_df = (
        agendamentos_df
        .sort_values(by='inicio', ascending=True)
        .drop_duplicates(subset=['matricula', 'evento'], keep='last')
        .assign(
            Data = lambda df: df['inicio'].dt.strftime('%d/%m/%Y'),
            Hora = lambda df: df['inicio'].dt.strftime('%H:%M')
        )
    )
    
    # Mapeamento de colunas
    colunas_padrao = {
        "matricula": "MATRICULA",
        "nomeAluno": "ALUNO",
        "evento": "TIPO DE TREINO",
        "Data": "DATA",
        "Hora": "HORA"
    }
    
    agendamentos_df = agendamentos_df.rename(columns=colunas_padrao)
    
    # --- NOVA LÓGICA ROBUSTA (Para 1, 2, 3 ou mais nomes) ---
    
    # 1. Cria uma LISTA de nomes (Explode a string pela vírgula)
    # Exemplo: "João, Maria, Pedro" vira ["João", "Maria", "Pedro"]
    lista_nomes = agendamentos_df['ALUNO'].str.split(r',\s*')

    # 2. Define o Principal (Pega sempre o primeiro item da lista)
    agendamentos_df['ALUNO_PRINCIPAL'] = lista_nomes.str[0]

    # 3. Define os Acompanhantes (Pega do segundo item em diante e junta com vírgula)
    # A função lambda verifica: se tem mais de 1 nome, junta o resto. Se não, deixa vazio.
    agendamentos_df['ALUNO_ACOMPANHANTE'] = lista_nomes.apply(
        lambda x: ', '.join(x[1:]) if isinstance(x, list) and len(x) > 1 else ""
    )

    # 4. Conta quantas pessoas tem no total (para estatística)
    agendamentos_df['QTD_PESSOAS'] = lista_nomes.str.len()

    # ---------------------------------------------------------

    # Lógica do Atendente
    agendamentos_df["ATENDENTE"] = agendamentos_df["HORA"].apply(
        lambda h: "ATENDENTE 1" if int(h[:2]) < 12 else "ATENDENTE 2"
    )
    
    # Seleção final das colunas
    colunas_ordenadas = [
        "MATRICULA",
        "ALUNO",              # Mantemos o original para conferência
        "ALUNO_PRINCIPAL",    # O pagante
        "ALUNO_ACOMPANHANTE", # Os convidados
        "QTD_PESSOAS",        # Total de pessoas na sala
        "TIPO DE TREINO",
        "ATENDENTE",
        "DATA",
        "HORA"
    ]

    print(f"Processamento concluído. Total de agendamentos válidos: {len(agendamentos_df)}")
    return agendamentos_df[colunas_ordenadas]