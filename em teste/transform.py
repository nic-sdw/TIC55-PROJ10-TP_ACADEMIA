import pandas as pd
from extract import getAgendamentosFiltrados, getMatriculas

# =============================================================================
# 1. TRANSFORMAÇÃO DE AGENDAMENTOS (AULAS EXPERIMENTAIS)
# =============================================================================

def getAgendamentosLimpos() -> pd.DataFrame:
    print("Iniciando transformação de AGENDAMENTOS...")
    
    # Agora getAgendamentosFiltrados já retorna um DataFrame com ALUNO e MATRICULA
    df = getAgendamentosFiltrados()

    if df.empty:
        print("Nenhum agendamento encontrado. Retornando estrutura padrão.")
        return pd.DataFrame(columns=[
            "MATRICULA", "ALUNO", "ALUNO_PRINCIPAL", "ALUNO_ACOMPANHANTE",
            "QTD_PESSOAS", "TIPO DE TREINO", "ATENDENTE", "DATA", "HORA"
        ])

    # 1. Tratamento de Tempo e Deduplicação
    if 'inicio' in df.columns:
        df['inicio'] = pd.to_datetime(df['inicio'], errors='coerce')
        df = df.sort_values(by='inicio', ascending=True)
        
        # Deduplicação: usamos 'MATRICULA' (maiúsculo) pois o extract.py já renomeou
        df = df.drop_duplicates(subset=['MATRICULA', 'evento'], keep='last')
        
        # Criação de colunas de Data e Hora amigáveis
        df['DATA'] = df['inicio'].dt.strftime('%d/%m/%Y')
        df['HORA'] = df['inicio'].dt.strftime('%H:%M')
    
    # 2. Renomeação de Colunas Remanescentes
    # Mapeamos 'evento' para 'TIPO DE TREINO' (os outros já vêm certos do extract)
    df = df.rename(columns={"evento": "TIPO DE TREINO"})
    
    # 3. Lógica de Alunos (Principal vs Acompanhante)
    if 'ALUNO' in df.columns:
        # Garante que é string para evitar erro no split
        df['ALUNO'] = df['ALUNO'].astype(str)
        lista_nomes = df['ALUNO'].str.split(r',\s*')
        
        df['ALUNO_PRINCIPAL'] = lista_nomes.str[0]
        df['ALUNO_ACOMPANHANTE'] = lista_nomes.apply(
            lambda x: ', '.join(x[1:]) if isinstance(x, list) and len(x) > 1 else ""
        )
        df['QTD_PESSOAS'] = lista_nomes.str.len()
    
    # 4. Lógica de Atendimento por Turno
    if "HORA" in df.columns:
        df["ATENDENTE"] = df["HORA"].apply(
            lambda h: "ATENDENTE 1" if pd.notnull(h) and int(h[:2]) < 12 else "ATENDENTE 2"
        )
    
    # 5. Seleção e Ordenação Final das Colunas
    colunas_ordenadas = [
        "MATRICULA", "ALUNO", "ALUNO_PRINCIPAL", "ALUNO_ACOMPANHANTE",
        "QTD_PESSOAS", "TIPO DE TREINO", "ATENDENTE", "DATA", "HORA"
    ]
    # Filtra apenas as que realmente existem para evitar novos KeyErrors
    cols_finais = [c for c in colunas_ordenadas if c in df.columns]

    print(f"Processamento de Agendamentos concluído. Total: {len(df)}")
    return df[cols_finais]


# =============================================================================
# 2. TRANSFORMAÇÃO DE MATRÍCULAS (VENDAS / CONVERSÃO)
# =============================================================================

def getMatriculasLimpas(dias=60) -> pd.DataFrame:
    print(f"Iniciando transformação de MATRÍCULAS ({dias} dias)...")
    
    # O extract.py já retorna um DataFrame com ALUNO e MATRICULA
    df = getMatriculas(dias_atras=dias)
    
    if df.empty:
        print("Nenhuma matrícula para transformar.")
        return pd.DataFrame(columns=["MATRICULA", "ALUNO", "DATA_VENDA", "VALOR", "VENDEDOR"])

    # 1. Tratamento Financeiro
    if 'VALOR' in df.columns:
        df['VALOR'] = pd.to_numeric(df['VALOR'], errors='coerce').fillna(0)

    # 2. Tratamento de Datas (Padrão BR para o CSV final)
    # Importante: converter para datetime antes de formatar como string para garantir a lógica
    for col in ['DATA_VENDA', 'DATA_INICIO', 'DATA_MATRICULA']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
            # Mantemos como datetime se quiser ordenar, ou string para o relatório
            df[col] = df[col].dt.strftime('%d/%m/%Y')

    print(f"Processamento de Matrículas concluído. Total: {len(df)}")
    # Retorna o DataFrame mantendo as colunas 'ALUNO' e 'MATRICULA' essenciais para o main.py
    return df