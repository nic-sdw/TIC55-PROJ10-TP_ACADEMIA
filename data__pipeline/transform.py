import pandas as pd
from extract import getAgendamentosFiltrados

# funçao para remover duplicatas e formata os dados.
# Retorna um DataFrame do pandas com os dados limpos e formatados
def getAgendamentosLimpos() -> pd.DataFrame:
    
    agendamentos_df = pd.DataFrame(getAgendamentosFiltrados())

    # Converter a coluna 'inicio' para o formato datetime
    agendamentos_df['inicio'] = pd.to_datetime(agendamentos_df['inicio'])

    colunas_planilha = ['matricula', 'nomeAluno', 'evento', 'Data', 'Hora']

    agendamentos_df = agendamentos_df.sort_values(by='inicio', ascending=True).drop_duplicates(subset=['matricula', 'evento'], keep='last').assign(
        Data = lambda df: df['inicio'].dt.strftime('%d/%m/%Y'),
        Hora = lambda df: df['inicio'].dt.strftime('%H:%M')
        )

    colunas_finais = [col for col in colunas_planilha if col in agendamentos_df.columns]

    print(f"Processamento concluído. Total de agendamentos válidos: {len(agendamentos_df[colunas_finais])}")
    print(agendamentos_df[colunas_finais])
    return agendamentos_df[colunas_finais]

getAgendamentosLimpos()