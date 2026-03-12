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

#Regex, filtra mês atual e identifica VENDEDORA_AGENDAMENTO.
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
                        'VENDEDORA_AGENDAMENTO': nome_vendedora,
                        'MES_REFERENCIA': mes_ref
                    })
    
    return pd.DataFrame(leads_limpos)
#Funçaõ pra pegar os contratos e o nome das pessoas, pra nao repetir codigo
def processar_contratos(lista_contratos_brutos):
    if not lista_contratos_brutos:
        return pd.DataFrame(), []

    contratos = []
    for c in lista_contratos_brutos:
        timestamp = c.get('dataMatriculaZW')
        data_formatada = '-'
        if timestamp:
            # Se o valor for muito grande, assumimos que está em milissegundos
            if timestamp > 1e10: 
                timestamp = timestamp / 1000.0

            try:
                data_formatada = datetime.fromtimestamp(timestamp).strftime('%d/%m/%Y')
            except (ValueError, OSError) as e:
                print(f"Erro ao formatar data: {e}")
                data_formatada = '-'

        contratos.append({
            'NOME_SISTEMA': str(c.get('nome', '')).upper().strip(),
            'PLANO_SISTEMA': c.get('planoZW', {}).get('nome', 'Sem Plano'),
            'DATA_MATR_SISTEMA': data_formatada,
            'MATRICULA_ZW': c.get('matriculaZW') 
        })

    df_contratos = pd.DataFrame(contratos)

    return df_contratos, df_contratos['NOME_SISTEMA'].tolist()

def validar_vendas_com_lista(df_mkt, lista_contratos_brutos, busca_hora=None):
    
    if not lista_contratos_brutos or df_mkt.empty:
        return df_mkt

    print(f"Validando vendas localmente contra {len(lista_contratos_brutos)} contratos...")
    
    df_contratos, nomes_sistema = processar_contratos(lista_contratos_brutos)

    resultados = []
    for _, row in df_mkt.iterrows():
        lead_dict = row.to_dict()
        nome_lead = str(row['ALUNO']).upper().strip()
        match = process.extractOne(nome_lead, nomes_sistema, scorer=fuzz.token_set_ratio, score_cutoff=85)

        if match:
            nome_encontrado, _, _ = match
            dados_v = df_contratos[df_contratos['NOME_SISTEMA'] == nome_encontrado].iloc[0]
            
            data_utc_str = busca_hora(dados_v.get('MATRICULA_ZW')) if busca_hora else None
            hora_exata, vendedora_fechamento = '-', 'Sistema/Sem Hora'
            
            if data_utc_str:
                dt_br = pd.to_datetime(data_utc_str) - pd.Timedelta(hours=3)
                hora_exata = dt_br.strftime('%H:%M')
                vendedora_fechamento = calcular_vendedora_por_escala(dt_br.strftime('%d/%m/%Y'), hora_exata)

            lead_dict.update({
                'COMPROU?': 'SIM', 
                'PLANO': dados_v['PLANO_SISTEMA'], 
                'DATA_MATRICULA': dados_v['DATA_MATR_SISTEMA'],
                'HORA_MATRICULA': hora_exata,
                'VENDEDORA_FECHAMENTO': vendedora_fechamento
            })
        else:
            lead_dict.update({
                'COMPROU?': 'NÃO', 'PLANO': 'Nenhum', 'DATA_MATRICULA': '-',
                'HORA_MATRICULA': '-', 'VENDEDORA_FECHAMENTO': '-'
            })
        resultados.append(lead_dict)

    df_vendas = pd.DataFrame(resultados)

    # Lógica Categórica para garantir a hierarquia na ordenação
    df_vendas['COMPROU?'] = pd.Categorical(df_vendas['COMPROU?'], categories=['NÃO', 'SIM'], ordered=True)
    df_vendas = df_vendas.sort_values(by=['ALUNO', 'COMPROU?'], ascending=[True, True])

    # Converte de volta para string após garantir a ordem. 
    # Isso evita o erro 'Cannot setitem on a Categorical' no salvamento.
    df_vendas['COMPROU?'] = df_vendas['COMPROU?'].astype(str)

    return df_vendas

# Recebe os dados limpos da Pacto e do Marketing e realiza o cruzamento.
# Retorna o DataFrame final pronto para salvar.
def consolidar_dados(df_pacto, df_mkt, lista_contratos_brutos=None, busca_hora=None):

    if df_pacto.empty: return pd.DataFrame()
    df_pacto_copy = df_pacto.copy()
    
    if df_mkt.empty:
        for col, val in [('ORIGEM', 'Orgânico/Outros'), ('VENDEDORA_AGENDAMENTO', 'Recepção/Sistema'), 
                         ('COMPROU?', 'NÃO'), ('PLANO', 'Nenhum'), ('DATA_MATRICULA', '-')]:
            df_pacto_copy[col] = val
        return df_pacto_copy

    df_mkt_copy = df_mkt.copy()
    for col in ['COMPROU?', 'PLANO', 'DATA_MATRICULA']:
        if col in df_mkt_copy.columns:
            df_mkt_copy[col] = df_mkt_copy[col].astype(str)

    if 'ALUNO' in df_pacto_copy.columns:
        lista_alunos_catraca = df_pacto_copy['ALUNO'].dropna().unique().tolist()
        df_mkt_copy['ALUNO'] = df_mkt_copy['ALUNO'].apply(
            lambda x: obter_nomes_cruzados(x, lista_alunos_catraca, corte=85)
        )

    df_pacto_copy['CHAVE_TEMP'] = df_pacto_copy['ALUNO'].astype(str).str.strip().str.upper()
    df_mkt_copy['CHAVE_TEMP'] = df_mkt_copy['ALUNO'].astype(str).str.strip().str.upper()

    df_mkt_copy = df_mkt_copy.drop_duplicates(subset=['CHAVE_TEMP'], keep='last')

    colunas_mkt = ['CHAVE_TEMP', 'ORIGEM', 'VENDEDORA_AGENDAMENTO', 'COMPROU?', 'PLANO', 'DATA_MATRICULA', 'HORA_MATRICULA', 'VENDEDORA_FECHAMENTO']
    colunas_existentes = [c for c in colunas_mkt if c in df_mkt_copy.columns]

    df_final = pd.merge(df_pacto_copy, df_mkt_copy[colunas_existentes], on='CHAVE_TEMP', how='left')

    df_final['ORIGEM'] = df_final['ORIGEM'].fillna('Orgânico/Outros').astype(str)
    df_final['VENDEDORA_AGENDAMENTO'] = df_final['VENDEDORA_AGENDAMENTO'].fillna('Recepção/Sistema').astype(str)
    df_final['COMPROU?'] = df_final['COMPROU?'].fillna('NÃO').astype(str)
    df_final['PLANO'] = df_final['PLANO'].fillna('Nenhum').astype(str)
    df_final['DATA_MATRICULA'] = df_final['DATA_MATRICULA'].fillna('-').astype(str)
    df_final['HORA_MATRICULA'] = df_final.get('HORA_MATRICULA', pd.Series(['-']*len(df_final))).fillna('-').astype(str)
    df_final['VENDEDORA_FECHAMENTO'] = df_final.get('VENDEDORA_FECHAMENTO', pd.Series(['-']*len(df_final))).fillna('-').astype(str)
    df_final = df_final.drop(columns=['CHAVE_TEMP'])

    
    # repescagem para garantir que todas as não vendas estao corretas.
    # acabou que botei dentro do if, mas da pra fazer uma funçao pra isso
    if lista_contratos_brutos:
        print("   Iniciando repescagem de Vendas Orgânicas e correção de colisões...")
        
        df_contratos, nomes_sistema = processar_contratos(lista_contratos_brutos)

        # Varre todo o mundo que está como 'NÃO' no Relatório
        repescados = 0
        for idx, row in df_final.iterrows():
            if row['COMPROU?'] == 'NÃO':
                nome_catraca = str(row['ALUNO']).upper().strip()
                
                # Pega as 3 melhores opções de match para não errar
                matches = process.extract(nome_catraca, nomes_sistema, scorer=fuzz.token_set_ratio, limit=3)
                
                for match_nome, score, _ in matches:
                    if score >= 85:
                        # Verifica as iniciais abreviadas
                        # Ex: Se a catraca é "MARCIA S. MULLER", extrai o ['S']
                        letras_abrev = re.findall(r'\b([A-Z])\.', nome_catraca)
                        iniciais_completo = [p[0] for p in match_nome.split()] # Iniciais do contrato
                        
                        valido = True
                        if letras_abrev:
                            # Garante que todas as letras abreviadas existam no nome completo
                            # A Marcia da (S)ilva passa. A Marcia (R)ejane reprova.
                            valido = all(letra in iniciais_completo for letra in letras_abrev)
                        
                        if valido:
                            dados_v = df_contratos[df_contratos['NOME_SISTEMA'] == match_nome].iloc[0]
                            
                            data_utc_str = busca_hora(dados_v.get('MATRICULA_ZW')) if busca_hora else None
                            hora_exata, vendedora_fechamento = '-', 'Sistema/Sem Hora'
                            
                            if data_utc_str:
                                dt_br = pd.to_datetime(data_utc_str) - pd.Timedelta(hours=3)
                                hora_exata = dt_br.strftime('%H:%M')
                                vendedora_fechamento = calcular_vendedora_por_escala(dt_br.strftime('%d/%m/%Y'), hora_exata)
                                
                            df_final.at[idx, 'COMPROU?'] = 'SIM'
                            df_final.at[idx, 'PLANO'] = dados_v['PLANO_SISTEMA']
                            df_final.at[idx, 'DATA_MATRICULA'] = dados_v['DATA_MATR_SISTEMA']
                            df_final.at[idx, 'HORA_MATRICULA'] = hora_exata
                            df_final.at[idx, 'VENDEDORA_FECHAMENTO'] = vendedora_fechamento
                            repescados += 1
                            break # Achou o aluno certo, ignora os outros matches
        
        print(f" Repescagem concluída: {repescados} vendas recuperadas com precisão.")

    matches = len(df_final[df_final['ORIGEM'] != 'Orgânico/Outros'])
    print(f"   [Transform] Cruzamento finalizado. {matches} atribuições encontradas.")
    
    return df_final

#Função pra ordernar por data
#Pode ser removida, usei na main
def ordenar_por_data_recente(df, coluna_data='DATA'):
   
    if df.empty or coluna_data not in df.columns:
        return df
        
    df_copy = df.copy()
    # Cria a coluna temporal apenas para ordenação
    df_copy['DATA_TEMP'] = pd.to_datetime(df_copy[coluna_data], format='%d/%m/%Y', errors='coerce')
    
    # Ordena decrescente e remove a coluna temporária
    df_copy = df_copy.sort_values(by='DATA_TEMP', ascending=False)
    return df_copy.drop(columns=['DATA_TEMP'])

def calcular_vendedora_por_escala(data_str, hora_str):
    try:
        if pd.isna(hora_str) or str(hora_str).strip() in ['-', '']: 
            return "Sem Horário"
        
        # Garante a leitura correta da data no padrão BR
        data_obj = pd.to_datetime(data_str, dayfirst=True, errors='coerce')
        
        if pd.isna(data_obj): 
            return "Data Inválida"
            
        dia_semana = data_obj.weekday() # 0=Seg ... 5=Sáb

        if dia_semana <= 4: # Segunda a Sexta
            hora, minuto = map(int, str(hora_str).split(':'))
            # Regra dos turnos durante a semana
            if (hora > 6 or (hora == 6 and minuto >= 0)) and (hora < 14 or (hora == 14 and minuto < 30)):
                return "Daniela Teixeira"
            elif (hora > 14 or (hora == 14 and minuto >= 30)) and (hora <= 22):
                return "Daniela Dala"
            else:
                return "Fora do Turno"
        
        elif dia_semana == 5: # Sábado (Lógica de Alternância)
            # Data de referência: Sábado, 14/03/2026 - Daniela Dalla
            data_ref = pd.to_datetime('14/03/2026', dayfirst=True)
            
            # Calcula a diferença de semanas
            semanas_dif = (data_obj - data_ref).days // 7
            
            # Se a diferença for par (0, 2, 4...), é a Dalla. Se for ímpar, é a Teixeira.
            if semanas_dif % 2 == 0:
                return "Daniela Dalla"
            else:
                return "Daniela Teixeira"
            
        else: # Domingo
            return "Domingo"
            
    except Exception as e:
        return f"Erro: {str(e)}"