import os
import requests
import pandas as pd
import time
from datetime import datetime
from dotenv import load_dotenv
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import matplotlib.pyplot as plt
import seaborn as sns

load_dotenv()

# --- CONFIGURAÇÕES DE AMBIENTE ---
TOKEN = os.getenv("TOKEN")
EMPRESA_ID = os.getenv("EMPRESA_ID")
URL_BASE = "https://apigw.pactosolucoes.com.br"

headers = {
    "Authorization": f"Bearer {TOKEN}",
    "empresaId": EMPRESA_ID,
    "Content-Type": "application/json"
}

# --- 1. FUNÇÕES DE INFRAESTRUTURA ---

def fetch_pacto_data(path, params):
    url = f"{URL_BASE}{path}"
    time.sleep(1.2) 
    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        return response.json() if response.status_code == 200 else None
    except Exception as e:
        print(f"Erro na API em {path}: {e}")
        return None

def atualizar_google_sheets(df, planilha_nome, aba_nome):
    print(f"--- ☁️ SINCRONIZANDO: {aba_nome} ---")
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    try:
        if not os.path.exists('credentials.json'):
            print("⚠️ Aviso: credentials.json não encontrado.")
            return
        creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
        client = gspread.authorize(creds)
        sheet = client.open(planilha_nome).worksheet(aba_nome)
        sheet.clear()
        df_sync = df.astype(str)
        dados = [df_sync.columns.values.tolist()] + df_sync.values.tolist()
        sheet.update('A1', dados)
        print(f"✅ Aba '{aba_nome}' atualizada.")
    except Exception as e:
        print(f"❌ Erro ao atualizar Google Sheets ({aba_nome}): {e}")

# --- 2. NÚCLEO DE TRANSFORMAÇÃO (ETL) ---

def extrair_texto(x):
    if isinstance(x, dict):
        return x.get('descricao', x.get('nome', str(x)))
    return str(x) if pd.notnull(x) and str(x) != "" else "Não Informado"

def processar_perfil_alunos(df_bruto):
    if df_bruto.empty:
        return pd.DataFrame()

    mapeamento = {
        'matriculaZW': 'MATRICULA',
        'nome': 'ALUNO',
        'sexo': 'SEXO',
        'situacaoAluno': 'STATUS_SISTEMA',
        'dataMatriculaZW': 'DATA_RAW',
        'consultor': 'CONSULTOR_RAW',
        'motivoCancelamento': 'MOTIVO_RAW',
        'fones': 'TELEFONE',
        'dataNascimento': 'NASCIMENTO'
    }
    
    # 🛡️ BLINDAGEM: Renomeia apenas o que existe e cria o resto como nulo
    df = df_bruto.rename(columns={k: v for k, v in mapeamento.items() if k in df_bruto.columns}).copy()
    for col in mapeamento.values():
        if col not in df.columns:
            df[col] = None

    df['CONSULTOR'] = df['CONSULTOR_RAW'].apply(extrair_texto)
    df['MOTIVO_EVASAO'] = df['MOTIVO_RAW'].apply(extrair_texto)
    df['DATA_ADESAO'] = pd.to_datetime(df['DATA_RAW'], unit='ms', errors='coerce')
    df['NASCIMENTO'] = pd.to_datetime(df['NASCIMENTO'], errors='coerce')
    
    # Inteligência de Idade
    ano_referencia = 2026
    df['IDADE'] = df['NASCIMENTO'].apply(lambda x: ano_referencia - x.year if pd.notnull(x) else 0)
    
    bins = [0, 18, 25, 35, 45, 60, 100]
    labels = ['Sub-18', '18-25', '26-35', '36-45', '46-60', '60+']
    df['PERFIL_ETARIO'] = pd.cut(df['IDADE'], bins=bins, labels=labels)
    df['SEXO'] = df['SEXO'].astype(str).str.title()
    
    return df

def gerar_graficos_teste(df_base, df_saude):
    """Gera visualizações locais."""
    if df_base.empty:
        print("⚠️ Base vazia, impossível gerar gráficos.")
        return

    print("\n--- 📊 GERANDO DASHBOARD DE VALIDAÇÃO LOCAL ---")
    sns.set_theme(style="whitegrid")
    fig, axes = plt.subplots(2, 2, figsize=(16, 10))
    fig.suptitle('Dashboard BI Academia - Inteligência 2025/2026', fontsize=18)

    # 1. Ranking de Vendas
    df_vendas = df_base.groupby('CONSULTOR').size().reset_index(name='VENDAS').sort_values('VENDAS', ascending=False)
    sns.barplot(ax=axes[0, 0], x='VENDAS', y='CONSULTOR', data=df_vendas.head(8), palette='viridis', hue='CONSULTOR', legend=False)
    axes[0, 0].set_title('Top Consultores (Vendas)')

    # 2. Perfil por Idade
    ordem = ['Sub-18', '18-25', '26-35', '36-45', '46-60', '60+']
    sns.countplot(ax=axes[0, 1], x='PERFIL_ETARIO', data=df_base, order=ordem, palette='coolwarm', hue='PERFIL_ETARIO', legend=False)
    axes[0, 1].set_title('Distribuição Demográfica (Idade)')

    # 3. Saúde da Base (Assiduidade)
    sns.countplot(ax=axes[1, 0], x='SAUDE_MATRICULA', data=df_saude, palette='RdYlGn_r', hue='SAUDE_MATRICULA', legend=False)
    axes[1, 0].set_title('Status de Saúde (Treinos)')

    # 4. Motivos de Churn
    df_churn = df_base[df_base['STATUS_SISTEMA'].isin(['Cancelado', 'Inativo', 'Evadido'])]
    if not df_churn.empty:
        df_motivos = df_churn.groupby('MOTIVO_EVASAO').size().reset_index(name='QTD').sort_values('QTD', ascending=False)
        axes[1, 1].pie(df_motivos['QTD'].head(5), labels=df_motivos['MOTIVO_EVASAO'].head(5), autopct='%1.1f%%', startangle=140)
        axes[1, 1].set_title('Causas de Evasão')
    else:
        axes[1, 1].text(0.5, 0.5, 'Sem dados de cancelamento', ha='center')

    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    print("✅ Gráficos prontos. Feche a janela para continuar a sincronização.")
    plt.show()

# --- 3. PIPELINE DE EXECUÇÃO ---

def executar_sistema_bi_completo():
    print(f"--- 🚀 INICIANDO ETL INTEGRADO - {datetime.now().strftime('%d/%m/%Y')} ---")
    planilha = "Dashboard_Academia_Looker"

    # PASSO A: Coletar Cadastro
    lista_alunos = []
    for p in range(86):
        dados = fetch_pacto_data("/psec/alunos/v2", {"page": p, "size": 100})
        if not dados or not dados.get('content'): break
        lista_alunos.extend(dados['content'])
        print(f"   > Coletando Perfis: {len(lista_alunos)}", end='\r')
    
    df_alunos_base = processar_perfil_alunos(pd.DataFrame(lista_alunos))

    # PASSO B: Coletar Frequência
    lista_freq = []
    print("\n--- 📡 BUSCANDO REGISTROS DE CATRACA ---")
    for p in range(50):
        dados = fetch_pacto_data("/psec/frequencias/v2", {"page": p, "size": 100})
        if not dados or not dados.get('content'): break
        lista_freq.extend(dados['content'])
        print(f"   > Coletando Check-ins: {len(lista_freq)}", end='\r')
    
    df_freq_bruta = pd.DataFrame(lista_freq)

    # PASSO C: Inteligência de Risco
    if not df_freq_bruta.empty:
        df_freq_bruta['DATA_ACESSO'] = pd.to_datetime(df_freq_bruta['dataHora'], unit='ms', errors='coerce')
        # 🛡️ Identifica a coluna de matrícula (pode mudar na API)
        col_mat = 'matriculaZW' if 'matriculaZW' in df_freq_bruta.columns else 'alunoId'
        
        df_ultimo = df_freq_bruta.groupby(col_mat)['DATA_ACESSO'].max().reset_index()
        df_ultimo.columns = ['MATRICULA', 'ULTIMO_TREINO']
        
        hoje = pd.Timestamp(datetime.now())
        df_saude = pd.merge(df_alunos_base, df_ultimo, on='MATRICULA', how='left')
        df_saude['DIAS_INATIVIDADE'] = (hoje - df_saude['ULTIMO_TREINO']).dt.days.fillna(99)
        
        def classificar(d):
            if d <= 7: return "Engajado"
            if d <= 15: return "Alerta"
            if d <= 30: return "Risco Moderado"
            return "Risco Crítico"
        df_saude['SAUDE_MATRICULA'] = df_saude['DIAS_INATIVIDADE'].apply(classificar)
    else:
        df_saude = df_alunos_base.copy()
        df_saude['SAUDE_MATRICULA'] = "Sem Treinos"

    # PASSO D: Gráficos e Nuvem
    df_2025_teste = df_alunos_base[df_alunos_base['DATA_ADESAO'].dt.year == 2025].copy()
    
    # Só gera gráficos se houver dados
    if not df_2025_teste.empty:
        gerar_graficos_teste(df_2025_teste, df_saude)
        atualizar_google_sheets(df_2025_teste, planilha, "Base_2025")
        atualizar_google_sheets(df_saude, planilha, "Saude_Base_Geral")
    
    print(f"\n\n📊 PROCESSO FINALIZADO!")

if __name__ == "__main__":
    executar_sistema_bi_completo()