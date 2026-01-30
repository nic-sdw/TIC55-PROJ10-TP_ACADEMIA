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
API_TOKEN = os.getenv("TOKEN")
ID_EMPRESA = os.getenv("EMPRESA_ID")
URL_BASE_PACTO = "https://apigw.pactosolucoes.com.br"

HEADERS_AUTENTICACAO = {
    "Authorization": f"Bearer {API_TOKEN}",
    "empresaId": ID_EMPRESA,
    "Content-Type": "application/json"
}

# --- 1. INFRAESTRUTURA (EXTRAÇÃO) ---

def requisitar_dados_pacto(path, parametros):
    url = f"{URL_BASE_PACTO}{path}"
    time.sleep(1.2) 
    try:
        resposta = requests.get(url, headers=HEADERS_AUTENTICACAO, params=parametros, timeout=30)
        return resposta.json() if resposta.status_code == 200 else None
    except Exception as erro:
        print(f"❌ Erro na extração ({path}): {erro}")
        return None

def sincronizar_google_sheets(df, planilha_nome, aba_nome):
    print(f"--- ☁️ SINCRONIZANDO: {aba_nome} ---")
    escopo = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    try:
        if not os.path.exists('credentials.json'):
            return
        creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', escopo)
        cliente = gspread.authorize(creds)
        aba = cliente.open(planilha_nome).worksheet(aba_nome)
        aba.clear()
        df_sync = df.astype(str)
        matriz = [df_sync.columns.values.tolist()] + df_sync.values.tolist()
        aba.update('A1', matriz)
        print(f"✅ Aba '{aba_nome}' atualizada.")
    except Exception as e:
        print(f"❌ Falha no Google Sheets: {e}")

# --- 2. NÚCLEO DE INTELIGÊNCIA (TRANSFORMAÇÃO) ---

def sanitizar_texto(dado):
    if isinstance(dado, dict):
        return dado.get('descricao', dado.get('nome', str(dado)))
    return str(dado) if pd.notnull(dado) and str(dado) != "" else "Não Informado"

def processar_base_alunos(df_bruto):
    if df_bruto.empty: return pd.DataFrame()
    mapeamento = {
        'matriculaZW': 'MATRICULA',
        'nome': 'ALUNO',
        'sexo': 'GENERO_RAW',
        'situacaoAluno': 'STATUS',
        'dataMatriculaZW': 'DATA_ADESAO_MS',
        'consultor': 'CONSULTOR_RAW',
        'dataNascimento': 'NASCIMENTO_RAW',
        'dataCancelamento': 'DATA_EVASAO_MS'
    }
    df = df_bruto.rename(columns={k: v for k, v in mapeamento.items() if k in df_bruto.columns}).copy()
    for col in mapeamento.values():
        if col not in df.columns: df[col] = None

    df['CONSULTOR'] = df['CONSULTOR_RAW'].apply(sanitizar_texto)
    df['DT_ADESAO'] = pd.to_datetime(df['DATA_ADESAO_MS'], unit='ms', errors='coerce')
    df['DT_EVASAO'] = pd.to_datetime(df['DATA_EVASAO_MS'], unit='ms', errors='coerce')
    df['DT_NASCIMENTO'] = pd.to_datetime(df['NASCIMENTO_RAW'], errors='coerce')
    
    def padronizar_genero(g):
        g = str(g).upper().strip()
        return 'Masculino' if g in ['M', 'MASCULINO'] else 'Feminino' if g in ['F', 'FEMININO'] else 'Outros'
    df['GENERO'] = df['GENERO_RAW'].apply(padronizar_genero)
    
    ano_atual = datetime.now().year
    df['IDADE'] = df['DT_NASCIMENTO'].apply(lambda x: ano_atual - x.year if pd.notnull(x) else 0)
    return df

def calcular_vcl_financeiro(df):
    TICKET_MEDIO = 120.00 
    hoje = pd.Timestamp(datetime.now())
    df['FIM_PERIODO'] = df['DT_EVASAO'].fillna(hoje)
    df['MESES_RETENCAO'] = ((df['FIM_PERIODO'] - df['DT_ADESAO']).dt.days / 30).clip(lower=1).round(1)
    df['VCL_TOTAL'] = df['MESES_RETENCAO'] * TICKET_MEDIO
    return df

# --- 3. MONITORAMENTO E ALERTAS ---

def verificar_saude_fidelizacao(df_fid):
    """Analisa se a taxa de fidelização caiu abaixo do limite aceitável."""
    LIMITE_CRITICO = 50.0
    alertas = df_fid[df_fid['TAXA'] < LIMITE_CRITICO]
    
    print("\n" + "="*50)
    print("🔍 AUDITORIA DE SAÚDE DA BASE")
    if not alertas.empty:
        print(f"🚨 ALERTA: Identificamos {len(alertas)} safras com Fidelização < {LIMITE_CRITICO}%")
        for _, row in alertas.iterrows():
            print(f"   ⚠️ Safra: {row['MES_ADESAO']} | Retenção: {row['TAXA']}% | Alunos: {row['TOTAL']}")
        print("💡 Sugestão: Verificar se houve falha no onboarding destes meses.")
    else:
        print("✅ Saúde da Base: Todas as safras estão acima da meta de 50%.")
    print("="*50 + "\n")

# --- 4. VISUALIZAÇÃO E PIPELINE ---

def exibir_dashboard_barras(df):
    df_2025 = df[df['DT_ADESAO'].dt.year == 2025].copy()
    if df_2025.empty: return None

    df_2025['MES_ADESAO'] = df_2025['DT_ADESAO'].dt.strftime('%m - %b')
    ordem_meses = sorted(df_2025['MES_ADESAO'].unique())

    sns.set_theme(style="whitegrid")
    fig, axes = plt.subplots(2, 2, figsize=(20, 12))
    fig.suptitle('Performance de Matrículas e Fidelização - Safra 2025', fontsize=22)

    # 1. Matrículas por Gênero
    df_mat = df_2025.groupby(['MES_ADESAO', 'GENERO']).size().reset_index(name='QTD')
    sns.barplot(ax=axes[0, 0], x='MES_ADESAO', y='QTD', hue='GENERO', data=df_mat, palette='pastel', order=ordem_meses)
    
    # 2. Taxa de Fidelização
    df_2025['ATIVO_BIT'] = df_2025['STATUS'].apply(lambda x: 1 if x in ['Ativo', 'Vigente'] else 0)
    df_fid = df_2025.groupby('MES_ADESAO').agg(TOTAL=('MATRICULA', 'count'), ATIVOS=('ATIVO_BIT', 'sum')).reset_index()
    df_fid['TAXA'] = (df_fid['ATIVOS'] / df_fid['TOTAL'] * 100).round(1)
    
    sns.barplot(ax=axes[0, 1], x='MES_ADESAO', y='TAXA', data=df_fid, palette='RdYlGn', order=ordem_meses)
    axes[0, 1].set_ylim(0, 100)

    # 3. Receita (VCL)
    df_vcl = df_2025.groupby('MES_ADESAO')['VCL_TOTAL'].sum().reset_index()
    sns.barplot(ax=axes[1, 0], x='MES_ADESAO', y='VCL_TOTAL', data=df_vcl, palette='Greens_d', order=ordem_meses)

    # 4. Total Absoluto
    sns.barplot(ax=axes[1, 1], x='MES_ADESAO', y='TOTAL', data=df_fid, color='steelblue', order=ordem_meses)

    for ax in axes.flat: ax.tick_params(axis='x', rotation=45)
    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    
    # Retornamos o DF de fidelização para o alerta no terminal
    return df_fid

def executar_sistema_bi_academia():
    print(f"--- 🚀 INICIANDO ETL ACADEMIA - {datetime.now().strftime('%d/%m/%Y')} ---")
    PLANILHA_LOOKER = "Dashboard_Academia_Looker"

    lista_bruta = []
    for p in range(86):
        dados = requisitar_dados_pacto("/psec/alunos/v2", {"page": p, "size": 100})
        if not dados or not dados.get('content'): break
        lista_bruta.extend(dados['content'])
        print(f"   > Extraindo: {len(lista_bruta)} registros", end='\r')
    
    if not lista_bruta: return

    df_final = processar_base_alunos(pd.DataFrame(lista_bruta))
    df_final = calcular_vcl_financeiro(df_final)

    # Visualização e Captura de Métricas para Alerta
    df_fid_mensal = exibir_dashboard_barras(df_final)
    
    # Aciona o Alerta no Terminal se houver dados
    if df_fid_mensal is not None:
        verificar_saude_fidelizacao(df_fid_mensal)
        plt.show() # Mostra o gráfico após o log de alerta

    # Carga Nuvem
    df_envio = df_final[df_final['DT_ADESAO'].dt.year == 2025].copy()
    sincronizar_google_sheets(df_envio, PLANILHA_LOOKER, "Base_Inteligencia_2025")
    
    print(f"\n📊 PIPELINE FINALIZADO!")

if __name__ == "__main__":
    executar_sistema_bi_academia()