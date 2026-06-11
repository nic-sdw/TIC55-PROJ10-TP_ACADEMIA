# TP Academia - Top Physicus Academia Esteio

Pipeline de dados ETL em Python desenvolvido para apoiar a operação comercial da **Top Physicus Academia Esteio**. A aplicação coleta dados da API Pacto, lê a planilha de marketing da academia, trata e cruza as informações de alunos, leads e contratos ativos, e grava os resultados finais em uma planilha do Google Sheets. Esses dados consolidados facilitam a leitura das informações em um dashboard final, incluindo a visualização do funil de vendas.

## Objetivo

Automatizar a consolidação de dados entre agendamentos, leads de marketing e vendas realizadas por meio de um pipeline de dados, facilitando o acompanhamento de:

- alunos que fizeram aula experimental ou primeiro treino;
- origem dos leads de marketing;
- vendedora responsável pelo agendamento;
- confirmação de compra/contrato ativo;
- plano contratado;
- data e horário da matrícula;
- vendedora responsável pelo fechamento;
- relatório final consolidado para análise da academia;
- dashboard com leitura facilitada dos dados;
- funil de vendas para acompanhar a conversão de leads em alunos.

## Tecnologias Utilizadas

- Python
- Pandas
- Requests
- Gspread
- Google Sheets API
- Python Dotenv
- RapidFuzz

## Estrutura do Projeto

```text
TIC55-PROJ10-TP_ACADEMIA/
├── data__pipeline/
│   ├── __init__.py
│   ├── config.py
│   ├── extract.py
│   ├── transform.py
│   └── load.py
├── main.py
├── requirements.txt
└── README.md
```

## Como Funciona

O projeto funciona como um pipeline de dados no modelo ETL:

1. **Extract**
   - Consulta agendamentos na API Pacto.
   - Busca alunos/contratos ativos na Pacto.
   - Lê os leads da planilha de marketing no Google Sheets.

2. **Transform**
   - Filtra eventos relevantes, como aula experimental e primeiro treino.
   - Remove duplicidades.
   - Padroniza datas, horários e nomes.
   - Processa leads de marketing.
   - Usa fuzzy matching para cruzar nomes entre marketing, Pacto e contratos.
   - Calcula a vendedora responsável conforme horário e escala.

3. **Load**
   - Grava os dados tratados no Google Sheets.
   - Atualiza abas de histórico, marketing, vendas e relatório final.

## Abas Geradas no Google Sheets

Durante a execução, o script pode criar ou atualizar as seguintes abas:

- `HISTORICO`: histórico dos agendamentos filtrados.
- `MKT_CLONE`: cópia atual da base bruta de marketing.
- `VENDAS_MKT`: leads de marketing validados contra contratos ativos.
- `RELATORIO_FINAL`: relatório consolidado entre Pacto, marketing e vendas.

Essas abas servem como base para a construção do dashboard final, permitindo acompanhar indicadores comerciais e visualizar o funil de vendas da academia.

## Pré-requisitos

Antes de executar o projeto, é necessário ter:

- Python 3 instalado;
- acesso à API Pacto;
- uma credencial de conta de serviço do Google;
- acesso às planilhas do Google Sheets usadas pelo projeto.

## Instalação

Clone o repositório:

```bash
git clone https://github.com/nic-sdw/TIC55-PROJ10-TP_ACADEMIA.git
cd TIC55-PROJ10-TP_ACADEMIA
```

Crie e ative um ambiente virtual:

```bash
python -m venv .venv
```

No Windows:

```bash
.venv\Scripts\activate
```

No Linux/macOS:

```bash
source .venv/bin/activate
```

Instale as dependências:

```bash
pip install -r requirements.txt
```

## Configuração

Crie um arquivo `.env` na raiz do projeto com as variáveis abaixo:

```env
TOKEN=seu_token_da_api_pacto
EMPRESA_ID=id_da_empresa_na_pacto
TP_ACADEMIA_DB_ID=id_da_planilha_de_saida
GOOGLE_SHEETS_MKT=id_da_planilha_de_marketing
GOOGLE_JSON_FILE=service_account.json
```

Também é necessário colocar o arquivo de credenciais do Google na raiz do projeto. Por padrão, o código procura por:

```text
service_account.json
```

Caso o arquivo tenha outro nome, atualize a variável `GOOGLE_JSON_FILE` no `.env`.

## Execução

Com o ambiente virtual ativado e as variáveis configuradas, execute:

```bash
python main.py
```

O script irá:

- coletar agendamentos da Pacto;
- processar os leads da planilha de marketing;
- buscar contratos ativos;
- validar quais leads compraram;
- consolidar os dados;
- salvar os resultados nas abas configuradas do Google Sheets;
- disponibilizar uma base organizada para dashboard e análise do funil de vendas.

## Principais Regras de Negócio

- Apenas eventos específicos são considerados no histórico:
  - `Aula Experimental`
  - `Primeiro Treino sem A.E`
  - `Primeiro Treino com A.E`
- A comparação de nomes usa fuzzy matching para reduzir falhas causadas por abreviações, diferenças de digitação ou nomes incompletos.
- A vendedora de fechamento é inferida pelo horário da matrícula e pela escala definida no código.
- As abas finais evitam duplicidades, mantendo os registros mais recentes.

## Observações de Segurança

Este projeto depende de tokens e credenciais sensíveis. Não envie para o GitHub:

- arquivo `.env`;
- arquivo `service_account.json`;
- tokens da API Pacto;
- IDs privados de planilhas, caso sejam confidenciais.

## Desenvolvido Para

Projeto acadêmico desenvolvido para a **Top Physicus Academia Esteio**, com foco em pipeline de dados, automação comercial, dashboard gerencial e análise do funil de vendas.
