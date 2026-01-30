import json
import pandas as pd
import re

def gerar_manual_csv(arquivo_json):
    print(f"Processando mapa técnico...")
    
    try:
        with open(arquivo_json, 'r', encoding='utf-8') as f:
            mapa = json.load(f)
        
        registros = []
        
        for metodo_java, url_bruta in mapa.items():
            # 1. Limpa a URL: [/caminho] -> /caminho
            url_limpa = url_bruta.replace('[', '').replace(']', '')
            
            # 2. Extrai o nome do Controller e do Método usando Regex
            # Busca o padrão: br.com.pacto.controller...NomeController.metodo
            match = re.search(r'\.([^.]+Controller|[^.]+JSONControle)\.([^(\s]+)', metodo_java)
            controlador = match.group(1) if match else "Geral"
            funcao = match.group(2) if match else "Executar"

            # 3. Define uma categoria baseada no controlador
            categoria = "BI / Relatórios" if "BITreino" in controlador else \
                        "Financeiro" if "Faturamento" in controlador or "Caixa" in controlador else \
                        "Cadastro / Alunos" if "Aluno" in controlador or "Pessoa" in controlador else "Sistema"

            registros.append({
                "CATEGORIA": categoria,
                "ENDPOINT": url_limpa,
                "FUNCAO_JAVA": funcao,
                "CONTROLADOR_ORIGEM": controlador,
                "DESCRICAO_SUGERIDA": f"Realiza a ação '{funcao}' no módulo de {categoria.lower()}."
            })

        # Cria o DataFrame e ordena por Categoria e Endpoint
        df = pd.DataFrame(registros).sort_values(by=["CATEGORIA", "ENDPOINT"])
        
        nome_csv = "manual_tecnico_pacto.csv"
        df.to_csv(nome_csv, index=False, sep=';', encoding='utf-8-sig')
        
        print(f"✅ Manual gerado com {len(df)} rotas: {nome_csv}")
        
    except Exception as e:
        print(f"🛑 Erro: {e}")

if __name__ == "__main__":
    gerar_manual_csv('response_1768956951693.json')