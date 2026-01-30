import json
import pandas as pd

def extrair_mapa_endpoints(caminho_arquivo):
    print(f"Lendo arquivo: {caminho_arquivo}...")
    
    try:
        with open(caminho_arquivo, 'r', encoding='utf-8') as f:
            dados = json.load(f)
        
        lista_final = []
        
        for metodo_java, path_bruto in dados.items():
            # Limpa os colchetes do path: [/exemplo] -> /exemplo
            path_limpo = path_bruto.replace('[', '').replace(']', '')
            
            # Tenta extrair o nome curto do controlador para facilitar a busca
            # Ex: br.com.pacto.controller.json.gestao.BITreinoController -> BITreinoController
            partes_metodo = metodo_java.split(' ')
            metodo_completo = partes_metodo[-1] if len(partes_metodo) > 1 else metodo_java
            
            lista_final.append({
                "ENDPOINT": path_limpo,
                "METODO_JAVA": metodo_completo,
                "ASSINATURA_COMPLETA": metodo_java
            })
            
        # Converte para DataFrame e ordena por endpoint
        df = pd.DataFrame(lista_final)
        df = df.sort_values(by="ENDPOINT")
        
        # Salva em CSV para análise em BI/Excel
        nome_saida = "mapeamento_completo_pacto.csv"
        df.to_csv(nome_saida, index=False, sep=';', encoding='utf-8-sig')
        
        print(f"✅ Sucesso! {len(df)} endpoints extraídos.")
        print(f"📂 Arquivo gerado: {nome_saida}")
        
        # Mostra os 10 primeiros para conferência
        print("\nExemplo das primeiras rotas encontradas:")
        print(df[["ENDPOINT", "METODO_JAVA"]].head(10))

    except FileNotFoundError:
        print("Erro: O arquivo JSON não foi encontrado na pasta.")
    except Exception as e:
        print(f"Erro inesperado: {e}")

if __name__ == "__main__":
    # Certifique-se que o nome do arquivo abaixo é exatamente o do seu JSON
    extrair_mapa_endpoints('response_1768956951693.json')