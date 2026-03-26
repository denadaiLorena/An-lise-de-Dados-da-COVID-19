import pandas as pd

def carregar_dados_es():
    """
    Carrega a base do ES previamente filtrada pelas colunas essenciais:
    Data, Município, Comorbidades, Óbito, Idade.
    
    O peso do app foi reduzido filtrando os dados originais no script prepare_data.py
    """
    # Otimização de memória usando "category" para colunas com poucos valores únicos
    dtype_dict = {
        'Municipio': 'category',
        'ComorbidadePulmao': 'category',
        'ComorbidadeCardio': 'category',
        'ComorbidadeRenal': 'category',
        'ComorbidadeDiabetes': 'category',
        'ComorbidadeTabagismo': 'category',
        'ComorbidadeObesidade': 'category',
        'Evolucao': 'category',
    }
    
    df = pd.read_csv(
        'dados_es_filtrados.csv', 
        dtype=dtype_dict,
        parse_dates=['DataNotificacao', 'DataObito']
    )
    return df

if __name__ == "__main__":
    print("Carregando a base do ES filtrada...")
    df = carregar_dados_es()
    print(f"\nDados carregados com sucesso! Total de linhas e colunas: {df.shape}")
    print("\nVisualização das primeiras linhas:\n", df.head())
