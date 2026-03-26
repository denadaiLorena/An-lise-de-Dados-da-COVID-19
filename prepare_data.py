import pandas as pd
import urllib.request
import os

url = "https://one.s3.es.gov.br/pr-dl-sesa/covid/MICRODADOS.csv"
output_file = "dados_es_filtrados.csv"

# The essential columns as requested by the user:
# Data, Município, Comorbidades, Óbito, Idade
columns_to_keep = [
    'DataNotificacao', 
    'Municipio', 
    'ComorbidadePulmao', 
    'ComorbidadeCardio', 
    'ComorbidadeRenal', 
    'ComorbidadeDiabetes', 
    'ComorbidadeTabagismo', 
    'ComorbidadeObesidade', 
    'Evolucao', 
    'DataObito', 
    'IdadeNaDataNotificacao'
]

print("Iniciando o download e filtragem dos dados em chunks...")

chunk_size = 100000
first_chunk = True

try:
    for chunk in pd.read_csv(url, sep=';', encoding='latin1', usecols=columns_to_keep, chunksize=chunk_size):
        # We write the header only for the first chunk
        chunk.to_csv(output_file, mode='a', index=False, header=first_chunk)
        first_chunk = False
        print(f"Processados {chunk_size} registros...")
    
    print(f"Filtragem concluída! Arquivo salvo como {output_file}")
except Exception as e:
    print(f"Erro durante o processamento: {e}")
