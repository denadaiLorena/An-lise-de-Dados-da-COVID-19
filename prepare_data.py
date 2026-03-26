import pandas as pd
import urllib.request
import os
import pyarrow as pa
import pyarrow.parquet as pq

url = "https://one.s3.es.gov.br/pr-dl-sesa/covid/MICRODADOS.csv"
output_file = "dados_es_filtrados.parquet"

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
writer = None

try:
    # Continuamos baixando da web em pedaços para não estourar a RAM
    for chunk in pd.read_csv(url, sep=';', encoding='latin1', usecols=columns_to_keep, chunksize=chunk_size):
        
        # Converter as colunas de datas para Datetime real (para otimização e gráficos de linha futuros)
        chunk['DataNotificacao'] = pd.to_datetime(chunk['DataNotificacao'], errors='coerce')
        chunk['DataObito'] = pd.to_datetime(chunk['DataObito'], errors='coerce')
        
        # Converte o pedaço (chunk) para o formato PyArrow Table
        table = pa.Table.from_pandas(chunk)
        
        # Se for o primeiro pedaço, inicializa o escritor Parquet com o esquema de colunas
        if writer is None:
            writer = pq.ParquetWriter(output_file, table.schema, compression='snappy')
            
        writer.write_table(table)
        print(f"Processados {len(chunk)} registros e salvos no Parquet...")
        
    if writer:
        writer.close()
    
    print(f"Processamento concluído! Arquivo otimizado salvo como {output_file}")
except Exception as e:
    print(f"Erro durante o processamento: {e}")

