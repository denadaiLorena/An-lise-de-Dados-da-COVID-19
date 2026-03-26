import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import sys

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
total_rows_estimate = 5200000 # Estimativa aproximada para a barra de progresso
rows_processed = 0

try:
    # Continuamos baixando da web em pedaços para não estourar a RAM
    for chunk in pd.read_csv(url, sep=';', encoding='latin1', usecols=columns_to_keep, chunksize=chunk_size):
        
        # Converter as colunas de datas para Datetime real (para otimização e gráficos de linha futuros)
        chunk['DataNotificacao'] = pd.to_datetime(chunk['DataNotificacao'], format='%Y-%m-%d', errors='coerce')
        chunk['DataObito'] = pd.to_datetime(chunk['DataObito'], format='%Y-%m-%d', errors='coerce')
        
        # Tratar hifens '-' na coluna Evolucao antes de salvar
        if 'Evolucao' in chunk.columns:
            chunk['Evolucao'] = chunk['Evolucao'].replace('-', 'Ignorado')
            chunk['Evolucao'] = chunk['Evolucao'].fillna('Ignorado')
            
            # Criar coluna booleana para óbitos (True se a evolução contém 'bito')
            chunk['Obito'] = chunk['Evolucao'].astype(str).str.contains('bito', case=False, na=False)

        # Tratar colunas de Comorbidade com mapeamento numérico (1, 0, -1)
        comorb_cols = [col for col in chunk.columns if 'Comorbidade' in col]
        mapping = {'Sim': 1, 'Não': 0, '-': -1}
        
        for col in comorb_cols:
            # .map() é MUITO mais rápido que .replace() para mapeamentos exatos
            # O .fillna(-1) cuida de nulos e o .astype('int8') economiza RAM
            chunk[col] = chunk[col].map(mapping).fillna(-1).astype('int8')
        
        # Converte o pedaço (chunk) para o formato PyArrow Table
        table = pa.Table.from_pandas(chunk)
        
        # Se for o primeiro pedaço, inicializa o escritor Parquet com o esquema de colunas
        if writer is None:
            writer = pq.ParquetWriter(output_file, table.schema, compression='snappy')
            
        writer.write_table(table)
        
        rows_processed += len(chunk)
        percent = min(100, int((rows_processed / total_rows_estimate) * 100))
        # Imprime o progresso garantindo flush e removendo caracteres especiais problemáticos
        sys.stdout.write(f"PROGRESS:{percent}\n")
        sys.stdout.flush()
        
    if writer:
        writer.close()
    
    print(f"Processamento concluído! Arquivo otimizado salvo como {output_file}")
except Exception as e:
    print(f"Erro durante o processamento: {e}")

