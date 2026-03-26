import pandas as pd
import os

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

print("Lendo CSV (pode demorar um pouquinho)...")
df = pd.read_csv('dados_es_filtrados.csv', dtype=dtype_dict, parse_dates=['DataNotificacao', 'DataObito'])

print("Convertendo e salvando para formato Parquet...")
# The pyarrow engine is required, but will be installed by pip
df.to_parquet('dados_es_filtrados.parquet', engine='pyarrow', index=False)

csv_size = os.path.getsize('dados_es_filtrados.csv') / (1024*1024)
pq_size = os.path.getsize('dados_es_filtrados.parquet') / (1024*1024)

print(f"Conversão concluída com sucesso!")
print(f"Tamanho CSV original: {csv_size:.2f} MB")
print(f"Novo tamanho Parquet: {pq_size:.2f} MB")
