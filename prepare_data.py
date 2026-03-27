import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import sys
import os
import argparse

DEFAULT_INPUT_FILE = "MICRODADOS.csv"
DEFAULT_OUTPUT_FILE = "dados_es_filtrados.parquet"

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

def _normalize_yes_no_unknown(value: object) -> int:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return -1
    text = str(value).strip().casefold()
    if text in {"sim", "s"}:
        return 1
    if text in {"não", "nao", "n"}:
        return 0
    if text in {"-", "", "nan", "ignorado", "ignorada"}:
        return -1
    return -1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Converte MICRODADOS.csv em Parquet filtrado (chunks).")
    parser.add_argument("--input", default=DEFAULT_INPUT_FILE, help="Caminho do CSV de entrada.")
    parser.add_argument("--output", default=DEFAULT_OUTPUT_FILE, help="Caminho do Parquet de saída.")
    args = parser.parse_args(argv)

    input_file = args.input
    output_file = args.output

    print("Iniciando a leitura e filtragem do CSV local em chunks...")

    if not os.path.exists(input_file):
        print(f"Erro: arquivo CSV não encontrado: {input_file}")
        return 1

    # Mantém compatibilidade com a UI do Streamlit (barra de download)
    sys.stdout.write("DOWNLOAD:100\n")
    sys.stdout.flush()

    chunk_size = 100000
    writer: pq.ParquetWriter | None = None
    total_rows_estimate = 5200000
    rows_processed = 0

    try:
        for chunk in pd.read_csv(
            input_file,
            sep=';',
            encoding='latin1',
            usecols=columns_to_keep,
            chunksize=chunk_size,
            low_memory=False,
        ):
            chunk['DataNotificacao'] = pd.to_datetime(chunk['DataNotificacao'], format='%Y-%m-%d', errors='coerce')
            chunk['DataObito'] = pd.to_datetime(chunk['DataObito'], format='%Y-%m-%d', errors='coerce')

            chunk['Evolucao'] = chunk['Evolucao'].replace('-', 'Ignorado').fillna('Ignorado')
            chunk['Obito'] = chunk['Evolucao'].astype(str).str.contains('bito', case=False, na=False).astype('bool')

            comorb_cols = [col for col in chunk.columns if 'Comorbidade' in col]
            for col in comorb_cols:
                # Normaliza valores como 'Sim', 'Não', 'Nao', '-', nulos
                chunk[col] = chunk[col].map(_normalize_yes_no_unknown).astype('int8')

            # Garante tipagem consistente do inteiro de idade quando possível
            if 'IdadeNaDataNotificacao' in chunk.columns:
                chunk['IdadeNaDataNotificacao'] = pd.to_numeric(chunk['IdadeNaDataNotificacao'], errors='coerce')

            table = pa.Table.from_pandas(chunk, preserve_index=False)

            if writer is None:
                writer = pq.ParquetWriter(output_file, table.schema, compression='snappy')

            writer.write_table(table)

            rows_processed += len(chunk)
            percent = min(100, int((rows_processed / total_rows_estimate) * 100))
            sys.stdout.write(f"PROCESSING:{percent}\n")
            sys.stdout.flush()

        print(f"Processamento concluído! Arquivo otimizado salvo como {output_file}")
        return 0
    except Exception as e:
        print(f"Erro durante o processamento: {e}")
        return 1
    finally:
        if writer is not None:
            writer.close()


if __name__ == "__main__":
    raise SystemExit(main())
