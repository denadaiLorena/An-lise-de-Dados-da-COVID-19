import os
import subprocess
import sys
from typing import Optional

import numpy as np
import pandas as pd
import requests
import streamlit as st


PARQUET_PATH = "dados_es_filtrados.parquet"
DEFAULT_INPUT_CSV = "MICRODADOS.csv"
DEFAULT_URL_ENV = "MICRODADOS_URL"
PARQUET_URL_ENV = "PARQUET_URL"


@st.cache_data(persist="disk")
def carregar_dados_es(cache_buster: Optional[float] = None) -> pd.DataFrame:
    """Carrega a base consolidada do ES no formato otimizado Parquet.

    Se o arquivo não existir, tenta gerá-lo usando o prepare_data.py.
    O parâmetro `cache_buster` existe apenas para invalidar o cache quando o Parquet mudar.
    """

    file_path = PARQUET_PATH

    if not os.path.exists(file_path):
        for _ in range(5):
            st.write("")

        def _secret_or_env(key: str) -> Optional[str]:
            val = None
            try:
                val = st.secrets.get(key)  # type: ignore[attr-defined]
            except Exception:
                val = None
            return val or os.getenv(key)

        parquet_url = _secret_or_env(PARQUET_URL_ENV)

        microdados_url = _secret_or_env(DEFAULT_URL_ENV)

        if parquet_url:
            st.info(
                "ℹ️ **Primeira execução:** baixando o Parquet pronto. "
                "Isso costuma ser bem mais rápido do que processar o CSV no Cloud."
            )

            col_down, col_proc = st.columns(2)
            with col_down:
                st.markdown("**📥 Download do Parquet**")
                progress_bar_down = st.progress(0)
                status_text_down = st.empty()
            with col_proc:
                st.markdown("**⚙️ Verificação**")
                progress_bar_proc = st.progress(0)
                status_text_proc = st.empty()

            try:
                with st.status("Baixando Parquet...", expanded=True) as status:
                    with requests.get(str(parquet_url), stream=True, timeout=120) as resp:
                        resp.raise_for_status()
                        total = int(resp.headers.get("Content-Length") or 0)
                        downloaded = 0
                        last_percent = -1
                        with open(file_path, "wb") as f:
                            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                                if not chunk:
                                    continue
                                f.write(chunk)
                                downloaded += len(chunk)
                                if total > 0:
                                    percent = min(100, int(downloaded * 100 / total))
                                    if percent != last_percent:
                                        progress_bar_down.progress(percent)
                                        status_text_down.text(f"Progresso: {percent}%")
                                        last_percent = percent

                    progress_bar_down.progress(100)
                    progress_bar_proc.progress(100)
                    status_text_proc.text("OK")
                    status.update(label="✅ Parquet baixado com sucesso!", state="complete", expanded=False)
            except Exception as e:
                st.error("Falha ao baixar o Parquet via `PARQUET_URL`.")
                st.exception(e)
                st.stop()

        else:
            if not os.path.exists(DEFAULT_INPUT_CSV) and not microdados_url:
                st.error(
                    "Não encontrei `dados_es_filtrados.parquet` e também não encontrei `MICRODADOS.csv`.\n\n"
                    "No Streamlit Community Cloud você pode:\n"
                    "- Configurar `PARQUET_URL` (recomendado) apontando para um Parquet pronto, ou\n"
                    "- Incluir `MICRODADOS.csv` no repositório (ou Git LFS), ou\n"
                    "- Configurar `MICRODADOS_URL` para baixar o CSV e processar no Cloud (mais pesado)."
                )
                st.stop()

            st.info(
                "ℹ️ **Primeira execução:** vamos preparar a base otimizada (Parquet) a partir do CSV. "
                "Isso pode demorar alguns minutos e pode ser pesado no Cloud."
            )

            col_down, col_proc = st.columns(2)
            with col_down:
                st.markdown("**📥 Download da Base**")
                progress_bar_down = st.progress(0)
                status_text_down = st.empty()
            with col_proc:
                st.markdown("**⚙️ Processamento de Dados**")
                progress_bar_proc = st.progress(0)
                status_text_proc = st.empty()

            with st.status("Preparando base de dados otimizada...", expanded=True) as status:
                cmd = [sys.executable, "prepare_data.py"]
                if microdados_url:
                    cmd += ["--url", str(microdados_url)]

                process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    encoding="latin1",
                    errors="replace",
                    bufsize=1,
                )

                if process.stdout:
                    for line in process.stdout:
                        if "DOWNLOAD:" in line:
                            try:
                                val = int(line.split("DOWNLOAD:")[1].strip())
                                progress_bar_down.progress(max(0, min(100, val)))
                                status_text_down.text(f"Progresso: {val}%")
                            except Exception:
                                pass
                        elif "PROCESSING:" in line:
                            try:
                                val = int(line.split("PROCESSING:")[1].strip())
                                progress_bar_proc.progress(max(0, min(100, val)))
                                status_text_proc.text(f"Progresso: {val}%")
                            except Exception:
                                pass

                process.wait()

                if process.returncode != 0:
                    stderr_text = ""
                    try:
                        stderr_text = (process.stderr.read() if process.stderr else "")
                    except Exception:
                        stderr_text = ""

                    status.update(label="❌ Falha ao preparar os dados", state="error", expanded=True)
                    if stderr_text.strip():
                        st.code(stderr_text[-4000:], language="text")
                    st.stop()

                status.update(label="✅ Dados preparados com sucesso!", state="complete", expanded=False)
                status_text_down.empty()
                status_text_proc.empty()
                progress_bar_down.empty()
                progress_bar_proc.empty()

        if not os.path.exists(file_path):
            st.error(
                "O arquivo Parquet não foi criado. Verifique os logs acima (download/processamento) e a configuração do `MICRODADOS_URL`, se aplicável."
            )
            st.stop()

    try:
        # Arrow-backed dtypes reduzem muito o uso de RAM (importante no Community Cloud).
        df = pd.read_parquet(file_path, dtype_backend="pyarrow")
    except Exception as e:
        st.error("Falha ao ler o Parquet de dados.")
        st.exception(e)
        st.stop()

    required_cols = {"Municipio", "DataNotificacao", "Obito", "Evolucao", "DataObito"}
    missing = sorted([c for c in required_cols if c not in df.columns])
    if missing:
        st.error(
            "O Parquet carregou, mas faltam colunas necessárias para o app: " + ", ".join(missing)
        )
        st.stop()

    # Padronizar a coluna Evolucao sem forçar dtype object (evita explosão de memória)
    evol = df["Evolucao"].astype("string")
    evol = evol.fillna("Ignorado")
    evol = evol.replace({"-": "Ignorado", "nan": "Ignorado"})
    df["Evolucao"] = evol

    # Remover registros com inconsistência: Evolucao indica Óbito, mas DataObito é nula
    obitos_mask = df["Evolucao"].astype("string").str.contains("bito", na=False, case=False)
    inconsistentes = obitos_mask & df["DataObito"].isnull()
    df = df[~inconsistentes]

    # ---  MÉTRICA DE OURO:  MUNDO REAL ---
    # Ao invés de df.apply(axis=1) que é lento, usamos np.select para vetorização máxima.
    cond_obito = df["Evolucao"].astype("string").str.contains("bito", case=False, na=False)
    cond_cura = df["Evolucao"].astype("string").str.contains("Cura", case=False, na=False)
    cond_aberto = df["DataObito"].isnull()

    df["Status_Analise"] = np.select(
        [cond_obito, cond_cura, cond_aberto],
        ["Óbito", "Recuperado", "Em Aberto / Ignorado"],
        default="Outros",
    )
    df["Status_Analise"] = df["Status_Analise"].astype("category")

    return df
