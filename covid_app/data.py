import os
import subprocess
import sys
from typing import Optional

import numpy as np
import pandas as pd
import streamlit as st


PARQUET_PATH = "dados_es_filtrados.parquet"
DEFAULT_INPUT_CSV = "MICRODADOS.csv"
DEFAULT_URL_ENV = "MICRODADOS_URL"


@st.cache_data
def carregar_dados_es(cache_buster: Optional[float] = None) -> pd.DataFrame:
    """Carrega a base consolidada do ES no formato otimizado Parquet.

    Se o arquivo não existir, tenta gerá-lo usando o prepare_data.py.
    O parâmetro `cache_buster` existe apenas para invalidar o cache quando o Parquet mudar.
    """

    file_path = PARQUET_PATH

    if not os.path.exists(file_path):
        for _ in range(5):
            st.write("")

        microdados_url = None
        try:
            microdados_url = st.secrets.get(DEFAULT_URL_ENV)  # type: ignore[attr-defined]
        except Exception:
            microdados_url = None
        microdados_url = microdados_url or os.getenv(DEFAULT_URL_ENV)

        if not os.path.exists(DEFAULT_INPUT_CSV) and not microdados_url:
            st.error(
                "Não encontrei `MICRODADOS.csv` no repositório e nenhuma URL foi configurada para baixar o arquivo.\n\n"
                "No Streamlit Community Cloud, inclua o arquivo no repositório (ou via Git LFS), "
                "ou configure um Secret/ENV chamado `MICRODADOS_URL` com um link direto do CSV."
            )
            st.stop()

        st.info(
            "ℹ️ **Primeira execução:** vamos preparar a base otimizada (Parquet). "
            "Isso pode demorar alguns minutos."
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

    df = pd.read_parquet(file_path)

    # Padronizar a coluna Evolucao removendo hifens residuais ou nulos
    df["Evolucao"] = df["Evolucao"].astype(str).replace({"-": "Ignorado", "nan": "Ignorado"})

    # Remover registros com inconsistência: Evolucao indica Óbito, mas DataObito é nula
    obitos_mask = df["Evolucao"].astype(str).str.contains("bito", na=False, case=False)
    inconsistentes = obitos_mask & df["DataObito"].isnull()
    df = df[~inconsistentes]

    # ---  MÉTRICA DE OURO:  MUNDO REAL ---
    # Ao invés de df.apply(axis=1) que é lento, usamos np.select para vetorização máxima.
    cond_obito = df["Evolucao"].astype(str).str.contains("bito", case=False, na=False)
    cond_cura = df["Evolucao"].astype(str).str.contains("Cura", case=False, na=False)
    cond_aberto = df["DataObito"].isnull()

    df["Status_Analise"] = np.select(
        [cond_obito, cond_cura, cond_aberto],
        ["Óbito", "Recuperado", "Em Aberto / Ignorado"],
        default="Outros",
    )
    df["Status_Analise"] = df["Status_Analise"].astype("category")

    return df
