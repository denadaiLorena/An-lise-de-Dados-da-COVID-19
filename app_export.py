import gzip
import io

import pandas as pd
import streamlit as st

from app_geo import normalizar_municipio
from app_nav import ANCHOR_EXPORTACAO


def _anchor(anchor_id: str) -> None:
    st.markdown(f'<a id="{anchor_id}"></a>', unsafe_allow_html=True)


@st.cache_data(show_spinner=False)
def dataframe_to_csv_gz_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        with io.TextIOWrapper(gz, encoding="utf-8-sig", newline="") as text_stream:
            df.to_csv(text_stream, index=False)
    return buf.getvalue()


def render_export_section(
    df: pd.DataFrame,
    *,
    municipio_sel: str,
    dt_ini,
    dt_fim,
    max_export_rows: int = 300_000,
) -> None:
    _anchor(ANCHOR_EXPORTACAO)
    st.subheader("Exportação")
    st.caption(
        "Baixe exatamente os microdados após os filtros aplicados (município e período)."
    )

    export_rows = len(df)

    if export_rows == 0:
        st.info("Sem registros no recorte atual para exportar.")
        st.divider()
        return

    if export_rows > max_export_rows:
        st.warning(
            f"O recorte atual tem {export_rows:,} linhas ".replace(",", ".")
            + f"(limite para exportação no app: {max_export_rows:,}).".replace(",", ".")
        )
        st.caption("Refine os filtros (município e/ou período) para habilitar o download.")
        st.divider()
        return

    municipio_tag = "TODOS" if municipio_sel == "Todos" else normalizar_municipio(str(municipio_sel))
    if dt_ini is not None and dt_fim is not None:
        periodo_tag = f"{dt_ini.date().isoformat()}_a_{dt_fim.date().isoformat()}"
    else:
        periodo_tag = "periodo_completo"

    file_name = f"dados_filtrados_es_{municipio_tag}_{periodo_tag}.csv.gz"
    with st.spinner("Preparando arquivo para download..."):
        export_bytes = dataframe_to_csv_gz_bytes(df)

    st.download_button(
        label="Baixar dados filtrados (CSV compactado)",
        data=export_bytes,
        file_name=file_name,
        mime="application/gzip",
    )

