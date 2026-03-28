from __future__ import annotations

import pandas as pd
import streamlit as st


def aplicar_filtros_sidebar(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """Renderiza filtros na sidebar e aplica o recorte.

    Retorna (df_filtrado, contexto), onde `contexto` inclui municipio_sel, date_col,
    periodo_sel, dt_ini e dt_fim.
    """
    st.sidebar.header("Filtros")

    # Filtro por Município
    if "Municipio" in df.columns:
        municipios = sorted(
            [m for m in df["Municipio"].dropna().astype(str).unique().tolist() if m.strip()]
        )
        municipios_opcoes = ["Todos"] + municipios
        municipio_sel = st.sidebar.selectbox(
            "Município", municipios_opcoes, index=0, key="municipio_sel"
        )
    else:
        municipio_sel = "Todos"
        st.sidebar.info("Coluna 'Municipio' não encontrada para filtrar.")

    # Filtro por Período (tenta descobrir a melhor coluna de data)
    date_col_candidates = [
        "DataNotificacao",
        "DataDiagnostico",
        "DataCadastro",
        "DataColeta",
        "DataInicioSintomas",
    ]
    date_col = next((c for c in date_col_candidates if c in df.columns), None)

    if date_col is not None:
        df_dates = pd.to_datetime(df[date_col], errors="coerce")
        min_date = df_dates.min()
        max_date = df_dates.max()

        if pd.notna(min_date) and pd.notna(max_date):
            periodo_sel = st.sidebar.date_input(
                "Período",
                value=(min_date.date(), max_date.date()),
                min_value=min_date.date(),
                max_value=max_date.date(),
                key="periodo_sel",
            )
        else:
            periodo_sel = None
            st.sidebar.info(f"Não foi possível inferir datas válidas em '{date_col}'.")
    else:
        periodo_sel = None
        st.sidebar.info(
            "Nenhuma coluna de data conhecida foi encontrada para filtrar o período."
        )

    # Aplicar filtros
    df_filtrado = df
    dt_ini = None
    dt_fim = None

    if municipio_sel != "Todos" and "Municipio" in df_filtrado.columns:
        df_filtrado = df_filtrado[df_filtrado["Municipio"].astype(str) == str(municipio_sel)]

    if date_col is not None and periodo_sel is not None:
        if isinstance(periodo_sel, (tuple, list)) and len(periodo_sel) == 2:
            dt_ini = pd.to_datetime(periodo_sel[0])
            dt_fim = pd.to_datetime(periodo_sel[1])
            dt_series = pd.to_datetime(df_filtrado[date_col], errors="coerce")
            df_filtrado = df_filtrado[(dt_series >= dt_ini) & (dt_series <= dt_fim)]

    ctx = {
        "municipio_sel": municipio_sel,
        "date_col": date_col,
        "periodo_sel": periodo_sel,
        "dt_ini": dt_ini,
        "dt_fim": dt_fim,
    }

    return df_filtrado, ctx
