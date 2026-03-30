import pandas as pd
import streamlit as st

from ..nav import ANCHOR_MUNICIPIO_TABLE
from .common import _anchor


def render_municipio_table(df: pd.DataFrame) -> None:
    _anchor(ANCHOR_MUNICIPIO_TABLE)
    st.markdown(
        """
<style>
.kdd-mun-title-row{
    width: 100%;
    display: flex;
    align-items: center;
    justify-content: flex-start;
    gap: 0.55rem;
    margin: 0.2rem 0 0.25rem 0;
}
.kdd-mun-title-row .kdd-mun-title{
    margin: 0;
    padding: 0;
    font-size: 1.55rem;
    font-weight: 700;
    line-height: 1.2;
    color: var(--text-color);
}
.kdd-info-icon{
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 1.6rem;
    height: 1.6rem;
    border-radius: 999px;
    background-color: rgba(128, 128, 128, 0.18);
    border: 1px solid color-mix(in srgb, var(--text-color) 38%, transparent);
    color: var(--text-color);
    opacity: 0.92;
    font-size: 0.95rem;
    cursor: help;
    user-select: none;
}
.kdd-table-legend-wrap{
    width: 100%;
    display: flex;
    justify-content: flex-end;
    align-items: center;
    gap: 0.5rem;
}
.kdd-table-legend{
    display: inline-flex;
    flex-wrap: wrap;
    gap: 0.8rem;
    align-items: center;
    padding: 0.45rem 0.6rem;
    border-radius: 0.5rem;
    background-color: rgba(128, 128, 128, 0.24);
    border: 1px solid color-mix(in srgb, var(--text-color) 38%, transparent);
}
.kdd-legend-item{
    display: inline-flex;
    align-items: center;
    font-size: 0.9rem;
    color: var(--text-color);
    opacity: 0.92;
}
.kdd-legend-dot{
    width: 0.75rem;
    height: 0.75rem;
    border-radius: 999px;
    display: inline-block;
    margin-right: 0.35rem;
    border: 1px solid color-mix(in srgb, var(--text-color) 28%, transparent);
}
.kdd-dot-green{ background-color: rgba(16, 185, 129, 0.55); }
.kdd-dot-white{
    /* Neutro fixo (alto contraste) para não “sumir” no tema escuro */
    background-color: rgba(148, 163, 184, 0.95);
    border: 2px solid rgba(71, 85, 105, 0.90);
    box-shadow: 0 0 0 1px rgba(0, 0, 0, 0.18);
}
.kdd-dot-red{ background-color: rgba(239, 68, 68, 0.55); }
</style>

<div class="kdd-mun-title-row">
    <h3 class="kdd-mun-title">Desempenho por Município (T.L.)</h3>
    <span
        class="kdd-info-icon"
        title="Observação: a letalidade por município é calculada apenas com desfechos finais (Óbito + Recuperado)."
        aria-label="Observação sobre o cálculo da letalidade"
    >ℹ</span>
</div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        "A tabela abaixo agrupa dados por município para facilitar a tomada de decisão focada na taxa de letalidade."
    )

    # Letalidade deve considerar apenas casos com desfecho final confirmado.
    if "Status_Analise" in df.columns:
        df_base = df[df["Status_Analise"].isin(["Óbito", "Recuperado"])].copy()
        df_municipio = (
            df_base.groupby("Municipio")
            .agg(
                Casos=("Municipio", "count"),
                Obitos=("Status_Analise", lambda x: (x == "Óbito").sum()),
            )
            .reset_index()
        )
    else:
        # Fallback: sem Status_Analise não dá para separar "Em Aberto" corretamente.
        if "Obito" in df.columns:
            df_municipio = (
                df.groupby("Municipio")
                .agg(
                    Casos=("Municipio", "count"),
                    Obitos=("Obito", lambda x: x.astype(int).sum()),
                )
                .reset_index()
            )
        else:
            df_municipio = df.groupby("Municipio").size().reset_index(name="Casos")
            df_municipio["Obitos"] = 0

    df_municipio["Taxa Letalidade (%)"] = (df_municipio["Obitos"] / df_municipio["Casos"]) * 100
    df_municipio = df_municipio.sort_values(by="Taxa Letalidade (%)", ascending=False)

    st.markdown(
        """
<div class="kdd-table-legend-wrap">
    <div class="kdd-table-legend" aria-label="Legenda (volume de casos)">
        <span class="kdd-legend-item"><span class="kdd-legend-dot kdd-dot-green"></span>&lt; 100 casos</span>
        <span class="kdd-legend-item"><span class="kdd-legend-dot kdd-dot-white"></span>demais casos (100–1000)</span>
        <span class="kdd-legend-item"><span class="kdd-legend-dot kdd-dot-red"></span>&gt; 1000 casos</span>
    </div>
</div>
        """,
        unsafe_allow_html=True,
    )

    def _row_bg_by_volume(row: pd.Series):
        try:
            casos = float(row.get("Casos", 0))
        except Exception:
            casos = 0.0

        if casos > 1000:
            bg = "background-color: rgba(239, 68, 68, 0.16);"  # vermelho claro
        elif casos < 100:
            bg = "background-color: rgba(16, 185, 129, 0.14);"  # verde claro
        else:
            bg = ""
        return [bg] * len(row)

    styler = df_municipio.style.apply(_row_bg_by_volume, axis=1).format(
        {
            "Casos": lambda v: f"{int(v):,}".replace(",", ".") if pd.notnull(v) else "",
            "Obitos": lambda v: f"{int(v):,}".replace(",", ".") if pd.notnull(v) else "",
            "Taxa Letalidade (%)": lambda v: f"{v:.2f}%" if pd.notnull(v) else "",
        }
    )

    # Compatibilidade pandas (hide vs hide_index)
    if hasattr(styler, "hide"):
        styler = styler.hide(axis="index")
    elif hasattr(styler, "hide_index"):
        styler = styler.hide_index()

    st.dataframe(styler, use_container_width=True)

    with st.expander("🔍 Visualizar Microdados (Amostra de 200 linhas)"):
        st.dataframe(df.head(200), use_container_width=True)

    st.divider()
