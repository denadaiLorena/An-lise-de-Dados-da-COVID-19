import os
import html

import streamlit as st

from app_data import PARQUET_PATH, carregar_dados_es
from app_nav import NAV_ITEMS
from app_export import render_export_section
from app_filters import aplicar_filtros_sidebar
from app_sections import (
    render_comorbidades_e_etaria,
    render_cura,
    render_kpis,
    render_mapa_es_e_ranking,
    render_municipio_table,
    render_sidebar_kdd_expander,
    render_sobrevida_kdd,
    render_temporal_letalidade,
)


def render_sidebar_sumario() -> None:
    st.sidebar.divider()
    st.sidebar.markdown("### Sumário")
    st.sidebar.caption("Clique em um item para ir direto ao gráfico/seção.")

    st.sidebar.markdown(
        """
<style>
/* Sumário (sidebar) */
.kdd-nav a.kdd-nav-btn{
  display: block;
  padding: 0.45rem 0.6rem;
  margin: 0.35rem 0;
  border: 1px solid var(--border-color, rgba(49, 51, 63, 0.20));
  border-radius: 0.5rem;
  text-decoration: none !important;
  color: var(--text-color);
  background: var(--secondary-background-color);
}
.kdd-nav a.kdd-nav-btn:hover{
  border-color: var(--primary-color);
}
</style>
""",
        unsafe_allow_html=True,
    )

    links_html = "\n".join(
        [
            f'<a class="kdd-nav-btn" href="#{anchor_id}">{html.escape(title)}</a>'
            for title, anchor_id in NAV_ITEMS
        ]
    )

    st.sidebar.markdown(
        f'<div class="kdd-nav">{links_html}</div>',
        unsafe_allow_html=True,
    )

def main():
    st.set_page_config(page_title="Painel COVID-19 ES", layout="wide")
    st.title("Painel COVID-19 - Espírito Santo")

    with st.spinner("Carregando e processando a base filtrada do ES..."):
        cache_buster = os.path.getmtime(PARQUET_PATH) if os.path.exists(PARQUET_PATH) else None
        df = carregar_dados_es(cache_buster)

    # Se o arquivo de dados mudou, reseta filtros salvos em session_state
    # (evita o widget de período ficar "preso" em um intervalo antigo, ex: apenas 2023).
    if st.session_state.get('_parquet_mtime') != cache_buster:
        st.session_state['_parquet_mtime'] = cache_buster
        for key in ('municipio_sel', 'periodo_sel'):
            if key in st.session_state:
                del st.session_state[key]

    # Toast de sucesso logo após o título
    st.toast(f"✅ Base carregada: {len(df):,} registros".replace(
        ',', '.'), icon='📊')

    render_sidebar_kdd_expander()

    df, ctx = aplicar_filtros_sidebar(df)

    render_sidebar_sumario()
    render_kpis(df)
    render_municipio_table(df)
    render_mapa_es_e_ranking(df)
    render_comorbidades_e_etaria(df)
    render_sobrevida_kdd(df)
    render_temporal_letalidade(df)
    render_cura(df)

    render_export_section(
        df,
        municipio_sel=ctx["municipio_sel"],
        dt_ini=ctx["dt_ini"],
        dt_fim=ctx["dt_fim"],
    )


if __name__ == "__main__":
    main()
