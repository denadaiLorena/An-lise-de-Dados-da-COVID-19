import os
import html

import streamlit as st


def render_sidebar_sumario() -> None:
    from covid_app.nav import NAV_ITEMS

    st.sidebar.divider()
    st.sidebar.markdown("### Sumário")
    st.sidebar.caption("Clique em um item para ir direto ao gráfico/seção.")

    st.sidebar.markdown(
        """
<style>
/* UI Accent: usar #0047AB apenas em títulos, botões e ícones */
:root{
    --kdd-ui-accent: #0047AB;
}

/* Sidebar: manter fundo padrão do Streamlit (evita faixa clara no Dark Mode) */
/* 1. Alvo direto no container de conteúdo do Sidebar */
        [data-testid="stSidebarUserContent"] {
            background-color: color-mix(in srgb, var(--background-color) 95%, var(--text-color) 5%) !important;
            height: 100%;
        }

        /* 2. Remove o fundo padrão da seção externa para não dar conflito */
        [data-testid="stSidebar"] {
            background-color: color-mix(in srgb, var(--background-color) 95%, var(--text-color) 5%) !important;
            border-right: 1px solid color-mix(in srgb, var(--text-color) 10%, transparent);
        }

        /* 3. Estilo dos links do Sumário */
        .nav-link {
            text-decoration: none;
            color: var(--text-color) !important;
            opacity: 0.7;
            display: block;
            padding: 8px 12px;
            border-radius: 5px;
            margin-bottom: 5px;
            transition: 0.3s;
        }

        .nav-link:hover {
            opacity: 1;
            background-color: color-mix(in srgb, var(--background-color) 90%, var(--text-color) 10%);
            padding-left: 20px;
        }

/* Títulos */
h1, h2, h3, h4{
    color: var(--kdd-ui-accent);
}


/* Botões (Streamlit) */
button[data-testid="baseButton-primary"],
.stButton > button[kind="primary"],
.stFormSubmitButton > button{
    background-color: var(--kdd-ui-accent) !important;
    border-color: var(--kdd-ui-accent) !important;
}

//* Botão de exportação (download) com cinza adaptativo */
.stDownloadButton > button {
    /* Cria um cinza que é 94% a cor do fundo e 6% a cor do texto (contraste suave) */
    background-color: color-mix(in srgb, var(--background-color) 94%, var(--text-color) 6%) !important;
    color: var(--text-color) !important;
    border: 1px solid color-mix(in srgb, var(--text-color) 10%, transparent) !important;
    border-radius: 8px !important;
    padding: 0.5rem 1rem !important;
    transition: all 0.2s ease-in-out !important;
    font-weight: 500 !important;
}

.stDownloadButton > button:hover {
    /* No hover, aumentamos um pouco a presença da cor do texto para escurecer/clarear o cinza */
    background-color: color-mix(in srgb, var(--background-color) 88%, var(--text-color) 12%) !important;
    border-color: color-mix(in srgb, var(--text-color) 30%, transparent) !important;
    box-shadow: 0px 2px 4px rgba(0, 0, 0, 0.05) !important;
}

.stDownloadButton > button:active {
    transform: scale(0.98) !important;
}

/* Ícones (principalmente dentro de botões e controles) */
button svg, button span[aria-hidden="true"]{
    color: inherit;
}
section[data-testid="stSidebar"] svg{
    color: var(--kdd-ui-accent);
}

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
    border-color: var(--kdd-ui-accent);
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

    try:
        from covid_app.data import PARQUET_PATH, carregar_dados_es
        from covid_app.export import render_export_section
        from covid_app.filters import aplicar_filtros_sidebar
        from covid_app.sections import (
            render_comorbidades_e_etaria,
            render_cura,
            render_kdd_footer_expander,
            render_kpis,
            render_mapa_es_e_ranking,
            render_municipio_table,
            render_sobrevida_kdd,
            render_temporal_letalidade,
        )
    except Exception as e:
        st.error("Falha ao inicializar/importar módulos do app no ambiente de deploy.")
        st.exception(e)
        st.stop()

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

    render_kdd_footer_expander()


if __name__ == "__main__":
    main()
