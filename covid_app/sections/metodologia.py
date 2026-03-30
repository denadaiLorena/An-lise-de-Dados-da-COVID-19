import os

import streamlit as st

from ..nav import ANCHOR_METODOLOGIA
from .common import _anchor


def render_sidebar_kdd_expander() -> None:
    # Mantido por compatibilidade: o conteúdo agora é exibido no rodapé (fim da página).
    render_kdd_footer_expander()


def render_kdd_footer_expander() -> None:
    _anchor(ANCHOR_METODOLOGIA)
    st.divider()

    st.markdown(
        "<div class='kdd-section-title' style='text-align:left; font-weight:600; font-size:27px;'>Metodologia</div>",
        unsafe_allow_html=True,
    )

    # Sem colunas: o expander usa a largura total disponível.
    with st.expander("Processo KDD e Qualidade de Dados", expanded=False):
        csv_path = "MICRODADOS.csv"
        parquet_path = "dados_es_filtrados.parquet"

        def _fmt_bytes(n: int) -> str:
            # Formatação simples e estável (evita dependências externas).
            gb = 1024**3
            mb = 1024**2
            if n >= gb:
                return f"{n / gb:.2f} GB"
            return f"{n / mb:.1f} MB"

        csv_size = _fmt_bytes(os.path.getsize(csv_path)) if os.path.exists(csv_path) else None
        parquet_size = _fmt_bytes(os.path.getsize(parquet_path)) if os.path.exists(parquet_path) else None

        if csv_size and parquet_size:
            st.caption(f"Base local: CSV ~{csv_size} → Parquet ~{parquet_size} (otimizado para análise).")

        st.markdown(
            """
**KDD (Knowledge Discovery in Databases)**

Este painel foi construído com foco em **ganho de performance**, **consistência dos microdados** e **reprodutibilidade** do pipeline (ETL → Parquet → App):

**1) Engenharia de dados (ETL) para performance**
- **Leitura em chunks (100k linhas):** evita estouro de RAM ao processar o CSV grande.
- **Seleção de colunas essenciais:** reduz custo de I/O e acelera tudo a jusante.
- **Conversão para Parquet (colunar) com compressão:** melhora muito o tempo de carga e a responsividade do dashboard.
- **Tipagens leves:** comorbidades normalizadas para `int8` (menor memória, mais cache-friendly).
- **Preservação da idade como texto:** evita perder informação (ex.: “X anos, Y meses”) e deixa o parsing para a camada analítica.

**2) Qualidade de dados (limpeza e coerência)**
- **Normalização de valores sujos:** “-”, nulos e variações de “Sim/Não” são padronizados.
- **Coerência de óbito:** registros em que a evolução indica óbito mas `DataObito` está vazia são removidos no carregamento.
- **`Status_Analise` vetorizado:** classificação “Óbito / Recuperado / Em Aberto / Outros” usando `np.select` (muito mais rápido que `apply`).

**3) Otimizações no app (eficiência e UX)**
- **Cache com invalidação por `mtime`:** o Parquet é recarregado automaticamente quando muda, evitando “filtros presos”.
- **Parsing vetorizado de idade:** regex em coluna inteira (sem loop linha-a-linha) para montar faixas etárias rápido.
- **Exportação eficiente:** download em `.csv.gz` e com limite de linhas para não travar a interface.

**4) KDD na análise de sobrevida (seção de datas/óbito)**
- O cálculo usa **IQR (Intervalo Interquartil)** para tratar outliers de “dias até óbito”, reduzindo ruído estatístico.
            """
        )
