import streamlit as st
from typing import Any, Callable


# Paleta inspirada na bandeira do Espírito Santo (Azul, Rosa e Branco)
ES_BLUE = "#005AA7"
ES_PINK = "#E91E63"
ES_WHITE = "#FFFFFF"
ES_SCALE = [
    [0.0, "#FFFFFF"],
    [0.45, "#F8BBD0"],
    [1.0, ES_BLUE],
]


def _anchor(anchor_id: str) -> None:
    """Insere um anchor no DOM para navegação via #hash."""

    st.markdown(f'<a id="{anchor_id}"></a>', unsafe_allow_html=True)


def plotly_chart_with_loader(
    build_fig: Callable[[], Any],
    *,
    message: str,
    use_container_width: bool = True,
) -> Any:
    """Mostra um loader visível até o gráfico Plotly estar pronto.

    Usa um placeholder (info) + spinner, e substitui o placeholder pelo gráfico.
    """

    placeholder = st.empty()
    placeholder.info(message, icon="⏳")
    with st.spinner(message):
        fig = build_fig()
    placeholder.plotly_chart(fig, use_container_width=use_container_width)
    return fig
