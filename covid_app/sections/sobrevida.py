import pandas as pd
import plotly.express as px
import streamlit as st

from ..nav import (
    ANCHOR_SOBREVIDA_BOX_ANTES,
    ANCHOR_SOBREVIDA_BOX_DEPOIS,
    ANCHOR_SOBREVIDA_HIST,
    ANCHOR_SOBREVIDA_SEC,
)
from .common import ES_BLUE, ES_PINK, _anchor, plotly_chart_with_loader


def render_sobrevida_kdd(df: pd.DataFrame) -> None:
    st.divider()
    _anchor(ANCHOR_SOBREVIDA_SEC)
    st.subheader("Análise de tempo da notificação ao óbito")

    _anchor(ANCHOR_SOBREVIDA_BOX_ANTES)
    _anchor(ANCHOR_SOBREVIDA_BOX_DEPOIS)
    _anchor(ANCHOR_SOBREVIDA_HIST)

    df_sobrevida = df[df["Obito"] == True].copy()
    df_sobrevida["DataNotificacao"] = pd.to_datetime(df_sobrevida["DataNotificacao"], errors="coerce")
    df_sobrevida["DataObito"] = pd.to_datetime(df_sobrevida["DataObito"], errors="coerce")
    df_sobrevida = df_sobrevida.dropna(subset=["DataNotificacao", "DataObito"])

    df_sobrevida["Dias_Sobrevida"] = (df_sobrevida["DataObito"] - df_sobrevida["DataNotificacao"]).dt.days

    df_base = df_sobrevida[df_sobrevida["Dias_Sobrevida"] >= 0].copy()

    if df_base.empty:
        st.info("Dados de datas insuficientes para o processo KDD.")
        return

    Q1 = df_base["Dias_Sobrevida"].quantile(0.25)
    Q3 = df_base["Dias_Sobrevida"].quantile(0.75)
    IQR = Q3 - Q1

    limite_inferior = Q1 - 1.5 * IQR
    limite_superior = Q3 + 1.5 * IQR

    df_limpo = df_base[
        (df_base["Dias_Sobrevida"] >= limite_inferior) & (df_base["Dias_Sobrevida"] <= limite_superior)
    ].copy()

    st.markdown("#### KDD: Tratamento de Outliers (IQR)")
    st.info(
        f"O Intervalo Interquartil (IQR) definiu registros acima de {int(limite_superior)} dias como outliers estatísticos."
    )

    col_box1, col_box2 = st.columns(2)

    with col_box1:
        def _build_box_pre():
            fig = px.box(
                df_base,
                y="Dias_Sobrevida",
                title="Antes: Com Outliers",
                labels={"Dias_Sobrevida": "Dias"},
                template="plotly_white",
                color_discrete_sequence=[ES_PINK],
            )
            fig.update_layout(height=400)
            return fig

        plotly_chart_with_loader(_build_box_pre, message="Carregando boxplot (antes da limpeza)...")

    with col_box2:
        def _build_box_pos():
            fig = px.box(
                df_limpo,
                y="Dias_Sobrevida",
                title="Depois: Limpeza IQR",
                labels={"Dias_Sobrevida": "Dias"},
                template="plotly_white",
                color_discrete_sequence=[ES_BLUE],
            )
            fig.update_layout(height=400)
            return fig

        plotly_chart_with_loader(_build_box_pos, message="Carregando boxplot (após a limpeza)...")

    media_saneada = df_limpo["Dias_Sobrevida"].mean()
    mediana_saneada = df_limpo["Dias_Sobrevida"].median()

    modal_calc = df_limpo[df_limpo["Dias_Sobrevida"] > 0]["Dias_Sobrevida"].mode()
    _moda_saneada = modal_calc[0] if not modal_calc.empty else 0

    st.markdown("#### Estatísticas Pós-Limpeza")
    cs1, cs2, _cs3 = st.columns(3)
    cs1.metric("Média Real", f"{media_saneada:.1f} dias")
    cs2.metric("Mediana", f"{int(mediana_saneada)} dias")

    # Relatório interpretativo (pós-limpeza)
    removidos = int(len(df_base) - len(df_limpo))
    total_pos = int(len(df_limpo))
    media_bruta = float(df_base["Dias_Sobrevida"].mean()) if len(df_base) > 0 else None
    p25 = float(df_limpo["Dias_Sobrevida"].quantile(0.25)) if total_pos > 0 else None
    p75 = float(df_limpo["Dias_Sobrevida"].quantile(0.75)) if total_pos > 0 else None
    p90 = float(df_limpo["Dias_Sobrevida"].quantile(0.90)) if total_pos > 0 else None

    linhas = []
    linhas.append(
        f"- Amostra analisada: **{total_pos}** registros após a limpeza (removidos **{removidos}** outliers pelo IQR)."
    )
    linhas.append(
        f"- Centro da distribuição: mediana **{int(mediana_saneada)}** dias (menos sensível a extremos que a média)."
    )
    if p25 is not None and p75 is not None:
        linhas.append(f"- Faixa típica (P25–P75): **{p25:.0f}–{p75:.0f}** dias (onde está o miolo dos casos).")
    if p90 is not None:
        linhas.append(
            "- Cauda (P90): "
            f"**{p90:.0f}** dias — 90% dos óbitos ocorreram até esse tempo após a notificação, no recorte atual."
        )

    st.markdown("**Relatório (interpretação do recorte pós-limpeza)**\n" + "\n".join(linhas))

    def _build_hist():
        fig = px.histogram(
            df_limpo,
            x="Dias_Sobrevida",
            nbins=30,
            title="Distribuição Final da Sobrevida (Dados Saneados)",
            labels={"Dias_Sobrevida": "Dias após Notificação", "count": "Frequência"},
            template="plotly_white",
            color_discrete_sequence=[ES_BLUE],
        )
        fig.add_vline(
            x=media_saneada,
            line_dash="dash",
            line_color=ES_PINK,
            annotation_text=f"Média: {media_saneada:.1f}",
        )
        fig.update_layout(margin=dict(l=20, r=20, t=40, b=20), height=400)
        return fig

    plotly_chart_with_loader(_build_hist, message="Carregando histograma de sobrevida...")

    st.caption(
        f"💡 *O processo removeu {len(df_base) - len(df_limpo)} registros considerados ruído estatístico, resultando em uma média muito mais próxima da realidade clínica.*"
    )
