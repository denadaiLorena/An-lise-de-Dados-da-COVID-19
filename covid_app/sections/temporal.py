import pandas as pd
import plotly.express as px
import streamlit as st

from ..nav import ANCHOR_TEMPORAL_LETALIDADE
from .common import ES_BLUE, ES_PINK, _anchor, plotly_chart_with_loader


def render_temporal_letalidade(df: pd.DataFrame) -> None:
    _anchor(ANCHOR_TEMPORAL_LETALIDADE)
    st.divider()
    st.subheader("Evolução Temporal da Taxa de Letalidade (%)")

    # Letalidade temporal deve considerar apenas casos com desfecho final.
    if "Status_Analise" in df.columns:
        df_temporal = df[df["Status_Analise"].isin(["Óbito", "Recuperado"])][["DataNotificacao", "Obito"]].copy()
    else:
        df_temporal = df[["DataNotificacao", "Obito"]].copy()

    df_temporal["DataNotificacao"] = pd.to_datetime(df_temporal["DataNotificacao"], errors="coerce")
    df_temporal = df_temporal.dropna(subset=["DataNotificacao"])

    # Compatível com datetime "pyarrow-backed" (ArrowTemporalProperties não possui .to_period)
    df_temporal["Mes_Ano"] = (
        df_temporal["DataNotificacao"].dt.year.astype("int64").astype(str)
        + "-"
        + df_temporal["DataNotificacao"].dt.month.astype("int64").astype(str).str.zfill(2)
    )

    stats_tempo = (
        df_temporal.groupby("Mes_Ano").agg(Casos=("Obito", "size"), Obitos=("Obito", "sum")).reset_index()
    )

    stats_tempo["Taxa Letalidade (%)"] = (stats_tempo["Obitos"] / stats_tempo["Casos"]) * 100

    pico = stats_tempo.loc[stats_tempo["Taxa Letalidade (%)"].idxmax()]

    ct1, ct2 = st.columns([1, 3])

    with ct1:
        st.write("")
        st.metric("Pior Período (Letalidade)", str(pico["Mes_Ano"]))
        st.metric("Taxa no Pico", f"{pico['Taxa Letalidade (%)']:.2f}%")
        st.caption(f"No mês de {pico['Mes_Ano']}, a letalidade foi a mais alta da série histórica.")

    with ct2:
        def _build_temporal_fig():
            fig = px.line(
                stats_tempo,
                x="Mes_Ano",
                y="Taxa Letalidade (%)",
                title="Taxa de Letalidade Mensal ao Longo do Tempo",
                labels={"Mes_Ano": "Mês de Notificação", "Taxa Letalidade (%)": "Letalidade (%)"},
                template="plotly_white",
                markers=True,
            )
            fig.add_annotation(
                x=pico["Mes_Ano"],
                y=pico["Taxa Letalidade (%)"],
                text="PICO",
                showarrow=True,
                arrowhead=1,
                arrowcolor=ES_PINK,
                font=dict(color=ES_PINK, size=12),
            )
            fig.update_traces(line_color=ES_BLUE)
            fig.update_layout(margin=dict(l=20, r=20, t=40, b=20), height=400)
            return fig

        plotly_chart_with_loader(_build_temporal_fig, message="Carregando gráfico temporal...")

        if not stats_tempo.empty:
            ultimo = stats_tempo.iloc[-1]
            st.markdown(
                (
                    f"Observa-se que o período de maior letalidade foi **{pico['Mes_Ano']}** "
                    f"(**{pico['Taxa Letalidade (%)']:.2f}%**). "
                    f"No período mais recente (**{ultimo['Mes_Ano']}**), a taxa está em **{ultimo['Taxa Letalidade (%)']:.2f}%**. "
                    "Isso ajuda a contextualizar se o cenário atual está acima ou abaixo do pico histórico dentro do recorte aplicado."
                )
            )
