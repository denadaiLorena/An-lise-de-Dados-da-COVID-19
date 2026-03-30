import pandas as pd
import plotly.express as px
import streamlit as st

from ..features import extrair_idade_anos
from ..geo import carregar_geojson_municipios_es, normalizar_municipio, _get_geojson_municipio_nome
from ..nav import ANCHOR_CURA_COMORB, ANCHOR_CURA_ETARIA, ANCHOR_CURA_MAPA, ANCHOR_CURA_SEC
from .common import ES_BLUE, _anchor, plotly_chart_with_loader


def render_cura(df: pd.DataFrame) -> None:
    st.divider()
    _anchor(ANCHOR_CURA_SEC)
    st.subheader("Análise de Pacientes Recuperados (Cura)")

    _anchor(ANCHOR_CURA_COMORB)
    _anchor(ANCHOR_CURA_ETARIA)
    _anchor(ANCHOR_CURA_MAPA)

    df_cura = df[df["Status_Analise"] == "Recuperado"].copy()

    if df_cura.empty:
        st.info("Não há dados de pacientes recuperados para exibir.")
        return

    col_cura1, col_cura2 = st.columns(2)

    with col_cura1:
        comorb_cols_cura = {
            "ComorbidadeDiabetes": "Diabetes",
            "ComorbidadeCardio": "Cardiopatia",
            "ComorbidadePulmao": "Pulmonar",
            "ComorbidadeRenal": "Renal",
            "ComorbidadeTabagismo": "Tabagismo",
            "ComorbidadeObesidade": "Obesidade",
        }
        existentes_cura = [c for c in comorb_cols_cura.keys() if c in df_cura.columns]

        if existentes_cura:
            dados_comorb_cura = df_cura[existentes_cura].sum().reset_index()
            dados_comorb_cura.columns = ["Comorbidade", "Total"]
            dados_comorb_cura["Comorbidade"] = dados_comorb_cura["Comorbidade"].map(comorb_cols_cura)
            dados_comorb_cura = dados_comorb_cura.sort_values("Total", ascending=True)

            def _build_comorb_cura_fig():
                fig = px.bar(
                    dados_comorb_cura,
                    x="Total",
                    y="Comorbidade",
                    orientation="h",
                    title="Comorbidades em Pacientes Recuperados",
                    labels={"Total": "Nº de Recuperados", "Comorbidade": ""},
                    template="plotly_white",
                    color_discrete_sequence=[ES_BLUE],
                )
                fig.update_layout(margin=dict(l=20, r=20, t=40, b=20), height=350)
                return fig

            plotly_chart_with_loader(
                _build_comorb_cura_fig,
                message="Carregando gráfico de comorbidades (recuperados)...",
            )

    with col_cura2:
        if "IdadeNaDataNotificacao" in df_cura.columns:
            df_cura_local = df_cura.copy()
            df_cura_local["idade_num"] = extrair_idade_anos(df_cura_local["IdadeNaDataNotificacao"])

            bins_cura = [0, 10, 20, 30, 40, 50, 60, 70, 80, 150]
            labels_cura = [
                "0-10",
                "11-20",
                "21-30",
                "31-40",
                "41-50",
                "51-60",
                "61-70",
                "71-80",
                "80+",
            ]
            df_cura_local["Faixa Etária"] = pd.cut(
                df_cura_local["idade_num"],
                bins=bins_cura,
                labels=labels_cura,
                right=False,
            )

            counts_cura = df_cura_local["Faixa Etária"].value_counts().reset_index()
            counts_cura.columns = ["Faixa Etária", "Recuperados"]
            counts_cura = counts_cura.sort_values("Faixa Etária")

            def _build_idade_cura_fig():
                fig = px.bar(
                    counts_cura,
                    x="Faixa Etária",
                    y="Recuperados",
                    title="Distribuição Etária dos Recuperados",
                    labels={"Recuperados": "Nº de Pessoas", "Faixa Etária": "Anos"},
                    template="plotly_white",
                    color_discrete_sequence=[ES_BLUE],
                )
                fig.update_layout(margin=dict(l=20, r=20, t=40, b=20), height=350)
                return fig

            plotly_chart_with_loader(
                _build_idade_cura_fig,
                message="Carregando gráfico etário (recuperados)...",
            )

    total_casos_val = len(df[df["Status_Analise"].isin(["Óbito", "Recuperado"])])
    if total_casos_val > 0:
        taxa_recuperacao = (len(df_cura) / total_casos_val) * 100
        st.info(
            f"💡 **Taxa de Recuperação Global:** {taxa_recuperacao:.2f}% dos casos com desfecho final confirmado resultaram em cura."
        )

    st.markdown("##### 🗺️ Mapa Coroplético: Eficiência de Recuperação por Município (%)")

    df_desfecho = df[df["Status_Analise"].isin(["Óbito", "Recuperado"])].copy()
    df_desfecho["Municipio_norm"] = df_desfecho["Municipio"].map(normalizar_municipio)

    stats_mapa_cura = (
        df_desfecho.groupby("Municipio_norm")
        .agg(
            Total_Desfechos=("Status_Analise", "size"),
            Total_Curas=("Status_Analise", lambda x: (x == "Recuperado").sum()),
        )
        .reset_index()
    )

    stats_mapa_cura["Taxa de Cura (%)"] = (stats_mapa_cura["Total_Curas"] / stats_mapa_cura["Total_Desfechos"]) * 100

    stats_mapa_cura = stats_mapa_cura[stats_mapa_cura["Total_Desfechos"] >= 50]

    try:
        def _build_mapa_cura_fig():
            geojson = carregar_geojson_municipios_es()
            for f in geojson.get("features", []):
                props = f.get("properties", {})
                props["nome_norm"] = normalizar_municipio(_get_geojson_municipio_nome(props))

            fig = px.choropleth(
                stats_mapa_cura,
                geojson=geojson,
                locations="Municipio_norm",
                featureidkey="properties.nome_norm",
                color="Taxa de Cura (%)",
                color_continuous_scale=[[0.0, "#FFFFFF"], [0.5, "#F8BBD0"], [1.0, ES_BLUE]],
                hover_name="Municipio_norm",
                labels={"Taxa de Cura (%)": "Taxa de Cura (%)"},
                hover_data={
                    "Total_Desfechos": ":,",
                    "Total_Curas": ":,",
                    "Taxa de Cura (%)": ":.2f",
                },
            )
            fig.update_geos(fitbounds="locations", visible=False)
            fig.update_layout(margin={"r": 0, "t": 30, "l": 0, "b": 0}, height=500)
            return fig

        plotly_chart_with_loader(_build_mapa_cura_fig, message="Carregando mapa de cura...")

        st.caption(
            "ℹ️ *Municípios em branco possuem volume estatístico insuficiente (< 50 casos confirmados com desfecho).*"
        )
    except Exception as e:
        st.info("Não foi possível carregar o mapa de cura agora.")
        st.caption(f"Detalhe: {e}")
