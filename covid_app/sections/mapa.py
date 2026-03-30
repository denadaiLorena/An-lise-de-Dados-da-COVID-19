import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st

from ..geo import carregar_geojson_municipios_es, normalizar_municipio, _get_geojson_municipio_nome
from ..nav import ANCHOR_MAPA_ES, ANCHOR_RANKING_RISCO
from .common import ES_SCALE, _anchor, plotly_chart_with_loader


def render_mapa_es_e_ranking(df: pd.DataFrame) -> None:
    _anchor(ANCHOR_MAPA_ES)
    st.subheader("Mapa de Casos por Município (ES)")

    grande_vitoria = {
        normalizar_municipio(n)
        for n in [
            "Vitória",
            "Vila Velha",
            "Serra",
            "Cariacica",
            "Viana",
            "Guarapari",
            "Fundão",
        ]
    }

    if "Municipio" not in df.columns or df.empty:
        return

    df_tmp = df.copy()

    if "Obito" in df_tmp.columns:
        df_tmp["Obito"] = df_tmp["Obito"].astype(int)

    df_tmp["Municipio_norm"] = df_tmp["Municipio"].map(normalizar_municipio)
    df_tmp["MacroRegiao"] = np.where(
        df_tmp["Municipio_norm"].isin(grande_vitoria), "Grande Vitória", "Interior"
    )

    dist = df_tmp["MacroRegiao"].value_counts(dropna=False)
    casos_gv = int(dist.get("Grande Vitória", 0))
    casos_int = int(dist.get("Interior", 0))
    total = int(len(df_tmp))
    pct_gv = (casos_gv / total * 100) if total else 0

    c1, c2, c3 = st.columns(3)
    c1.metric("Casos (Grande Vitória)", f"{casos_gv:,}".replace(",", "."))
    c2.metric("Casos (Interior)", f"{casos_int:,}".replace(",", "."))
    c3.metric("% na Grande Vitória", f"{pct_gv:.2f}%")

    # Volume total (notificações) por município
    df_stats_mun = (
        df_tmp.groupby("Municipio_norm", dropna=False)
        .agg(Notificacoes=("Municipio_norm", "size"), Obitos=("Obito", "sum"))
        .reset_index()
    )

    # Desfechos finais (Óbito + Recuperado) para cálculo de letalidade
    if "Status_Analise" in df_tmp.columns:
        df_desfecho = df_tmp[df_tmp["Status_Analise"].isin(["Óbito", "Recuperado"])].copy()
        df_desfecho_stats = (
            df_desfecho.groupby("Municipio_norm", dropna=False)
            .agg(Desfechos=("Municipio_norm", "size"))
            .reset_index()
        )
        df_stats_mun = df_stats_mun.merge(df_desfecho_stats, on="Municipio_norm", how="left")
    else:
        df_stats_mun["Desfechos"] = np.nan

    df_stats_mun["Desfechos"] = df_stats_mun["Desfechos"].fillna(0).astype(int)
    df_stats_mun["Taxa Letalidade (%)"] = np.where(
        df_stats_mun["Desfechos"] > 0,
        (df_stats_mun["Obitos"] / df_stats_mun["Desfechos"]) * 100,
        0.0,
    )

    df_top_risco = (
        df_stats_mun[df_stats_mun["Desfechos"] >= 50]
        .sort_values("Taxa Letalidade (%)", ascending=False)
        .head(10)
    )

    try:
        map_container = st.empty()
        map_container.info("Carregando mapa...", icon="⏳")

        with st.spinner("Carregando mapa..."):
            geojson = carregar_geojson_municipios_es()

            for f in geojson.get("features", []):
                props = f.get("properties", {})
                props["nome_norm"] = normalizar_municipio(_get_geojson_municipio_nome(props))

            geo_muns = {
                feat.get("properties", {}).get("nome_norm", "") for feat in geojson.get("features", [])
            }
            df_muns = set(df_stats_mun["Municipio_norm"].dropna().astype(str))
            matched = len(df_muns & geo_muns)

            if matched > 0:
                st.caption(f"Municípios casados no mapa: {matched}/{len(geo_muns)}")

            fig = px.choropleth(
                df_stats_mun,
                geojson=geojson,
                locations="Municipio_norm",
                featureidkey="properties.nome_norm",
                color="Notificacoes",
                color_continuous_scale=ES_SCALE,
                hover_name="Municipio_norm",
                labels={"Notificacoes": "Notificações"},
                hover_data={
                    "Notificacoes": ":,",
                    "Desfechos": ":,",
                    "Obitos": ":,",
                    "Taxa Letalidade (%)": ":.2f",
                },
            )
            fig.update_geos(fitbounds="locations", visible=False)
            fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0}, height=600)

        # Substitui o placeholder pelo gráfico quando terminar.
        map_container.plotly_chart(fig, use_container_width=True)

        st.markdown(
            "Observa-se que o mapa destaca **onde há maior volume de notificações** no recorte atual. "
            "Ao passar o mouse, a **Taxa de Letalidade** exibida considera apenas **desfechos finais (Óbito+Recuperado)**, "
            "o que evita subestimar/mascarar risco com casos ainda em aberto."
        )
    except Exception as e:
        st.info("Não foi possível carregar o mapa agora.")
        st.caption(f"Detalhe: {e}")

    st.markdown("---")
    _anchor(ANCHOR_RANKING_RISCO)
    st.markdown("##### ⚠️ Top 10 Municípios com maior Risco (Letalidade)")
    if not df_top_risco.empty:
        def _build_ranking_fig():
            fig = px.bar(
                df_top_risco,
                x="Taxa Letalidade (%)",
                y="Municipio_norm",
                orientation="h",
                color="Taxa Letalidade (%)",
                color_continuous_scale=ES_SCALE,
                text_auto=True,
                labels={
                    "Municipio_norm": "Município",
                    "Taxa Letalidade (%)": "Taxa de Letalidade (%)",
                },
            )
            fig.update_traces(texttemplate="%{x:.2f}")
            fig.update_layout(
                yaxis={"categoryorder": "total ascending"},
                margin={"r": 20, "t": 10, "l": 10, "b": 10},
                height=450,
                showlegend=False,
                coloraxis_showscale=True,
            )
            return fig

        plotly_chart_with_loader(_build_ranking_fig, message="Carregando ranking de risco...")

        top1 = df_top_risco.iloc[0]
        st.markdown(
            (
                f"Observa-se que **{top1['Municipio_norm']}** lidera a letalidade no recorte "
                f"(**{top1['Taxa Letalidade (%)']:.2f}%**), considerando apenas municípios com **≥ 50 desfechos**. "
                "Isso sugere que ações de vigilância e suporte assistencial podem ser priorizadas onde o risco é maior."
            )
        )
    else:
        st.info("Dados insuficientes para o ranking.")
