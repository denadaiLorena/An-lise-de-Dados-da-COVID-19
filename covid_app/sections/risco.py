import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st

from typing import Union

from ..features import extrair_idade_anos
from ..nav import (
    ANCHOR_ANALISE_RISCO_COMORB,
    ANCHOR_COMORB_RISCO,
    ANCHOR_COMORB_VOLUME,
    ANCHOR_FAIXA_ETARIA_RISCO,
    ANCHOR_FAIXA_ETARIA_VOLUME,
)
from .common import ES_BLUE, ES_PINK, _anchor, plotly_chart_with_loader


def render_comorbidades_e_etaria(df: pd.DataFrame) -> None:
    st.divider()
    _anchor(ANCHOR_ANALISE_RISCO_COMORB)
    st.subheader("Análise de Risco e Comorbidades")

    def _fmt_int(n: Union[int, float]) -> str:
        return f"{int(n):,}".replace(",", ".")

    def _fmt_pct(x: float, decimals: int = 1) -> str:
        return f"{x:.{decimals}f}%"

    col_c1, col_c2 = st.columns(2)

    with col_c1:
        _anchor(ANCHOR_COMORB_VOLUME)
        _anchor(ANCHOR_COMORB_RISCO)

        comorb_cols = {
            "ComorbidadeDiabetes": "Diabetes",
            "ComorbidadeCardio": "Cardiopatia",
            "ComorbidadePulmao": "Pulmonar",
            "ComorbidadeRenal": "Renal",
            "ComorbidadeTabagismo": "Tabagismo",
            "ComorbidadeObesidade": "Obesidade",
        }

        existentes = [c for c in comorb_cols.keys() if c in df.columns]

        if existentes:
            df_desfechos = (
                df[df["Status_Analise"].isin(["Óbito", "Recuperado"])].copy()
                if "Status_Analise" in df.columns
                else None
            )

            baseline_letalidade = None
            baseline_desfechos = None
            if df_desfechos is not None and not df_desfechos.empty:
                baseline_desfechos = int(len(df_desfechos))
                baseline_obitos = float(df_desfechos["Obito"].sum())
                baseline_letalidade = (
                    baseline_obitos / baseline_desfechos * 100 if baseline_desfechos > 0 else None
                )

            resumo_comorb = []
            for col, nome in comorb_cols.items():
                if col in df.columns:
                    # Volume: usa todas as notificações (melhor leitura de tamanho amostral)
                    df_sub = df[df[col] == 1]
                    total_notif = int(len(df_sub))

                    # Risco/letalidade: usa apenas desfechos finais (Óbito+Recuperado)
                    if df_desfechos is not None:
                        df_sub_des = df_desfechos[df_desfechos[col] == 1]
                        total_des = int(len(df_sub_des))
                        obitos = float(df_sub_des["Obito"].sum()) if total_des > 0 else 0.0
                        letalidade = (obitos / total_des * 100) if total_des > 0 else 0.0
                    else:
                        total_des = 0
                        letalidade = 0.0

                    resumo_comorb.append(
                        {
                            "Comorbidade": nome,
                            "Volume (Casos)": total_notif,
                            "Desfechos": total_des,
                            "Risco de Óbito (%)": letalidade,
                        }
                    )

            df_comorb_final = pd.DataFrame(resumo_comorb).sort_values("Volume (Casos)", ascending=True)

            def _build_fig_comorb_volume():
                fig = px.bar(
                    df_comorb_final,
                    x="Volume (Casos)",
                    y="Comorbidade",
                    orientation="h",
                    title="Volume de Casos por Comorbidade",
                    labels={"Volume (Casos)": "Nº de Pacientes", "Comorbidade": ""},
                    template="plotly_white",
                    color_discrete_sequence=[ES_BLUE],
                )
                fig.update_layout(margin=dict(l=20, r=20, t=40, b=20), height=350)
                return fig

            plotly_chart_with_loader(
                _build_fig_comorb_volume,
                message="Carregando gráfico (volume por comorbidade)...",
            )

            if not df_comorb_final.empty:
                top_vol = df_comorb_final.sort_values("Volume (Casos)", ascending=False).iloc[0]
                st.markdown(
                    (
                        f"Observa-se que **{top_vol['Comorbidade']}** aparece com maior volume "
                        f"({_fmt_int(top_vol['Volume (Casos)'])} notificações com esta comorbidade nos filtros atuais). "
                        "Isso sugere que intervenções para esse grupo tendem a ter maior alcance populacional."
                    )
                )

            df_comorb_risco = df_comorb_final.sort_values("Risco de Óbito (%)", ascending=True)

            def _build_fig_comorb_risco():
                fig = px.bar(
                    df_comorb_risco,
                    x="Risco de Óbito (%)",
                    y="Comorbidade",
                    orientation="h",
                    title="Qual comorbidade mais mata? (Letalidade %)",
                    labels={"Risco de Óbito (%)": "Chance de Óbito (%)", "Comorbidade": ""},
                    template="plotly_white",
                    color_discrete_sequence=[ES_PINK],
                )
                fig.update_traces(texttemplate="%{x:.1f}%", textposition="outside")
                fig.update_layout(
                    margin=dict(l=20, r=20, t=40, b=20), height=350, xaxis_ticksuffix="%"
                )
                return fig

            plotly_chart_with_loader(
                _build_fig_comorb_risco,
                message="Carregando gráfico (letalidade por comorbidade)...",
            )

            if baseline_letalidade is not None and not df_comorb_final.empty:
                # Para evitar frases com amostras muito pequenas, prioriza desfechos >= 30.
                df_story = df_comorb_final[df_comorb_final["Desfechos"] >= 30].copy()
                if df_story.empty:
                    df_story = df_comorb_final.copy()
                top_risk = df_story.sort_values("Risco de Óbito (%)", ascending=False).iloc[0]

                if baseline_letalidade > 0:
                    diff_rel = (
                        (top_risk["Risco de Óbito (%)"] - baseline_letalidade) / baseline_letalidade
                    ) * 100
                    st.markdown(
                        (
                            f"Observa-se que a letalidade em pacientes com **{top_risk['Comorbidade']}** "
                            f"é **{_fmt_pct(abs(diff_rel), 1)} {'maior' if diff_rel >= 0 else 'menor'}** "
                            f"do que a taxa global dos desfechos finais (baseline **{_fmt_pct(baseline_letalidade, 2)}**). "
                            "Isso sugere que a tomada de decisão deve priorizar ações de proteção (ex.: vacinação/monitoramento) "
                            "para esse grupo dentro do recorte filtrado."
                        )
                    )
                else:
                    st.markdown(
                        (
                            "No recorte atual, a taxa global de letalidade (por desfechos) é muito baixa/zero. "
                            f"Ainda assim, **{top_risk['Comorbidade']}** concentra a maior letalidade observada entre comorbidades."
                        )
                    )
        else:
            st.info("Colunas de comorbidades não encontradas.")

    with col_c2:
        _anchor(ANCHOR_FAIXA_ETARIA_VOLUME)
        _anchor(ANCHOR_FAIXA_ETARIA_RISCO)

        if "IdadeNaDataNotificacao" in df.columns:
            cols_age = ["IdadeNaDataNotificacao", "Obito"]
            if "Status_Analise" in df.columns:
                cols_age.append("Status_Analise")
            df_age = df[cols_age].copy()
            df_age["idade_num"] = extrair_idade_anos(df_age["IdadeNaDataNotificacao"])

            bins = [0, 10, 20, 30, 40, 50, 60, 70, 80, 150]
            labels = [
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

            df_age["Faixa Etária"] = pd.cut(df_age["idade_num"], bins=bins, labels=labels, right=False)

            df_perfil_age = (
                df_age.dropna(subset=["Faixa Etária"])
                .groupby("Faixa Etária", observed=True)
                .agg(Casos=("idade_num", "size"), Obitos=("Obito", "sum"))
                .reset_index()
            )
            df_perfil_age = df_perfil_age[df_perfil_age["Casos"] > 0]

            # Recalcula risco por desfechos finais (Óbito+Recuperado), quando disponível
            if "Status_Analise" in df_age.columns:
                df_age_des = df_age[df_age["Status_Analise"].isin(["Óbito", "Recuperado"])].copy()
                df_age_des = df_age_des.dropna(subset=["Faixa Etária"])
                df_age_des_stats = (
                    df_age_des.groupby("Faixa Etária", observed=True)
                    .agg(Desfechos=("idade_num", "size"))
                    .reset_index()
                )
                df_perfil_age = df_perfil_age.merge(df_age_des_stats, on="Faixa Etária", how="left")
                df_perfil_age["Desfechos"] = df_perfil_age["Desfechos"].fillna(0).astype(int)
                df_perfil_age["Risco de Óbito (%)"] = np.where(
                    df_perfil_age["Desfechos"] > 0,
                    (df_perfil_age["Obitos"] / df_perfil_age["Desfechos"]) * 100,
                    0.0,
                )
                baseline_des = int(len(df_age_des))
                baseline_ob = float(df_age_des["Obito"].sum()) if baseline_des > 0 else 0.0
                baseline_age_letalidade = (baseline_ob / baseline_des * 100) if baseline_des > 0 else None
            else:
                baseline_age_letalidade = None
                df_perfil_age["Risco de Óbito (%)"] = (df_perfil_age["Obitos"] / df_perfil_age["Casos"]) * 100

            if df_perfil_age.empty:
                st.info("Sem dados suficientes para montar o perfil etário com os filtros atuais.")
            else:
                def _build_fig_idade_volume():
                    fig = px.bar(
                        df_perfil_age,
                        x="Faixa Etária",
                        y="Casos",
                        title="Distribuição Etária (Volume)",
                        labels={"Casos": "Casos Conf.", "Faixa Etária": "Anos"},
                        template="plotly_white",
                        color_discrete_sequence=[ES_BLUE],
                    )
                    fig.update_layout(margin=dict(l=20, r=20, t=40, b=20), height=350)
                    return fig

                plotly_chart_with_loader(
                    _build_fig_idade_volume,
                    message="Carregando gráfico (volume por faixa etária)...",
                )

                top_age_vol = df_perfil_age.sort_values("Casos", ascending=False).iloc[0]
                st.markdown(
                    (
                        f"Observa-se que a faixa **{top_age_vol['Faixa Etária']}** concentra o maior volume "
                        f"({_fmt_int(top_age_vol['Casos'])} notificações no recorte). "
                        "Isso ajuda a dimensionar onde a maior parte da demanda se encontra."
                    )
                )

                def _build_fig_idade_risco():
                    fig = px.bar(
                        df_perfil_age,
                        x="Faixa Etária",
                        y="Risco de Óbito (%)",
                        title="Risco de Óbito por Faixa Etária (%)",
                        labels={"Risco de Óbito (%)": "Letalidade (%)", "Faixa Etária": "Anos"},
                        template="plotly_white",
                        color_discrete_sequence=[ES_PINK],
                    )
                    fig.update_traces(texttemplate="%{y:.1f}%", textposition="outside")
                    fig.update_layout(
                        margin=dict(l=20, r=20, t=40, b=20), height=350, yaxis_ticksuffix="%"
                    )
                    return fig

                plotly_chart_with_loader(
                    _build_fig_idade_risco,
                    message="Carregando gráfico (letalidade por faixa etária)...",
                )

                top_age_risk = df_perfil_age.sort_values("Risco de Óbito (%)", ascending=False).iloc[0]
                if baseline_age_letalidade is not None and baseline_age_letalidade > 0:
                    diff_rel_age = (
                        (top_age_risk["Risco de Óbito (%)"] - baseline_age_letalidade)
                        / baseline_age_letalidade
                    ) * 100
                    st.markdown(
                        (
                            f"Observa-se que a letalidade é mais alta na faixa **{top_age_risk['Faixa Etária']}** "
                            f"(**{_fmt_pct(top_age_risk['Risco de Óbito (%)'], 2)}**), "
                            f"o que representa **{_fmt_pct(abs(diff_rel_age), 1)} {'acima' if diff_rel_age >= 0 else 'abaixo'}** "
                            f"da taxa global por desfechos (**{_fmt_pct(baseline_age_letalidade, 2)}**). "
                            "Isso sugere priorização de estratégias de prevenção e acompanhamento clínico para esse grupo."
                        )
                    )
                else:
                    st.markdown(
                        (
                            f"A maior letalidade observada no recorte está na faixa **{top_age_risk['Faixa Etária']}** "
                            f"(**{_fmt_pct(top_age_risk['Risco de Óbito (%)'], 2)}**)."
                        )
                    )
        else:
            st.info("Coluna de idade não encontrada.")
