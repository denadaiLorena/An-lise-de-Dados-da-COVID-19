import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st
import os
import html

from app_features import extrair_idade_anos
from app_geo import carregar_geojson_municipios_es, normalizar_municipio, _get_geojson_municipio_nome
from app_nav import (
    ANCHOR_COMORB_RISCO,
    ANCHOR_COMORB_VOLUME,
    ANCHOR_CURA_COMORB,
    ANCHOR_CURA_ETARIA,
    ANCHOR_CURA_MAPA,
    ANCHOR_FAIXA_ETARIA_RISCO,
    ANCHOR_FAIXA_ETARIA_VOLUME,
    ANCHOR_KPIS,
    ANCHOR_MAPA_ES,
    ANCHOR_MUNICIPIO_TABLE,
    ANCHOR_RANKING_RISCO,
    ANCHOR_SOBREVIDA_BOX_ANTES,
    ANCHOR_SOBREVIDA_BOX_DEPOIS,
    ANCHOR_SOBREVIDA_HIST,
    ANCHOR_TEMPORAL_LETALIDADE,
)


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
    # Links do tipo [texto](#anchor_id) funcionam quando existe um elemento com id no DOM.
    st.markdown(f'<a id="{anchor_id}"></a>', unsafe_allow_html=True)


def _inject_kpi_card_css() -> None:
    st.markdown(
        """
<style>
.kdd-metric-card{
    --kdd-accent: var(--primary-color);
    --kdd-bg: var(--secondary-background-color);
  padding: 0.75rem 0.9rem;
    border: 1px solid var(--kdd-accent);
    border-radius: 0.55rem;
    background: var(--kdd-bg);
}

.kdd-metric-card.kdd-accent-red{ --kdd-accent: #ef4444; --kdd-bg: rgba(239, 68, 68, 0.14); }
.kdd-metric-card.kdd-accent-yellow{ --kdd-accent: #f59e0b; --kdd-bg: rgba(245, 158, 11, 0.16); }
.kdd-metric-card.kdd-accent-green{ --kdd-accent: #10b981; --kdd-bg: rgba(16, 185, 129, 0.14); }
.kdd-metric-card.kdd-accent-blue{ --kdd-accent: #3b82f6; --kdd-bg: rgba(59, 130, 246, 0.14); }

.kdd-metric-card .kdd-kpi-label{
    font-size: 0.9rem;
    font-weight: 650;
    line-height: 1.2;
    color: var(--text-color);
    opacity: 0.9;
}

.kdd-metric-card .kdd-kpi-value{
    font-size: 2.1rem;
    font-weight: 750;
    line-height: 1.05;
    margin-top: 0.25rem;
    color: var(--text-color);
}

.kdd-metric-card .kdd-kpi-delta{
    font-size: 0.85rem;
    margin-top: 0.35rem;
    color: var(--text-color);
    opacity: 0.85;
}
</style>
""",
        unsafe_allow_html=True,
    )


def _metric_card(
    label: str,
    value: str,
    *,
    accent: str,
    delta: str | None = None,
    delta_color: str = "normal",
) -> None:
    accent_class = {
        "red": "kdd-accent-red",
        "yellow": "kdd-accent-yellow",
        "green": "kdd-accent-green",
        "blue": "kdd-accent-blue",
    }.get(accent, "")

    safe_label = html.escape(str(label))
    safe_value = html.escape(str(value))

    delta_html = ""
    if delta is not None and str(delta).strip() != "":
        safe_delta = html.escape(str(delta))
        # Mantém a API delta/delta_color, mas com renderização simples.
        # (Caso queira setas e cores exatas, a gente parametriza aqui depois.)
        delta_html = f'<div class="kdd-kpi-delta">{safe_delta}</div>'

    st.markdown(
        f"""
<div class="kdd-metric-card {accent_class}">
  <div class="kdd-kpi-label">{safe_label}</div>
  <div class="kdd-kpi-value">{safe_value}</div>
  {delta_html}
</div>
""",
        unsafe_allow_html=True,
    )


def render_sidebar_kdd_expander() -> None:
    # Mantido por compatibilidade: o conteúdo agora é exibido no rodapé (fim da página).
    render_kdd_footer_expander()


def render_kdd_footer_expander() -> None:
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
        parquet_size = (
            _fmt_bytes(os.path.getsize(parquet_path)) if os.path.exists(parquet_path) else None
        )

        if csv_size and parquet_size:
            st.caption(
                f"Base local: CSV ~{csv_size} → Parquet ~{parquet_size} (otimizado para análise)."
            )

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


def render_kpis(df: pd.DataFrame) -> None:
    _anchor(ANCHOR_KPIS)
    st.subheader("Indicadores Gerais de Saúde")

    _inject_kpi_card_css()

    total_notificacoes = len(df)

    if "Status_Analise" in df.columns:
        total_recuperados = (df["Status_Analise"] == "Recuperado").sum()
        total_investigacao = (df["Status_Analise"] == "Em Aberto / Ignorado").sum()
        total_obitos = (df["Status_Analise"] == "Óbito").sum()
        total_desfechos = int(total_obitos + total_recuperados)
    else:
        # Fallback defensivo (caso alguém chame render_kpis com um df não enriquecido pelo loader)
        total_obitos = (
            df["Obito"].astype(int).sum() if "Obito" in df.columns else 0
        )
        total_recuperados = 0
        total_investigacao = 0
        total_desfechos = 0

    taxa_letalidade = (total_obitos / total_desfechos * 100) if total_desfechos > 0 else 0.0

    k1, k2, k3, k4 = st.columns(4)
    with k1:
        _metric_card(
            "Total de Notificações",
            f"{total_notificacoes:,}".replace(",", "."),
            accent="blue",
        )
    with k2:
        _metric_card(
            "Recuperados",
            f"{total_recuperados:,}".replace(",", "."),
            accent="green",
        )
    with k3:
        _metric_card(
            "Casos em Investigação",
            f"{total_investigacao:,}".replace(",", "."),
            accent="yellow",
        )
    with k4:
        _metric_card(
            "Óbitos",
            f"{total_obitos:,}".replace(",", "."),
            accent="red",
        )

    st.caption(
        f"Letalidade (apenas desfechos finais: Óbito/(Óbito+Recuperado)): {taxa_letalidade:.2f}%"
    )

    st.divider()


def render_municipio_table(df: pd.DataFrame) -> None:
    _anchor(ANCHOR_MUNICIPIO_TABLE)
    st.subheader("Desempenho por Município (T.L.)")
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

    df_municipio["Taxa Letalidade (%)"] = (
        df_municipio["Obitos"] / df_municipio["Casos"]
    ) * 100
    df_municipio = df_municipio.sort_values(by="Taxa Letalidade (%)", ascending=False)

    st.caption(
        "Letalidade por município calculada apenas com desfechos finais (Óbito+Recuperado)."
    )

    def _row_bg_by_volume(row: pd.Series) -> list[str]:
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

    styler = (
        df_municipio.style.apply(_row_bg_by_volume, axis=1)
        .format(
            {
                "Casos": lambda v: f"{int(v):,}".replace(",", ".") if pd.notnull(v) else "",
                "Obitos": lambda v: f"{int(v):,}".replace(",", ".") if pd.notnull(v) else "",
                "Taxa Letalidade (%)": lambda v: f"{v:.2f}%" if pd.notnull(v) else "",
            }
        )
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
        with st.spinner("Carregando mapa..."):
            geojson = carregar_geojson_municipios_es()

            for f in geojson.get("features", []):
                props = f.get("properties", {})
                props["nome_norm"] = normalizar_municipio(_get_geojson_municipio_nome(props))

            geo_muns = {
                feat.get("properties", {}).get("nome_norm", "")
                for feat in geojson.get("features", [])
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
            st.plotly_chart(fig, use_container_width=True)

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
        with st.spinner("Carregando ranking de risco..."):
            fig_bar = px.bar(
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
            fig_bar.update_traces(texttemplate="%{x:.2f}")
            fig_bar.update_layout(
                yaxis={"categoryorder": "total ascending"},
                margin={"r": 20, "t": 10, "l": 10, "b": 10},
                height=450,
                showlegend=False,
                coloraxis_showscale=True,
            )
            st.plotly_chart(fig_bar, use_container_width=True)

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


def render_comorbidades_e_etaria(df: pd.DataFrame) -> None:
    st.divider()
    st.subheader("Análise de Risco e Comorbidades")

    def _fmt_int(n: int | float) -> str:
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
            with st.spinner("Carregando gráficos de comorbidades..."):
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
                        baseline_obitos / baseline_desfechos * 100
                        if baseline_desfechos > 0
                        else None
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

                df_comorb_final = pd.DataFrame(resumo_comorb).sort_values(
                    "Volume (Casos)", ascending=True
                )

                fig_comorb = px.bar(
                    df_comorb_final,
                    x="Volume (Casos)",
                    y="Comorbidade",
                    orientation="h",
                    title="Volume de Casos por Comorbidade",
                    labels={"Volume (Casos)": "Nº de Pacientes", "Comorbidade": ""},
                    template="plotly_white",
                    color_discrete_sequence=[ES_BLUE],
                )
                fig_comorb.update_layout(margin=dict(l=20, r=20, t=40, b=20), height=350)
                st.plotly_chart(fig_comorb, use_container_width=True)

                if not df_comorb_final.empty:
                    top_vol = df_comorb_final.sort_values("Volume (Casos)", ascending=False).iloc[0]
                    st.markdown(
                        (
                            f"Observa-se que **{top_vol['Comorbidade']}** aparece com maior volume "
                            f"({_fmt_int(top_vol['Volume (Casos)'])} notificações com esta comorbidade nos filtros atuais). "
                            "Isso sugere que intervenções para esse grupo tendem a ter maior alcance populacional."
                        )
                    )

                df_comorb_risco = df_comorb_final.sort_values(
                    "Risco de Óbito (%)", ascending=True
                )
                fig_risco = px.bar(
                    df_comorb_risco,
                    x="Risco de Óbito (%)",
                    y="Comorbidade",
                    orientation="h",
                    title="Qual comorbidade mais mata? (Letalidade %)",
                    labels={"Risco de Óbito (%)": "Chance de Óbito (%)", "Comorbidade": ""},
                    template="plotly_white",
                    color_discrete_sequence=[ES_PINK],
                )
                fig_risco.update_traces(texttemplate="%{x:.1f}%", textposition="outside")
                fig_risco.update_layout(
                    margin=dict(l=20, r=20, t=40, b=20),
                    height=350,
                    xaxis_ticksuffix="%",
                )
                st.plotly_chart(fig_risco, use_container_width=True)

                if baseline_letalidade is not None and not df_comorb_final.empty:
                    # Para evitar frases com amostras muito pequenas, prioriza desfechos >= 30.
                    df_story = df_comorb_final[df_comorb_final["Desfechos"] >= 30].copy()
                    if df_story.empty:
                        df_story = df_comorb_final.copy()
                    top_risk = df_story.sort_values("Risco de Óbito (%)", ascending=False).iloc[0]

                    if baseline_letalidade > 0:
                        diff_rel = ((top_risk["Risco de Óbito (%)"] - baseline_letalidade) / baseline_letalidade) * 100
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
                                f"No recorte atual, a taxa global de letalidade (por desfechos) é muito baixa/zero. "
                                f"Ainda assim, **{top_risk['Comorbidade']}** concentra a maior letalidade observada entre comorbidades."
                            )
                        )
        else:
            st.info("Colunas de comorbidades não encontradas.")

    with col_c2:
        _anchor(ANCHOR_FAIXA_ETARIA_VOLUME)
        _anchor(ANCHOR_FAIXA_ETARIA_RISCO)

        if "IdadeNaDataNotificacao" in df.columns:
            with st.spinner("Carregando gráficos de faixa etária..."):
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

                df_age["Faixa Etária"] = pd.cut(
                    df_age["idade_num"], bins=bins, labels=labels, right=False
                )

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
                    df_perfil_age["Risco de Óbito (%)"] = (
                        df_perfil_age["Obitos"] / df_perfil_age["Casos"]
                    ) * 100

                if df_perfil_age.empty:
                    st.info(
                        "Sem dados suficientes para montar o perfil etário com os filtros atuais."
                    )
                else:
                    fig_idade = px.bar(
                        df_perfil_age,
                        x="Faixa Etária",
                        y="Casos",
                        title="Distribuição Etária (Volume)",
                        labels={"Casos": "Casos Conf.", "Faixa Etária": "Anos"},
                        template="plotly_white",
                        color_discrete_sequence=[ES_BLUE],
                    )
                    fig_idade.update_layout(
                        margin=dict(l=20, r=20, t=40, b=20), height=350
                    )
                    st.plotly_chart(fig_idade, use_container_width=True)

                    top_age_vol = df_perfil_age.sort_values("Casos", ascending=False).iloc[0]
                    st.markdown(
                        (
                            f"Observa-se que a faixa **{top_age_vol['Faixa Etária']}** concentra o maior volume "
                            f"({_fmt_int(top_age_vol['Casos'])} notificações no recorte). "
                            "Isso ajuda a dimensionar onde a maior parte da demanda se encontra."
                        )
                    )

                    fig_risco_age = px.bar(
                        df_perfil_age,
                        x="Faixa Etária",
                        y="Risco de Óbito (%)",
                        title="Risco de Óbito por Faixa Etária (%)",
                        labels={
                            "Risco de Óbito (%)": "Letalidade (%)",
                            "Faixa Etária": "Anos",
                        },
                        template="plotly_white",
                        color_discrete_sequence=[ES_PINK],
                    )
                    fig_risco_age.update_traces(
                        texttemplate="%{y:.1f}%", textposition="outside"
                    )
                    fig_risco_age.update_layout(
                        margin=dict(l=20, r=20, t=40, b=20),
                        height=350,
                        yaxis_ticksuffix="%",
                    )
                    st.plotly_chart(fig_risco_age, use_container_width=True)

                    top_age_risk = df_perfil_age.sort_values("Risco de Óbito (%)", ascending=False).iloc[0]
                    if baseline_age_letalidade is not None and baseline_age_letalidade > 0:
                        diff_rel_age = ((top_age_risk["Risco de Óbito (%)"] - baseline_age_letalidade) / baseline_age_letalidade) * 100
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


def render_sobrevida_kdd(df: pd.DataFrame) -> None:
    st.divider()
    st.subheader("Análise de tempo da notificação ao óbito")

    _anchor(ANCHOR_SOBREVIDA_BOX_ANTES)
    _anchor(ANCHOR_SOBREVIDA_BOX_DEPOIS)
    _anchor(ANCHOR_SOBREVIDA_HIST)

    df_sobrevida = df[df["Obito"] == True].copy()
    df_sobrevida["DataNotificacao"] = pd.to_datetime(df_sobrevida["DataNotificacao"], errors="coerce")
    df_sobrevida["DataObito"] = pd.to_datetime(df_sobrevida["DataObito"], errors="coerce")
    df_sobrevida = df_sobrevida.dropna(subset=["DataNotificacao", "DataObito"])

    df_sobrevida["Dias_Sobrevida"] = (
        df_sobrevida["DataObito"] - df_sobrevida["DataNotificacao"]
    ).dt.days

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
        (df_base["Dias_Sobrevida"] >= limite_inferior)
        & (df_base["Dias_Sobrevida"] <= limite_superior)
    ].copy()

    st.write("### 🧪 KDD: Tratamento de Outliers (IQR)")
    st.info(
        f"O Intervalo Interquartil (IQR) definiu registros acima de {int(limite_superior)} dias como outliers estatísticos."
    )

    col_box1, col_box2 = st.columns(2)

    with col_box1:
        with st.spinner("Carregando boxplot (antes da limpeza)..."):
            fig_box_pre = px.box(
                df_base,
                y="Dias_Sobrevida",
                title="Antes: Com Outliers",
                labels={"Dias_Sobrevida": "Dias"},
                template="plotly_white",
                color_discrete_sequence=[ES_PINK],
            )
            fig_box_pre.update_layout(height=400)
            st.plotly_chart(fig_box_pre, use_container_width=True)

    with col_box2:
        with st.spinner("Carregando boxplot (após a limpeza)..."):
            fig_box_pos = px.box(
                df_limpo,
                y="Dias_Sobrevida",
                title="Depois: Limpeza IQR",
                labels={"Dias_Sobrevida": "Dias"},
                template="plotly_white",
                color_discrete_sequence=[ES_BLUE],
            )
            fig_box_pos.update_layout(height=400)
            st.plotly_chart(fig_box_pos, use_container_width=True)

    media_saneada = df_limpo["Dias_Sobrevida"].mean()
    mediana_saneada = df_limpo["Dias_Sobrevida"].median()

    modal_calc = df_limpo[df_limpo["Dias_Sobrevida"] > 0]["Dias_Sobrevida"].mode()
    _moda_saneada = modal_calc[0] if not modal_calc.empty else 0

    st.markdown("#### Estatísticas Pós-Limpeza")
    cs1, cs2, _cs3 = st.columns(3)
    cs1.metric(
        "Média Real",
        f"{media_saneada:.1f} dias",
        delta=f"{media_saneada - df_base['Dias_Sobrevida'].mean():.1f} vs brute",
        delta_color="inverse",
    )
    cs2.metric("Mediana", f"{int(mediana_saneada)} dias")

    with st.spinner("Carregando histograma de sobrevida..."):
        fig_hist_final = px.histogram(
            df_limpo,
            x="Dias_Sobrevida",
            nbins=30,
            title="Distribuição Final da Sobrevida (Dados Saneados)",
            labels={"Dias_Sobrevida": "Dias após Notificação", "count": "Frequência"},
            template="plotly_white",
            color_discrete_sequence=[ES_BLUE],
        )
        fig_hist_final.add_vline(
            x=media_saneada,
            line_dash="dash",
            line_color=ES_PINK,
            annotation_text=f"Média: {media_saneada:.1f}",
        )
        fig_hist_final.update_layout(margin=dict(l=20, r=20, t=40, b=20), height=400)
        st.plotly_chart(fig_hist_final, use_container_width=True)

    st.caption(
        f"💡 *O processo removeu {len(df_base) - len(df_limpo)} registros considerados ruído estatístico, resultando em uma média muito mais próxima da realidade clínica.*"
    )


def render_temporal_letalidade(df: pd.DataFrame) -> None:
    _anchor(ANCHOR_TEMPORAL_LETALIDADE)
    st.divider()
    st.subheader("Evolução Temporal da Taxa de Letalidade (%)")

    # Letalidade temporal deve considerar apenas casos com desfecho final.
    if "Status_Analise" in df.columns:
        df_temporal = df[df["Status_Analise"].isin(["Óbito", "Recuperado"])][
            ["DataNotificacao", "Obito"]
        ].copy()
    else:
        df_temporal = df[["DataNotificacao", "Obito"]].copy()
    df_temporal["DataNotificacao"] = pd.to_datetime(df_temporal["DataNotificacao"], errors="coerce")
    df_temporal = df_temporal.dropna(subset=["DataNotificacao"])

    df_temporal["Mes_Ano"] = df_temporal["DataNotificacao"].dt.to_period("M").astype(str)

    stats_tempo = (
        df_temporal.groupby("Mes_Ano")
        .agg(Casos=("Obito", "size"), Obitos=("Obito", "sum"))
        .reset_index()
    )

    stats_tempo["Taxa Letalidade (%)"] = (stats_tempo["Obitos"] / stats_tempo["Casos"]) * 100

    pico = stats_tempo.loc[stats_tempo["Taxa Letalidade (%)"].idxmax()]

    ct1, ct2 = st.columns([1, 3])

    with ct1:
        st.write("")
        st.metric("Pior Período (Letalidade)", str(pico["Mes_Ano"]))
        st.metric("Taxa no Pico", f"{pico['Taxa Letalidade (%)']:.2f}%")
        st.caption(
            f"No mês de {pico['Mes_Ano']}, a letalidade foi a mais alta da série histórica."
        )

    with ct2:
        with st.spinner("Carregando gráfico temporal..."):
            fig_evolucao = px.line(
                stats_tempo,
                x="Mes_Ano",
                y="Taxa Letalidade (%)",
                title="Taxa de Letalidade Mensal ao Longo do Tempo",
                labels={"Mes_Ano": "Mês de Notificação", "Taxa Letalidade (%)": "Letalidade (%)"},
                template="plotly_white",
                markers=True,
            )
            fig_evolucao.add_annotation(
                x=pico["Mes_Ano"],
                y=pico["Taxa Letalidade (%)"],
                text="PICO",
                showarrow=True,
                arrowhead=1,
                arrowcolor=ES_PINK,
                font=dict(color=ES_PINK, size=12),
            )
            fig_evolucao.update_traces(line_color=ES_BLUE)
            fig_evolucao.update_layout(margin=dict(l=20, r=20, t=40, b=20), height=400)
            st.plotly_chart(fig_evolucao, use_container_width=True)

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


def render_cura(df: pd.DataFrame) -> None:
    st.divider()
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
            with st.spinner("Carregando gráfico de comorbidades (recuperados)..."):
                dados_comorb_cura = df_cura[existentes_cura].sum().reset_index()
                dados_comorb_cura.columns = ["Comorbidade", "Total"]
                dados_comorb_cura["Comorbidade"] = dados_comorb_cura["Comorbidade"].map(comorb_cols_cura)
                dados_comorb_cura = dados_comorb_cura.sort_values("Total", ascending=True)

                fig_comorb_cura = px.bar(
                    dados_comorb_cura,
                    x="Total",
                    y="Comorbidade",
                    orientation="h",
                    title="Comorbidades em Pacientes Recuperados",
                    labels={"Total": "Nº de Recuperados", "Comorbidade": ""},
                    template="plotly_white",
                    color_discrete_sequence=[ES_BLUE],
                )
                fig_comorb_cura.update_layout(margin=dict(l=20, r=20, t=40, b=20), height=350)
                st.plotly_chart(fig_comorb_cura, use_container_width=True)

    with col_cura2:
        if "IdadeNaDataNotificacao" in df_cura.columns:
            with st.spinner("Carregando gráfico etário (recuperados)..."):
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

                fig_idade_cura = px.bar(
                    counts_cura,
                    x="Faixa Etária",
                    y="Recuperados",
                    title="Distribuição Etária dos Recuperados",
                    labels={"Recuperados": "Nº de Pessoas", "Faixa Etária": "Anos"},
                    template="plotly_white",
                    color_discrete_sequence=[ES_BLUE],
                )
                fig_idade_cura.update_layout(margin=dict(l=20, r=20, t=40, b=20), height=350)
                st.plotly_chart(fig_idade_cura, use_container_width=True)

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

    stats_mapa_cura["Taxa de Cura (%)"] = (
        stats_mapa_cura["Total_Curas"] / stats_mapa_cura["Total_Desfechos"]
    ) * 100

    stats_mapa_cura = stats_mapa_cura[stats_mapa_cura["Total_Desfechos"] >= 50]

    try:
        with st.spinner("Carregando mapa de cura..."):
            geojson = carregar_geojson_municipios_es()
            for f in geojson.get("features", []):
                props = f.get("properties", {})
                props["nome_norm"] = normalizar_municipio(_get_geojson_municipio_nome(props))

            fig_mapa_cura = px.choropleth(
                stats_mapa_cura,
                geojson=geojson,
                locations="Municipio_norm",
                featureidkey="properties.nome_norm",
                color="Taxa de Cura (%)",
                color_continuous_scale=[
                    [0.0, "#FFFFFF"],
                    [0.5, "#F8BBD0"],
                    [1.0, ES_BLUE],
                ],
                hover_name="Municipio_norm",
                labels={"Taxa de Cura (%)": "Taxa de Cura (%)"},
                hover_data={
                    "Total_Desfechos": ":,",
                    "Total_Curas": ":,",
                    "Taxa de Cura (%)": ":.2f",
                },
            )
            fig_mapa_cura.update_geos(fitbounds="locations", visible=False)
            fig_mapa_cura.update_layout(margin={"r": 0, "t": 30, "l": 0, "b": 0}, height=500)
            st.plotly_chart(fig_mapa_cura, use_container_width=True)
            st.caption(
                "ℹ️ *Municípios em branco possuem volume estatístico insuficiente (< 50 casos confirmados com desfecho).*"
            )
    except Exception as e:
        st.info("Não foi possível carregar o mapa de cura agora.")
        st.caption(f"Detalhe: {e}")
