import html

import pandas as pd
import streamlit as st

from typing import Optional

from ..nav import ANCHOR_KPIS
from .common import _anchor


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
    delta: Optional[str] = None,
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
        total_obitos = df["Obito"].astype(int).sum() if "Obito" in df.columns else 0
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
