"""Seções do dashboard (reexports).

Importe a partir daqui no app principal:

    from covid_app.sections import render_kpis, render_municipio_table, ...

"""

from .common import ES_BLUE, ES_PINK, ES_SCALE, ES_WHITE
from .cura import render_cura
from .kpis import render_kpis
from .mapa import render_mapa_es_e_ranking
from .metodologia import render_kdd_footer_expander, render_sidebar_kdd_expander
from .municipio import render_municipio_table
from .risco import render_comorbidades_e_etaria
from .sobrevida import render_sobrevida_kdd
from .temporal import render_temporal_letalidade

__all__ = [
    "ES_BLUE",
    "ES_PINK",
    "ES_SCALE",
    "ES_WHITE",
    "render_comorbidades_e_etaria",
    "render_cura",
    "render_kdd_footer_expander",
    "render_kpis",
    "render_mapa_es_e_ranking",
    "render_municipio_table",
    "render_sidebar_kdd_expander",
    "render_sobrevida_kdd",
    "render_temporal_letalidade",
]
