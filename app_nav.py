# Navegação/âncoras do app (sem imports de Streamlit para evitar dependências/ciclos)

ANCHOR_KPIS = "kpis"
ANCHOR_MUNICIPIO_TABLE = "desempenho-municipio"
ANCHOR_MAPA_ES = "mapa-es"
ANCHOR_RANKING_RISCO = "ranking-risco"
ANCHOR_COMORB_VOLUME = "comorb-volume"
ANCHOR_COMORB_RISCO = "comorb-risco"
ANCHOR_FAIXA_ETARIA_VOLUME = "faixa-etaria-volume"
ANCHOR_FAIXA_ETARIA_RISCO = "faixa-etaria-risco"
ANCHOR_SOBREVIDA_BOX_ANTES = "sobrevida-box-antes"
ANCHOR_SOBREVIDA_BOX_DEPOIS = "sobrevida-box-depois"
ANCHOR_SOBREVIDA_HIST = "sobrevida-hist"
ANCHOR_TEMPORAL_LETALIDADE = "temporal-letalidade"
ANCHOR_CURA_COMORB = "cura-comorb"
ANCHOR_CURA_ETARIA = "cura-etaria"
ANCHOR_CURA_MAPA = "cura-mapa"
ANCHOR_EXPORTACAO = "exportacao"


NAV_ITEMS: list[tuple[str, str]] = [
    ("Indicadores Gerais", ANCHOR_KPIS),
    ("Desempenho por Município (Tabela)", ANCHOR_MUNICIPIO_TABLE),
    ("Mapa de Casos por Município", ANCHOR_MAPA_ES),
    ("Top 10 Risco (Letalidade)", ANCHOR_RANKING_RISCO),
    ("Comorbidades: Volume", ANCHOR_COMORB_VOLUME),
    ("Comorbidades: Risco", ANCHOR_COMORB_RISCO),
    ("Faixa Etária: Volume", ANCHOR_FAIXA_ETARIA_VOLUME),
    ("Faixa Etária: Risco", ANCHOR_FAIXA_ETARIA_RISCO),
    ("Sobrevida: Boxplot (Antes)", ANCHOR_SOBREVIDA_BOX_ANTES),
    ("Sobrevida: Boxplot (Depois)", ANCHOR_SOBREVIDA_BOX_DEPOIS),
    ("Sobrevida: Histograma", ANCHOR_SOBREVIDA_HIST),
    ("Evolução Temporal (Letalidade)", ANCHOR_TEMPORAL_LETALIDADE),
    ("Cura: Comorbidades", ANCHOR_CURA_COMORB),
    ("Cura: Perfil Etário", ANCHOR_CURA_ETARIA),
    ("Cura: Mapa", ANCHOR_CURA_MAPA),
    ("Exportação (Dados Filtrados)", ANCHOR_EXPORTACAO),
]
