import pandas as pd


def extrair_idade_anos(serie: pd.Series) -> pd.Series:
    """Extrai a idade em anos a partir de strings como '45 anos, 3 meses...' (vetorizado)."""
    s = serie.astype(str).str.lower()
    anos = pd.to_numeric(s.str.extract(r"(\d+)\s*ano")[0], errors="coerce")
    # Fallback: se já vier numérico puro
    return anos.fillna(pd.to_numeric(serie, errors="coerce"))
