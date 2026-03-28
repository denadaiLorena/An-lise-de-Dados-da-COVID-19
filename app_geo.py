import requests
import streamlit as st


@st.cache_data(show_spinner=False)
def carregar_geojson_municipios_es() -> dict:
    """Baixa e cacheia o GeoJSON dos municípios do ES.

    Tenta primeiro o endpoint oficial de malhas do IBGE. Se falhar (instabilidade/rota),
    usa um fallback público com GeoJSON de municípios por UF.
    """
    ibge_url = "https://servicodados.ibge.gov.br/api/v2/malhas/estados/32?formato=application/vnd.geo+json&qualidade=intermediaria"
    try:
        resp = requests.get(ibge_url, timeout=30)
        resp.raise_for_status()
        _geo = resp.json()
        # O endpoint acima tende a retornar apenas o estado; portanto, forçamos fallback
        # para ter municípios (necessário para o choropleth por município).
        raise ValueError("IBGE retornou malha do estado (sem municípios)")
    except Exception:
        fallback_url = "https://raw.githubusercontent.com/tbrugz/geodata-br/master/geojson/geojs-32-mun.json"
        resp2 = requests.get(fallback_url, timeout=30)
        resp2.raise_for_status()
        return resp2.json()


def normalizar_municipio(nome: str) -> str:
    if nome is None:
        return ""
    nome = str(nome).strip().upper()
    nome = (
        nome.replace("Á", "A")
        .replace("À", "A")
        .replace("Â", "A")
        .replace("Ã", "A")
        .replace("É", "E")
        .replace("Ê", "E")
        .replace("Í", "I")
        .replace("Ó", "O")
        .replace("Ô", "O")
        .replace("Õ", "O")
        .replace("Ú", "U")
        .replace("Ç", "C")
    )
    return nome


def _get_geojson_municipio_nome(props: dict) -> str:
    if not isinstance(props, dict):
        return ""
    return props.get("nome") or props.get("name") or ""
