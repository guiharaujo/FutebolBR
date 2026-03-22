"""
Cliente HTTP para API-Football (api-sports.io).
"""
import requests
from config import API_BASE_URL, API_HEADERS


def get(endpoint, params=None):
    """Faz GET na API-Football e retorna o JSON completo."""
    url = f"{API_BASE_URL}/{endpoint}"
    response = requests.get(url, headers=API_HEADERS, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    erros = data.get("errors")
    if erros:
        raise Exception(f"Erro da API: {erros}")

    return data
