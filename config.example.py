# Configurações do projeto FutebolBR
# Copie este arquivo para config.py e preencha com seus valores

# API-Football (api-sports.io) — https://www.api-sports.io/
API_KEY = "SUA_API_KEY_AQUI"
API_BASE_URL = "https://v3.football.api-sports.io"
API_HEADERS = {
    "x-apisports-key": API_KEY
}

# Liga Brasileira Série A
LIGA_ID = 71
TEMPORADA = 2024

# SQL Server
CONNECTION_STRING = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "Server=localhost;"
    "Database=FutebolBR;"
    "Trusted_Connection=yes;"
    "TrustServerCertificate=yes;"
)
