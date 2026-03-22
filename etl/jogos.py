"""
ETL: busca partidas da Série A e salva no SQL Server.
"""
import pyodbc
from datetime import datetime
from api.client import get
from config import CONNECTION_STRING, LIGA_ID, TEMPORADA


def _id_time_local(cursor, id_api):
    cursor.execute("SELECT id_time FROM Times WHERE id_api = ?", id_api)
    row = cursor.fetchone()
    return row[0] if row else None


def buscar_e_salvar():
    print("[Jogos] Buscando partidas na API...")
    data = get("fixtures", {"league": LIGA_ID, "season": TEMPORADA})
    partidas = data.get("response", [])
    print(f"[Jogos] {len(partidas)} partidas encontradas.")

    conn = pyodbc.connect(CONNECTION_STRING)
    cursor = conn.cursor()
    inseridos = 0

    for item in partidas:
        fix = item["fixture"]
        teams = item["teams"]
        goals = item["goals"]

        id_api = fix["id"]
        data_hora_raw = fix.get("date")
        data_hora = None
        if data_hora_raw:
            try:
                data_hora = datetime.fromisoformat(data_hora_raw.replace("Z", "+00:00"))
            except Exception:
                data_hora = None

        rodada = item.get("league", {}).get("round")
        estadio = fix.get("venue", {}).get("name")
        arbitro = fix.get("referee")
        status = fix.get("status", {}).get("long")

        id_time_casa = _id_time_local(cursor, teams["home"]["id"])
        id_time_fora = _id_time_local(cursor, teams["away"]["id"])

        cursor.execute("""
            MERGE Jogos AS target
            USING (SELECT ? AS id_api) AS source ON target.id_api = source.id_api
            WHEN MATCHED THEN UPDATE SET
                data_hora    = ?,
                rodada       = ?,
                temporada    = ?,
                id_time_casa = ?,
                id_time_fora = ?,
                gols_casa    = ?,
                gols_fora    = ?,
                status       = ?,
                estadio      = ?,
                arbitro      = ?
            WHEN NOT MATCHED THEN INSERT
                (id_api, data_hora, rodada, temporada, id_time_casa, id_time_fora,
                 gols_casa, gols_fora, status, estadio, arbitro)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """,
            id_api,
            data_hora, rodada, TEMPORADA, id_time_casa, id_time_fora,
            goals.get("home"), goals.get("away"), status, estadio, arbitro,
            id_api, data_hora, rodada, TEMPORADA, id_time_casa, id_time_fora,
            goals.get("home"), goals.get("away"), status, estadio, arbitro
        )
        inseridos += 1

    conn.commit()
    conn.close()
    print(f"[Jogos] {inseridos} partidas salvas no banco.")
