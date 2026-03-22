"""
ETL: busca times da Série A e salva no SQL Server.
"""
import pyodbc
from api.client import get
from config import CONNECTION_STRING, LIGA_ID, TEMPORADA


def buscar_e_salvar():
    print("[Times] Buscando na API...")
    data = get("teams", {"league": LIGA_ID, "season": TEMPORADA})
    times = data.get("response", [])
    print(f"[Times] {len(times)} times encontrados.")

    conn = pyodbc.connect(CONNECTION_STRING)
    cursor = conn.cursor()
    inseridos = 0

    for item in times:
        t = item["team"]
        v = item["venue"]

        cursor.execute("""
            MERGE Times AS target
            USING (SELECT ? AS id_api) AS source ON target.id_api = source.id_api
            WHEN MATCHED THEN UPDATE SET
                nome       = ?,
                nome_abrev = ?,
                pais       = ?,
                fundado    = ?,
                estadio    = ?,
                cidade     = ?,
                logo_url   = ?
            WHEN NOT MATCHED THEN INSERT
                (id_api, nome, nome_abrev, pais, fundado, estadio, cidade, logo_url)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?);
        """,
            t["id"],
            t["name"], t.get("code"), t.get("country"),
            t.get("founded"), v.get("name"), v.get("city"), t.get("logo"),
            t["id"], t["name"], t.get("code"), t.get("country"),
            t.get("founded"), v.get("name"), v.get("city"), t.get("logo")
        )
        inseridos += 1

    conn.commit()
    conn.close()
    print(f"[Times] {inseridos} times salvos no banco.")
