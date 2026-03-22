"""
ETL: busca jogadores de cada time da Série A e salva no SQL Server.
"""
import pyodbc
from api.client import get
from config import CONNECTION_STRING, LIGA_ID, TEMPORADA


def _get_times_do_banco(cursor):
    cursor.execute("SELECT id_time, id_api FROM Times")
    return cursor.fetchall()


def buscar_e_salvar():
    print("[Jogadores] Iniciando busca por time...")
    conn = pyodbc.connect(CONNECTION_STRING)
    cursor = conn.cursor()
    times = _get_times_do_banco(cursor)
    total = 0

    for id_time, id_api_time in times:
        page = 1
        while True:
            data = get("players", {
                "league": LIGA_ID,
                "season": TEMPORADA,
                "team": id_api_time,
                "page": page
            })
            jogadores = data.get("response", [])
            if not jogadores:
                break

            for item in jogadores:
                p = item["player"]
                stats = item.get("statistics", [{}])[0]
                games = stats.get("games", {})

                cursor.execute("""
                    MERGE Jogadores AS target
                    USING (SELECT ? AS id_api) AS source ON target.id_api = source.id_api
                    WHEN MATCHED THEN UPDATE SET
                        nome            = ?,
                        data_nascimento = ?,
                        nacionalidade   = ?,
                        altura          = ?,
                        peso            = ?,
                        posicao         = ?,
                        id_time         = ?,
                        numero_camisa   = ?
                    WHEN NOT MATCHED THEN INSERT
                        (id_api, nome, data_nascimento, nacionalidade, altura, peso, posicao, id_time, numero_camisa)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
                """,
                    p["id"],
                    p["name"], p.get("birth", {}).get("date"),
                    p.get("nationality"), p.get("height"), p.get("weight"),
                    games.get("position"), id_time, games.get("number"),
                    p["id"], p["name"], p.get("birth", {}).get("date"),
                    p.get("nationality"), p.get("height"), p.get("weight"),
                    games.get("position"), id_time, games.get("number")
                )
                total += 1

            paging = data.get("paging", {})
            if page >= paging.get("total", 1):
                break
            page += 1

        print(f"  Time id_api={id_api_time} processado.")

    conn.commit()
    conn.close()
    print(f"[Jogadores] {total} jogadores salvos no banco.")
