"""
ETL: busca a tabela de classificação da Série A e salva no SQL Server.
"""
import pyodbc
from api.client import get
from config import CONNECTION_STRING, LIGA_ID, TEMPORADA


def _id_time_local(cursor, id_api):
    cursor.execute("SELECT id_time FROM Times WHERE id_api = ?", id_api)
    row = cursor.fetchone()
    return row[0] if row else None


def buscar_e_salvar():
    print("[Classificacao] Buscando tabela na API...")
    data = get("standings", {"league": LIGA_ID, "season": TEMPORADA})

    standings_raw = (
        data.get("response", [{}])[0]
            .get("league", {})
            .get("standings", [[]])[0]
    )
    print(f"[Classificacao] {len(standings_raw)} times na tabela.")

    conn = pyodbc.connect(CONNECTION_STRING)
    cursor = conn.cursor()

    for item in standings_raw:
        id_api_time = item["team"]["id"]
        id_time = _id_time_local(cursor, id_api_time)
        if not id_time:
            continue

        all_g = item.get("all", {})
        goals = all_g.get("goals", {})

        cursor.execute("""
            MERGE Classificacao AS target
            USING (SELECT ? AS temporada, ? AS id_time) AS source
                ON target.temporada = source.temporada AND target.id_time = source.id_time
            WHEN MATCHED THEN UPDATE SET
                posicao     = ?,
                pontos      = ?,
                jogos       = ?,
                vitorias    = ?,
                empates     = ?,
                derrotas    = ?,
                gols_pro    = ?,
                gols_contra = ?,
                saldo_gols  = ?,
                forma       = ?
            WHEN NOT MATCHED THEN INSERT
                (temporada, id_time, posicao, pontos, jogos, vitorias, empates,
                 derrotas, gols_pro, gols_contra, saldo_gols, forma)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?);
        """,
            TEMPORADA, id_time,
            item.get("rank"), item.get("points"),
            all_g.get("played"), all_g.get("win"), all_g.get("draw"), all_g.get("lose"),
            goals.get("for"), goals.get("against"),
            item.get("goalsDiff"), item.get("form"),
            TEMPORADA, id_time,
            item.get("rank"), item.get("points"),
            all_g.get("played"), all_g.get("win"), all_g.get("draw"), all_g.get("lose"),
            goals.get("for"), goals.get("against"),
            item.get("goalsDiff"), item.get("form")
        )

    conn.commit()
    conn.close()
    print("[Classificacao] Tabela salva no banco.")
