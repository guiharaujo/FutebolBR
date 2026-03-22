"""
ETL: busca estatísticas de cada partida (por time e por jogador) e salva no SQL Server.
Processa apenas jogos com status 'Match Finished'.
"""
import time
import pyodbc
from api.client import get
from config import CONNECTION_STRING


def _parse_int(valor):
    if valor is None:
        return None
    try:
        return int(str(valor).replace("%", "").strip())
    except (ValueError, TypeError):
        return None


def _jogos_finalizados(cursor):
    cursor.execute("""
        SELECT id_jogo, id_api, id_time_casa, id_time_fora
        FROM Jogos
        WHERE status = 'Match Finished'
    """)
    return cursor.fetchall()


def _id_time_local(cursor, id_api):
    cursor.execute("SELECT id_time FROM Times WHERE id_api = ?", id_api)
    row = cursor.fetchone()
    return row[0] if row else None


def _id_jogador_local(cursor, id_api):
    cursor.execute("SELECT id_jogador FROM Jogadores WHERE id_api = ?", id_api)
    row = cursor.fetchone()
    return row[0] if row else None


def _salvar_estat_jogo(cursor, id_jogo, id_time, stats_lista):
    """Salva estatísticas por time numa partida."""
    stat_map = {s["type"]: s["value"] for s in stats_lista}

    cursor.execute("""
        IF NOT EXISTS (SELECT 1 FROM Estatisticas_Jogo WHERE id_jogo=? AND id_time=?)
        INSERT INTO Estatisticas_Jogo
            (id_jogo, id_time, chutes_total, chutes_no_gol, chutes_fora,
             posse_bola, passes_total, passes_certos, faltas, escanteios,
             impedimentos, defesas_goleiro, cartoes_amarelos, cartoes_vermelhos, desarmes)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """,
        id_jogo, id_time,
        id_jogo, id_time,
        _parse_int(stat_map.get("Total Shots")),
        _parse_int(stat_map.get("Shots on Goal")),
        _parse_int(stat_map.get("Shots off Goal")),
        stat_map.get("Ball Possession"),
        _parse_int(stat_map.get("Total passes")),
        _parse_int(stat_map.get("Passes accurate")),
        _parse_int(stat_map.get("Fouls")),
        _parse_int(stat_map.get("Corner Kicks")),
        _parse_int(stat_map.get("Offsides")),
        _parse_int(stat_map.get("Goalkeeper Saves")),
        _parse_int(stat_map.get("Yellow Cards")),
        _parse_int(stat_map.get("Red Cards")),
        None  # Desarmes não disponível nesse endpoint
    )


def _salvar_estat_jogadores(cursor, id_jogo, jogadores_stats):
    """Salva estatísticas por jogador numa partida."""
    for item in jogadores_stats:
        p = item["player"]
        stats = item.get("statistics", [{}])[0]

        id_jogador = _id_jogador_local(cursor, p["id"])
        if not id_jogador:
            continue

        id_time = _id_time_local(cursor, stats.get("team", {}).get("id")) if "team" in stats else None

        g = stats.get("games", {})
        s = stats.get("shots", {})
        ps = stats.get("passes", {})
        tk = stats.get("tackles", {})
        fls = stats.get("fouls", {})
        cards = stats.get("cards", {})
        gl = stats.get("goals", {})
        gk = stats.get("goalkeeper", {})

        minutos = g.get("minutes")
        if isinstance(minutos, str):
            minutos = _parse_int(minutos)

        cursor.execute("""
            IF NOT EXISTS (SELECT 1 FROM Estatisticas_Jogador WHERE id_jogo=? AND id_jogador=?)
            INSERT INTO Estatisticas_Jogador
                (id_jogo, id_jogador, id_time, minutos_jogados, gols, assistencias,
                 chutes_total, chutes_no_gol, passes_total, passes_certos,
                 desarmes, interceptacoes, faltas_cometidas, faltas_sofridas,
                 cartao_amarelo, cartao_vermelho, defesas)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
            id_jogo, id_jogador,
            id_jogo, id_jogador, id_time,
            minutos,
            _parse_int(gl.get("total")),
            _parse_int(gl.get("assists")),
            _parse_int(s.get("total")),
            _parse_int(s.get("on")),
            _parse_int(ps.get("total")),
            _parse_int(ps.get("accuracy")),
            _parse_int(tk.get("total")),
            _parse_int(tk.get("interceptions")),
            _parse_int(fls.get("committed")),
            _parse_int(fls.get("drawn")),
            _parse_int(cards.get("yellow")),
            _parse_int(cards.get("red")),
            _parse_int(gk.get("saves"))
        )


def buscar_e_salvar():
    print("[Estatisticas] Iniciando busca por jogo...")
    conn = pyodbc.connect(CONNECTION_STRING)
    cursor = conn.cursor()
    jogos = _jogos_finalizados(cursor)
    print(f"[Estatisticas] {len(jogos)} jogos finalizados para processar.")

    for i, (id_jogo, id_api_jogo, _, _) in enumerate(jogos):
        # Estatísticas por time
        data_stat = get("fixtures/statistics", {"fixture": id_api_jogo})
        for team_stat in data_stat.get("response", []):
            id_api_time = team_stat["team"]["id"]
            id_time = _id_time_local(cursor, id_api_time)
            if id_time:
                _salvar_estat_jogo(cursor, id_jogo, id_time, team_stat.get("statistics", []))

        time.sleep(7)  # respeita rate limit de 10 req/min

        # Estatísticas por jogador
        data_play = get("fixtures/players", {"fixture": id_api_jogo})
        for team_data in data_play.get("response", []):
            _salvar_estat_jogadores(cursor, id_jogo, team_data.get("players", []))

        conn.commit()

        time.sleep(7)  # respeita rate limit de 10 req/min
        if (i + 1) % 10 == 0:
            print(f"[Estatisticas] {i + 1}/{len(jogos)} jogos processados...")

    conn.close()
    print("[Estatisticas] Concluido.")
