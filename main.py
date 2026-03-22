"""
Pipeline principal — FutebolBR
Busca dados da API-Football e salva no SQL Server.

Uso:
    python main.py               → roda tudo
    python main.py --skip-jogadores  → pula jogadores (economiza requests)
"""
import sys
from database.setup import criar_banco, criar_tabelas
from etl import times, jogadores, jogos, estatisticas, classificacao


def main():
    skip_jogadores = "--skip-jogadores" in sys.argv

    print("=" * 50)
    print("  FutebolBR — Pipeline de Dados")
    print("=" * 50)

    print("\n[1/6] Verificando banco e tabelas...")
    criar_banco()
    criar_tabelas()

    print("\n[2/6] Times...")
    times.buscar_e_salvar()

    if not skip_jogadores:
        print("\n[3/6] Jogadores...")
        print("      (use --skip-jogadores para pular e economizar requests)")
        jogadores.buscar_e_salvar()
    else:
        print("\n[3/6] Jogadores... PULADO")

    print("\n[4/6] Jogos/Partidas...")
    jogos.buscar_e_salvar()

    print("\n[5/6] Estatisticas...")
    estatisticas.buscar_e_salvar()

    print("\n[6/6] Classificacao...")
    classificacao.buscar_e_salvar()

    print("\n" + "=" * 50)
    print("  Pipeline concluido! Abra o Power BI e")
    print("  conecte ao SQL Server > FutebolBR.")
    print("=" * 50)


if __name__ == "__main__":
    main()
