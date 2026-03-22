"""
Cria o banco de dados FutebolBR e todas as tabelas no SQL Server.
Execute uma vez antes de rodar o pipeline principal.
"""
import pyodbc

# Conecta ao master para criar o banco
MASTER_CONN = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "Server=localhost;"
    "Database=master;"
    "Trusted_Connection=yes;"
    "TrustServerCertificate=yes;"
)

DB_CONN = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "Server=localhost;"
    "Database=FutebolBR;"
    "Trusted_Connection=yes;"
    "TrustServerCertificate=yes;"
)


def criar_banco():
    conn = pyodbc.connect(MASTER_CONN, autocommit=True)
    cursor = conn.cursor()
    cursor.execute("""
        IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'FutebolBR')
        BEGIN
            CREATE DATABASE FutebolBR
        END
    """)
    conn.close()
    print("[OK] Banco de dados FutebolBR verificado/criado.")


def criar_tabelas():
    conn = pyodbc.connect(DB_CONN)
    cursor = conn.cursor()

    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Times' AND xtype='U')
        CREATE TABLE Times (
            id_time       INT IDENTITY(1,1) PRIMARY KEY,
            id_api        INT NOT NULL UNIQUE,
            nome          NVARCHAR(100) NOT NULL,
            nome_abrev    NVARCHAR(20),
            pais          NVARCHAR(50),
            fundado       INT,
            estadio       NVARCHAR(100),
            cidade        NVARCHAR(100),
            logo_url      NVARCHAR(300)
        )
    """)

    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Jogadores' AND xtype='U')
        CREATE TABLE Jogadores (
            id_jogador       INT IDENTITY(1,1) PRIMARY KEY,
            id_api           INT NOT NULL UNIQUE,
            nome             NVARCHAR(100) NOT NULL,
            data_nascimento  DATE,
            nacionalidade    NVARCHAR(50),
            altura           NVARCHAR(10),
            peso             NVARCHAR(10),
            posicao          NVARCHAR(30),
            id_time          INT REFERENCES Times(id_time),
            numero_camisa    INT
        )
    """)

    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Jogos' AND xtype='U')
        CREATE TABLE Jogos (
            id_jogo       INT IDENTITY(1,1) PRIMARY KEY,
            id_api        INT NOT NULL UNIQUE,
            data_hora     DATETIME,
            rodada        NVARCHAR(50),
            temporada     INT,
            id_time_casa  INT REFERENCES Times(id_time),
            id_time_fora  INT REFERENCES Times(id_time),
            gols_casa     INT,
            gols_fora     INT,
            status        NVARCHAR(30),
            estadio       NVARCHAR(100),
            arbitro       NVARCHAR(100)
        )
    """)

    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Estatisticas_Jogo' AND xtype='U')
        CREATE TABLE Estatisticas_Jogo (
            id                  INT IDENTITY(1,1) PRIMARY KEY,
            id_jogo             INT REFERENCES Jogos(id_jogo),
            id_time             INT REFERENCES Times(id_time),
            chutes_total        INT,
            chutes_no_gol       INT,
            chutes_fora         INT,
            posse_bola          NVARCHAR(10),
            passes_total        INT,
            passes_certos       INT,
            faltas              INT,
            escanteios          INT,
            impedimentos        INT,
            defesas_goleiro     INT,
            cartoes_amarelos    INT,
            cartoes_vermelhos   INT,
            desarmes            INT
        )
    """)

    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Estatisticas_Jogador' AND xtype='U')
        CREATE TABLE Estatisticas_Jogador (
            id                  INT IDENTITY(1,1) PRIMARY KEY,
            id_jogo             INT REFERENCES Jogos(id_jogo),
            id_jogador          INT REFERENCES Jogadores(id_jogador),
            id_time             INT REFERENCES Times(id_time),
            minutos_jogados     INT,
            gols                INT,
            assistencias        INT,
            chutes_total        INT,
            chutes_no_gol       INT,
            passes_total        INT,
            passes_certos       INT,
            desarmes            INT,
            interceptacoes      INT,
            faltas_cometidas    INT,
            faltas_sofridas     INT,
            cartao_amarelo      INT,
            cartao_vermelho     INT,
            defesas             INT
        )
    """)

    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Classificacao' AND xtype='U')
        CREATE TABLE Classificacao (
            id              INT IDENTITY(1,1) PRIMARY KEY,
            temporada       INT,
            id_time         INT REFERENCES Times(id_time),
            posicao         INT,
            pontos          INT,
            jogos           INT,
            vitorias        INT,
            empates         INT,
            derrotas        INT,
            gols_pro        INT,
            gols_contra     INT,
            saldo_gols      INT,
            forma           NVARCHAR(20),
            CONSTRAINT UQ_Classificacao UNIQUE (temporada, id_time)
        )
    """)

    conn.commit()
    conn.close()
    print("[OK] Todas as tabelas verificadas/criadas.")


if __name__ == "__main__":
    criar_banco()
    criar_tabelas()
    print("\nSetup concluido! Banco FutebolBR pronto para uso.")
