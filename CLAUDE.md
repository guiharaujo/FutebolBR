# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

ETL pipeline that fetches Brazilian Série A football data from API-Football (api-sports.io) and stores it in SQL Server for Power BI consumption.

## Commands

```bash
# First-time setup: create database and tables
python database/setup.py

# Run full pipeline
python main.py

# Run pipeline skipping players (saves API requests)
python main.py --skip-jogadores

# Install dependencies
pip install -r requirements.txt
```

## Architecture

**Flow:** `main.py` → `database/setup.py` (idempotent DDL) → `etl/*.py` modules (in order: times → jogadores → jogos → estatisticas → classificacao)

**Key files:**
- `config.py` — all shared constants: `API_KEY`, `API_BASE_URL`, `LIGA_ID`, `TEMPORADA`, `CONNECTION_STRING`
- `api/client.py` — single `get(endpoint, params)` function wrapping requests to API-Football
- `database/setup.py` — creates `FutebolBR` database and all 6 tables using `IF NOT EXISTS` guards
- `etl/*.py` — each module exposes one `buscar_e_salvar()` function

**Database tables:** `Times`, `Jogadores`, `Jogos`, `Estatisticas_Jogo`, `Estatisticas_Jogador`, `Classificacao`

All ETL modules use SQL `MERGE` (upsert) on `id_api` as the natural key from the API.

## Retomada do Pipeline (limite diário da API)

O plano gratuito da API-Football tem limite diário de ~100 requests. O pipeline pode ser interrompido no meio das estatísticas. Ao retomar no dia seguinte, siga esta ordem:

**1. Verificar quantos jogos já foram processados:**
```bash
cd "C:\Users\guiha\OneDrive\Documentos\FutebolBR"
python -c "
import pyodbc
from config import CONNECTION_STRING
conn = pyodbc.connect(CONNECTION_STRING)
cur = conn.cursor()
cur.execute('SELECT COUNT(DISTINCT id_jogo) FROM Estatisticas_Jogo')
print('Jogos com estatisticas:', cur.fetchone()[0])
cur.execute('SELECT COUNT(*) FROM Jogos WHERE status = \'Match Finished\'')
print('Total jogos finalizados:', cur.fetchone()[0])
conn.close()
"
```

**2. Continuar de onde parou (retomada automática — pula jogos já processados):**
```bash
python main.py --skip-jogadores
```

O pipeline detecta automaticamente quais jogos já têm estatísticas salvas e processa apenas os pendentes. Repetir este comando diariamente até completar todos os 380 jogos (~8 dias no plano gratuito).

**Progresso atual (atualizado em 2026-03-29):** 301/380 jogos com estatísticas processadas.

**3. Quando concluir (180 jogos restantes), conferir classificação:**
```bash
python -c "
import pyodbc
from config import CONNECTION_STRING
conn = pyodbc.connect(CONNECTION_STRING)
cur = conn.cursor()
cur.execute('SELECT COUNT(*) FROM Classificacao')
print('Registros em Classificacao:', cur.fetchone()[0])
conn.close()
"
```

## API Rate Limits

The free plan allows **10 requests/minute** and only seasons **2022–2024**. The `estatisticas.py` module adds `time.sleep(7)` between each of the 2 calls per game — with 380 games, the full statistics step takes ~1h30.

## SQL Server Connection

Uses Windows Authentication (`Trusted_Connection=yes`) with `TrustServerCertificate=yes`. Requires **ODBC Driver 17 for SQL Server** installed locally. Server is `localhost`, database is `FutebolBR`.

## GitHub Repository

**URL:** https://github.com/guiharaujo/FutebolBR

**Auto-sync:** A `post-commit` git hook automatically pushes to GitHub after every commit. To publish changes:

```bash
git add .
git commit -m "descrição das mudanças"
# O push para o GitHub acontece automaticamente
```

**Segurança:** `config.py` está no `.gitignore` e nunca é enviado ao GitHub (contém API key e connection string). Use `config.example.py` como template ao clonar o repositório.
