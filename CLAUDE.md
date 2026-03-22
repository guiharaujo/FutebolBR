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
