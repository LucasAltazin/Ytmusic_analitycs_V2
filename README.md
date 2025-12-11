# Ytmusic_analitycs_V2
This project is a personal product &amp; tech sandbox to turn my YouTube Music history into a real analytics product. I use it to practice end-to-end Product Owner work (user research, user stories, iterations, backlog) combined with data engineering and dashboarding (ETL, data model, BI).
🎵 YTMusic Analytics
A full-stack data platform built from Google Takeout, Spotify API, BigQuery, dbt, n8n, and Looker Studio.

This project reconstructs and enriches my entire YouTube Music Library and Listening History into a complete analytics ecosystem with automated ETL pipelines and dashboards.

🚀 Project Overview
This repository contains three data products, each structured as a set of Epics and deliverables.

Product A — YT Music Library (src/library/)
Extract, clean, enrich and analyse my full saved library from Google Takeout.

Core features:

Extract music library + playlists
Standardize metadata (track, artist, album)
Enrich via Spotify (genres, duration, popularity…)
Build dbt models (stg → int → mart)
Publish Library dashboard in Looker Studio
Product B — Listening History (src/history/)
Parse my full YouTube + YouTube Music watch history.

Core features:

Extract and normalize watch-history.json
Detect music vs non-music
Join with Spotify enrichment
Build fact_listening mart
Dashboard: Listening patterns, top tracks, session metrics
Product C — ETL Automation (src/automation/)
Orchestrate all ETL with n8n, dbt Cloud and automated monitoring.

Core features:

Automated monthly Library refresh
Bi-weekly History ingestion
CI + testing pipeline
Monitoring dashboard
🗂 Project Structure
ytmusic-analytics/ ├─ dashboards/ # Looker Studio captures & documentation ├─ data/ │ ├─ raw/ │ │ ├─ takeout/ │ │ │ ├─ youtube_music/ │ │ │ │ ├─ history/ # watch-history.json, search-history.json │ │ │ │ ├─ music_library/ # music library songs.csv │ │ │ │ └─ playlists/ # playlist-videos.csv files │ │ │ └─ samples/ # sample files for dev │ │ └─ ytmusic/ # (legacy) raw ytmusicapi extractions │ ├─ interim/ # ETL staging outputs │ └─ processed/ # aggregated outputs (history_clean, dq logs…) │ ├─ dbt/ # dbt models, tests, documentation │ ├─ orchestration/ # n8n workflows, shell jobs, CI triggers │ ├─ secrets/ (gitignored) # credentials: GCP SA, Spotify, OAuth │ ├─ src/ │ ├─ config/ # whitelist, constants, params │ ├─ library/ # Product A – Library │ │ ├─ a1_extract_load/ # Epic A1: Takeout → BigQuery │ │ ├─ a2_spotify_enrich/ # Epic A2: Spotify enrichment │ │ ├─ a3_dbt/ # Epic A3: dbt models │ │ └─ a4_dashboard/ # Epic A4: Library dashboard prep │ │ │ ├─ history/ # Product B – Listening History │ │ ├─ b1_extract_load/ # Epic B1: Parse takeout history → BQ │ │ ├─ b2_spotify_enrich/ # Epic B2: Spotify enrichment │ │ ├─ b3_dbt/ # Epic B3: dbt history models │ │ └─ b4_dashboard/ # Epic B4: listening dashboard │ │ │ └─ automation/ # Product C – ETL Automation │ ├─ c1_n8n/ │ ├─ c2_dbt_automation/ │ └─ c3_monitoring/ │ ├─ .gitignore ├─ README.md └─ setup_structure.py # bootstrap the folder structure

🏗 ETL Pipeline — Product A
A1 — Extract & Load (Google Takeout → BigQuery)
✔ Extract library + whitelisted playlists
✔ Deduplicate
✔ Merge playlists metadata from library
✔ Load into BigQuery table raw_library
✔ Perform data quality checks (missing artists, missing albums…)

Scripts used:

src/library/a1_extract_load/extract_library_takeout.py
src/library/a1_extract_load/load_library_bq.py
src/library/a1_extract_load/dq_check_library.py
BigQuery tables: ytmusic_raw.raw_library

🎧 Product B — Listening History
Pipeline:

Parse detailed Watch History Takeout
Normalize timestamps
Detect “music” events
Join with Spotify metadata
Build fact_listening via dbt
🤖 Product C — ETL Automation
Automations handled by:

n8n workflows (monthly library refresh, bi-weekly history refresh)
dbt Cloud jobs triggered by API
logging / alerting in BigQuery + Looker
🛠 Installation & Usage
Create virtual environment
python -m venv venv .\venv\Scripts\Activate.ps1 # Windows

Install dependencies
pip install -r requirements.txt

Run extraction (Library + Playlists)
python src/library/a1_extract_load/extract_library_takeout.py

Run Data Quality checks
python src/library/a1_extract_load/dq_check_library.py

📊 Dashboards
Looker Studio dashboards (screenshots coming soon):

Library Overview
Playlist Explorer
Listening History Trends
Artist/Genre explorer
📚 Jira Epics Mapping
Epic	Description
A1	Extract & Load Library (Takeout → BQ)
A2	Spotify Enrichment (Genres & Metadata)
A3	dbt Models (Library)
A4	Library Dashboard
B1-B4	Listening History Product
C1-C3	Pipeline Automation & Monitoring
📌 Roadmap (Next Steps)
 Spotify enrichment (A2)
 Build dbt staging models
 Generate enriched mart tables
 Build Library dashboard MVP
 Automate ETL via n8n
 Monitoring dashboard
👤 Author
Lucas Altazin
Product Owner & Data Analyst
Brussels, Belgium

📧 Contact available on demand
🐙 GitHub: LucasAltazin


