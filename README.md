# 🎵 YT Music Analytics

Production-oriented analytics project demonstrating how to design, build, and operate a modern data pipeline around personal but realistic music data sources.

This project showcases **end-to-end analytics engineering skills** across ingestion, enrichment, modeling, and analytics delivery, with a clear separation of responsibilities:

- **Python** for extraction, normalization, enrichment, and data quality  
- **BigQuery** as the analytical cloud data warehouse  
- **dbt** for analytics engineering and semantic modeling  

The focus is **not the dataset itself**, but the **architecture, data quality, and analytical rigor** behind the pipeline.

---

## 🚀 Project Overview

The goal of this project is to transform raw **YouTube Music Google Takeout** data into a structured analytics platform, enriched with **Spotify metadata** and exposed through **analytics-ready marts**.

The repository combines:

- Python ingestion & enrichment pipelines  
- BigQuery as a scalable data warehouse  
- dbt for layered analytics modeling  
- Production-oriented project structure and naming conventions  

This repository is designed as a **data product**, not a collection of scripts.

---

## 🧠 Architecture & Design Philosophy

YouTube Music (Google Takeout)
↓
Python extraction & enrichment
↓
BigQuery (raw / enriched tables)
↓
dbt (staging → intermediate → mart)
↓
Analytics-ready datasets

yaml
Copier le code

### Core principles

- Clear boundary between **data engineering** and **analytics engineering**
- Explicit, step-based pipelines
- Analytics models designed for **direct BI consumption**

---

## 📁 Repository Structure

ytmusic-analytics/
├── src/ # Python ingestion & enrichment
│ ├── library/ # YouTube Music Library pipelines
│ ├── history/ # Listening history pipelines
│ └── config/ # Centralized configuration
│
├── models/ # dbt analytics models
│ ├── staging/
│ ├── intermediate/
│ └── mart/
│
├── macros/
├── tests/
├── seeds/
│
├── dbt_project.yml
├── packages.yml
└── README.md

markdown
Copier le code

---

## 🎧 Product A — YouTube Music Library

**Status:** Python ingestion complete · dbt models implemented

This product focuses on the extraction and enrichment of a saved **YouTube Music library** (tracks, artists, albums).

### Responsibility split

#### Python — Ingestion & Enrichment

Located in:

- `src/library/a1_extract_load/`
- `src/library/a2_spotify_enrich/`

Main responsibilities:

- Parse Google Takeout exports  
- Normalize raw metadata (track, artist, album)  
- Generate stable identifiers and YouTube Music URLs  
- Enrich tracks with Spotify metadata  
- Run data-quality checks before loading  

Key scripts:

- `extract_library_takeout.py`
- `load_library_bq.py`
- `enrich_spotify_library.py`
- `dq_check_library.py`
- `dq_check_spotify_enriched_library.py`

#### dbt — Analytics Modeling

Located in:

models/
├── staging/raw/
├── intermediate/
└── mart/

markdown
Copier le code

- `stg_*` → cleaned and standardized sources  
- `int_*` → enriched joins and derived metrics  
- `mart_*` → analytics-ready KPI tables  

---

## 🎧 Product B — Listening History

**Status:** Python ingestion complete · dbt models in progress

This product processes **YouTube & YouTube Music watch history** to reconstruct listening behavior.

### Responsibility split

#### Python — Ingestion & Enrichment

Located in:

- `src/history/b1_extract_load/`
- `src/history/b2_spotify_enrich/`

Main responsibilities:

- Parse watch-history Takeout JSON  
- Normalize timestamps and sessions  
- Filter music vs non-music events  
- Enrich tracks with Spotify metadata  
- Apply data-quality checks  

Key scripts:

- `extract_watch_history.py`
- `load_history_bq.py`
- `enrich_spotify_history.py`
- `dq_check_watch_history_youtube_music.py`

#### dbt — Analytics Modeling (ongoing)

Planned models:

- Listening fact tables  
- Aggregations by artist, genre, and time  
- Consumption-ready KPIs  

---

## 🧱 Analytics Modeling (dbt)

The dbt project follows a classic layered approach:

models/
├── staging/ # Light cleaning, renaming, casting
├── intermediate/ # Business logic & joins
└── mart/ # Analytics-ready tables

yaml
Copier le code

Tests and documentation are centralized in:

- `models/schema.yml`

---

## 📊 Analytics Outputs

Final marts are designed to expose:

- Track popularity & duration  
- Release year and track age  
- Genre classification (main / sub)  
- Library-level and listening KPIs  

These tables are built to be consumed directly by:

- BI tools (Looker Studio, Power BI)  
- Ad-hoc SQL analysis  

---

## 🛠 Tooling & Stack

- Python 3.11  
- Google BigQuery  
- dbt (BigQuery adapter)  
- Spotify Web API  
- Google Takeout  
- SQL (analytics engineering)  

---

## 🗺 Roadmap (Realistic)

This roadmap reflects incremental, production-oriented steps:

- Incremental models for listening history  
- Snapshotting for slowly changing attributes  
- Pipeline orchestration & automation  
- BI dashboards on top of analytics marts  

---

## 👤 Author

**Lucas Altazin**  
Product Owner · Data Analyst  
Brussels, Belgium  

GitHub: https://github.com/LucasAltazin

