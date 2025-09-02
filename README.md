# Basketball-ETL

[![Issues](https://img.shields.io/github/issues/arperez22/Basketball-ETL)](https://github.com/arperez22/Basketball-ETL/issues) [![Last Commit](https://img.shields.io/github/last-commit/arperez22/Basketball-ETL)](https://github.com/arperez22/Basketball-ETL/commits)

Compact ETL pipeline for scraping, cleaning, and loading NCAA postseason basketball stats into PostgreSQL.

---

## Table of contents
- [What is this](#what-is-this)
- [Technologies](#technologies)
- [Quick start](#quick-start)
- [Configuration](#configuration)
- [Potential Improvements](#potential-improvements)

---

## What is this
**Basketball-ETL** is a focused extract-transform-load pipeline that scrapes postseason NCAA basketball data, normalizes and enriches it, and stores structured tables in a PostgreSQL database for analysis. It’s intended as a reproducible, small-scale example of a production-style data pipeline useful for analytics, ML experiments, and dashboards.

---

## Technologies
- **Python** - core language for scraping & transforms  
- **pandas** - tabular data cleaning and transformations  
- **requests** / **BeautifulSoup** - HTML fetching and parsing  
- **psycopg2** - database interaction and loading  
- **PostgreSQL** - target relational storage  

---

## Quick start

1. Clone the repo
```bash
git clone https://github.com/arperez22/Basketball-ETL.git
cd Basketball-ETL
```

2. Create and activate a virtual environment
```bash
python -m venv .venv
.venv\Scripts\activate
```

3. Install dependencies
```bash
pip install -r requirements.txt
```

4. Run the pipeline (See [Configuration](#configuration))
```bash
python main.py
```

---

## Configuration

Before running the pipeline, create a file named `.env` in the project directory and populate with environment variables

```env
# Example
DB_HOST = localhost
DB_NAME = baskbetball
DB_USER = user
DB_PASSWORD = secret
DB_PORT = 5432
```

---

## Potential Improvements

- Containerize the pipeline (Dockerfile) and provide docker-compose.yml with PostgreSQL for local dev.
- Testing & CI: unit tests for parsers, integration tests for DB loads, and GitHub Actions.
- Documentation: add example notebooks showing analyses, and a CONTRIBUTING.md.