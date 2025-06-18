# Mongo\_DB\_Writer

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Features](#features)
4. [Installation](#installation)
5. [Configuration](#configuration)
6. [Quick Start](#quick-start)
7. [Detailed Script Reference](#detailed-script-reference)
8. [Logging & Monitoring](#logging--monitoring)
9. [Data Quality & Error Handling](#data-quality--error-handling)
10. [Extending the Pipeline](#extending-the-pipeline)
11. [Roadmap](#roadmap)
12. [Contributing](#contributing)
13. [License](#license)
14. [Authors & Acknowledgements](#authors--acknowledgements)

---

## Overview

**Mongo\_DB\_Writer** is a modular Python toolkit for **extracting**, **transforming**, and **loading** (ETL) hospitality‑related datasets into a MongoDB database.  The scripts turn raw CSV/JSON files sourced from booking platforms into fully‑normalised MongoDB collections that can power analytics dashboards, machine‑learning prototypes, or data‑warehousing initiatives.

The repository grew out of a need to **spin‑up realistic demo data** for research on revenue management and reputation modelling in hotels and short‑term rentals.  Out‑of‑the‑box you can seed:

* 💼 **Properties** – core listing information (name, address, geolocation, amenities, etc.)
* 🛏️ **Rooms** – room types, occupancy, bed configuration
* 🏷️ **BAR (Best Available Rate)** – pricing snapshots
* ⭐ **Reputation KPI** – aggregated review scores
* 📝 **Reviews** – individual guest reviews
* 🌍 **Cities** – multilingual city metadata

Although written for hospitality, the pipeline is generic enough to be repurposed for other verticals with minimal tweaks.

---

## Architecture

```
                ┌────────────────────┐
                │   Raw Data Files   │
                └────────┬───────────┘
                         │ 1. Parse & Validate (parsers.py)
                ┌────────▼───────────┐
                │  Intermediate CSV  │
                └────────┬───────────┘
                         │ 2. Transform & Enrich (seed_* scripts)
                ┌────────▼───────────┐
                │   Pydantic Model   │
                └────────┬───────────┘
                         │ 3. Insert (pymongo)
                ┌────────▼───────────┐
                │   MongoDB Atlas    │
                └────────────────────┘
```

* Every **seed\_\*.py** script focuses on one entity type and follows the same 3‑stage pattern: parse → model → insert.
* MongoDB connection details are pulled from **environment variables** (see [Configuration](#configuration)).
* All ETL steps emit **structured JSON logs** via `logfire`, ready for centralised aggregation.

---

## Features

| Category                 | Capability                                 | Details                                                                                                              |
| ------------------------ | ------------------------------------------ | -------------------------------------------------------------------------------------------------------------------- |
| **Parsing**              | Robust primitive parsers (`parsers.py`)    | Gracefully handles empty strings, malformed numbers, and date formats.                                               |
| **Normalisation**        | Property/room types & accommodation levels | `property_types.py` and `extract_accommodation_level.py` map platform‑specific labels to a canonical taxonomy.       |
| **ID Mapping**           | Booking‑platform → internal IDs            | `map_booking_ids.py` ensures referential integrity across collections.                                               |
| **Multilingual Support** | City name translation                      | `translate_city.py` leverages a static dictionary (or external API if configured) to convert local names to English. |
| **Selective Seeding**    | Run only what you need                     | Execute any individual **seed\_**\* script, or call `seed_full.py` for a one‑shot import.                            |
| **Re‑entrancy**          | Skip duplicates idempotently               | Collections are indexed on natural keys; re‑running a script safely upserts/ignores existing docs.                   |
| **Error Logging**        | Skipped rows & IDs                         | Bad records funnel into `skipped_records.txt` & `skipped_ids.txt` for later inspection.                              |
| **Purging**              | Wipe collections                           | `purge_table.py` quickly drops or truncates target collections when you need a clean slate.                          |

---

## Installation

### Prerequisites

* **Python ≥ 3.11** (a `pyvenv.cfg` is included for reference)
* **MongoDB 6.0+** – local instance *or* MongoDB Atlas cluster
* (Optional) **make** for shortcut commands on macOS/Linux

### Steps

```bash
# 1. Clone the repo
$ git clone https://github.com/FMerlino02/Mongo_DB_Writer.git
$ cd Mongo_DB_Writer

# 2. Create & activate a virtual env (Unix)
$ python3 -m venv .venv
$ source .venv/bin/activate

# 3. Install dependencies
$ pip install -r requirements.txt
```

A suite of convenience scripts lives under **Scripts/** for Windows users (`activate.bat`, `pip.exe`, etc.).

---

## Configuration

Configuration is zero‑code and entirely environment‑driven.  Create a `.env` file at the project root:

```ini
# Mongo connection
MONGO_URI=mongodb+srv://<user>:<password>@cluster0.xyz.mongodb.net/
MONGO_DB_NAME=hospitality_demo

# Optional flags
SEED_BATCH_SIZE=500        # Bulk‑insert chunk size
TRANSLATION_LANG=en        # Target language for city names
```

The `.env` file is loaded automatically by `python‑dotenv`.  Any variable can instead be exported in your shell or injected by your CI/CD pipeline.

---

## Quick Start

```bash
# 1. Verify DB connectivity (auto‑created if missing)
python Include/setup.py

# 2. Import everything
python Include/seed_full.py               # ≈60 seconds for 50k rows

# 3. Check results (mongo shell)
> use hospitality_demo
> db.Properties.countDocuments()          # → 50 000
> db.Reviews.findOne({ rating: { $gte: 9 } })
```

Need only reviews for an A/B test?  Run:

```bash
python Include/seed_reviews.py
```

---

## Detailed Script Reference

| Script                               | Purpose                                                                                                     | Key Classes / Functions                             |
| ------------------------------------ | ----------------------------------------------------------------------------------------------------------- | --------------------------------------------------- |
| **extract\_accommodation\_level.py** | Classifies a property into *Budget*, *Mid‑scale*, or *Luxury* based on amenities and star rating.           | `extract_accommodation_level()`                     |
| **map\_booking\_ids.py**             | Generates a dictionary mapping external booking IDs → Mongo `_id` values, ensuring foreign‑key consistency. | `get_booking_id_map()`                              |
| **parsers.py**                       | Shared helpers to coerce strings into `int`, `float`, `datetime`.  Null‑safe & locale‑aware.                | `parse_int()`, `parse_float()`, `parse_date()`      |
| **property\_types.py**               | Normalises diverse property type labels (e.g., “apt”, “condo”) to a finite controlled vocabulary.           | `PropertyType` (Enum), `main()`                     |
| **purge\_table.py**                  | Drops one or more collections – handy for integration tests.                                                | `main()`                                            |
| **seed\_bar.py**                     | Imports Best Available Rate (BAR) snapshots.                                                                | `BarInfo` (Pydantic model), `main()`                |
| **seed\_cities.py**                  | Seeds city metadata; optionally translates names with `translate_city.py`.                                  | `main()`                                            |
| **seed\_full.py**                    | Orchestrator that calls every other `seed_*` script in dependency order.                                    | –                                                   |
| **seed\_properties.py**              | Core property import – massive CSVs supported via batch inserts.                                            | `PropertiesInfo` (model), helper `translate_city()` |
| **seed\_reputation.py**              | Aggregates review scores into KPI documents.                                                                | `ReputationKPI`, `main()`                           |
| **seed\_reviews.py**                 | High‑volume guest reviews; performs sentiment filtering and deduplication.                                  | `ReviewInfo`, `main()`                              |
| **seed\_rooms.py**                   | Room‑level details with occupancy parsing and accommodation level tagging.                                  | `RoomInfo`, `parse_occupancy()`                     |
| **setup.py**                         | Pre‑flight checks: validates env vars, pings the database, creates indexes.                                 | –                                                   |
| **translate\_city.py**               | Simple translation utility (can be replaced with a real API).                                               | `translate_city()`                                  |

> **Tip:** Each script supports `--help` for CLI arguments (batch size, dry‑run, etc.).

---

## Logging & Monitoring

All scripts write **JSON‑formatted** logs via [**logfire**](https://pypi.org/project/logfire/).  By default, logs stream to `stdout` and rotate daily to `logs/`.

| Field       | Example           | Description            |
| ----------- | ----------------- | ---------------------- |
| `level`     | `INFO`            | Log severity           |
| `script`    | `seed_reviews`    | Originating script     |
| `record_id` | `rvw_8N8G5`       | Affected record        |
| `msg`       | `Inserted review` | Human‑readable message |

Forward logs to Elastic, Datadog, or CloudWatch by setting `LOGFIRE_EXPORTER` in your `.env`.

---

## Data Quality & Error Handling

* **Pydantic validation** – strict field typing; bad rows fail fast.
* **Graceful degradation** – non‑critical errors are logged and the row is appended to:

  * `Include/skipped_records.txt` – raw CSV line with reason.
  * `Include/skipped_ids.txt` – orphaned IDs lacking parent records.
* Summary statistics (inserted vs skipped) print at script completion.

---

## Extending the Pipeline

1. **Add a new collection**: scaffold `seed_<entity>.py` from an existing script.
2. Define a **Pydantic model** describing the schema.
3. Implement `transform_row()` to map raw CSV → model instance.
4. Register the script in `seed_full.py` so it participates in the global run.
5. Write tests ‑ we recommend `pytest` + `mongomock`.

---

## Roadmap

* ☐ Switch to **PyMongo’s** `bulk_write` for \~2× speed‑up.
* ☐ Plug‑in system for third‑party data enrichers (geocoding, sentiment).
* ☐ Docker Compose for one‑line local dev.
* ☐ CI pipeline (GitHub Actions) with linting & tests.

---

## Contributing

Pull requests are welcome!  To propose a change:

1. Fork → create feature branch → commit descriptive messages.
2. Ensure `pre‑commit run --all-files` passes (black, isort, flake8).
3. Submit PR and describe **what** & **why**.  One of the maintainers will review.

For larger features, open an **issue** first to discuss design.

---

## License

This repository currently ships **without an explicit license**.  All rights reserved; please contact the author for commercial or redistribution use.

---

## Authors & Acknowledgements

Created and maintained by **[FMerlino02](https://github.com/FMerlino02)**.  Inspired by data‑engineering patterns from Booking.com.

Special thanks to contributors who reported bugs and shared datasets for testing.
