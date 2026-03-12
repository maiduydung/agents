# Aviation Data Platform — Medallion Architecture

A fully working local data platform that simulates an airline's data engineering backbone. Ingests synthetic Vietnamese aviation data through a **medallion architecture** (Bronze/Silver/Gold) using DuckDB as the local analytical engine, with Pydantic schema enforcement, automated data quality checks, and structured observability.

## Why This Matters

Airlines operate dozens of interconnected source systems — reservation (PSS), flight operations (OPS), crew management, and weather services. A data engineering backbone must **integrate, validate, and model** this data for downstream analytics, revenue management, and operational decision-making.

This project demonstrates production-grade patterns that map directly to the Microsoft Data Platform stack used at scale:

| Local Component | Azure Equivalent | Purpose |
|---|---|---|
| DuckDB | Azure Synapse / Microsoft Fabric Lakehouse | Analytical query engine |
| Local JSON files | Azure Data Lake Storage Gen2 (ADLS) | Raw data landing zone |
| Python orchestrator | Azure Data Factory (ADF) / Fabric Pipelines | Pipeline orchestration |
| Pydantic models | Fabric schema enforcement / Data contracts | Schema validation |
| Quality framework | Great Expectations / Fabric Data Quality | Automated quality gates |
| structlog metrics | Azure Monitor / Log Analytics | Observability & alerting |
| CLI (`main.py`) | ADF triggers / Fabric notebooks | Execution interface |

## Tech Stack

- **Python 3.11+** — pipeline orchestration and data processing
- **DuckDB 1.1+** — embedded analytical database (OLAP)
- **Pydantic 2.5+** — schema validation and data contracts
- **structlog 24.1+** — structured logging for observability
- **Click 8.1+** — CLI interface
- **Faker 22.0+** — realistic synthetic data generation
- **tabulate 0.9+** — quality report formatting

## Architecture

### Medallion Architecture Flow

```mermaid
flowchart LR
    subgraph Sources["Source Systems"]
        RES[Reservation System<br/>PSS/Amadeus]
        OPS[Flight Operations<br/>ACARS/OPS]
        CRW[Crew Management<br/>Rostering]
        WX[Weather Service<br/>METAR/TAF]
    end

    subgraph Bronze["Bronze Layer — Raw"]
        B1[bronze_reservations]
        B2[bronze_flights]
        B3[bronze_crew]
        B4[bronze_weather]
    end

    subgraph Silver["Silver Layer — Curated"]
        S1[silver_reservations]
        S2[silver_flights]
        S3[silver_crew]
        S4[silver_weather]
    end

    subgraph Gold["Gold Layer — Serving"]
        F1[fact_flights]
        F2[fact_bookings]
        D1[dim_time]
        D2[dim_aircraft]
        D3[dim_routes]
        D4[dim_crew]
    end

    subgraph Consumers["Consumers"]
        BI[Power BI<br/>Dashboards]
        ML[ML Models<br/>Delay Prediction]
        RPT[Reports<br/>Revenue/OTP]
    end

    RES --> B1
    OPS --> B2
    CRW --> B3
    WX --> B4

    B1 -->|"Validate<br/>Deduplicate"| S1
    B2 -->|"Clean<br/>Type Cast"| S2
    B3 -->|"Schema Enforce<br/>SCD Type 2"| S3
    B4 -->|"Range Check<br/>Null Handle"| S4

    S1 --> F2
    S2 --> F1
    S3 --> D4
    S2 --> D2
    S2 --> D3
    S1 --> D1
    S2 --> D1

    F1 --> BI
    F2 --> BI
    F1 --> ML
    F2 --> RPT
```

### Data Quality Framework

```mermaid
flowchart TD
    subgraph Checks["Quality Check Categories"]
        C1["Completeness<br/>Null rates per column"]
        C2["Validity<br/>Status in allowed values<br/>delay >= 0"]
        C3["Uniqueness<br/>No duplicate PKs"]
        C4["Referential Integrity<br/>FK resolution"]
        C5["Freshness<br/>Data within expected window"]
    end

    subgraph Execution["Validation Engine"]
        E1["Execute SQL checks<br/>against DuckDB"]
        E2["Compare actual vs threshold"]
        E3["Classify: PASS / WARN / FAIL"]
    end

    subgraph Output["Reporting"]
        O1["Quality Report<br/>per layer"]
        O2["Pipeline Metrics<br/>rejection rates"]
        O3["Alert Thresholds<br/>rejection > 5%"]
    end

    C1 --> E1
    C2 --> E1
    C3 --> E1
    C4 --> E1
    C5 --> E1

    E1 --> E2 --> E3

    E3 --> O1
    E3 --> O2
    E3 --> O3
```

### Star Schema ERD

```mermaid
erDiagram
    dim_time {
        int date_key PK
        date full_date
        int year
        int quarter
        int month
        string month_name
        int day_of_week
        string day_name
        boolean is_weekend
    }

    dim_aircraft {
        int aircraft_key PK
        string aircraft_registration
        string aircraft_type
        string airline_code
        int seat_capacity
        date effective_from
        date effective_to
        boolean is_current
    }

    dim_routes {
        int route_key PK
        string origin
        string destination
        string airline_code
        string route_code
        int distance_km
        boolean domestic
    }

    dim_crew {
        int crew_key PK
        string employee_id
        string crew_name
        string role
        string airline_code
        string base_airport
        boolean is_current
    }

    fact_flights {
        int flight_key PK
        string flight_id
        string flight_number
        int date_key FK
        int route_key FK
        int aircraft_key FK
        string status
        int delay_minutes
        int pax_count
        float load_factor
        float fuel_kg
    }

    fact_bookings {
        int booking_key PK
        string reservation_id
        string flight_number
        int date_key FK
        int route_key FK
        string airline_code
        string fare_class
        float fare_amount
        string booking_channel
        string status
    }

    fact_flights ||--o{ dim_time : "date_key"
    fact_flights ||--o{ dim_routes : "route_key"
    fact_flights ||--o{ dim_aircraft : "aircraft_key"
    fact_bookings ||--o{ dim_time : "date_key"
    fact_bookings ||--o{ dim_routes : "route_key"
```

## Setup & Run

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Generate synthetic aviation data (1,000 flights + 3,000 bookings + crew + weather)
python main.py generate --records 1000

# 3. Run the full pipeline (bronze -> silver -> gold)
python main.py run --full

# 4. Run data quality checks
python main.py quality

# 5. Run a specific layer only
python main.py run --layer bronze
python main.py run --layer silver
python main.py run --layer gold

# 6. Backfill historical data (idempotent)
python main.py backfill --start-date 2024-01-01 --end-date 2024-01-31

# 7. Run tests
python -m pytest tests/ -v
```

### CLI Options

```
python main.py --help
python main.py generate --records 5000 --start-date 2024-06-01 --end-date 2024-06-30
python main.py run --full --log-level DEBUG
python main.py quality --layer silver
```

## Sample Output

### Pipeline Run

```
$ python main.py generate --records 1000 && python main.py run --full

Generated data:
  flights: 1,000 records
  reservations: 3,000 records
  crew: 1,000 records
  weather: 1,000 records

INFO  pipeline_stage_complete  stage=ingest_flights     layer=bronze  rows_in=1000  rows_out=1000  duration_ms=10974
INFO  pipeline_stage_complete  stage=ingest_reservations layer=bronze  rows_in=3000  rows_out=3000  duration_ms=800
INFO  pipeline_stage_complete  stage=process_flights    layer=silver  rows_in=1000  rows_out=976   rows_rejected=24   rejection_rate=0.024
INFO  pipeline_stage_complete  stage=process_reservations layer=silver rows_in=3000 rows_out=2913  rows_rejected=87   rejection_rate=0.029
INFO  gold_sql_executed  file=gold_dim_time.sql       table=dim_time       new_rows=31    total_rows=31
INFO  gold_sql_executed  file=gold_dim_aircraft.sql   table=dim_aircraft   new_rows=971   total_rows=971
INFO  gold_sql_executed  file=gold_fact_flights.sql   table=fact_flights   new_rows=1006  total_rows=1006
INFO  gold_sql_executed  file=gold_fact_bookings.sql  table=fact_bookings  new_rows=2913  total_rows=2913

Pipeline complete. Batch: batch-0bf4f1c2
  Gold totals: {'dim_time': 31, 'dim_aircraft': 971, 'dim_routes': 281, 'fact_flights': 1006, 'fact_bookings': 2913}
```

### Quality Report

```
================================================================================
  DATA QUALITY REPORT
================================================================================

  Total checks: 14
  Passed:       14
  Failed:       0
  Warnings:     0

  --- SILVER LAYER ---
  [PASS] silver_flights_valid_status         All flights have valid status values
  [PASS] silver_flights_positive_delay       No unrealistic negative delays
  [PASS] silver_reservations_positive_fare   All reservations have non-negative fares
  [PASS] silver_flights_null_rate_origin     Null rate 0.0 <= threshold 0.05
  [PASS] silver_flights_no_duplicates        No duplicate flight_id in silver

  --- GOLD LAYER ---
  [PASS] gold_fact_flights_referential_routes     All FK references valid
  [PASS] gold_fact_flights_referential_aircraft   All FK references valid
  [PASS] gold_fact_flights_referential_time       All FK references valid
  [PASS] gold_load_factor_range                   Load factor between 0 and 1.5

  VERDICT: PASSED — All quality checks passed
================================================================================
```

## Key Design Decisions

1. **Idempotent pipeline**: Silver uses `INSERT OR REPLACE` (upsert on PK), Gold uses `INSERT ... WHERE NOT IN`. Running twice produces no duplicates.

2. **Schema-as-code**: Pydantic models define and enforce the contract between layers. Invalid records are rejected with structured error logs.

3. **Bad data injection**: The generator intentionally produces ~2% invalid records (empty origins, negative fares, unknown aircraft types) to demonstrate the quality gates.

4. **Batch lineage**: Every bronze record carries `_batch_id`, linking it through silver to the pipeline_runs table for full lineage tracking.

5. **Observability-first**: Every stage logs `rows_in`, `rows_out`, `rows_rejected`, `duration_ms` to both structured logs and a `pipeline_runs` DuckDB table.

## Production Deployment Notes

To deploy this on Azure/Microsoft Fabric:

| Pattern | Local | Production |
|---|---|---|
| Storage | Local JSON files | ADLS Gen2 (Parquet/Delta) |
| Compute | DuckDB | Synapse Serverless / Fabric Lakehouse |
| Orchestration | Python CLI | ADF Pipelines / Fabric Notebooks |
| Schema enforcement | Pydantic | Delta Lake schema evolution + contracts |
| Quality gates | Custom SQL checks | Great Expectations / Fabric Data Quality |
| Observability | structlog + DuckDB | Azure Monitor + Log Analytics |
| Scheduling | Manual / cron | ADF triggers / Fabric schedules |
| Secrets | N/A | Azure Key Vault |
| CI/CD | pytest | Azure DevOps / GitHub Actions |

### Scale Considerations

- **140M+ records/day**: Replace row-by-row inserts with batch `COPY` / Spark DataFrames
- **Late-arriving data**: Watermark-based incremental processing (already demonstrated in Gold SQL)
- **SCD Type 2**: dim_aircraft and dim_crew include `effective_from`/`effective_to`/`is_current` columns
- **Multi-source reconciliation**: Bronze preserves raw data for audit; silver applies business rules
