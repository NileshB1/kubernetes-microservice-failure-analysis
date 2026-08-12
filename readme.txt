This file explains how to re-run the code and reproduce the results.
A longer, formatted version is in README.md; this plain-text file is the
authoritative set of instructions for the submitted archive.



1. WHAT THIS PROJECT DOES

It analyses real Kubernetes microservice telemetry to answer three
research questions:

  RQ1  How can distributed analysis identify cross-service failure
       propagation?
  RQ2  How effectively can distributed processing identify abnormal
       failure patterns?
  RQ3  How does distributed Spark processing scale with data volume?

The pipeline: source dataset -> MinIO blob storage -> Apache Spark
-> PostgreSQL -> Streamlit dashboard.



2. THE DATASET

Source : Nezha - multi-modal microservice telemetry captured from the
           Online Boutique and Train Ticket benchmarks running on
           Kubernetes, published with the FSE 2023 paper.
Location : https://github.com/IntelligentDDS/Nezha
Retrieval: programmatic, over the GitHub REST API, by
           modules/dataset_acquisition.py. Nothing is web-scraped and no
           data files are committed to this archive.

The dataset is NOT bundled with this submission because of its size.
The acquisition step downloads it. It has two parts, and the project
uses both:

  construct_data  fault-free baseline capture
  rca_data        fault-injection windows, with a ground-truth list of
                  which pod had which fault injected at what time

Volume actually used (capture date 2022-08-22, traces + metrics):
  1,598,095 source rows  ->  1,596,884 canonical spans
  10 microservices, 24 ground-truth fault injections, 5 fault types

This is far above the 10,000-record minimum.

IMPORTANT - how "failure" is defined for this data. The Nezha authors
deliberately tuned the injected faults to degrade performance WITHOUT
producing trivially detectable errors, so almost every HTTP response is
a 200. A span is therefore treated as failed when its duration exceeds
the p99 for that same operation measured in the fault-free baseline.
That threshold is learned only from construct_data and never reads the
fault labels, so evaluating detections against those labels is not
circular. This is documented in modules/source_adapter.py.



3. QUICKEST WAY TO SEE IT WORKING

No Docker, no database, no Spark. Seeds a local SQLite database with
sample data and opens the dashboard:

    pip install -r requirements.txt
    python run_streamlit.py --local --browser

Dashboard: http://localhost:8501

Note: this demo mode uses generated sample data so that the interface
can be inspected offline. It is NOT the analysis path. The real
analysis is section 4.



4. FULL PIPELINE ON THE REAL DATASET (the actual results)

Prerequisites: Docker and Docker Compose v2, ~8 GB RAM, ~10 GB disk.

  Step 1  Configure secrets (one time)

            cp .env.example .env

          Then edit .env and set these two values to anything you like:
            POSTGRES_PASSWORD=<choose a password>
            MINIO_SECRET_KEY=<choose a secret>

          Compose deliberately refuses to start without them rather
          than falling back to a well-known default.

  Step 2  Run everything

            docker compose up --build

          This starts MinIO, the Spark master and workers, PostgreSQL,
          and then runs the whole pipeline in one process:

            1. Ingestion  acquires the real dataset over the
                                 GitHub API, uploads the untouched
                           source tree to MinIO, then runs a Spark
                            job that reads it back OUT of blob
                                storage and writes the canonical schema
            2. Preprocessing  Spark cleaning, joins, feature engineering
            3. Cross-service  RQ1 propagation analysis
            4. Failure detection RQ2 anomaly detection
            5. Scalability  RQ3 benchmarks
            6. Visualization  static plots into output/
            7. Spark SQL  declarative analysis + ground-truth  evaluation

          Every result is written directly to PostgreSQL by Spark.

  Step 3  Open the dashboard

            http://localhost:8501

          Six pages: Overview, RQ1, RQ2, RQ3, Service explorer,
          Operations.

Other endpoints (all bound to localhost only):
    Spark master UI  http://localhost:8080
    MinIO console http://localhost:9001
    PostgreSQL localhost:5432



5. RUNNING INDIVIDUAL STEPS

Acquire and validate the dataset only:

    python -m modules.dataset_acquisition --data-dir ./data --dates 2022-08-22
    python -m modules.dataset_acquisition --validate-only --data-dir ./data

Run one pipeline module inside the stack:

    docker compose run --rm pipeline python -m modules.ingestion
    docker compose run --rm pipeline python -m modules.preprocessing
    docker compose run --rm pipeline python -m modules.cross_service_analysis
    docker compose run --rm pipeline python -m modules.failure_detection
    docker compose run --rm pipeline python -m modules.scalability_analysis
    docker compose run --rm pipeline python -m modules.visualization
    docker compose run --rm pipeline python -m modules.spark_sql_analysis

Offline fallback (generates telemetry instead of acquiring it - for
tests and demos only, not for results):

    python -m modules.ingestion --synthetic


6. TESTS

Fast tier - no Java, Docker, or database required:

    bash scripts/run_tests.sh
    (or: make test)

Full tier - includes Spark tests, needs a local JVM:

    python -m pytest tests/
    (or: make test-all)

Full tier inside Docker:

    make test-docker

Lint and formatting (what CI enforces):

    make lint



7. TWO DATA PROCESSING LANGUAGES

The project uses both of Spark's processing languages, chosen per task:

  PySpark DataFrame API   modules/preprocessing.py, cross_service_analysis.py,
                          failure_detection.py, scalability_analysis.py,
                          source_adapter.py
                          - used where logic needs control flow,
                            parameters, and reuse

  Spark SQL   sql/analysis_queries.sql, executed by
            modules/spark_sql_analysis.py
            - used for set-oriented questions: grouping, ranking, window functions, percentiles

Both compile to the same Catalyst plans and run on the same cluster.



8. CODE LAYOUT

modules/
  settings.py   typed configuration and validation
  dataset_acquisition.py    programmatic dataset retrieval (GitHub API)
  source_adapter.py  Spark: real source -> canonical schema
  ingestion.py   module 1 - blob upload and normalisation
  preprocessing.py  module 2 - Spark ETL
  cross_service_analysis.py module 3 -> RQ1
  failure_detection.py  module 4 - RQ2
  scalability_analysis.py  module 5 - RQ3
  visualization.py  module 6 - plots
  spark_sql_analysis.py   module 7 - Spark SQL layer
  db_adapter.py   PostgreSQL / SQLite access
  dashboard.py + ui/     Streamlit dashboard

sql/
  init.sql   PostgreSQL schema
  analysis_queries.sql  Spark SQL analysis statements

scripts/
  run_pipeline.sh  runs the whole pipeline in one flow
  run_tests.sh   fast test tier
  run_spark_tests.sh  full test tier

config/config.yaml   analysis parameters (thresholds, windows)
tests/   test suite
docs/ARCHITECTURE.md   data flow, schema, RQ mapping



9. TROUBLESHOOTING

"POSTGRES_PASSWORD ... is required" when starting Compose
    You have not created .env yet. See section 4, step 1.

Dashboard shows "Database down"
    PostgreSQL is not running: docker compose up -d postgres
    Or use the offline demo: python run_streamlit.py --local

Every dashboard page says "No processed telemetry yet"
    The pipeline has not run. See section 4, step 2.

Acquisition fails with a network error
    GitHub rate-limits unauthenticated requests. Wait a few minutes and
    re-run; already-downloaded files are kept and skipped.

Spark fails on Windows with UnsatisfiedLinkError / NativeIO
    Hadoop's native libraries are not installed on Windows. The Docker
    path (section 4) is unaffected. The code avoids Hadoop globbing on
    local filesystems for this reason.

Out of memory during the Spark stage
    Lower SPARK_WORKER_MEMORY in .env, or reduce data_sizes in
    config/config.yaml.



10. NOTES
- No web scraping is used. The dataset is retrieved through a public
  API from a repository that publishes it for research use.
- Results are written to PostgreSQL, never to CSV files. The dashboard
  offers CSV export for the reader's convenience only.
- .env is not included in this archive and is not tracked in version
  control; .env.example is the template.
