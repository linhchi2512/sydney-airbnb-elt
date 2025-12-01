## Quick orientation for AI coding agents

This is a dbt-based analytics project (see `dbt_project.yml`) that builds a bronze → silver → gold pipeline. Use the examples in `Models/models/` when modifying or adding models.

Key facts an agent should know (short and actionable):

- Project layout
  - `dbt_project.yml` — central config: materializations and schema mapping are set per folder (bronze/silver/gold). Avoid changing these unless asked.
  - Models live under `Models/models/` with subfolders: `bronze/`, `silver/`, `gold/star/`, `gold/mart/`.
  - Snapshots are in `Snapshots/` (e.g. `host_snapshot.sql`, `property_snapshot.sql`).
  - Orchestration/orchestration artifacts may exist outside dbt (e.g. `Part_3_dag.py`) — inspect before editing.

- dbt conventions and patterns in this repo
  - Models use Jinja `config(...)` at the top of SQL files to set `unique_key`, `alias`, and other options. Example: `Models/models/silver/s_listings.sql` uses:
    - `config(unique_key='id', alias='s_listings')` — the `alias` controls the final table/view name.
  - Models reference upstream artifacts via `{{ ref('...') }}` (e.g. `ref('b_listings')`, `ref('s_listings')`). Keep `ref()` calls intact when renaming models.
  - Data cleaning is done inline using casts and NaN handling. Example: `s_listings.sql` converts `'NaN'` strings to NULL and uses `::numeric` / `::int` style casts.
  - Gold-layer star schemas use `unique_key` and `alias` similarly (see `Models/models/gold/star/g_fact.sql`).

- Materialization and schema rules (from `dbt_project.yml`)
  - bronze: materialized as table under schema `bronze`
  - silver: materialized as table under schema `silver`
  - gold.star: materialized as table under schema `gold`
  - gold.mart: materialized as view under schema `gold`
  - snapshots target schema `silver`

- What to modify and where
  - Add new raw ingestion models in `Models/models/bronze/`.
  - Add cleaned models in `Models/models/silver/` and ensure types/casts follow existing patterns (look for `case when 'NaN'` patterns).
  - Add marts/views in `Models/models/gold/mart/` and star tables in `Models/models/gold/star/`.
  - When introducing a new model, include a `config(...)` header with `unique_key` when appropriate and set an `alias` if the filename should differ from the compiled relation name.

- Running, testing, and debugging
  - The project uses dbt standard flow. Common commands to run locally (assumes a valid profile named `default` as in `dbt_project.yml`):
    - `dbt debug` — validate profile/connection
    - `dbt run --models <model_name>` — build models (use `--models path.to.model` or tags)
    - `dbt test` — run tests (this repo declares `test-paths` in `dbt_project.yml` even if tests are not present)
    - `dbt snapshot` — materialize snapshots in `Snapshots/`
  - Before changing SQL that casts or parses dates, run `dbt run` for the affected model and inspect compiled SQL in `target/` to confirm correctness.

- Integration points & caution
  - `Part_3_dag.py` likely orchestrates runs — do not hardcode credentials or environment-specific changes; prefer adding configurable params.
  - SQL uses `::` cast syntax and `to_date(..., 'YYYY-MM-DD')` styles — these are dialect-specific (Snowflake/Postgres-style). Preserve dialect-specific patterns.
  - Aliases in `config(alias=...)` determine compiled object names; renaming a file without updating `alias` (or vice versa) can break `ref()` calls elsewhere.

- Examples to cite when editing or creating models
  - `Models/models/silver/s_listings.sql` — shows NaN handling, casting, and `config(unique_key='id', alias='s_listings')`.
  - `Models/models/gold/star/g_fact.sql` — simple fact model that references `s_listings` and demonstrates `alias` + `unique_key` usage.

If anything above is unclear or you want the file to include more examples (compiled SQL, snapshot examples, or recommended dbt flags), tell me which sections to expand and I will iterate.
