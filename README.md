# Down Data

A PySide6 desktop application for exploring NFL statistics powered by the
[`nfl_data_py`](https://github.com/cooperdff/nfl_data_py) dataset. Data flows
through a lakehouse-style layout of Parquet files (raw → clean → modeled) with
DuckDB providing lightning-fast analytics directly against those files.

## Getting started

1. Create and activate a Python 3.10+ environment.
2. Install dependencies:

   ```bash
   pip install -e .[dev]
   ```

3. Prime the data lake with the provided helper script:

   ```bash
   python scripts/bootstrap_data.py 2023 --dataset weekly
   ```

   This downloads data with `nfl_data_py`, stores it in the `var/data` folder,
   and materialises any DuckDB caches requested on the command line.

4. Launch the desktop app:

   ```bash
   python -m app.main
   ```

   The interface always opens in a maximised 16:9 layout to stay consistent on
   different displays.

## Project layout

```
app/            # PySide6 UI code and application entry point
  ui/           # Navigation shell and individual pages
  main.py       # Entry point wiring the data and UI layers together
data/           # Data lake orchestration helpers
scripts/        # Utilities for fetching and transforming datasets
var/data/       # Created on demand; holds Parquet partitions and DuckDB cache
```

## Data architecture

* **Parquet storage** – Data is persisted to Parquet files partitioned by
  `league/season` for the `raw`, `clean`, and `modeled` stages. Versioned files
  are created automatically so you can roll back if necessary.
* **DuckDB caching** – DuckDB reads the Parquet files directly for ad-hoc
  exploration. When queries become heavy or frequently reused, you can
  materialise them as tables or views in `warehouse.duckdb`.
* **Cache invalidation** – After ETL or data recomputation, invalidate dependent
  DuckDB objects to avoid stale results. The helper classes provide convenience
  methods for this workflow.

## Development

Run the test suite to verify changes:

```bash
pytest
```
