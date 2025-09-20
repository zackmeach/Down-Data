# Down Data

A PySide6 desktop application for exploring NFL statistics powered by the
[`nfl_data_py`](https://github.com/cooperdff/nfl_data_py) dataset. Data flows
through a lakehouse-style layout of Parquet files (raw → clean → modeled) with
DuckDB providing lightning-fast analytics directly against those files.

## Getting started

1. Create and activate a Python 3.10.x environment (3.10 only).
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
   python -m app.main  # using Python 3.10
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

Down Data organises every `nfl_data_py` export into a miniature lakehouse so the
desktop UI and supporting scripts all work with the same, reliable datasets.

### Storage layout

*The `DataRepository` class provides the storage backbone.* It initialises a
`var/data` directory with `raw`, `clean`, and `modeled` zones alongside a
`warehouse` folder for DuckDB caches. Within each zone, datasets are partitioned
by league and season (for example `raw/nfl/season=2023/weekly/`) and each write
is versioned with a timestamped Parquet filename plus a convenience `latest`
file for quick access.

### Lifecycle of a dataset

1. **Ingestion from `nfl_data_py`.** `DataPipeline.ingest_raw` looks up the
   appropriate loader (e.g. `import_weekly_data`) and downloads the selected
   season before persisting the result in the raw zone.
2. **Promotion to the clean layer.** `promote_to_clean` reloads the raw data,
   applies an arbitrary transformation callable, writes the output to the clean
   zone, and clears any dependent DuckDB caches so downstream queries see the
   update.
3. **Modeled datasets and caching.** `build_modeled_dataset` executes DuckDB SQL
   against the Parquet lake, stores the modeled table back to Parquet, and can
   optionally materialise the same query in DuckDB while invalidating related
   caches.

### DuckDB integration

The `DuckDBWarehouse` wrapper keeps a single DuckDB connection pointed at the
repository’s `warehouse.duckdb` file, registers Parquet views on demand, and
offers helper methods to run parameterised queries, materialise tables, and drop
stale objects when the pipeline invalidates caches.

### How the application uses the data

Both the GUI entry point and the CLI bootstrap script instantiate the
`DataRepository`, `DuckDBWarehouse`, and `DataPipeline` together so they share
the same on-disk assets. The PySide6 app passes the pipeline into the main
window for interactive exploration, while the `scripts/bootstrap_data.py`
utility drives the same pipeline to fetch, clean, and model seasons from the
command line.

## Development

Run the test suite to verify changes:

```bash
pytest
```
