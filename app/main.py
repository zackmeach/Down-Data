"""Application entry point."""

from __future__ import annotations

import sys

from data import DataPipeline, DataRepository, DuckDBWarehouse

from .application import create_app
from .ui import MainWindow


def main() -> int:
    app = create_app()
    repository = DataRepository()
    warehouse = DuckDBWarehouse(repository)
    pipeline = DataPipeline(repository, warehouse)

    window = MainWindow(pipeline)
    window.show()

    def _cleanup() -> None:
        warehouse.close()

    app.aboutToQuit.connect(_cleanup)
    return app.exec()


if __name__ == "__main__":
    sys.exit(main())
