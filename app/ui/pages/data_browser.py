"""Page for exploring tabular datasets."""

from __future__ import annotations

from typing import Optional

import pandas as pd
from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt
from PySide6.QtWidgets import QLabel, QTableView, QVBoxLayout

from .base import BasePage


class _DataFrameModel(QAbstractTableModel):
    def __init__(self, df: Optional[pd.DataFrame] = None) -> None:
        super().__init__()
        self._df = df if df is not None else pd.DataFrame()

    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:  # type: ignore[override]
        return 0 if parent.isValid() else len(self._df.index)

    def columnCount(self, parent: QModelIndex = QModelIndex()) -> int:  # type: ignore[override]
        return 0 if parent.isValid() else len(self._df.columns)

    def data(self, index: QModelIndex, role: int = Qt.DisplayRole):  # type: ignore[override]
        if not index.isValid() or role not in (Qt.DisplayRole, Qt.EditRole):
            return None
        value = self._df.iat[index.row(), index.column()]
        return "" if pd.isna(value) else str(value)

    def headerData(  # type: ignore[override]
        self, section: int, orientation: Qt.Orientation, role: int = Qt.DisplayRole
    ):
        if role != Qt.DisplayRole:
            return None
        if orientation == Qt.Horizontal:
            return str(self._df.columns[section])
        return str(self._df.index[section])

    def update(self, df: pd.DataFrame) -> None:
        self.beginResetModel()
        self._df = df
        self.endResetModel()


class DataBrowserPage(BasePage):
    title = "Data Browser"
    description = "Inspect league datasets and understand the available fields."

    def __init__(self, *, parent=None) -> None:
        super().__init__(parent=parent)
        self._table_model = _DataFrameModel()

    def _init_layout(self) -> None:
        layout = QVBoxLayout(self)
        header = QLabel(self.title)
        header.setObjectName("pageHeader")
        summary = QLabel(self.description)
        summary.setWordWrap(True)
        self._table_view = QTableView()
        self._table_view.setModel(self._table_model)
        self._table_view.horizontalHeader().setStretchLastSection(True)

        layout.addWidget(header)
        layout.addWidget(summary)
        layout.addWidget(self._table_view, stretch=1)

    def update_data(self, df: pd.DataFrame) -> None:
        self._table_model.update(df)


__all__ = ["DataBrowserPage"]
