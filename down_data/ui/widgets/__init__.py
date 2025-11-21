"""Reusable UI widgets with deferred imports.

The lazy import mechanism keeps heavy Qt dependencies from loading during
backend-only workflows while preserving direct attribute access for callers.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Callable, Dict

if TYPE_CHECKING:  # pragma: no cover
    from .grid_overlay import GridOverlay
    from .grid_layout import GridCell, GridLayoutManager
    from .grid_demo import GridDemoBox, create_demo_boxes
    from .nav_bar import NavBar
    from .context_bar import ContextBar
    from .menu_bar import MenuBar
    from .panel import Panel, FilterPanel, ContentPanel, DetailPanel
    from .player_detail_panels import PersonalDetailsWidget, BasicRatingsWidget
    from .range_selector import RangeSelector
    from .table_panel import TablePanel, create_stats_table, create_roster_table, create_results_table

__all__ = [
    "GridOverlay",
    "GridCell",
    "GridLayoutManager",
    "GridDemoBox",
    "create_demo_boxes",
    "NavBar",
    "ContextBar",
    "MenuBar",
    "Panel",
    "FilterPanel",
    "ContentPanel",
    "DetailPanel",
    "PersonalDetailsWidget",
    "BasicRatingsWidget",
    "RangeSelector",
    "TablePanel",
    "create_stats_table",
    "create_roster_table",
    "create_results_table",
]


_LAZY_IMPORTS: Dict[str, Callable[[], object]] = {
    "GridOverlay": lambda: __import__("down_data.ui.widgets.grid_overlay", fromlist=["GridOverlay"]).GridOverlay,
    "GridCell": lambda: __import__("down_data.ui.widgets.grid_layout", fromlist=["GridCell"]).GridCell,
    "GridLayoutManager": lambda: __import__(
        "down_data.ui.widgets.grid_layout", fromlist=["GridLayoutManager"]
    ).GridLayoutManager,
    "GridDemoBox": lambda: __import__("down_data.ui.widgets.grid_demo", fromlist=["GridDemoBox"]).GridDemoBox,
    "create_demo_boxes": lambda: __import__(
        "down_data.ui.widgets.grid_demo", fromlist=["create_demo_boxes"]
    ).create_demo_boxes,
    "NavBar": lambda: __import__("down_data.ui.widgets.nav_bar", fromlist=["NavBar"]).NavBar,
    "ContextBar": lambda: __import__("down_data.ui.widgets.context_bar", fromlist=["ContextBar"]).ContextBar,
    "MenuBar": lambda: __import__("down_data.ui.widgets.menu_bar", fromlist=["MenuBar"]).MenuBar,
    "Panel": lambda: __import__("down_data.ui.widgets.panel", fromlist=["Panel"]).Panel,
    "FilterPanel": lambda: __import__("down_data.ui.widgets.panel", fromlist=["FilterPanel"]).FilterPanel,
    "ContentPanel": lambda: __import__("down_data.ui.widgets.panel", fromlist=["ContentPanel"]).ContentPanel,
    "DetailPanel": lambda: __import__("down_data.ui.widgets.panel", fromlist=["DetailPanel"]).DetailPanel,
    "PersonalDetailsWidget": lambda: __import__(
        "down_data.ui.widgets.player_detail_panels", fromlist=["PersonalDetailsWidget"]
    ).PersonalDetailsWidget,
    "BasicRatingsWidget": lambda: __import__(
        "down_data.ui.widgets.player_detail_panels", fromlist=["BasicRatingsWidget"]
    ).BasicRatingsWidget,
    "RangeSelector": lambda: __import__("down_data.ui.widgets.range_selector", fromlist=["RangeSelector"]).RangeSelector,
    "TablePanel": lambda: __import__("down_data.ui.widgets.table_panel", fromlist=["TablePanel"]).TablePanel,
    "create_stats_table": lambda: __import__(
        "down_data.ui.widgets.table_panel", fromlist=["create_stats_table"]
    ).create_stats_table,
    "create_roster_table": lambda: __import__(
        "down_data.ui.widgets.table_panel", fromlist=["create_roster_table"]
    ).create_roster_table,
    "create_results_table": lambda: __import__(
        "down_data.ui.widgets.table_panel", fromlist=["create_results_table"]
    ).create_results_table,
}


def __getattr__(name: str):
    try:
        loader = _LAZY_IMPORTS[name]
    except KeyError:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    return loader()
