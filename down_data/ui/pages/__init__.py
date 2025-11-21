"""UI pages for different application sections.

Imports are deferred so that non-Qt environments can still inspect the package
(e.g., during backend-only test discovery) without loading PySide6 upfront.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Callable, Dict

if TYPE_CHECKING:  # pragma: no cover
    from .base_page import SectionPage
    from .content_page import ContentPage
    from .player_detail_page import PlayerDetailPage
    from .player_search_page import PlayerSearchPage
    from .placeholder_page import PlaceholderPage

__all__ = [
    "SectionPage",
    "ContentPage",
    "PlayerSearchPage",
    "PlaceholderPage",
    "PlayerDetailPage",
]


_LAZY_IMPORTS: Dict[str, Callable[[], object]] = {
    "SectionPage": lambda: __import__("down_data.ui.pages.base_page", fromlist=["SectionPage"]).SectionPage,
    "ContentPage": lambda: __import__("down_data.ui.pages.content_page", fromlist=["ContentPage"]).ContentPage,
    "PlayerDetailPage": lambda: __import__(
        "down_data.ui.pages.player_detail_page", fromlist=["PlayerDetailPage"]
    ).PlayerDetailPage,
    "PlayerSearchPage": lambda: __import__(
        "down_data.ui.pages.player_search_page", fromlist=["PlayerSearchPage"]
    ).PlayerSearchPage,
    "PlaceholderPage": lambda: __import__(
        "down_data.ui.pages.placeholder_page", fromlist=["PlaceholderPage"]
    ).PlaceholderPage,
}


def __getattr__(name: str):
    try:
        loader = _LAZY_IMPORTS[name]
    except KeyError:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    return loader()
