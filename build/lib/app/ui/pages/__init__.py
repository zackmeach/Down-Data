"""Individual application pages used by the stacked navigation."""

from .base import BasePage
from .home import HomePage
from .data_browser import DataBrowserPage
from .player_watch import PlayerWatchPage

__all__ = ["BasePage", "HomePage", "DataBrowserPage", "PlayerWatchPage"]
