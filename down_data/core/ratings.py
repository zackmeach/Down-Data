"""Shared rating dataclasses and helpers."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RatingBreakdown:
    """Represents a rating with optional nested subratings."""

    label: str
    current: int
    potential: int
    subratings: tuple["RatingBreakdown", ...] = ()



