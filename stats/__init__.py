"""
Statistical calculation modules for NFL data analysis.

This package provides specialized statistical calculations organized by position group:
- passing: Quarterback and passing efficiency metrics
- rushing: Running back and rushing efficiency metrics  
- receiving: Wide receiver and tight end metrics
- specialteams: Kicking, punting, and return metrics

All functions accept Polars DataFrames and return Polars DataFrames with added columns.
"""

from stats.passing import (
    calculate_adjusted_yards_per_attempt,
    calculate_passer_rating,
    calculate_completion_percentage_above_expectation,
    calculate_air_yards_metrics,
    calculate_pressure_adjusted_stats,
)

from stats.rushing import (
    calculate_yards_per_carry,
    calculate_broken_tackle_rate,
    calculate_yards_after_contact_rate,
    calculate_rushing_efficiency,
    calculate_success_rate,
)

from stats.receiving import (
    calculate_yards_per_route_run,
    calculate_target_share,
    calculate_catch_rate_above_expectation,
    calculate_yards_after_catch_efficiency,
    calculate_contested_catch_rate,
)

from stats.specialteams import (
    calculate_field_goal_percentage,
    calculate_punt_average,
    calculate_kickoff_return_average,
    calculate_punt_return_average,
)

__all__ = [
    # Passing
    "calculate_adjusted_yards_per_attempt",
    "calculate_passer_rating",
    "calculate_completion_percentage_above_expectation",
    "calculate_air_yards_metrics",
    "calculate_pressure_adjusted_stats",
    # Rushing
    "calculate_yards_per_carry",
    "calculate_broken_tackle_rate",
    "calculate_yards_after_contact_rate",
    "calculate_rushing_efficiency",
    "calculate_success_rate",
    # Receiving
    "calculate_yards_per_route_run",
    "calculate_target_share",
    "calculate_catch_rate_above_expectation",
    "calculate_yards_after_catch_efficiency",
    "calculate_contested_catch_rate",
    # Special Teams
    "calculate_field_goal_percentage",
    "calculate_punt_average",
    "calculate_kickoff_return_average",
    "calculate_punt_return_average",
]

