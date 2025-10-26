"""
Special teams statistics and efficiency metrics.

Functions for calculating kicking, punting, and return performance metrics.
"""

import polars as pl
from typing import Optional


def calculate_field_goal_percentage(df: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate Field Goal Percentage by distance ranges.
    
    Metrics:
    - Overall FG%
    - FG% by distance range (0-29, 30-39, 40-49, 50+)
    
    Args:
        df: DataFrame with field goal data
    
    Returns:
        DataFrame with FG percentage metrics
    """
    expressions = []
    
    # Overall FG percentage
    if "field_goals_made" in df.columns and "field_goal_attempts" in df.columns:
        expressions.append(
            ((pl.col("field_goals_made") / pl.col("field_goal_attempts")) * 100)
            .alias("field_goal_percentage")
        )
    
    # By distance (if we have play-by-play data)
    if "kick_distance" in df.columns and "field_goal_result" in df.columns:
        expressions.extend([
            # Categorize by distance
            pl.when(pl.col("kick_distance") < 30)
            .then(pl.lit("0-29"))
            .when(pl.col("kick_distance") < 40)
            .then(pl.lit("30-39"))
            .when(pl.col("kick_distance") < 50)
            .then(pl.lit("40-49"))
            .otherwise(pl.lit("50+"))
            .alias("fg_distance_range")
        ])
    
    if expressions:
        return df.with_columns(expressions)
    return df


def calculate_punt_average(df: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate punting efficiency metrics.
    
    Metrics:
    - Gross punt average
    - Net punt average
    - Punt inside 20 percentage
    - Touchback percentage
    
    Args:
        df: DataFrame with punting data
    
    Returns:
        DataFrame with punting metrics
    """
    expressions = []
    
    # Gross average
    if "punt_yards" in df.columns and "punts" in df.columns:
        expressions.append(
            (pl.col("punt_yards") / pl.col("punts")).alias("gross_punt_average")
        )
    
    # Net average
    if "net_punt_yards" in df.columns and "punts" in df.columns:
        expressions.append(
            (pl.col("net_punt_yards") / pl.col("punts")).alias("net_punt_average")
        )
    
    # Inside 20 percentage
    if "punts_inside_20" in df.columns and "punts" in df.columns:
        expressions.append(
            ((pl.col("punts_inside_20") / pl.col("punts")) * 100).alias("punt_inside_20_pct")
        )
    
    # Touchback percentage
    if "punt_touchbacks" in df.columns and "punts" in df.columns:
        expressions.append(
            ((pl.col("punt_touchbacks") / pl.col("punts")) * 100).alias("punt_touchback_pct")
        )
    
    # Blocked punt rate
    if "punts_blocked" in df.columns and "punts" in df.columns:
        expressions.append(
            ((pl.col("punts_blocked") / pl.col("punts")) * 100).alias("punt_blocked_pct")
        )
    
    if expressions:
        return df.with_columns(expressions)
    return df


def calculate_kickoff_return_average(df: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate kickoff return efficiency metrics.
    
    Metrics:
    - Average yards per return
    - Return TD rate
    - Touchback avoidance (for kickers)
    
    Args:
        df: DataFrame with kickoff return data
    
    Returns:
        DataFrame with kickoff return metrics
    """
    expressions = []
    
    # Returner metrics
    if "kickoff_return_yards" in df.columns and "kickoff_returns" in df.columns:
        expressions.append(
            (pl.col("kickoff_return_yards") / pl.col("kickoff_returns"))
            .alias("avg_kickoff_return_yards")
        )
    
    if "kickoff_return_tds" in df.columns and "kickoff_returns" in df.columns:
        expressions.append(
            ((pl.col("kickoff_return_tds") / pl.col("kickoff_returns")) * 100)
            .alias("kickoff_return_td_rate")
        )
    
    # Kicker metrics (prevent returns)
    if "kickoff_touchbacks" in df.columns and "kickoffs" in df.columns:
        expressions.append(
            ((pl.col("kickoff_touchbacks") / pl.col("kickoffs")) * 100)
            .alias("kickoff_touchback_pct")
        )
    
    if "kickoff_out_of_bounds" in df.columns and "kickoffs" in df.columns:
        expressions.append(
            ((pl.col("kickoff_out_of_bounds") / pl.col("kickoffs")) * 100)
            .alias("kickoff_oob_pct")
        )
    
    if expressions:
        return df.with_columns(expressions)
    return df


def calculate_punt_return_average(df: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate punt return efficiency metrics.
    
    Metrics:
    - Average yards per return
    - Return TD rate
    - Fair catch rate
    
    Args:
        df: DataFrame with punt return data
    
    Returns:
        DataFrame with punt return metrics
    """
    expressions = []
    
    # Returner metrics
    if "punt_return_yards" in df.columns and "punt_returns" in df.columns:
        expressions.append(
            (pl.col("punt_return_yards") / pl.col("punt_returns"))
            .alias("avg_punt_return_yards")
        )
    
    if "punt_return_tds" in df.columns and "punt_returns" in df.columns:
        expressions.append(
            ((pl.col("punt_return_tds") / pl.col("punt_returns")) * 100)
            .alias("punt_return_td_rate")
        )
    
    if "punt_fair_catches" in df.columns and "punts_received" in df.columns:
        expressions.append(
            ((pl.col("punt_fair_catches") / pl.col("punts_received")) * 100)
            .alias("fair_catch_rate")
        )
    
    if expressions:
        return df.with_columns(expressions)
    return df


def calculate_extra_point_percentage(df: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate Extra Point efficiency.
    
    Args:
        df: DataFrame with extra point data
    
    Returns:
        DataFrame with XP metrics
    """
    if "extra_points_made" in df.columns and "extra_point_attempts" in df.columns:
        return df.with_columns([
            ((pl.col("extra_points_made") / pl.col("extra_point_attempts")) * 100)
            .alias("extra_point_percentage")
        ])
    return df


def calculate_coverage_unit_efficiency(df: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate special teams coverage unit efficiency.
    
    Measures how well coverage units limit return yards.
    
    Args:
        df: Play-by-play DataFrame with return data
    
    Returns:
        DataFrame with coverage efficiency metrics
    """
    expressions = []
    
    # Kickoff coverage
    if "kickoff_return_yards_allowed" in df.columns and "kickoffs" in df.columns:
        expressions.append(
            (pl.col("kickoff_return_yards_allowed") / pl.col("kickoffs"))
            .alias("avg_kickoff_return_allowed")
        )
    
    # Punt coverage
    if "punt_return_yards_allowed" in df.columns and "punts" in df.columns:
        expressions.append(
            (pl.col("punt_return_yards_allowed") / pl.col("punts"))
            .alias("avg_punt_return_allowed")
        )
    
    if expressions:
        return df.with_columns(expressions)
    return df


def calculate_special_teams_epa(df: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate Expected Points Added (EPA) for special teams plays.
    
    Requires play-by-play data with EPA calculations.
    
    Args:
        df: Play-by-play DataFrame with special teams EPA
    
    Returns:
        DataFrame with special teams EPA metrics
    """
    if "epa" in df.columns and "special" in df.columns:
        return df.filter(pl.col("special") == 1).with_columns([
            pl.col("epa").mean().alias("avg_st_epa"),
            pl.col("epa").sum().alias("total_st_epa")
        ])
    return df


def calculate_onside_kick_recovery_rate(df: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate onside kick recovery rate.
    
    Args:
        df: DataFrame with onside kick data
    
    Returns:
        DataFrame with onside kick metrics
    """
    if "onside_kicks_recovered" in df.columns and "onside_kick_attempts" in df.columns:
        return df.with_columns([
            ((pl.col("onside_kicks_recovered") / pl.col("onside_kick_attempts")) * 100)
            .alias("onside_kick_recovery_rate")
        ])
    return df

