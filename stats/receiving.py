"""
Receiving statistics and efficiency metrics.

Functions for calculating wide receiver and tight end performance metrics including
route efficiency, target metrics, and contested catch statistics.
"""

import polars as pl
from typing import Optional


def calculate_yards_per_route_run(df: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate Yards per Route Run (YPRR).
    
    YPRR = Receiving Yards / Routes Run
    
    Considered one of the best measures of receiver efficiency.
    Requires PFF data with route tracking.
    
    Args:
        df: DataFrame with columns: receiving_yards, routes_run
    
    Returns:
        DataFrame with added column: yards_per_route_run
    """
    if "routes_run" in df.columns and "receiving_yards" in df.columns:
        return df.with_columns([
            (pl.col("receiving_yards") / pl.col("routes_run")).alias("yards_per_route_run")
        ])
    return df


def calculate_target_share(df: pl.DataFrame, team_targets_col: str = "team_targets") -> pl.DataFrame:
    """
    Calculate Target Share.
    
    Target Share = Player Targets / Team Total Targets
    
    Measures usage in passing offense.
    
    Args:
        df: DataFrame with player targets and team targets
        team_targets_col: Column name for team's total targets
    
    Returns:
        DataFrame with target share metrics
    """
    expressions = []
    
    if "targets" in df.columns and team_targets_col in df.columns:
        expressions.append(
            ((pl.col("targets") / pl.col(team_targets_col)) * 100).alias("target_share_pct")
        )
    
    # Air Yards Share (if available)
    if "air_yards" in df.columns and "team_air_yards" in df.columns:
        expressions.append(
            ((pl.col("air_yards") / pl.col("team_air_yards")) * 100).alias("air_yards_share_pct")
        )
    
    if expressions:
        return df.with_columns(expressions)
    return df


def calculate_catch_rate_above_expectation(df: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate Catch Rate Above Expectation.
    
    CRAE = Actual Catch Rate - Expected Catch Rate
    
    Requires NextGen Stats or PFF data with expected catch rate.
    
    Args:
        df: DataFrame with catch data and expected catch rate
    
    Returns:
        DataFrame with catch rate metrics
    """
    expressions = []
    
    # Basic catch rate
    if "receptions" in df.columns and "targets" in df.columns:
        expressions.append(
            ((pl.col("receptions") / pl.col("targets")) * 100).alias("catch_rate")
        )
    
    # Catch Rate Above Expectation
    if all(col in df.columns for col in ["receptions", "targets", "expected_catch_rate"]):
        actual_rate = (pl.col("receptions") / pl.col("targets")) * 100
        expressions.append(
            (actual_rate - pl.col("expected_catch_rate")).alias("catch_rate_above_expectation")
        )
    
    if expressions:
        return df.with_columns(expressions)
    return df


def calculate_yards_after_catch_efficiency(df: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate Yards After Catch (YAC) efficiency metrics.
    
    Requires data with YAC tracking from PFR, NextGen, or PFF.
    
    Metrics:
    - YAC per reception
    - YAC percentage of total yards
    - YAC above expected (if available)
    
    Args:
        df: DataFrame with YAC data
    
    Returns:
        DataFrame with YAC efficiency metrics
    """
    expressions = []
    
    # YAC per reception
    if "yards_after_catch" in df.columns and "receptions" in df.columns:
        expressions.append(
            (pl.col("yards_after_catch") / pl.col("receptions")).alias("yac_per_reception")
        )
    
    # YAC percentage
    if "yards_after_catch" in df.columns and "receiving_yards" in df.columns:
        expressions.append(
            ((pl.col("yards_after_catch") / pl.col("receiving_yards")) * 100).alias("yac_percentage")
        )
    
    # YAC Above Expected (NextGen)
    if all(col in df.columns for col in ["avg_yac", "avg_expected_yac"]):
        expressions.append(
            (pl.col("avg_yac") - pl.col("avg_expected_yac")).alias("yac_above_expected")
        )
    
    # Air Yards metrics
    if "air_yards" in df.columns and "targets" in df.columns:
        expressions.append(
            (pl.col("air_yards") / pl.col("targets")).alias("avg_depth_of_target")
        )
    
    if expressions:
        return df.with_columns(expressions)
    return df


def calculate_contested_catch_rate(df: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate Contested Catch Rate.
    
    Contested Catch Rate = Contested Catches / Contested Targets
    
    Requires PFF data with contested target tracking.
    
    Args:
        df: DataFrame with contested catch data
    
    Returns:
        DataFrame with contested catch metrics
    """
    if "contested_catches" in df.columns and "contested_targets" in df.columns:
        return df.with_columns([
            ((pl.col("contested_catches") / pl.col("contested_targets")) * 100)
            .alias("contested_catch_rate")
        ])
    return df


def calculate_separation_metrics(df: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate receiver separation metrics.
    
    Requires NextGen Stats data with separation tracking.
    
    Metrics:
    - Average separation at target
    - Average cushion at snap
    - Separation score
    
    Args:
        df: DataFrame with NextGen separation data
    
    Returns:
        DataFrame with separation metrics
    """
    expressions = []
    
    if "avg_separation" in df.columns:
        expressions.extend([
            pl.col("avg_separation").alias("avg_separation_yards"),
            pl.when(pl.col("avg_separation") >= 3.0)
            .then(pl.lit("Wide Open"))
            .when(pl.col("avg_separation") >= 2.0)
            .then(pl.lit("Open"))
            .otherwise(pl.lit("Tight Coverage"))
            .alias("separation_category")
        ])
    
    if "avg_cushion" in df.columns:
        expressions.append(
            pl.col("avg_cushion").alias("avg_cushion_yards")
        )
    
    if expressions:
        return df.with_columns(expressions)
    return df


def calculate_drop_rate(df: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate Drop Rate.
    
    Drop Rate = Drops / Catchable Targets
    
    Requires PFF or manual charting data with drop tracking.
    
    Args:
        df: DataFrame with drop data
    
    Returns:
        DataFrame with drop rate metrics
    """
    expressions = []
    
    if "drops" in df.columns and "catchable_targets" in df.columns:
        expressions.append(
            ((pl.col("drops") / pl.col("catchable_targets")) * 100).alias("drop_rate")
        )
    elif "drops" in df.columns and "targets" in df.columns:
        # Less accurate but better than nothing
        expressions.append(
            ((pl.col("drops") / pl.col("targets")) * 100).alias("drop_rate_estimate")
        )
    
    if expressions:
        return df.with_columns(expressions)
    return df


def calculate_target_quality(df: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate target quality metrics.
    
    Measures the quality of targets a receiver gets based on factors like
    air yards, expected completion rate, and coverage.
    
    Args:
        df: DataFrame with target quality data
    
    Returns:
        DataFrame with target quality metrics
    """
    expressions = []
    
    # Average target air yards
    if "intended_air_yards" in df.columns and "targets" in df.columns:
        expressions.append(
            (pl.col("intended_air_yards") / pl.col("targets")).alias("avg_target_depth")
        )
    
    # Expected catch rate (if available)
    if "expected_completions" in df.columns and "targets" in df.columns:
        expressions.append(
            ((pl.col("expected_completions") / pl.col("targets")) * 100)
            .alias("avg_expected_catch_rate")
        )
    
    # Target quality score (composite)
    if all(col in df.columns for col in ["avg_separation", "avg_cushion", "intended_air_yards"]):
        expressions.append(
            (
                pl.col("avg_separation") * 2 +  # Separation is important
                pl.col("avg_cushion") +
                (pl.col("intended_air_yards") / pl.col("targets"))
            ).alias("target_quality_score")
        )
    
    if expressions:
        return df.with_columns(expressions)
    return df


def calculate_receiving_epa(df: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate Expected Points Added (EPA) metrics for receivers.
    
    Requires play-by-play data with EPA calculations.
    
    Args:
        df: Play-by-play DataFrame with EPA columns
    
    Returns:
        DataFrame with receiving EPA metrics
    """
    expressions = []
    
    if "epa" in df.columns:
        expressions.extend([
            pl.col("epa").mean().alias("avg_epa_per_target"),
            pl.col("epa").sum().alias("total_receiving_epa")
        ])
    
    if "xyac_epa" in df.columns:  # Expected YAC EPA
        expressions.append(
            pl.col("xyac_epa").mean().alias("avg_xyac_epa")
        )
    
    if expressions:
        return df.with_columns(expressions)
    return df


def calculate_yards_per_reception(df: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate Yards per Reception (YPR).
    
    YPR = Receiving Yards / Receptions
    
    Args:
        df: DataFrame with columns: receiving_yards, receptions
    
    Returns:
        DataFrame with added column: yards_per_reception
    """
    if "receiving_yards" in df.columns and "receptions" in df.columns:
        return df.with_columns([
            (pl.col("receiving_yards") / pl.col("receptions")).alias("yards_per_reception")
        ])
    return df

