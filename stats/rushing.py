"""
Rushing statistics and efficiency metrics.

Functions for calculating running back performance metrics including traditional
yards per carry, advanced efficiency metrics, and contact-adjusted statistics.
"""

import polars as pl
from typing import Optional


def calculate_yards_per_carry(df: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate Yards per Carry (YPC).
    
    YPC = Rushing Yards / Carries
    
    Args:
        df: DataFrame with columns: rushing_yards, carries
    
    Returns:
        DataFrame with added column: yards_per_carry
    """
    return df.with_columns([
        (pl.col("rushing_yards") / pl.col("carries")).alias("yards_per_carry")
    ])


def calculate_broken_tackle_rate(df: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate Broken Tackle Rate.
    
    Broken Tackle Rate = Broken Tackles / Carries
    
    Requires PFF data with broken tackle tracking.
    
    Args:
        df: DataFrame with columns: broken_tackles, carries
    
    Returns:
        DataFrame with added column: broken_tackle_rate
    """
    if "broken_tackles" in df.columns and "carries" in df.columns:
        return df.with_columns([
            (pl.col("broken_tackles") / pl.col("carries")).alias("broken_tackle_rate")
        ])
    return df


def calculate_yards_after_contact_rate(df: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate Yards After Contact metrics.
    
    Requires PFR Advanced or PFF data with yards after contact tracking.
    
    Metrics:
    - Yards after contact per carry
    - Percentage of yards after contact
    - Yards before contact per carry
    
    Args:
        df: DataFrame with yards after contact data
    
    Returns:
        DataFrame with YAC metrics
    """
    expressions = []
    
    if "yards_after_contact" in df.columns and "carries" in df.columns:
        expressions.append(
            (pl.col("yards_after_contact") / pl.col("carries")).alias("yac_per_carry")
        )
    
    if "yards_after_contact" in df.columns and "rushing_yards" in df.columns:
        expressions.append(
            ((pl.col("yards_after_contact") / pl.col("rushing_yards")) * 100).alias("yac_percentage")
        )
    
    if "yards_before_contact" in df.columns and "carries" in df.columns:
        expressions.append(
            (pl.col("yards_before_contact") / pl.col("carries")).alias("yards_before_contact_per_carry")
        )
    
    # True YPC: accounts for blocking quality
    if "yards_before_contact" in df.columns and "yards_after_contact" in df.columns:
        expressions.append(
            (
                pl.col("yards_after_contact") / (pl.col("yards_before_contact") + 1)
            ).alias("true_ypc_ratio")
        )
    
    if expressions:
        return df.with_columns(expressions)
    return df


def calculate_rushing_efficiency(df: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate rushing efficiency metrics.
    
    Requires NextGen Stats data with rush yards over expected.
    
    Metrics:
    - Yards over expected per carry
    - Rush percentage over expected
    - Efficiency rating
    
    Args:
        df: DataFrame with NextGen rushing data
    
    Returns:
        DataFrame with efficiency metrics
    """
    expressions = []
    
    if "rush_yards_over_expected" in df.columns and "carries" in df.columns:
        expressions.append(
            (pl.col("rush_yards_over_expected") / pl.col("carries"))
            .alias("yards_over_expected_per_carry")
        )
    
    if "rush_pct_over_expected" in df.columns:
        expressions.append(
            pl.col("rush_pct_over_expected").alias("pct_over_expected")
        )
    
    # Composite efficiency score
    if "rush_yards_over_expected" in df.columns and "broken_tackles" in df.columns:
        expressions.append(
            (
                pl.col("rush_yards_over_expected") + (pl.col("broken_tackles") * 5)
            ).alias("composite_efficiency_score")
        )
    
    if expressions:
        return df.with_columns(expressions)
    return df


def calculate_success_rate(df: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate rushing success rate.
    
    Success criteria (standard definition):
    - 1st down: 40%+ of yards needed
    - 2nd down: 60%+ of yards needed
    - 3rd/4th down: 100%+ of yards needed (conversion)
    
    Requires play-by-play data with down, distance, and yards gained.
    
    Args:
        df: Play-by-play DataFrame with down and distance
    
    Returns:
        DataFrame with success metrics
    """
    if all(col in df.columns for col in ["down", "ydstogo", "yards_gained"]):
        success = (
            pl.when(pl.col("down") == 1)
            .then(pl.col("yards_gained") >= (pl.col("ydstogo") * 0.4))
            .when(pl.col("down") == 2)
            .then(pl.col("yards_gained") >= (pl.col("ydstogo") * 0.6))
            .otherwise(pl.col("yards_gained") >= pl.col("ydstogo"))
        )
        
        return df.with_columns([
            success.alias("is_success"),
            success.cast(pl.Int8).alias("success_binary")
        ])
    return df


def calculate_explosive_play_rate(df: pl.DataFrame, threshold: int = 10) -> pl.DataFrame:
    """
    Calculate explosive play rate for rushers.
    
    Explosive play = Rush of 10+ yards (default threshold).
    
    Args:
        df: DataFrame with rushing yards per play
        threshold: Yards threshold for explosive play (default: 10)
    
    Returns:
        DataFrame with explosive play metrics
    """
    if "yards_gained" in df.columns:
        return df.with_columns([
            (pl.col("yards_gained") >= threshold).alias("is_explosive"),
            ((pl.col("yards_gained") >= threshold).cast(pl.Int8).sum() / pl.len()).alias("explosive_play_rate")
        ])
    elif "rushing_yards" in df.columns and "carries" in df.columns:
        # Game-level data - estimate explosive plays
        return df.with_columns([
            # Rough estimate: if YPC > threshold, likely had explosive plays
            (pl.col("rushing_yards") / pl.col("carries") > threshold).alias("had_explosive_runs")
        ])
    return df


def calculate_stuff_rate(df: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate stuff rate (tackles at or behind line of scrimmage).
    
    Stuff Rate = Runs for 0 or negative yards / Total Carries
    
    Args:
        df: Play-by-play DataFrame with yards gained
    
    Returns:
        DataFrame with stuff rate metrics
    """
    if "yards_gained" in df.columns:
        return df.with_columns([
            (pl.col("yards_gained") <= 0).alias("is_stuffed"),
            ((pl.col("yards_gained") <= 0).cast(pl.Int8).sum() / pl.len()).alias("stuff_rate")
        ])
    return df


def calculate_defenders_in_box_impact(df: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate rushing efficiency vs defenders in the box.
    
    Requires NextGen Stats data with defenders in box tracking.
    
    Args:
        df: DataFrame with NextGen defenders in box data
    
    Returns:
        DataFrame with box count impact metrics
    """
    expressions = []
    
    if "percent_attempts_gte_eight_defenders" in df.columns:
        expressions.append(
            pl.col("percent_attempts_gte_eight_defenders").alias("eight_plus_box_pct")
        )
    
    # Performance against stacked boxes
    if all(col in df.columns for col in ["eight_plus_box_yards", "eight_plus_box_attempts"]):
        expressions.append(
            (pl.col("eight_plus_box_yards") / pl.col("eight_plus_box_attempts"))
            .alias("ypc_vs_stacked_box")
        )
    
    if expressions:
        return df.with_columns(expressions)
    return df


def calculate_rushing_epa(df: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate Expected Points Added (EPA) metrics for rushers.
    
    Requires play-by-play data with EPA calculations.
    
    Args:
        df: Play-by-play DataFrame with EPA columns
    
    Returns:
        DataFrame with rushing EPA metrics
    """
    if "epa" in df.columns:
        return df.with_columns([
            pl.col("epa").mean().alias("avg_epa_per_rush"),
            pl.col("epa").sum().alias("total_rushing_epa")
        ])
    return df

