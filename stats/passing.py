"""
Passing statistics and efficiency metrics.

Functions for calculating quarterback performance metrics including traditional
passer rating, advanced efficiency metrics, and pressure-adjusted statistics.
"""

import polars as pl
from typing import Optional


def calculate_adjusted_yards_per_attempt(df: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate Adjusted Yards per Attempt (AY/A).
    
    AY/A = (Passing Yards + 20*TD - 45*INT) / Pass Attempts
    
    This metric weights touchdowns and interceptions to give a better picture
    of passing efficiency than raw yards per attempt.
    
    Args:
        df: DataFrame with columns: passing_yards, passing_tds, interceptions, attempts
    
    Returns:
        DataFrame with added column: adj_yards_per_attempt
    """
    return df.with_columns([
        (
            (pl.col("passing_yards") + 20 * pl.col("passing_tds") - 45 * pl.col("interceptions"))
            / pl.col("attempts")
        ).alias("adj_yards_per_attempt")
    ])


def calculate_passer_rating(df: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate NFL Passer Rating (0-158.3 scale).
    
    Uses the official NFL formula with four components:
    - Completion percentage
    - Yards per attempt
    - Touchdown percentage
    - Interception percentage
    
    Args:
        df: DataFrame with columns: attempts, completions, passing_yards, passing_tds, interceptions
    
    Returns:
        DataFrame with added column: passer_rating
    """
    # Helper function to clamp values between 0 and 2.375
    def clamp_component(expr):
        return pl.when(expr < 0).then(0).when(expr > 2.375).then(2.375).otherwise(expr)
    
    # Component A: Completion percentage
    comp_pct = ((pl.col("completions") / pl.col("attempts")) - 0.3) * 5
    a = clamp_component(comp_pct)
    
    # Component B: Yards per attempt
    ypa = ((pl.col("passing_yards") / pl.col("attempts")) - 3) * 0.25
    b = clamp_component(ypa)
    
    # Component C: Touchdown percentage
    td_pct = (pl.col("passing_tds") / pl.col("attempts")) * 20
    c = clamp_component(td_pct)
    
    # Component D: Interception percentage
    int_pct = 2.375 - ((pl.col("interceptions") / pl.col("attempts")) * 25)
    d = clamp_component(int_pct)
    
    # Final rating
    passer_rating = ((a + b + c + d) / 6) * 100
    
    return df.with_columns([
        passer_rating.alias("passer_rating")
    ])


def calculate_completion_percentage_above_expectation(
    df: pl.DataFrame,
    expected_comp_pct_col: str = "expected_completion_percentage"
) -> pl.DataFrame:
    """
    Calculate Completion Percentage Above Expectation (CPAE).
    
    CPAE = Actual Completion % - Expected Completion %
    
    This metric compares actual completion percentage against expected completion
    percentage based on factors like throw distance, receiver separation, etc.
    Requires NextGen Stats or PFF data for expected completion percentage.
    
    Args:
        df: DataFrame with columns: completions, attempts, and expected completion %
        expected_comp_pct_col: Name of column containing expected completion percentage
    
    Returns:
        DataFrame with added column: completion_pct_above_expectation
    """
    actual_comp_pct = (pl.col("completions") / pl.col("attempts")) * 100
    
    if expected_comp_pct_col in df.columns:
        cpae = actual_comp_pct - pl.col(expected_comp_pct_col)
    else:
        # If no expected completion % available, just calculate actual
        cpae = pl.lit(None)
    
    return df.with_columns([
        actual_comp_pct.alias("completion_percentage"),
        cpae.alias("completion_pct_above_expectation")
    ])


def calculate_air_yards_metrics(df: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate advanced air yards metrics.
    
    Metrics include:
    - Average Depth of Target (aDOT)
    - Air Yards per Attempt
    - Completed Air Yards per Completion
    - Yards After Catch per Completion
    - YAC percentage
    
    Requires PFR Advanced or NextGen Stats data.
    
    Args:
        df: DataFrame with air yards columns from PFR or NextGen
    
    Returns:
        DataFrame with added air yards metrics
    """
    expressions = []
    
    # Average Depth of Target
    if "intended_air_yards" in df.columns and "attempts" in df.columns:
        expressions.append(
            (pl.col("intended_air_yards") / pl.col("attempts")).alias("avg_depth_of_target")
        )
    
    # Completed Air Yards per Completion
    if "completed_air_yards" in df.columns and "completions" in df.columns:
        expressions.append(
            (pl.col("completed_air_yards") / pl.col("completions")).alias("completed_air_yards_per_comp")
        )
    
    # Yards After Catch metrics
    if "yards_after_catch" in df.columns and "completions" in df.columns:
        expressions.append(
            (pl.col("yards_after_catch") / pl.col("completions")).alias("yac_per_completion")
        )
    
    if "yards_after_catch" in df.columns and "passing_yards" in df.columns:
        expressions.append(
            ((pl.col("yards_after_catch") / pl.col("passing_yards")) * 100).alias("yac_percentage")
        )
    
    # Air Yards per Attempt
    if "intended_air_yards" in df.columns and "attempts" in df.columns:
        expressions.append(
            (pl.col("intended_air_yards") / pl.col("attempts")).alias("air_yards_per_attempt")
        )
    
    if expressions:
        return df.with_columns(expressions)
    return df


def calculate_pressure_adjusted_stats(
    df: pl.DataFrame,
    pressure_col: str = "times_pressured"
) -> pl.DataFrame:
    """
    Calculate passing stats split by pressure situations.
    
    Requires PFF or PFR advanced data with pressure tracking.
    
    Metrics:
    - Pressured completion percentage
    - Clean pocket completion percentage
    - Pressure rate
    - Yards per attempt under pressure vs clean
    
    Args:
        df: DataFrame with pressure data from PFF/PFR
        pressure_col: Column name for times pressured
    
    Returns:
        DataFrame with pressure-adjusted metrics
    """
    expressions = []
    
    if pressure_col in df.columns and "attempts" in df.columns:
        expressions.append(
            ((pl.col(pressure_col) / pl.col("attempts")) * 100).alias("pressure_rate")
        )
    
    # If we have pressured/clean splits
    if "completions_under_pressure" in df.columns and "attempts_under_pressure" in df.columns:
        expressions.extend([
            (
                (pl.col("completions_under_pressure") / pl.col("attempts_under_pressure")) * 100
            ).alias("pressured_completion_pct"),
            (
                pl.col("yards_under_pressure") / pl.col("attempts_under_pressure")
            ).alias("pressured_yards_per_attempt"),
        ])
    
    if "completions_clean_pocket" in df.columns and "attempts_clean_pocket" in df.columns:
        expressions.extend([
            (
                (pl.col("completions_clean_pocket") / pl.col("attempts_clean_pocket")) * 100
            ).alias("clean_pocket_completion_pct"),
            (
                pl.col("yards_clean_pocket") / pl.col("attempts_clean_pocket")
            ).alias("clean_pocket_yards_per_attempt"),
        ])
    
    if expressions:
        return df.with_columns(expressions)
    return df


def calculate_time_to_throw_efficiency(df: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate efficiency metrics based on time to throw.
    
    Requires NextGen Stats data with avg_time_to_throw column.
    
    Args:
        df: DataFrame with NextGen time to throw data
    
    Returns:
        DataFrame with time-based efficiency metrics
    """
    expressions = []
    
    if "avg_time_to_throw" in df.columns:
        # Categorize QBs as quick/average/slow release
        expressions.append(
            pl.when(pl.col("avg_time_to_throw") < 2.5)
            .then(pl.lit("Quick Release"))
            .when(pl.col("avg_time_to_throw") > 2.8)
            .then(pl.lit("Slow Release"))
            .otherwise(pl.lit("Average Release"))
            .alias("release_speed_category")
        )
    
    if "avg_time_to_throw" in df.columns and "passing_yards" in df.columns:
        # Yards per second in pocket
        expressions.append(
            (pl.col("passing_yards") / (pl.col("attempts") * pl.col("avg_time_to_throw")))
            .alias("yards_per_second_in_pocket")
        )
    
    if expressions:
        return df.with_columns(expressions)
    return df


def calculate_qb_epa(df: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate Expected Points Added (EPA) metrics for quarterbacks.
    
    Requires play-by-play data with EPA calculations.
    
    Args:
        df: Play-by-play DataFrame with EPA columns
    
    Returns:
        DataFrame with QB EPA metrics
    """
    expressions = []
    
    if "qb_epa" in df.columns:
        expressions.extend([
            pl.col("qb_epa").mean().alias("avg_epa_per_play"),
            pl.col("qb_epa").sum().alias("total_epa"),
        ])
    
    if "cpoe" in df.columns:  # Completion Percentage Over Expected
        expressions.append(
            pl.col("cpoe").mean().alias("avg_cpoe")
        )
    
    if expressions:
        return df.with_columns(expressions)
    return df

