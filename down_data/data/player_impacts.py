"""Pre-computed seasonal EPA/WPA lookup for every player-season.

This module builds a parquet cache containing the expensive EPA/WPA metrics that
are normally derived from play-by-play data. The cache spans every season from
1999 through 2024 (inclusive) and stores impact metrics for the key positional
groups used by the player detail UI:

* Quarterbacks   – qb_epa / qb_wpa
* Skill players  – skill_epa / skill_wpa plus explosive-play counts
* Defensive      – def_epa / def_wpa
* Offensive line – ol_epa / ol_wpa
* Kickers        – kicker_epa / kicker_wpa
* Punters        – punter_epa / punter_wpa

Once populated, the cache allows the UI to read these slow-to-compute metrics
instantly without touching the play-by-play source.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from pathlib import Path
import logging

import polars as pl

try:  # pragma: no cover - runtime dependency
    from nflreadpy import load_pbp
except ImportError:  # pragma: no cover - handled by callers
    load_pbp = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CACHE_DIRECTORY = PROJECT_ROOT / "data" / "cache"
CACHE_PATH = CACHE_DIRECTORY / "player_impacts_1999_2024.parquet"
IMPACT_SEASONS = list(range(1999, 2025))

IMPACT_SCHEMA: dict[str, pl.DataType] = {
    "player_id": pl.Utf8,
    "season": pl.Int16,
    "qb_epa": pl.Float64,
    "qb_wpa": pl.Float64,
    "skill_epa": pl.Float64,
    "skill_wpa": pl.Float64,
    "skill_rush_20_plus": pl.Int32,
    "skill_rec_20_plus": pl.Int32,
    "skill_rec_first_downs": pl.Int32,
    "def_epa": pl.Float64,
    "def_wpa": pl.Float64,
    "ol_epa": pl.Float64,
    "ol_wpa": pl.Float64,
    "kicker_epa": pl.Float64,
    "kicker_wpa": pl.Float64,
    "punter_epa": pl.Float64,
    "punter_wpa": pl.Float64,
}

_QB_PLAYER_COLUMNS = (
    "passer_player_id",
    "rusher_player_id",
    "lateral_rusher_player_id",
)
_SKILL_RUSHER_COLUMNS = (
    "rusher_player_id",
    "lateral_rusher_player_id",
)
_SKILL_RECEIVER_COLUMNS = (
    "receiver_player_id",
    "lateral_receiver_player_id",
    "target_player_id",
    "targeted_player_id",
)
_DEFENSIVE_COLUMNS = (
    "solo_tackle_1_player_id",
    "solo_tackle_2_player_id",
    "assist_tackle_1_player_id",
    "assist_tackle_2_player_id",
    "assist_tackle_3_player_id",
    "assist_tackle_4_player_id",
    "tackle_with_assist_1_player_id",
    "tackle_with_assist_2_player_id",
    "tackle_with_assist_3_player_id",
    "tackle_with_assist_4_player_id",
    "pass_defense_1_player_id",
    "pass_defense_2_player_id",
    "interception_player_id",
    "sack_player_id",
    "half_sack_1_player_id",
    "half_sack_2_player_id",
    "forced_fumble_player_1_player_id",
    "forced_fumble_player_2_player_id",
    "fumble_recovery_1_player_id",
    "fumble_recovery_2_player_id",
)
_OFFENSIVE_LINE_COLUMNS = (
    "penalty_player_id",
    "penalty_player_id_1",
    "penalty_player_id_2",
)
_KICKER_COLUMNS = (
    "kicker_player_id",
    "kickoff_player_id",
)
_PUNTER_COLUMNS = ("punter_player_id",)


def cache_exists() -> bool:
    """Return ``True`` when the impact parquet exists on disk."""

    return CACHE_PATH.exists()


def load_player_impacts() -> pl.DataFrame:
    """Load the cached player impacts into memory."""

    return pl.read_parquet(CACHE_PATH)


def scan_player_impacts() -> pl.LazyFrame:
    """Return a lazy scanner over the cached impacts."""

    return pl.scan_parquet(CACHE_PATH)


def build_player_impacts_cache(
    *,
    seasons: Iterable[int] | None = None,
    force_refresh: bool = False,
) -> pl.DataFrame:
    """Build (or rebuild) the EPA/WPA cache for every player-season."""

    if cache_exists() and not force_refresh:
        logger.info("Player impact cache already exists at %s; skipping rebuild.", CACHE_PATH)
        return load_player_impacts()

    if load_pbp is None:
        raise RuntimeError("nflreadpy is not available; install dependencies to build the impact cache.")

    target_seasons = sorted({int(season) for season in seasons} if seasons else IMPACT_SEASONS)
    logger.info(
        "Building player impact cache for seasons %s-%s.",
        target_seasons[0],
        target_seasons[-1],
    )

    aggregated_frames: list[pl.DataFrame] = []
    for season in target_seasons:
        try:
            raw = load_pbp(seasons=[season])
        except Exception as exc:  # pragma: no cover - network/runtime fetch
            logger.warning("Failed to load play-by-play for %s: %s", season, exc)
            continue

        frame = _to_polars(raw)
        if frame.is_empty():
            continue

        season_frame = _prepare_pbp_frame(frame, seasons=[season])
        if season_frame.is_empty():
            continue

        aggregated = aggregate_player_impacts(season_frame)
        if aggregated.height > 0:
            aggregated_frames.append(aggregated)
        logger.info("Aggregated impacts for %s (%s player-season rows).", season, aggregated.height)

    if aggregated_frames:
        combined = pl.concat(aggregated_frames, how="vertical_relaxed")
    else:
        combined = _empty_impact_frame()

    CACHE_DIRECTORY.mkdir(parents=True, exist_ok=True)
    combined.write_parquet(CACHE_PATH, compression="zstd")
    logger.info("Wrote player impact cache to %s", CACHE_PATH)
    return combined


def aggregate_player_impacts(frame: pl.DataFrame) -> pl.DataFrame:
    """Aggregate play-by-play rows into per-season impact metrics."""

    if frame.is_empty():
        return _empty_impact_frame()

    qb = _aggregate_qb_impacts(frame)
    skill = _aggregate_skill_impacts(frame)
    defense = _aggregate_role_impacts(frame, _DEFENSIVE_COLUMNS, prefix="def")
    offensive_line = _aggregate_role_impacts(frame, _OFFENSIVE_LINE_COLUMNS, prefix="ol")
    kicker = _aggregate_role_impacts(frame, _KICKER_COLUMNS, prefix="kicker")
    punter = _aggregate_role_impacts(frame, _PUNTER_COLUMNS, prefix="punter")

    frames = [qb, skill, defense, offensive_line, kicker, punter]
    merged = _merge_impact_frames(frames)
    return merged


def _empty_impact_frame() -> pl.DataFrame:
    return pl.DataFrame(schema=IMPACT_SCHEMA)


def _to_polars(frame: object) -> pl.DataFrame:
    if isinstance(frame, pl.DataFrame):
        return frame
    try:
        return pl.DataFrame(frame)  # type: ignore[arg-type]
    except (TypeError, ValueError) as exc:  # pragma: no cover - defensive helper
        raise TypeError(f"Unsupported play-by-play frame type: {type(frame)!r}") from exc


def _prepare_pbp_frame(frame: pl.DataFrame, *, seasons: Sequence[int] | None = None) -> pl.DataFrame:
    if "season" not in frame.columns:
        raise ValueError("play-by-play data is missing the 'season' column.")

    working = frame
    if seasons:
        working = working.filter(pl.col("season").is_in(list(seasons)))

    if "season_type" in working.columns:
        working = working.filter(pl.col("season_type").str.to_uppercase() == "REG")

    if "_pbp_row_id" not in working.columns:
        working = working.with_row_count("_pbp_row_id")

    return working


def _numeric_expr(frame: pl.DataFrame, column: str, fallback: str | None = None) -> pl.Expr:
    if column in frame.columns:
        return pl.col(column).cast(pl.Float64, strict=False).fill_null(0.0)
    if fallback and fallback in frame.columns:
        return pl.col(fallback).cast(pl.Float64, strict=False).fill_null(0.0)
    return pl.lit(0.0)


def _qb_player_expr(frame: pl.DataFrame) -> pl.Expr:
    candidates: list[pl.Expr] = []
    if "passer_player_id" in frame.columns:
        candidates.append(pl.col("passer_player_id").cast(pl.Utf8, strict=False))

    qb_epa_available = "qb_epa" in frame.columns
    qb_dropback_available = "qb_dropback" in frame.columns

    def _rusher_expr(column: str) -> pl.Expr:
        base = pl.col(column).cast(pl.Utf8, strict=False)
        if qb_epa_available:
            condition = pl.col("qb_epa").is_not_null()
        elif qb_dropback_available:
            condition = pl.col("qb_dropback") == 1
        else:
            condition = pl.lit(False)
        return pl.when(condition).then(base).otherwise(pl.lit(None, dtype=pl.Utf8))

    for column in ("rusher_player_id", "lateral_rusher_player_id"):
        if column in frame.columns:
            candidates.append(_rusher_expr(column))

    if not candidates:
        return pl.lit(None, dtype=pl.Utf8)
    return pl.coalesce(candidates).alias("_qb_player_id")


def _aggregate_qb_impacts(frame: pl.DataFrame) -> pl.DataFrame:
    qb_player_expr = _qb_player_expr(frame)
    epa_expr = _numeric_expr(frame, "qb_epa", fallback="epa").alias("_epa")
    wpa_expr = _numeric_expr(frame, "qb_wpa", fallback="wpa").alias("_wpa")

    qb_rows = (
        frame.select(
            pl.col("season").cast(pl.Int16, strict=False).alias("season"),
            qb_player_expr,
            epa_expr,
            wpa_expr,
        )
        .filter(pl.col("_qb_player_id").is_not_null())
        .rename({"_qb_player_id": "player_id"})
    )

    if qb_rows.is_empty():
        return pl.DataFrame(
            schema={
                "player_id": pl.Utf8,
                "season": pl.Int16,
                "qb_epa": pl.Float64,
                "qb_wpa": pl.Float64,
            }
        )

    aggregated = (
        qb_rows.group_by(["player_id", "season"])
        .agg(
            pl.col("_epa").sum().alias("qb_epa"),
            pl.col("_wpa").sum().alias("qb_wpa"),
        )
        .with_columns(
            pl.col("qb_epa").cast(pl.Float64, strict=False),
            pl.col("qb_wpa").cast(pl.Float64, strict=False),
        )
    )
    return aggregated


def _aggregate_skill_impacts(frame: pl.DataFrame) -> pl.DataFrame:
    available_rusher_cols = [column for column in _SKILL_RUSHER_COLUMNS if column in frame.columns]
    available_receiver_cols = [column for column in _SKILL_RECEIVER_COLUMNS if column in frame.columns]
    if not available_rusher_cols and not available_receiver_cols:
        return pl.DataFrame(
            schema={
                "player_id": pl.Utf8,
                "season": pl.Int16,
                "skill_epa": pl.Float64,
                "skill_wpa": pl.Float64,
                "skill_rush_20_plus": pl.Int32,
                "skill_rec_20_plus": pl.Int32,
                "skill_rec_first_downs": pl.Int32,
            }
        )

    epa_expr = _numeric_expr(frame, "epa").alias("_epa")
    wpa_expr = _numeric_expr(frame, "wpa").alias("_wpa")
    yards_expr = (
        pl.col("yards_gained").cast(pl.Float64, strict=False).fill_null(0.0)
        if "yards_gained" in frame.columns
        else pl.lit(0.0)
    )
    complete_expr = (
        pl.col("complete_pass").cast(pl.Int8, strict=False).fill_null(0)
        if "complete_pass" in frame.columns
        else pl.lit(0)
    )
    first_down_expr = (
        pl.col("first_down").cast(pl.Int8, strict=False).fill_null(0)
        if "first_down" in frame.columns
        else pl.lit(0)
    )

    entries: list[pl.DataFrame] = []
    for column in available_rusher_cols:
        entries.append(
            frame.select(
                pl.col("season").cast(pl.Int16, strict=False).alias("season"),
                pl.col("game_id") if "game_id" in frame.columns else pl.lit(0).alias("game_id"),
                (
                    pl.col("play_id")
                    if "play_id" in frame.columns
                    else pl.col("_pbp_row_id")
                ).alias("play_id"),
                pl.col(column).cast(pl.Utf8, strict=False).alias("player_id"),
                epa_expr,
                wpa_expr,
                pl.when(yards_expr >= 20).then(1).otherwise(0).alias("_rush_20"),
                pl.lit(0).alias("_rec_20"),
                pl.lit(0).alias("_rec_fd"),
            )
        )

    for column in available_receiver_cols:
        entries.append(
            frame.select(
                pl.col("season").cast(pl.Int16, strict=False).alias("season"),
                pl.col("game_id") if "game_id" in frame.columns else pl.lit(0).alias("game_id"),
                (
                    pl.col("play_id")
                    if "play_id" in frame.columns
                    else pl.col("_pbp_row_id")
                ).alias("play_id"),
                pl.col(column).cast(pl.Utf8, strict=False).alias("player_id"),
                epa_expr,
                wpa_expr,
                pl.lit(0).alias("_rush_20"),
                pl.when((yards_expr >= 20) & (complete_expr == 1))
                .then(1)
                .otherwise(0)
                .alias("_rec_20"),
                pl.when((first_down_expr == 1) & (complete_expr == 1)).then(1).otherwise(0).alias("_rec_fd"),
            )
        )

    stacked = pl.concat(entries, how="vertical_relaxed").filter(pl.col("player_id").is_not_null())
    if stacked.is_empty():
        return pl.DataFrame(
            schema={
                "player_id": pl.Utf8,
                "season": pl.Int16,
                "skill_epa": pl.Float64,
                "skill_wpa": pl.Float64,
                "skill_rush_20_plus": pl.Int32,
                "skill_rec_20_plus": pl.Int32,
                "skill_rec_first_downs": pl.Int32,
            }
        )

    key_columns = ["season", "game_id", "play_id", "player_id"]
    deduped = (
        stacked.group_by(key_columns)
        .agg(
            pl.col("_epa").first().alias("_epa"),
            pl.col("_wpa").first().alias("_wpa"),
            pl.col("_rush_20").max().alias("_rush_20"),
            pl.col("_rec_20").max().alias("_rec_20"),
            pl.col("_rec_fd").max().alias("_rec_fd"),
        )
        .drop(["game_id", "play_id"])
    )

    aggregated = (
        deduped.group_by(["player_id", "season"])
        .agg(
            pl.col("_epa").sum().alias("skill_epa"),
            pl.col("_wpa").sum().alias("skill_wpa"),
            pl.col("_rush_20").sum().alias("skill_rush_20_plus"),
            pl.col("_rec_20").sum().alias("skill_rec_20_plus"),
            pl.col("_rec_fd").sum().alias("skill_rec_first_downs"),
        )
        .with_columns(
            pl.col("skill_epa").cast(pl.Float64, strict=False),
            pl.col("skill_wpa").cast(pl.Float64, strict=False),
            pl.col("skill_rush_20_plus").cast(pl.Int32, strict=False),
            pl.col("skill_rec_20_plus").cast(pl.Int32, strict=False),
            pl.col("skill_rec_first_downs").cast(pl.Int32, strict=False),
        )
    )
    return aggregated


def _aggregate_role_impacts(frame: pl.DataFrame, columns: Sequence[str], *, prefix: str) -> pl.DataFrame:
    available = [column for column in columns if column in frame.columns]
    if not available:
        schema = {
            "player_id": pl.Utf8,
            "season": pl.Int16,
            f"{prefix}_epa": pl.Float64,
            f"{prefix}_wpa": pl.Float64,
        }
        return pl.DataFrame(schema=schema)

    epa_expr = _numeric_expr(frame, "epa").alias("_epa")
    wpa_expr = _numeric_expr(frame, "wpa").alias("_wpa")

    entries: list[pl.DataFrame] = []
    for column in available:
        entries.append(
            frame.select(
                pl.col("season").cast(pl.Int16, strict=False).alias("season"),
                pl.col("game_id") if "game_id" in frame.columns else pl.lit(0).alias("game_id"),
                (
                    pl.col("play_id")
                    if "play_id" in frame.columns
                    else pl.col("_pbp_row_id")
                ).alias("play_id"),
                pl.col(column).cast(pl.Utf8, strict=False).alias("player_id"),
                epa_expr,
                wpa_expr,
            )
        )

    stacked = pl.concat(entries, how="vertical_relaxed").filter(pl.col("player_id").is_not_null())
    if stacked.is_empty():
        schema = {
            "player_id": pl.Utf8,
            "season": pl.Int16,
            f"{prefix}_epa": pl.Float64,
            f"{prefix}_wpa": pl.Float64,
        }
        return pl.DataFrame(schema=schema)

    key_columns = ["season", "game_id", "play_id", "player_id"]
    deduped = (
        stacked.group_by(key_columns)
        .agg(
            pl.col("_epa").first().alias("_epa"),
            pl.col("_wpa").first().alias("_wpa"),
        )
        .drop(["game_id", "play_id"])
    )

    aggregated = (
        deduped.group_by(["player_id", "season"])
        .agg(
            pl.col("_epa").sum().alias(f"{prefix}_epa"),
            pl.col("_wpa").sum().alias(f"{prefix}_wpa"),
        )
        .with_columns(
            pl.col(f"{prefix}_epa").cast(pl.Float64, strict=False),
            pl.col(f"{prefix}_wpa").cast(pl.Float64, strict=False),
        )
    )
    return aggregated


def _merge_impact_frames(frames: Sequence[pl.DataFrame]) -> pl.DataFrame:
    non_empty = [frame for frame in frames if frame.height > 0]
    if not non_empty:
        return _empty_impact_frame()

    merged = non_empty[0]
    redundant = [col for col in merged.columns if col.endswith("_right")]
    if redundant:
        merged = merged.drop(redundant)
    existing_columns = set(merged.columns)
    for frame in non_empty[1:]:
        new_columns = [col for col in frame.columns if col not in {"player_id", "season"} and col not in existing_columns]
        if not new_columns:
            continue
        trimmed = frame.select(["player_id", "season", *new_columns])
        merged = merged.join(trimmed, on=["player_id", "season"], how="full", coalesce=True)
        redundant = [col for col in merged.columns if col.endswith("_right")]
        if redundant:
            merged = merged.drop(redundant)
        existing_columns.update(new_columns)

    for column, dtype in IMPACT_SCHEMA.items():
        if column not in merged.columns:
            merged = merged.with_columns(pl.lit(0).cast(dtype, strict=False).alias(column))

    filled = merged.select(
        pl.col("player_id").cast(pl.Utf8, strict=False).fill_null(""),
        pl.col("season").cast(pl.Int16, strict=False),
        *[
            pl.col(column).cast(dtype, strict=False).fill_null(0)
            for column, dtype in IMPACT_SCHEMA.items()
            if column not in {"player_id", "season"}
        ],
    )

    ordered = filled.select(list(IMPACT_SCHEMA.keys())).sort(["player_id", "season"])
    return ordered


__all__ = [
    "IMPACT_SEASONS",
    "IMPACT_SCHEMA",
    "CACHE_PATH",
    "aggregate_player_impacts",
    "build_player_impacts_cache",
    "cache_exists",
    "load_player_impacts",
    "scan_player_impacts",
]


