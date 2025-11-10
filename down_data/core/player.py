"""Player object powered by nflreadpy.

This module provides a high-level Player class for NFL player data extraction
and analysis, with support for basic stats (1999+) and NextGen advanced metrics (2016+).
"""
from __future__ import annotations

from collections.abc import Iterable
from dataclasses import asdict, dataclass
from datetime import date
import logging
from pathlib import Path
import re
from typing import Any

import polars as pl
from nflreadpy import load_ff_playerids, load_nextgen_stats, load_pbp, load_player_stats, load_players, load_teams

logger = logging.getLogger(__name__)

# NFLverse data availability constants
# Player stats data is available from 1999 to present
EARLIEST_SEASON_AVAILABLE = 1999
LATEST_SEASON_AVAILABLE = 2025  # Updated as new seasons become available

# NextGen Stats availability (NFL's official advanced tracking metrics)
EARLIEST_NEXTGEN_SEASON = 2016

# Pro Football Reference Advanced Stats availability
EARLIEST_PFR_SEASON = 2018
LATEST_PFR_SEASON = 2024
PFR_DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "raw" / "pfr"


class PlayerNotFoundError(RuntimeError):
    """Raised when no player can be resolved for the provided query."""


class SeasonNotAvailableError(RuntimeError):
    """Raised when requested season data is not available in nflverse."""


@dataclass(frozen=True)
class PlayerProfile:
    """Immutable snapshot of key player metadata."""

    full_name: str
    birth_date: date | None
    college: str | None
    nflverse_player_id: str | None
    gsis_id: str | None
    pfr_id: str | None
    pff_id: str | None
    espn_id: str | None
    sportradar_id: str | None
    esb_id: str | None
    otc_id: str | None
    height: int | None
    weight: int | None
    draft_year: int | None
    draft_round: int | None
    draft_pick: int | None
    draft_team: str | None
    position: str | None
    position_group: str | None

    def to_dict(self) -> dict[str, Any]:
        """Return the profile as a serialisable dictionary."""

        payload = asdict(self)
        # Convert date objects to ISO formatted strings for easier display/serialisation.
        if isinstance(payload.get("birth_date"), date):
            payload["birth_date"] = payload["birth_date"].isoformat()
        return payload

    @staticmethod
    def _parse_date(value: Any) -> date | None:
        if value is None or value == "":
            return None
        if isinstance(value, date):
            return value
        if isinstance(value, str):
            try:
                return date.fromisoformat(value)
            except ValueError:
                logger.debug("Unable to parse date value '%s'", value)
                return None
        return None

    @staticmethod
    def _parse_int(value: Any) -> int | None:
        if value is None or value == "":
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            logger.debug("Unable to parse integer value '%s'", value)
            return None

    @staticmethod
    def _first_non_empty(*values: Any) -> Any | None:
        for value in values:
            if value is None:
                continue
            if isinstance(value, str) and value.strip() == "":
                continue
            return value
        return None

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> "PlayerProfile":
        """Create a profile instance from a joined player row."""

        birth_date = cls._first_non_empty(row.get("birth_date"), row.get("birthdate"))
        college = cls._first_non_empty(row.get("college_name"), row.get("college"))
        draft_year = cls._first_non_empty(row.get("draft_year"), row.get("draft_year_ff"))
        draft_round = cls._first_non_empty(row.get("draft_round"), row.get("draft_round_ff"))
        draft_pick = cls._first_non_empty(row.get("draft_pick"), row.get("draft_pick_ff"))
        draft_team = cls._first_non_empty(row.get("draft_team"))
        height = cls._first_non_empty(row.get("height"), row.get("height_ff"))
        weight = cls._first_non_empty(row.get("weight"), row.get("weight_ff"))
        position = cls._first_non_empty(row.get("position"), row.get("position_ff"))

        pfr_id = cls._first_non_empty(row.get("pfr_id"), row.get("pfr_id_ff"))
        pff_id = cls._first_non_empty(row.get("pff_id"), row.get("pff_id_ff"))
        espn_id = cls._first_non_empty(row.get("espn_id"), row.get("espn_id_ff"))

        return cls(
            full_name=str(
                cls._first_non_empty(row.get("full_name"), row.get("display_name"), row.get("name"))
                or "Unknown Player"
            ),
            birth_date=cls._parse_date(birth_date),
            college=college,
            nflverse_player_id=cls._first_non_empty(row.get("gsis_id"), row.get("nfl_id")),
            gsis_id=row.get("gsis_id"),
            pfr_id=pfr_id,
            pff_id=pff_id,
            espn_id=espn_id,
            sportradar_id=row.get("sportradar_id"),
            esb_id=row.get("esb_id"),
            otc_id=row.get("otc_id"),
            height=cls._parse_int(height),
            weight=cls._parse_int(weight),
            draft_year=cls._parse_int(draft_year),
            draft_round=cls._parse_int(draft_round),
            draft_pick=cls._parse_int(draft_pick),
            draft_team=draft_team,
            position=position,
            position_group=row.get("position_group"),
        )


@dataclass(frozen=True)
class PlayerQuery:
    """Criteria used to identify a player."""

    name: str
    team: str | None = None
    draft_year: int | None = None
    draft_team: str | None = None
    position: str | None = None

    def normalised_position(self) -> str | None:
        return self.position.lower() if self.position else None

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", self.name.strip())
        if self.team is not None:
            object.__setattr__(self, "team", self.team.strip())
        if self.draft_team is not None:
            object.__setattr__(self, "draft_team", self.draft_team.strip())


class TeamDirectory:
    """Utility for normalising team identifiers to abbreviations."""

    _mapping: dict[str, str] | None = None

    @classmethod
    def _build_mapping(cls) -> dict[str, str]:
        teams = load_teams()
        mapping: dict[str, str] = {}
        for row in teams.unique(subset=["team"]).iter_rows(named=True):
            abbr = row.get("team")
            if not isinstance(abbr, str):
                continue
            abbr_clean = abbr.upper()
            candidates = {
                abbr_clean,
                row.get("full"),
                row.get("nickname"),
                row.get("location"),
                row.get("hyphenated"),
                row.get("team"),
                row.get("nfl"),
                row.get("pfr"),
                row.get("espn"),
            }
            for candidate in candidates:
                if not candidate or not isinstance(candidate, str):
                    continue
                mapping[candidate.strip().lower()] = abbr_clean
            full_combo = "{} {}".format(row.get("location", ""), row.get("nickname", "")).strip()
            if full_combo:
                mapping[full_combo.lower()] = abbr_clean
        return mapping

    @classmethod
    def normalise(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip()
        if not value:
            return None
        mapping = cls._mapping or cls._build_mapping()
        cls._mapping = mapping
        key = value.lower()
        if key in mapping:
            return mapping[key]
        # Try without punctuation/spaces for additional flexibility.
        compact_key = "".join(ch for ch in key if ch.isalnum())
        for candidate_key, abbr in mapping.items():
            if "".join(ch for ch in candidate_key if ch.isalnum()) == compact_key:
                return abbr
        if len(value) == 3 and value.isalpha():
            return value.upper()
        logger.debug("Unable to normalise team identifier '%s'", value)
        return None


class PlayerDataSource:
    """Caches raw nflverse datasets for player lookups."""

    _players: pl.DataFrame | None = None
    _player_ids: pl.DataFrame | None = None
    _combined: pl.DataFrame | None = None

    @classmethod
    def players(cls) -> pl.DataFrame:
        if cls._players is None:
            players = load_players().with_columns(
                pl.concat_str(
                    [
                        pl.col("first_name").fill_null(""),
                        pl.lit(" "),
                        pl.col("last_name").fill_null(""),
                    ]
                )
                .str.strip_chars()
                .alias("full_name")
            )
            cls._players = players
        return cls._players

    @classmethod
    def player_ids(cls) -> pl.DataFrame:
        if cls._player_ids is None:
            cls._player_ids = load_ff_playerids()
        return cls._player_ids

    @classmethod
    def combined(cls) -> pl.DataFrame:
        if cls._combined is None:
            cls._combined = cls.players().join(
                cls.player_ids(),
                on="gsis_id",
                how="left",
                suffix="_ff",
            )
        return cls._combined


class PlayerFinder:
    """Encapsulates the logic for resolving a player query."""

    NAME_COLUMNS = ["display_name", "full_name", "football_name", "short_name", "name", "merge_name"]

    @classmethod
    def resolve(cls, query: PlayerQuery) -> dict[str, Any]:
        dataset = PlayerDataSource.combined()
        name_lower = query.name.lower()
        name_filters = [
            pl.col(column).fill_null("").str.to_lowercase() == name_lower
            for column in cls.NAME_COLUMNS
            if column in dataset.columns
        ]
        if not name_filters:
            raise PlayerNotFoundError(f"No name columns available to resolve '{query.name}'.")
        filtered = dataset.filter(pl.any_horizontal(name_filters))

        fallback = cls._fallback_name_match(dataset, query)
        if fallback is not None:
            if filtered.height == 0:
                filtered = fallback
            else:
                filtered = pl.concat([filtered, fallback], how="vertical_relaxed").unique(subset=["gsis_id"])

        if filtered.height == 0:
            raise PlayerNotFoundError(
                "No player matched the provided filters. Try relaxing your criteria or checking spellings."
            )

        if query.position:
            position = query.normalised_position()
            position_filters = [
                pl.col("position").fill_null("").str.to_lowercase() == position,
                pl.col("position_group").fill_null("").str.to_lowercase() == position,
                pl.col("position_ff").fill_null("").str.to_lowercase() == position,
            ]
            filtered = filtered.filter(pl.any_horizontal(position_filters))

        if query.draft_year is not None:
            filtered = filtered.filter(
                pl.any_horizontal(
                    [
                        pl.col("draft_year") == query.draft_year,
                        pl.col("draft_year_ff") == query.draft_year,
                    ]
                )
            )

        if query.team:
            team_abbr = TeamDirectory.normalise(query.team)
            if team_abbr:
                filtered = filtered.filter(
                    pl.any_horizontal(
                        [
                            pl.col("latest_team").fill_null("").str.to_uppercase() == team_abbr,
                            pl.col("team").fill_null("").str.to_uppercase() == team_abbr,
                        ]
                    )
                )
            else:
                logger.warning("Team filter '%s' could not be normalised; ignoring filter", query.team)

        if query.draft_team:
            draft_team_abbr = TeamDirectory.normalise(query.draft_team)
            if draft_team_abbr:
                filtered = filtered.filter(pl.col("draft_team").fill_null("").str.to_uppercase() == draft_team_abbr)
            else:
                logger.warning(
                    "Draft team filter '%s' could not be normalised; ignoring filter", query.draft_team
                )

        if filtered.height == 0:
            raise PlayerNotFoundError(
                "No player matched the provided filters. Try relaxing your criteria or checking spellings."
            )

        resolved = cls._choose_most_notable(filtered)
        return resolved

    @staticmethod
    def _tokenize(value: str) -> list[str]:
        return [token for token in re.split(r"[^a-z0-9]+", value.lower()) if token]

    @classmethod
    def _fallback_name_match(cls, dataset: pl.DataFrame, query: PlayerQuery) -> pl.DataFrame | None:
        token_list = cls._tokenize(query.name)
        if not token_list:
            return None
        tokens = set(token_list)

        last_name = query.name.split()[-1].lower() if query.name.strip() else None
        narrowed = dataset
        if last_name:
            narrowed = narrowed.filter(
                pl.col("last_name").fill_null("").str.to_lowercase().str.contains(last_name)
            )

        matched_rows: list[dict[str, Any]] = []
        for row in narrowed.iter_rows(named=True):
            candidate_tokens: set[str] = set()
            for column in cls.NAME_COLUMNS:
                value = row.get(column)
                if isinstance(value, str):
                    candidate_tokens.update(cls._tokenize(value))
            if tokens.issubset(candidate_tokens):
                matched_rows.append(row)

        if not matched_rows:
            return None

        return pl.DataFrame(matched_rows)

    @staticmethod
    def _choose_most_notable(candidates: pl.DataFrame) -> dict[str, Any]:
        scoring = candidates.with_columns(
            pl.when(pl.col("status") == "ACT").then(1).otherwise(0).alias("_active_score"),
            pl.col("years_of_experience").fill_null(0).alias("_experience_score"),
            pl.col("last_season")
            .fill_null(pl.col("draft_year"))
            .fill_null(pl.col("rookie_season"))
            .fill_null(0)
            .alias("_last_season_score"),
            pl.when(pl.col("draft_round").is_null())
            .then(99)
            .otherwise(pl.col("draft_round"))
            .alias("_draft_round_score"),
        )
        sorted_candidates = scoring.sort(
            by=[
                "_active_score",
                "_experience_score",
                "_last_season_score",
                "_draft_round_score",
            ],
            descending=[True, True, True, False],
        )
        selected = sorted_candidates.row(0, named=True)
        logger.debug("Selected player record: %s", selected)
        return selected


class Player:
    """High level representation of an NFL player."""

    def __init__(
        self,
        *,
        name: str,
        team: str | None = None,
        draft_year: int | None = None,
        draft_team: str | None = None,
        position: str | None = None,
    ) -> None:
        self.query = PlayerQuery(name=name, team=team, draft_year=draft_year, draft_team=draft_team, position=position)
        self._raw_row = PlayerFinder.resolve(self.query)
        self.profile = PlayerProfile.from_row(self._raw_row)
        self._cache: dict[str, Any] = {}

    def to_rich_table(self):
        """Render the player's profile as a Rich table."""

        try:
            from rich.table import Table
            from rich.text import Text
        except ImportError as exc:  # pragma: no cover - defensive programming
            raise RuntimeError("Rich must be installed to render tables") from exc

        table = Table(title=self.profile.full_name)
        table.add_column("Field", style="cyan", no_wrap=True)
        table.add_column("Value", style="white", overflow="fold")
        for field, value in self.profile.to_dict().items():
            if value is None:
                display = Text("None", style="dim")
            else:
                display = Text(str(value))
            table.add_row(field, display)
        return table

    def info(self) -> dict[str, Any]:
        """Return the stored profile information."""

        return self.profile.to_dict()

    @staticmethod
    def validate_seasons(
        seasons: Iterable[int] | None = None,
        *,
        earliest: int = EARLIEST_SEASON_AVAILABLE,
        latest: int = LATEST_SEASON_AVAILABLE,
    ) -> tuple[list[int], list[int]]:
        """Split requested seasons into valid and invalid collections."""

        if seasons is None:
            return ([], [])

        season_list = list(seasons)
        valid = [season for season in season_list if earliest <= season <= latest]
        invalid = [season for season in season_list if season < earliest or season > latest]
        return (valid, invalid)
    
    @staticmethod
    def _prepare_season_param(seasons: bool | Iterable[int] | None) -> bool | list[int] | None:
        """Normalise season filters for nflreadpy functions."""
        if seasons is None or seasons is True:
            return seasons
        return list(seasons)

    def fetch_stats(
        self,
        *,
        seasons: bool | Iterable[int] | None = None,
        season_type: str | None = None,
    ) -> pl.DataFrame:
        """Return nflverse player stats for the supplied season filters."""

        if seasons not in (None, True):
            valid_seasons, invalid_seasons = self.validate_seasons(seasons)
            if invalid_seasons:
                raise SeasonNotAvailableError(
                    "Stats are available from "
                    f"{EARLIEST_SEASON_AVAILABLE} to {LATEST_SEASON_AVAILABLE}; "
                    f"invalid seasons requested: {invalid_seasons}."
                )

        params: dict[str, Any] = {}
        prepared_seasons = self._prepare_season_param(seasons)
        if prepared_seasons is not None:
            params["seasons"] = prepared_seasons
        if season_type is not None:
            params["season_type"] = season_type

        try:
            stats = load_player_stats(**params)
            filtered = stats.filter(pl.col("player_id") == self.profile.gsis_id)
            self._cache["stats"] = filtered
            return filtered
        except ConnectionError as e:
            # Provide helpful context if the download fails
            error_msg = str(e)
            if "404" in error_msg:
                raise SeasonNotAvailableError(
                    f"Failed to download stats data. This may indicate the requested season is not available. "
                    f"Stats are only available from {EARLIEST_SEASON_AVAILABLE} to {LATEST_SEASON_AVAILABLE}."
                ) from e
            raise

    def cached_stats(self) -> pl.DataFrame | None:
        """Return cached stats if they have been fetched previously."""

        return self._cache.get("stats")

    def fetch_nextgen_stats(
        self,
        *,
        seasons: bool | Iterable[int] | None = None,
        stat_type: str = "passing",
    ) -> pl.DataFrame:
        """Load NFL Next Gen Stats data (available from 2016 onwards)."""

        if seasons not in (None, True):
            valid_base, invalid_base = self.validate_seasons(
                seasons, earliest=EARLIEST_SEASON_AVAILABLE, latest=LATEST_SEASON_AVAILABLE
            )
            invalid_nextgen = [season for season in valid_base if season < EARLIEST_NEXTGEN_SEASON]
            if invalid_base or invalid_nextgen:
                invalid = sorted(set(invalid_base + invalid_nextgen))
                raise SeasonNotAvailableError(
                    "Next Gen Stats are available from "
                    f"{EARLIEST_NEXTGEN_SEASON} to {LATEST_SEASON_AVAILABLE}; "
                    f"invalid seasons requested: {invalid}."
                )

        params: dict[str, Any] = {"stat_type": stat_type}
        prepared_seasons = self._prepare_season_param(seasons)
        if prepared_seasons is not None:
            params["seasons"] = prepared_seasons
        
        try:
            nextgen_stats = load_nextgen_stats(**params)
            
            # NextGen stats use player_display_name - match against full_name
            # This is less precise but NextGen doesn't always have GSIS IDs
            player_name = self.profile.full_name
            filtered = nextgen_stats.filter(
                pl.col("player_display_name").str.to_lowercase() == player_name.lower()
            )
            
            # Cache with a key that includes the stat type
            cache_key = f"nextgen_stats_{stat_type}"
            self._cache[cache_key] = filtered
            
            return filtered
        except ConnectionError as e:
            error_msg = str(e)
            if "404" in error_msg:
                raise SeasonNotAvailableError(
                    f"Failed to download NextGen stats. "
                    f"Stats are only available from {EARLIEST_NEXTGEN_SEASON} onwards."
                ) from e
            raise

    def cached_nextgen_stats(self, stat_type: str = "passing") -> pl.DataFrame | None:
        """Return cached NextGen stats if they have been fetched previously."""
        cache_key = f"nextgen_stats_{stat_type}"
        return self._cache.get(cache_key)

    def fetch_pfr_advanced_stats(
        self,
        *,
        seasons: Iterable[int] | None = None,
        stat_type: str = "passing",
    ) -> pl.DataFrame:
        """Load PFR advanced stats from local CSV exports (available 2018–2024)."""

        if not self.profile.pfr_id:
            raise ValueError(
                f"Player {self.profile.full_name} does not have a Pro Football Reference ID."
            )

        if seasons is None:
            seasons_to_load = list(range(EARLIEST_PFR_SEASON, LATEST_PFR_SEASON + 1))
        else:
            seasons_to_load = list(seasons)
            invalid_seasons = [
                season for season in seasons_to_load if season < EARLIEST_PFR_SEASON or season > LATEST_PFR_SEASON
            ]
            if invalid_seasons:
                raise SeasonNotAvailableError(
                    "PFR advanced stats are available from "
                    f"{EARLIEST_PFR_SEASON} to {LATEST_PFR_SEASON}; "
                    f"invalid seasons requested: {invalid_seasons}."
                )

        file_prefix_map = {
            "passing": "pfr_passing_advanced",
            "rushing": "pfr_rushing_advanced",
            "receiving": "pfr_receiving_advanced",
            "defense": "pfr_defense_advanced",
        }
        if stat_type not in file_prefix_map:
            options = ", ".join(file_prefix_map)
            raise ValueError(f"Invalid stat_type '{stat_type}'. Choose one of: {options}.")
        file_prefix = file_prefix_map[stat_type]

        collected: list[pl.DataFrame] = []
        for season in seasons_to_load:
            csv_path = PFR_DATA_DIR / f"{file_prefix}_{season}.csv"
            if not csv_path.exists():
                logger.warning("PFR CSV file not found: %s", csv_path)
                continue
            try:
                dataframe = pl.read_csv(csv_path)
            except Exception as exc:  # pragma: no cover - I/O errors depend on deployment
                logger.warning("Error reading %s: %s", csv_path, exc)
                continue

            pfr_id_column = dataframe.columns[-1]
            player_data = dataframe.filter(pl.col(pfr_id_column) == self.profile.pfr_id)
            if player_data.height > 0:
                collected.append(player_data.with_columns(pl.lit(season).alias("season")))

        if not collected:
            logger.info(
                "No PFR %s data found for %s (%s) in seasons %s",
                stat_type,
                self.profile.full_name,
                self.profile.pfr_id,
                seasons_to_load,
            )
            return pl.DataFrame()

        combined = pl.concat(collected, how="vertical_relaxed")
        cache_key = f"pfr_stats_{stat_type}"
        self._cache[cache_key] = combined
        return combined

    def cached_pfr_stats(self, stat_type: str = "passing") -> pl.DataFrame | None:
        """Return cached PFR stats if they have been fetched previously."""
        cache_key = f"pfr_stats_{stat_type}"
        return self._cache.get(cache_key)

    def fetch_pbp(
        self,
        *,
        seasons: bool | Iterable[int] | None = None,
    ) -> pl.DataFrame:
        """Load play-by-play data that references this player."""

        if seasons not in (None, True):
            _, invalid_seasons = self.validate_seasons(seasons)
            if invalid_seasons:
                raise SeasonNotAvailableError(
                    "Play-by-play data is available from "
                    f"{EARLIEST_SEASON_AVAILABLE} to {LATEST_SEASON_AVAILABLE}; "
                    f"invalid seasons requested: {invalid_seasons}."
                )

        try:
            pbp = load_pbp(seasons=self._prepare_season_param(seasons))
        except ConnectionError as exc:  # pragma: no cover - network error
            raise SeasonNotAvailableError("Failed to download play-by-play data.") from exc

        player_id = self.profile.gsis_id
        if not player_id:
            logger.info("Player %s has no GSIS ID; returning empty play-by-play frame.", self.profile.full_name)
            return pl.DataFrame()
        player_filters = [
            pl.col("passer_player_id") == player_id,
            pl.col("rusher_player_id") == player_id,
            pl.col("receiver_player_id") == player_id,
            pl.col("lateral_receiver_player_id") == player_id,
            pl.col("solo_tackle_1_player_id") == player_id,
            pl.col("solo_tackle_2_player_id") == player_id,
            pl.col("assist_tackle_1_player_id") == player_id,
            pl.col("assist_tackle_2_player_id") == player_id,
            pl.col("assist_tackle_3_player_id") == player_id,
            pl.col("assist_tackle_4_player_id") == player_id,
            pl.col("tackle_with_assist_1_player_id") == player_id,
            pl.col("tackle_with_assist_2_player_id") == player_id,
            pl.col("pass_defense_1_player_id") == player_id,
            pl.col("pass_defense_2_player_id") == player_id,
            pl.col("interception_player_id") == player_id,
            pl.col("sack_player_id") == player_id,
            pl.col("half_sack_1_player_id") == player_id,
            pl.col("half_sack_2_player_id") == player_id,
            pl.col("fumbled_1_player_id") == player_id,
            pl.col("fumbled_2_player_id") == player_id,
            pl.col("fumble_recovery_1_player_id") == player_id,
            pl.col("fumble_recovery_2_player_id") == player_id,
            pl.col("forced_fumble_player_1_player_id") == player_id,
            pl.col("forced_fumble_player_2_player_id") == player_id,
            pl.col("kicker_player_id") == player_id,
            pl.col("punter_player_id") == player_id,
            pl.col("kickoff_returner_player_id") == player_id,
            pl.col("punt_returner_player_id") == player_id,
            pl.col("penalty_player_id") == player_id,
        ]

        player_plays = pbp.filter(pl.any_horizontal(player_filters))
        self._cache["pbp"] = player_plays

        logger.info(
            "Loaded %s plays for %s across %s season(s)",
            player_plays.height,
            self.profile.full_name,
            player_plays["season"].n_unique() if "season" in player_plays.columns else "unknown",
        )

        return player_plays

    def cached_pbp(self) -> pl.DataFrame | None:
        """Return cached play-by-play data if it has been fetched previously."""
        return self._cache.get("pbp")

    def fetch_coverage_stats(
        self,
        *,
        seasons: bool | Iterable[int] | None = None,
    ) -> dict[str, Any]:
        """Derive partial coverage stats from play-by-play data."""

        if not self.is_defensive():
            logger.warning(
                f"{self.profile.full_name} is not a defensive player (position: {self.profile.position}). "
                "Coverage stats are only relevant for defensive players."
            )
        
        if seasons not in (None, True):
            _, invalid_seasons = self.validate_seasons(seasons)
            if invalid_seasons:
                raise SeasonNotAvailableError(
                    "Play-by-play data is available from "
                    f"{EARLIEST_SEASON_AVAILABLE} to {LATEST_SEASON_AVAILABLE}; "
                    f"invalid seasons requested: {invalid_seasons}."
                )

        try:
            pbp = load_pbp(seasons=self._prepare_season_param(seasons))
        except ConnectionError as exc:  # pragma: no cover - network error
            raise SeasonNotAvailableError("Failed to download play-by-play data.") from exc

        player_id = self.profile.gsis_id
        if not player_id:
            return {
                "plays_credited": 0,
                "completions_allowed": 0,
                "yards_allowed": 0,
                "tds_allowed": 0,
                "interceptions": 0,
                "pass_breakups": 0,
                "note": "Player does not have a GSIS ID; coverage stats are unavailable.",
            }
        coverage_plays = pbp.filter(
            (pl.col("pass_defense_1_player_id") == player_id)
            | (pl.col("pass_defense_2_player_id") == player_id)
        )
        if coverage_plays.height == 0:
            return {
                "plays_credited": 0,
                "completions_allowed": 0,
                "yards_allowed": 0,
                "tds_allowed": 0,
                "interceptions": 0,
                "pass_breakups": 0,
                "note": (
                    "No plays with defender attribution were recorded. "
                    "NFLverse data only attributes defenders on roughly 11% of pass plays, "
                    "so this is not a full coverage log."
                ),
            }

        yards_total = coverage_plays.select(pl.col("yards_gained").fill_null(0).sum()).item()
        breakups = coverage_plays.filter(
            (pl.col("incomplete_pass") == 1) & (pl.col("interception") == 0)
        ).height
        return {
            "plays_credited": coverage_plays.height,
            "completions_allowed": coverage_plays.filter(pl.col("complete_pass") == 1).height,
            "yards_allowed": float(yards_total) if yards_total else 0.0,
            "tds_allowed": coverage_plays.filter(pl.col("pass_touchdown") == 1).height,
            "interceptions": coverage_plays.filter(pl.col("interception") == 1).height,
            "pass_breakups": breakups,
            "note": (
                "Coverage stats derive from nflverse defender attributions and are incomplete. "
                "Treat counts as lower bounds rather than totals."
            ),
        }

    def is_defensive(self) -> bool:
        """Check if the player is a defensive player based on position."""

        defensive_positions = {"DB", "LB", "DL", "CB", "S", "DE", "DT", "OLB", "ILB", "MLB", "FS", "SS", "NT"}
        position = self.profile.position
        position_group = self.profile.position_group

        if position and position.upper() in defensive_positions:
            return True
        if position_group and position_group.upper() in {"DB", "LB", "DL"}:
            return True
        return False

    def get_career_stats(
        self,
        *,
        seasons: bool | Iterable[int] | None = None,
        season_type: str | None = None,
    ) -> dict[str, Any]:
        """Calculate career totals for key statistics."""
        stats = self.fetch_stats(seasons=seasons, season_type=season_type)
        if stats.height == 0:
            return {}

        totals: dict[str, Any] = {"games_played": stats.height}

        if self.is_defensive():
            # Defensive stat aggregations
            defensive_cols = {
                "tackles_solo": "def_tackles_solo",
                "tackle_assists": "def_tackle_assists",
                "tackles_for_loss": "def_tackles_for_loss",
                "sacks": "def_sacks",
                "interceptions": "def_interceptions",
                "passes_defended": "def_pass_defended",
                "fumbles_forced": "def_fumbles_forced",
                "fumble_recoveries": "fumble_recovery_opp",
                "defensive_tds": "def_tds",
                "safeties": "def_safeties",
            }
            for key, col in defensive_cols.items():
                if col in stats.columns:
                    total = stats[col].fill_null(0).sum()
                    if total is not None:
                        totals[key] = total
        else:
            # Offensive stat aggregations
            offensive_cols = {
                "passing_yards": "passing_yards",
                "passing_tds": "passing_tds",
                "interceptions_thrown": "interceptions",
                "rushing_yards": "rushing_yards",
                "rushing_tds": "rushing_tds",
                "receiving_yards": "receiving_yards",
                "receiving_tds": "receiving_tds",
                "receptions": "receptions",
                "targets": "targets",
                "fantasy_points": "fantasy_points",
            }
            for key, col in offensive_cols.items():
                if col in stats.columns:
                    total = stats[col].fill_null(0).sum()
                    if total is not None:
                        totals[key] = total

        return totals

    def get_relevant_stat_columns(self) -> list[str]:
        """Return a list of stat column names relevant to this player's position."""

        base_columns = ["season", "week", "team", "opponent_team"]

        if self.is_defensive():
            return base_columns + [
                "def_tackles_solo",
                "def_tackle_assists",
                "def_sacks",
                "def_interceptions",
                "def_pass_defended",
                "def_fumbles_forced",
                "def_tds",
            ]
        else:
            return base_columns + [
                "passing_yards",
                "passing_tds",
                "rushing_yards",
                "rushing_tds",
                "receiving_yards",
                "receptions",
                "fantasy_points",
            ]

    def get_nextgen_stat_type(self) -> str:
        """
        Determine the appropriate NextGen stat type based on player position.
        
        Returns:
            "passing", "rushing", or "receiving"
        """
        position = (self.profile.position or "").upper()
        mapping = {
            "QB": "passing",
            "RB": "rushing",
            "FB": "rushing",
            "WR": "receiving",
            "TE": "receiving",
        }
        return mapping.get(position, "passing")

    @staticmethod
    def _build_aggregation_exprs(df: pl.DataFrame, exclude_cols: set[str]) -> list[pl.Expr]:
        """
        Build Polars aggregation expressions for season-level stats.
        
        Args:
            df: DataFrame to build expressions for.
            exclude_cols: Columns to exclude from aggregation.
        
        Returns:
            List of Polars aggregation expressions.
        """
        agg_exprs = []
        seen = set()
        
        # Add metadata columns
        if "player_id" in df.columns:
            agg_exprs.append(pl.col("player_id").first().alias("player_id"))
            seen.add("player_id")
        
        if "player_display_name" in df.columns:
            agg_exprs.append(pl.col("player_display_name").first().alias("player_name"))
            seen.add("player_name")
        
        # Add games played counter
        agg_exprs.append(pl.len().alias("games_played"))
        seen.add("games_played")
        
        # Add all numeric columns as sums
        numeric_dtypes = {pl.Float64, pl.Float32, pl.Int64, pl.Int32, pl.Int16, pl.Int8}
        for col in df.columns:
            if col not in exclude_cols and col not in seen and df[col].dtype in numeric_dtypes:
                agg_exprs.append(pl.col(col).fill_null(0).sum().alias(col))
                seen.add(col)
        
        return agg_exprs
    
    def get_master_stats_table(
        self,
        *,
        seasons: Iterable[int] | None = None,
        include_nextgen: bool = True,
        include_playoffs: bool = True,
    ) -> Any:  # Returns pd.DataFrame
        """Aggregate per-season stats (and optional Next Gen data) into a single table."""
        try:
            import pandas as pd
        except ImportError:
            raise ImportError(
                "Pandas is required for get_master_stats_table(). "
                "Install it with: pip install pandas"
            )
        
        # Fetch basic stats
        # If no seasons specified, fetch all available data (True loads all seasons)
        if seasons is None:
            basic_stats = self.fetch_stats(seasons=True)
        else:
            basic_stats = self.fetch_stats(seasons=seasons)
        
        if basic_stats.height == 0:
            # Return empty DataFrame with appropriate structure
            return pd.DataFrame()
        
        # Filter out playoff stats if requested
        if not include_playoffs and "season_type" in basic_stats.columns:
            basic_stats = basic_stats.filter(pl.col("season_type") != "POST")
        
        # Aggregate to season level using helper method
        exclude_cols = {'season', 'week', 'player_id', 'player_name', 'player_display_name', 
                       'team', 'opponent_team', 'game_id', 'season_type', 'position', 
                       'position_group', 'headshot_url', 'recent_team'}
        
        agg_exprs = self._build_aggregation_exprs(basic_stats, exclude_cols)
        season_stats = basic_stats.group_by("season").agg(agg_exprs).sort("season")
        
        # Convert to pandas
        master_df = season_stats.to_pandas()
        
        # Add NextGen stats if requested and available
        if include_nextgen:
            try:
                # Determine which seasons are 2016+
                available_seasons = master_df["season"].tolist()
                nextgen_seasons = [s for s in available_seasons if s >= EARLIEST_NEXTGEN_SEASON]
                
                if nextgen_seasons:
                    # Fetch NextGen stats for appropriate stat type
                    stat_type = self.get_nextgen_stat_type()
                    
                    try:
                        # If we have a significant number of seasons, just fetch all NextGen data
                        if len(nextgen_seasons) > 5:
                            nextgen_stats = self.fetch_nextgen_stats(
                                seasons=True,
                                stat_type=stat_type
                            )
                            # Filter to only the seasons we have in basic stats
                            nextgen_stats = nextgen_stats.filter(pl.col("season").is_in(nextgen_seasons))
                        else:
                            nextgen_stats = self.fetch_nextgen_stats(
                                seasons=nextgen_seasons,
                                stat_type=stat_type
                            )
                        
                        if nextgen_stats.height > 0:
                            # Aggregate NextGen stats by season
                            nextgen_agg_exprs = [pl.col("season").first().alias("season")]
                            
                            # Exclude non-stat columns
                            nextgen_exclude = {'season', 'season_type', 'week', 'player_display_name', 
                                             'player_position', 'team_abbr'}
                            numeric_dtypes = {pl.Float64, pl.Float32, pl.Int64, pl.Int32}
                            
                            # For NextGen, we want averages for rate stats, sums for counting stats
                            counting_keywords = {'attempts', 'completions', 'carries', 'targets', 'receptions'}
                            
                            for col in nextgen_stats.columns:
                                if col in nextgen_exclude:
                                    continue
                                if nextgen_stats[col].dtype not in numeric_dtypes:
                                    continue
                                
                                # Determine aggregation type
                                is_counting_stat = any(keyword in col.lower() for keyword in counting_keywords)
                                agg_func = pl.col(col).fill_null(0).sum() if is_counting_stat else pl.col(col).fill_null(0).mean()
                                nextgen_agg_exprs.append(agg_func.alias(f"nextgen_{col}"))
                            
                            nextgen_season = nextgen_stats.group_by("season").agg(nextgen_agg_exprs)
                            nextgen_df = nextgen_season.to_pandas()
                            
                            # Merge with master table
                            master_df = master_df.merge(nextgen_df, on="season", how="left")
                    
                    except Exception as e:
                        logger.debug(f"Could not fetch NextGen stats: {e}")
                        # Continue without NextGen stats
            
            except Exception as e:
                logger.debug(f"Error processing NextGen stats: {e}")
                # Continue with basic stats only
        
        # Clean up column names and order
        # Move season to first column, games_played to second
        cols = master_df.columns.tolist()
        priority_cols = ['season', 'games_played', 'player_name', 'player_id']
        other_cols = [col for col in cols if col not in priority_cols]
        master_df = master_df[[col for col in priority_cols if col in cols] + other_cols]
        
        return master_df

    def get_relevant_nextgen_columns(self, stat_type: str | None = None) -> list[str]:
        """
        Return relevant NextGen stat columns based on stat type.
        
        Args:
            stat_type: The stat type ("passing", "rushing", "receiving"). 
                      If None, automatically determines based on player position.
        """
        if stat_type is None:
            stat_type = self.get_nextgen_stat_type()
        
        base = ["season", "week", "player_display_name"]
        
        if stat_type == "passing":
            return base + [
                "attempts", "completions", "pass_yards",
                "pass_touchdowns", "interceptions",
                "avg_time_to_throw", "avg_completed_air_yards",
                "avg_intended_air_yards", "avg_air_yards_differential",
                "max_completed_air_distance", "aggressiveness",
                "completion_percentage_above_expectation",
            ]
        elif stat_type == "rushing":
            return base + [
                "carries", "rush_yards", "rush_touchdowns",
                "efficiency", "percent_attempts_gte_eight_defenders",
                "avg_time_to_los", "rush_yards_over_expected",
                "rush_yards_over_expected_per_att",
                "rush_pct_over_expected",
            ]
        elif stat_type == "receiving":
            return base + [
                "receptions", "targets", "receiving_yards",
                "receiving_touchdowns",
                "avg_cushion", "avg_separation",
                "avg_intended_air_yards", "percent_share_of_intended_air_yards",
                "catch_percentage", "avg_yac", "avg_expected_yac",
                "avg_yac_above_expectation",
            ]
        
        return base

    def __repr__(self) -> str:  # pragma: no cover - debug helper
        return f"Player(full_name={self.profile.full_name!r}, gsis_id={self.profile.gsis_id!r})"
