"""Player object powered by nflreadpy."""
from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import date
import logging
import re
from typing import Any, Dict, Iterable, List, Optional

import polars as pl
from nflreadpy import load_ff_playerids, load_player_stats, load_players, load_teams

logger = logging.getLogger(__name__)


class PlayerNotFoundError(RuntimeError):
    """Raised when no player can be resolved for the provided query."""


@dataclass(frozen=True)
class PlayerProfile:
    """Immutable snapshot of key player metadata."""

    full_name: str
    birth_date: Optional[date]
    college: Optional[str]
    nflverse_player_id: Optional[str]
    gsis_id: Optional[str]
    pfr_id: Optional[str]
    pff_id: Optional[str]
    espn_id: Optional[str]
    sportradar_id: Optional[str]
    esb_id: Optional[str]
    otc_id: Optional[str]
    height: Optional[int]
    weight: Optional[int]
    draft_year: Optional[int]
    draft_round: Optional[int]
    draft_pick: Optional[int]
    draft_team: Optional[str]
    position: Optional[str]
    position_group: Optional[str]

    def to_dict(self) -> Dict[str, Any]:
        """Return the profile as a serialisable dictionary."""

        payload = asdict(self)
        # Convert date objects to ISO formatted strings for easier display/serialisation.
        if isinstance(payload.get("birth_date"), date):
            payload["birth_date"] = payload["birth_date"].isoformat()
        return payload

    @staticmethod
    def _parse_date(value: Any) -> Optional[date]:
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
    def _parse_int(value: Any) -> Optional[int]:
        if value is None or value == "":
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            logger.debug("Unable to parse integer value '%s'", value)
            return None

    @staticmethod
    def _first_non_empty(*values: Any) -> Optional[Any]:
        for value in values:
            if value is None:
                continue
            if isinstance(value, str) and value.strip() == "":
                continue
            return value
        return None

    @classmethod
    def from_row(cls, row: Dict[str, Any]) -> "PlayerProfile":
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
    team: Optional[str] = None
    draft_year: Optional[int] = None
    draft_team: Optional[str] = None
    position: Optional[str] = None

    def normalised_position(self) -> Optional[str]:
        return self.position.lower() if self.position else None

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", self.name.strip())
        if self.team is not None:
            object.__setattr__(self, "team", self.team.strip())
        if self.draft_team is not None:
            object.__setattr__(self, "draft_team", self.draft_team.strip())


class TeamDirectory:
    """Utility for normalising team identifiers to abbreviations."""

    _mapping: Dict[str, str] | None = None

    @classmethod
    def _build_mapping(cls) -> Dict[str, str]:
        teams = load_teams()
        mapping: Dict[str, str] = {}
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
    def normalise(cls, value: Optional[str]) -> Optional[str]:
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
    def resolve(cls, query: PlayerQuery) -> Dict[str, Any]:
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
    def _tokenize(value: str) -> List[str]:
        return [token for token in re.split(r"[^a-z0-9]+", value.lower()) if token]

    @classmethod
    def _fallback_name_match(cls, dataset: pl.DataFrame, query: PlayerQuery) -> Optional[pl.DataFrame]:
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

        matched_rows: List[Dict[str, Any]] = []
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
    def _choose_most_notable(candidates: pl.DataFrame) -> Dict[str, Any]:
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
        team: Optional[str] = None,
        draft_year: Optional[int] = None,
        draft_team: Optional[str] = None,
        position: Optional[str] = None,
    ) -> None:
        self.query = PlayerQuery(name=name, team=team, draft_year=draft_year, draft_team=draft_team, position=position)
        self._raw_row = PlayerFinder.resolve(self.query)
        self.profile = PlayerProfile.from_row(self._raw_row)
        self._cache: Dict[str, Any] = {}

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

    def info(self) -> Dict[str, Any]:
        """Return the stored profile information."""

        return self.profile.to_dict()

    def fetch_stats(self, *, seasons: Optional[Iterable[int]] = None, season_type: Optional[str] = None) -> pl.DataFrame:
        """Load player stats using nflreadpy and cache the result."""

        params: Dict[str, Any] = {}
        if seasons is not None:
            params["seasons"] = list(seasons)
        if season_type is not None:
            params["season_type"] = season_type

        stats = load_player_stats(**params)
        filtered = stats.filter(pl.col("player_id") == self.profile.gsis_id)
        self._cache["stats"] = filtered
        return filtered

    def cached_stats(self) -> Optional[pl.DataFrame]:
        """Return cached stats if they have been fetched previously."""

        return self._cache.get("stats")

    def __repr__(self) -> str:  # pragma: no cover - debug helper
        return f"Player(full_name={self.profile.full_name!r}, gsis_id={self.profile.gsis_id!r})"
