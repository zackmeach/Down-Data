"""
Statistical calculation engine for NFL data objects.

This module provides a unified interface for applying statistical calculations
to Player and Team objects. It handles data fetching, stat calculation routing,
and result caching automatically.

The StatsEngine intelligently determines which statistics are relevant based on
the object type (Player/Team), position, and available data sources.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Union

import polars as pl

# Avoid circular imports
if TYPE_CHECKING:
    from player import Player

logger = logging.getLogger(__name__)


class StatsEngine:
    """
    Orchestrates statistical calculations for NFL data objects.
    
    The StatsEngine provides a consistent interface for calculating statistics
    across different object types (Player, Team) and automatically handles:
    - Data source selection (basic stats, PFR, NextGen, PBP)
    - Position-appropriate stat routing
    - Caching and performance optimization
    - Error handling and logging
    
    Examples:
        >>> from player import Player
        >>> from stats_engine import StatsEngine
        >>> 
        >>> player = Player(name="Patrick Mahomes")
        >>> engine = StatsEngine()
        >>> 
        >>> # Calculate specific stat
        >>> result = engine.calculate(player, "passer_rating", seasons=[2023])
        >>> 
        >>> # Calculate all relevant stats for position
        >>> all_stats = engine.calculate_all(player, seasons=[2023])
        >>> 
        >>> # Use specific data source
        >>> pfr_stats = engine.calculate(
        ...     player, 
        ...     "pressure_adjusted_stats",
        ...     data_source="pfr_advanced",
        ...     seasons=[2023]
        ... )
    """
    
    # Map stat names to their calculation functions and required data sources
    STAT_REGISTRY: Dict[str, Dict[str, Any]] = {
        # Passing stats
        "passer_rating": {
            "module": "stats.passing",
            "function": "calculate_passer_rating",
            "data_source": "basic",
            "positions": ["QB"],
        },
        "adj_yards_per_attempt": {
            "module": "stats.passing",
            "function": "calculate_adjusted_yards_per_attempt",
            "data_source": "basic",
            "positions": ["QB"],
        },
        "completion_pct_above_expectation": {
            "module": "stats.passing",
            "function": "calculate_completion_percentage_above_expectation",
            "data_source": "nextgen",
            "positions": ["QB"],
        },
        "air_yards_metrics": {
            "module": "stats.passing",
            "function": "calculate_air_yards_metrics",
            "data_source": "pfr_advanced",
            "positions": ["QB"],
        },
        "pressure_adjusted_stats": {
            "module": "stats.passing",
            "function": "calculate_pressure_adjusted_stats",
            "data_source": "pfr_advanced",
            "positions": ["QB"],
        },
        "time_to_throw_efficiency": {
            "module": "stats.passing",
            "function": "calculate_time_to_throw_efficiency",
            "data_source": "nextgen",
            "positions": ["QB"],
        },
        
        # Rushing stats
        "yards_per_carry": {
            "module": "stats.rushing",
            "function": "calculate_yards_per_carry",
            "data_source": "basic",
            "positions": ["RB", "QB", "WR", "FB"],
        },
        "broken_tackle_rate": {
            "module": "stats.rushing",
            "function": "calculate_broken_tackle_rate",
            "data_source": "pff",
            "positions": ["RB", "QB", "WR", "FB"],
        },
        "yards_after_contact_rate": {
            "module": "stats.rushing",
            "function": "calculate_yards_after_contact_rate",
            "data_source": "pfr_advanced",
            "positions": ["RB", "QB", "WR", "FB"],
        },
        "rushing_efficiency": {
            "module": "stats.rushing",
            "function": "calculate_rushing_efficiency",
            "data_source": "nextgen",
            "positions": ["RB", "QB", "WR", "FB"],
        },
        "success_rate": {
            "module": "stats.rushing",
            "function": "calculate_success_rate",
            "data_source": "pbp",
            "positions": ["RB", "QB", "WR", "FB"],
        },
        
        # Receiving stats
        "yards_per_route_run": {
            "module": "stats.receiving",
            "function": "calculate_yards_per_route_run",
            "data_source": "pff",
            "positions": ["WR", "TE", "RB"],
        },
        "target_share": {
            "module": "stats.receiving",
            "function": "calculate_target_share",
            "data_source": "basic",
            "positions": ["WR", "TE", "RB"],
        },
        "catch_rate_above_expectation": {
            "module": "stats.receiving",
            "function": "calculate_catch_rate_above_expectation",
            "data_source": "nextgen",
            "positions": ["WR", "TE", "RB"],
        },
        "yac_efficiency": {
            "module": "stats.receiving",
            "function": "calculate_yards_after_catch_efficiency",
            "data_source": "pfr_advanced",
            "positions": ["WR", "TE", "RB"],
        },
        "separation_metrics": {
            "module": "stats.receiving",
            "function": "calculate_separation_metrics",
            "data_source": "nextgen",
            "positions": ["WR", "TE"],
        },
        
        # Special teams stats
        "field_goal_percentage": {
            "module": "stats.specialteams",
            "function": "calculate_field_goal_percentage",
            "data_source": "basic",
            "positions": ["K"],
        },
        "punt_average": {
            "module": "stats.specialteams",
            "function": "calculate_punt_average",
            "data_source": "basic",
            "positions": ["P"],
        },
    }
    
    def __init__(self, cache_results: bool = True):
        """
        Initialize the StatsEngine.
        
        Args:
            cache_results: Whether to cache calculation results (default: True)
        """
        self.cache_results = cache_results
        self._function_cache: Dict[str, Callable] = {}
    
    def calculate(
        self,
        obj: Union[Player, Any],
        stat_name: str,
        *,
        data_source: Optional[str] = None,
        seasons: Union[None, bool, List[int]] = None,
        **kwargs: Any
    ) -> pl.DataFrame:
        """
        Calculate a specific statistic for a Player or Team object.
        
        Args:
            obj: Player or Team object to calculate stats for
            stat_name: Name of the statistic to calculate (see STAT_REGISTRY)
            data_source: Override data source ("basic", "pfr_advanced", "nextgen", "pbp")
            seasons: Seasons to include in calculation
            **kwargs: Additional arguments passed to the calculation function
        
        Returns:
            Polars DataFrame with calculated statistics
        
        Raises:
            ValueError: If stat_name is not recognized or incompatible with object
            RuntimeError: If required data is unavailable
        
        Examples:
            >>> engine = StatsEngine()
            >>> player = Player(name="Justin Jefferson")
            >>> yprr = engine.calculate(player, "yards_per_route_run", seasons=[2023])
        """
        # Validate stat name
        if stat_name not in self.STAT_REGISTRY:
            available_stats = ", ".join(sorted(self.STAT_REGISTRY.keys()))
            raise ValueError(
                f"Unknown stat '{stat_name}'. Available stats: {available_stats}"
            )
        
        stat_config = self.STAT_REGISTRY[stat_name]
        
        # Validate position compatibility (for Player objects)
        if hasattr(obj, "profile") and hasattr(obj.profile, "position"):
            player_pos = obj.profile.position
            allowed_positions = stat_config.get("positions", [])
            if player_pos and allowed_positions and player_pos not in allowed_positions:
                logger.warning(
                    f"Stat '{stat_name}' is typically for positions {allowed_positions}, "
                    f"but {obj.profile.full_name} is a {player_pos}. Proceeding anyway."
                )
        
        # Determine data source
        source = data_source or stat_config["data_source"]
        
        # Fetch data from appropriate source
        data = self._fetch_data(obj, source, seasons=seasons, **kwargs)
        
        if data is None or data.height == 0:
            logger.warning(
                f"No data available for {stat_name} calculation. "
                f"Data source: {source}, Seasons: {seasons}"
            )
            return pl.DataFrame()
        
        # Get calculation function
        calc_func = self._get_function(stat_config["module"], stat_config["function"])
        
        # Apply calculation
        try:
            result = calc_func(data, **kwargs)
            logger.info(
                f"Successfully calculated {stat_name} for "
                f"{getattr(obj.profile, 'full_name', 'object')}"
            )
            return result
        except Exception as e:
            logger.error(f"Error calculating {stat_name}: {e}")
            raise RuntimeError(f"Failed to calculate {stat_name}: {e}") from e
    
    def calculate_all(
        self,
        obj: Union[Player, Any],
        *,
        seasons: Union[None, bool, List[int]] = None,
        include_advanced: bool = True,
    ) -> pl.DataFrame:
        """
        Calculate all relevant statistics for an object based on position.
        
        Automatically determines which statistics are appropriate and calculates
        them all, combining results into a single DataFrame.
        
        Args:
            obj: Player or Team object to calculate stats for
            seasons: Seasons to include in calculation
            include_advanced: Whether to include advanced stats requiring PFF/NextGen
        
        Returns:
            Polars DataFrame with all calculated statistics
        
        Examples:
            >>> player = Player(name="Christian McCaffrey")
            >>> all_stats = engine.calculate_all(player, seasons=[2023])
        """
        if not hasattr(obj, "profile") or not hasattr(obj.profile, "position"):
            raise ValueError("Object must have a profile with position information")
        
        position = obj.profile.position
        logger.info(f"Calculating all stats for {obj.profile.full_name} ({position})")
        
        # Determine relevant stats based on position
        relevant_stats = self._get_relevant_stats(position, include_advanced)
        
        # Start with basic stats as the base
        base_data = self._fetch_data(obj, "basic", seasons=seasons)
        if base_data is None or base_data.height == 0:
            logger.warning("No basic stats available")
            return pl.DataFrame()
        
        result = base_data
        
        # Apply each relevant stat calculation
        for stat_name in relevant_stats:
            try:
                stat_config = self.STAT_REGISTRY[stat_name]
                source = stat_config["data_source"]
                
                # Skip advanced sources if not requested
                if not include_advanced and source in ["pfr_advanced", "nextgen", "pff"]:
                    continue
                
                # Fetch appropriate data for this stat
                if source != "basic":
                    data = self._fetch_data(obj, source, seasons=seasons)
                    if data is None or data.height == 0:
                        logger.debug(f"Skipping {stat_name} - no {source} data available")
                        continue
                else:
                    data = result
                
                # Apply calculation
                calc_func = self._get_function(stat_config["module"], stat_config["function"])
                calculated = calc_func(data)
                
                # Merge new columns into result
                new_cols = [col for col in calculated.columns if col not in result.columns]
                if new_cols:
                    for col in new_cols:
                        result = result.with_columns(calculated[col])
                
                logger.debug(f"Added {stat_name} ({len(new_cols)} new columns)")
                
            except Exception as e:
                logger.warning(f"Could not calculate {stat_name}: {e}")
                continue
        
        return result
    
    def _fetch_data(
        self,
        obj: Union[Player, Any],
        source: str,
        *,
        seasons: Union[None, bool, List[int]] = None,
        **kwargs: Any
    ) -> Optional[pl.DataFrame]:
        """
        Fetch data from the appropriate source on the object.
        
        Args:
            obj: Object to fetch data from (Player, Team, etc.)
            source: Data source name
            seasons: Seasons to fetch
            **kwargs: Additional arguments for fetch methods
        
        Returns:
            Polars DataFrame or None if unavailable
        """
        # Map source names to object methods
        source_map = {
            "basic": ("fetch_stats", "cached_stats"),
            "pfr_advanced": ("fetch_pfr_advanced_stats", "cached_pfr_stats"),
            "nextgen": ("fetch_nextgen_stats", "cached_nextgen_stats"),
            "pbp": ("fetch_pbp", "cached_pbp"),
        }
        
        if source not in source_map:
            raise ValueError(f"Unknown data source: {source}")
        
        fetch_method_name, cache_method_name = source_map[source]
        
        # Try cache first
        if self.cache_results and hasattr(obj, cache_method_name):
            cache_method = getattr(obj, cache_method_name)
            cached = cache_method(**kwargs) if kwargs else cache_method()
            if cached is not None and cached.height > 0:
                logger.debug(f"Using cached {source} data")
                return cached
        
        # Fetch fresh data
        if not hasattr(obj, fetch_method_name):
            raise ValueError(
                f"Object does not have method '{fetch_method_name}' for {source} data"
            )
        
        fetch_method = getattr(obj, fetch_method_name)
        
        try:
            # Build fetch arguments
            fetch_kwargs = kwargs.copy()
            if seasons is not None:
                fetch_kwargs["seasons"] = seasons
            
            # Special handling for pfr_advanced and nextgen (need stat_type)
            if source in ["pfr_advanced", "nextgen"]:
                if "stat_type" not in fetch_kwargs and hasattr(obj, "get_nextgen_stat_type"):
                    fetch_kwargs["stat_type"] = obj.get_nextgen_stat_type()
            
            data = fetch_method(**fetch_kwargs)
            logger.debug(f"Fetched {source} data: {data.height} rows")
            return data
            
        except Exception as e:
            logger.warning(f"Could not fetch {source} data: {e}")
            return None
    
    def _get_function(self, module_name: str, function_name: str) -> Callable:
        """
        Get a calculation function from the stats modules.
        
        Uses caching to avoid repeated imports.
        
        Args:
            module_name: Fully qualified module name (e.g., "stats.passing")
            function_name: Function name to import
        
        Returns:
            The calculation function
        """
        cache_key = f"{module_name}.{function_name}"
        
        if cache_key in self._function_cache:
            return self._function_cache[cache_key]
        
        # Dynamic import
        import importlib
        module = importlib.import_module(module_name)
        func = getattr(module, function_name)
        
        self._function_cache[cache_key] = func
        return func
    
    def _get_relevant_stats(self, position: str, include_advanced: bool) -> List[str]:
        """
        Determine which stats are relevant for a given position.
        
        Args:
            position: Player position (QB, RB, WR, etc.)
            include_advanced: Whether to include advanced metrics
        
        Returns:
            List of relevant stat names
        """
        relevant = []
        
        for stat_name, config in self.STAT_REGISTRY.items():
            allowed_positions = config.get("positions", [])
            
            # If no position filter, include for all
            if not allowed_positions:
                relevant.append(stat_name)
                continue
            
            # Check if position matches
            if position in allowed_positions:
                relevant.append(stat_name)
        
        return relevant
    
    def list_available_stats(
        self,
        position: Optional[str] = None,
        data_source: Optional[str] = None
    ) -> List[str]:
        """
        List all available statistics, optionally filtered.
        
        Args:
            position: Filter by position (e.g., "QB", "RB")
            data_source: Filter by data source (e.g., "basic", "nextgen")
        
        Returns:
            List of stat names
        
        Examples:
            >>> engine.list_available_stats(position="QB")
            ['passer_rating', 'adj_yards_per_attempt', ...]
            
            >>> engine.list_available_stats(data_source="nextgen")
            ['completion_pct_above_expectation', 'rushing_efficiency', ...]
        """
        available = []
        
        for stat_name, config in self.STAT_REGISTRY.items():
            # Filter by position
            if position is not None:
                allowed_positions = config.get("positions", [])
                if allowed_positions and position not in allowed_positions:
                    continue
            
            # Filter by data source
            if data_source is not None:
                if config["data_source"] != data_source:
                    continue
            
            available.append(stat_name)
        
        return sorted(available)
    
    def get_stat_info(self, stat_name: str) -> Dict[str, Any]:
        """
        Get detailed information about a specific statistic.
        
        Args:
            stat_name: Name of the statistic
        
        Returns:
            Dictionary with stat configuration details
        
        Examples:
            >>> engine.get_stat_info("passer_rating")
            {
                'module': 'stats.passing',
                'function': 'calculate_passer_rating',
                'data_source': 'basic',
                'positions': ['QB']
            }
        """
        if stat_name not in self.STAT_REGISTRY:
            raise ValueError(f"Unknown stat: {stat_name}")
        
        return self.STAT_REGISTRY[stat_name].copy()

