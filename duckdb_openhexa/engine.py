"""
DuckDB OpenHexa Engine Spec - Minimal engine spec for duckdb_oh:// dialect
"""

import logging
from typing import Any, Optional, Dict
from datetime import datetime

from superset.db_engine_specs.duckdb import DuckDBEngineSpec

logger = logging.getLogger(__name__)


class DuckDBPlusEngineSpec(DuckDBEngineSpec):
    """
    Minimal custom DuckDB engine spec for the duckdb_oh:// dialect.
    
    Per-user connection pooling is handled by connection_pool.py patching
    duckdb_engine.Dialect.connect(), so this class only needs to:
    1. Define the engine name/dialect
    2. Inherit all functionality from DuckDBEngineSpec
    
    Uses the "duckdb_oh://" dialect, separate from standard "duckdb://".
    """
    
    engine = "duckdb_oh"
    engine_name = "DuckDB OpenHexa"
    
    @classmethod
    def convert_dttm(
        cls, target_type: str, dttm: datetime, db_extra: Optional[Dict[str, Any]] = None
    ) -> Optional[str]:
        """Use parent implementation."""
        return super().convert_dttm(target_type, dttm, db_extra)
