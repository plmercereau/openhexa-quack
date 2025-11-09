"""Custom DuckDB dialect with per-user connection pooling and UDF registration.

This dialect is exclusively for duckdb_oh:// URIs and does not affect standard duckdb:// connections.
"""

import logging
import os
import time
from threading import Lock
from typing import Dict, Tuple

import duckdb
from duckdb_engine import Dialect, ConnectionWrapper, get_core_config, apply_config

logger = logging.getLogger(__name__)

# Connection TTL configuration
_CONNECTION_TTL_SECONDS = int(os.getenv("DUCKDB_OH_CONNECTION_TTL_MINUTES", "60")) * 60
logger.info(f"Connection TTL set to {_CONNECTION_TTL_SECONDS}s ({_CONNECTION_TTL_SECONDS/60:.0f}min)")


class DuckDBOpenHexaDialect(Dialect):
    """
    Custom DuckDB dialect with per-user connection pooling and automatic UDF registration.
    
    This dialect maintains one persistent connection per (user, database) pair to enable:
    - DuckDB's HTTP metadata cache persistence across queries
    - Parquet metadata cache reuse
    - Automatic registration of custom UDFs (get_dataset_file_url, openhexa_dataset_files)
    
    Only affects duckdb_oh:// connections, leaving standard duckdb:// unmodified.
    """
    
    # Class-level connection pool: (user_id, database_path) -> (connection, timestamp)
    _connection_pool: Dict[Tuple[str, str], Tuple[duckdb.DuckDBPyConnection, float]] = {}
    _pool_lock = Lock()
    
    @classmethod
    def _get_or_create_connection(cls, user_id: str, database_path: str) -> duckdb.DuckDBPyConnection:
        """Get or create a persistent per-user DuckDB connection from the pool.
        
        Args:
            user_id: User identifier (username or "default")
            database_path: Path to database file
            
        Returns:
            Persistent DuckDB connection with UDFs registered
        """
        cache_key = (user_id, database_path)
        
        with cls._pool_lock:
            # Check if we have a connection for this user+database
            if cache_key in cls._connection_pool:
                conn, created_at = cls._connection_pool[cache_key]
                age_seconds = time.time() - created_at
                
                # Check if connection has expired
                if age_seconds > _CONNECTION_TTL_SECONDS:
                    logger.info(f"Connection expired for user={user_id} (age: {age_seconds:.1f}s > TTL: {_CONNECTION_TTL_SECONDS}s), recreating")
                    try:
                        conn.close()
                    except Exception:
                        pass
                    del cls._connection_pool[cache_key]
                else:
                    try:
                        # Verify connection is still alive
                        conn.execute("SELECT 1")
                        logger.debug(f"Reusing pooled connection for user={user_id}, db={database_path} (age: {age_seconds:.1f}s)")
                        return conn
                    except Exception as e:
                        logger.warning(f"Pooled connection dead, removing: {e}")
                        del cls._connection_pool[cache_key]
            
            # Create new connection
            logger.info(f"Creating new pooled connection for user={user_id}, db={database_path}")
            conn = duckdb.connect(database_path)
            
            # Configure HTTP caching for optimal performance
            conn.execute("SET http_keep_alive=true")
            conn.execute("SET enable_http_metadata_cache=true")
            
            # Install required extensions
            conn.execute("INSTALL httpfs; LOAD httpfs;")
            conn.execute("INSTALL parquet; LOAD parquet;")
            
            # Register UDFs
            cls._register_udfs(conn)
            
            logger.info(f"Configured HTTP caching, extensions, and UDFs for pooled connection")
            
            cls._connection_pool[cache_key] = (conn, time.time())
            return conn
    
    @staticmethod
    def _register_udfs(conn: duckdb.DuckDBPyConnection) -> None:
        """Register custom UDFs on a DuckDB connection.
        
        Args:
            conn: DuckDB connection to register UDFs on
        """
        try:
            from duckdb_openhexa.functions import get_dataset_file_url, openhexa_dataset_files
            
            # Register scalar function with SPECIAL null_handling to allow NULL returns
            conn.create_function(
                "get_dataset_file_url",
                get_dataset_file_url,
                side_effects=True,
                null_handling="special",
            )
            
            # Note: openhexa_dataset_files is a table function that needs special handling
            # It's registered via execute-time interception if needed
            
            logger.debug("Registered UDFs on connection")
        except Exception as e:
            logger.error(f"Failed to register UDFs: {e}", exc_info=True)
    
    def connect(self, *cargs, **cparams):
        """
        Override connect method to use per-user connection pooling.
        
        This method intercepts connection creation and routes file-based databases
        to the connection pool, while allowing in-memory databases to use standard behavior.
        """
        # Extract database path from connection args
        database_path = None
        if cargs:
            database_path = cargs[0]
        elif "database" in cparams:
            database_path = cparams["database"]
        
        logger.debug(f"DuckDBOpenHexaDialect.connect called with database_path={database_path}")
        
        # If we have a database path (not :memory:), use our pool
        if database_path and database_path != ":memory:":
            # Get user_id from Flask context (only once per connection request)
            user_id = "default"
            try:
                from flask import g
                if hasattr(g, 'user') and g.user:
                    user_id = g.user.username
            except Exception:
                pass
            
            logger.info(f"Using per-user connection pool for user={user_id}, db={database_path}")
            
            # Process config like original Dialect does
            core_keys = get_core_config()
            preload_extensions = cparams.pop("preload_extensions", [])
            config = dict(cparams.get("config", {}))
            cparams["config"] = config
            config.update(cparams.pop("url_config", {}))
            
            # Extract ext config (non-core keys)
            ext = {k: config.pop(k) for k in list(config) if k not in core_keys}
            
            # Get pooled connection (efficient - lock only on miss)
            pooled_conn = self._get_or_create_connection(user_id, database_path)
            
            # Apply any extensions/filesystems that were requested
            for extension in preload_extensions:
                pooled_conn.execute(f"LOAD {extension}")
            
            filesystems = cparams.pop("register_filesystems", [])
            for filesystem in filesystems:
                pooled_conn.register_filesystem(filesystem)
            
            # Apply config like original Dialect does
            apply_config(self, pooled_conn, ext)
            
            # Create pool key for wrapper
            pool_key = (user_id, database_path)
            
            # Create a thin wrapper that prevents connection from being closed
            # This is much faster than using __getattr__ delegation
            class PooledConnectionWrapper(ConnectionWrapper):
                """Thin wrapper that only overrides close() - no __getattr__ overhead."""
                def __init__(self, c: duckdb.DuckDBPyConnection, key: Tuple[str, str]):
                    super().__init__(c)
                    self._pool_key = key
                
                def close(self) -> None:
                    """Don't actually close pooled connections, just mark as closed."""
                    self.closed = True
                    # Connection stays alive in pool for reuse
                    logger.debug(f"ConnectionWrapper.close() called but connection kept alive (user={self._pool_key[0]})")
            
            # Return thin wrapper that prevents real closure
            return PooledConnectionWrapper(pooled_conn, pool_key)
        
        # For in-memory or unknown cases, use parent's connect method
        logger.debug("Using parent Dialect.connect method (in-memory or unknown path)")
        return super().connect(*cargs, **cparams)

