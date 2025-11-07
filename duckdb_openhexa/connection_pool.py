"""Connection pool for DuckDB to maintain HTTP cache across SQLAlchemy sessions."""

import logging
import os
from threading import Lock
from typing import Dict, Optional

import duckdb

logger = logging.getLogger(__name__)

# Global connection pool: database_path -> connection
_connection_pool: Dict[str, duckdb.DuckDBPyConnection] = {}
_pool_lock = Lock()


def get_or_create_connection(database_path: str, config: Optional[dict] = None) -> duckdb.DuckDBPyConnection:
    """Get or create a persistent DuckDB connection from the pool.
    
    This maintains a single connection per database file, allowing
    DuckDB's internal HTTP cache to persist across SQLAlchemy sessions.
    """
    with _pool_lock:
        # Check if we have a connection for this database
        if database_path in _connection_pool:
            conn = _connection_pool[database_path]
            try:
                # Verify connection is still alive
                conn.execute("SELECT 1")
                logger.debug(f"Reusing pooled connection for {database_path}")
                return conn
            except Exception as e:
                logger.warning(f"Pooled connection dead, removing: {e}")
                del _connection_pool[database_path]
        
        # Create new connection
        logger.info(f"Creating new pooled connection for {database_path}")
        
        # Merge config with environment variables
        final_config = {}
        if temp_dir := os.getenv("DUCKDB_TEMP_DIR"):
            final_config["temp_directory"] = temp_dir
        if memory_limit := os.getenv("DUCKDB_MEMORY_LIMIT"):
            final_config["memory_limit"] = memory_limit
        if threads := os.getenv("DUCKDB_THREADS"):
            final_config["threads"] = int(threads)
        
        # Merge with provided config (provided config takes precedence)
        if config:
            final_config.update(config)
        
        # duckdb.connect expects database as first positional arg, not kwarg
        if final_config:
            conn = duckdb.connect(database_path, config=final_config)
        else:
            conn = duckdb.connect(database_path)
        
        # Configure HTTP caching for optimal performance
        conn.execute("SET http_keep_alive=true")
        conn.execute("SET enable_http_metadata_cache=true")
        logger.info(f"Configured HTTP caching for pooled connection to {database_path}")
        
        _connection_pool[database_path] = conn
        return conn


def patch_duckdb_engine_dialect():
    """Patch duckdb_engine.Dialect to use our connection pool."""
    try:
        import duckdb_engine
        from duckdb_engine import Dialect
        
        logger.info("Attempting to patch duckdb_engine.Dialect.connect...")
        
        # Store original connect method
        _original_connect = Dialect.connect
        logger.debug(f"Original connect method: {_original_connect}")
        
        def patched_connect(self, *cargs, **cparams):
            """Intercept connection creation to use our pool."""
            # Extract database path from connection args
            # duckdb-engine passes it as first positional arg or in cparams
            database_path = None
            if cargs:
                database_path = cargs[0]
            elif "database" in cparams:
                database_path = cparams["database"]
            
            logger.debug(f"Dialect.connect called with database_path={database_path}, cargs={cargs}, cparams={cparams}")
            
            # If we have a database path (not :memory:), use our pool
            if database_path and database_path != ":memory:":
                logger.info(f"Using connection pool for database: {database_path}")
                
                # Import required functions from duckdb_engine
                from duckdb_engine import ConnectionWrapper, get_core_config, apply_config
                
                # Process config like original code does
                core_keys = get_core_config()
                preload_extensions = cparams.pop("preload_extensions", [])
                config = dict(cparams.get("config", {}))
                cparams["config"] = config
                config.update(cparams.pop("url_config", {}))
                
                # Extract ext config (non-core keys) before passing to pool
                ext = {k: config.pop(k) for k in list(config) if k not in core_keys}
                
                # Get pooled connection (pass core config only, ext will be applied later via apply_config)
                # Make a copy of config since we already popped ext keys
                core_config = dict(config)
                pooled_conn = get_or_create_connection(database_path, config=core_config)
                
                # Apply any extensions/filesystems that were requested (from original logic)
                for extension in preload_extensions:
                    pooled_conn.execute(f"LOAD {extension}")
                
                filesystems = cparams.pop("register_filesystems", [])
                for filesystem in filesystems:
                    pooled_conn.register_filesystem(filesystem)
                
                # Apply config like original code does
                apply_config(self, pooled_conn, ext)
                
                # Create a custom wrapper that prevents connection from being closed
                # This allows SQLAlchemy to "close" it without actually closing the pooled connection
                class PooledConnectionWrapper(ConnectionWrapper):
                    """ConnectionWrapper that doesn't actually close pooled connections."""
                    def __init__(self, c: duckdb.DuckDBPyConnection, pool_key: str):
                        super().__init__(c)
                        self._pool_key = pool_key
                        self._is_pooled = True
                    
                    def close(self) -> None:
                        """Don't actually close pooled connections, just mark as closed for SQLAlchemy."""
                        # Mark as closed for SQLAlchemy's benefit
                        self.closed = True
                        # But DON'T actually close the underlying connection
                        # so it can be reused from the pool
                        logger.debug(f"ConnectionWrapper.close() called but connection kept alive for pool reuse (key: {self._pool_key})")
                
                # Return custom wrapper that prevents real closure
                return PooledConnectionWrapper(pooled_conn, database_path)
            
            # For in-memory or unknown cases, use original
            logger.debug("Using original connect method (in-memory or unknown path)")
            return _original_connect(self, *cargs, **cparams)
        
        # Apply the patch
        Dialect.connect = patched_connect
        logger.info("✅ Successfully patched duckdb_engine.Dialect.connect to use connection pool")
        
        # Verify the patch
        if Dialect.connect == patched_connect:
            logger.info("✅ Patch verification: connect method successfully replaced")
        else:
            logger.warning("⚠️ Patch verification failed: connect method not replaced")
        
    except ImportError as e:
        logger.warning(f"duckdb_engine not found, skipping connection pool patch: {e}")
    except Exception as e:
        logger.error(f"Failed to patch duckdb_engine.Dialect: {e}", exc_info=True)

