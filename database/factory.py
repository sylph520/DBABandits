"""
Database adapter factory for creating appropriate database connections.
"""

import configparser
from typing import Dict, Any

import constants
from database.base import DatabaseInterface

# Import adapters with graceful fallback for missing dependencies
try:
    from database.adapters.mssql_adapter import MSSQLAdapter
except ImportError:
    MSSQLAdapter = None

try:
    from database.adapters.postgresql_adapter import PostgreSQLAdapter
except ImportError:
    PostgreSQLAdapter = None

try:
    from database.adapters.hypopg_adapter import HypoPGAdapter
except ImportError:
    HypoPGAdapter = None


# Registry of available database adapters
ADAPTER_REGISTRY = {}

# Register MSSQL if available
if MSSQLAdapter:
    ADAPTER_REGISTRY['mssql'] = MSSQLAdapter
    ADAPTER_REGISTRY['sqlserver'] = MSSQLAdapter

# Register PostgreSQL with HypoPG as default (for hypothetical indexes support)
if HypoPGAdapter:
    # HypoPG is default for PostgreSQL (supports hypothetical indexes)
    ADAPTER_REGISTRY['postgresql'] = HypoPGAdapter
    ADAPTER_REGISTRY['postgres'] = HypoPGAdapter
    ADAPTER_REGISTRY['hypopg'] = HypoPGAdapter
elif PostgreSQLAdapter:
    # Fallback to regular PostgreSQL if HypoPG not available
    ADAPTER_REGISTRY['postgresql'] = PostgreSQLAdapter
    ADAPTER_REGISTRY['postgres'] = PostgreSQLAdapter


def create_db_adapter(db_type: str, connection_params: Dict[str, Any]) -> DatabaseInterface:
    """
    Factory function to create a database adapter.
    
    Args:
        db_type: Type of database ('mssql', 'postgresql', etc.)
        connection_params: Dictionary with connection parameters
        
    Returns:
        DatabaseInterface implementation
        
    Raises:
        ValueError: If database type is not supported
    """
    db_type = db_type.lower()
    
    if db_type not in ADAPTER_REGISTRY:
        available = ', '.join(ADAPTER_REGISTRY.keys())
        raise ValueError(f"Unsupported database type '{db_type}'. "
                        f"Available: {available}")
    
    adapter_class = ADAPTER_REGISTRY[db_type]
    return adapter_class(connection_params)


def create_db_adapter_from_config(config_path: str = None) -> DatabaseInterface:
    """
    Create database adapter from configuration file.
    
    Args:
        config_path: Path to config file (default: constants.DB_CONFIG)
        
    Returns:
        Configured DatabaseInterface implementation
    """
    if config_path is None:
        config_path = constants.ROOT_DIR + constants.DB_CONFIG
    
    # Read configuration
    db_config = configparser.ConfigParser()
    db_config.read(config_path)
    
    db_type = db_config['SYSTEM']['db_type']
    
    # Build connection parameters
    params = {
        'server': db_config[db_type]['server'],
        'database': db_config[db_type]['database'],
        'schema': db_config[db_type].get('schema', 'dbo'),
        'driver': db_config[db_type].get('driver', 'ODBC Driver 18 for SQL Server'),
        'username': db_config[db_type].get('username', 'sa'),
        'password': db_config[db_type].get('password', 'Sql123456'),
    }
    
    return create_db_adapter(db_type, params)


def register_adapter(db_type: str, adapter_class: type):
    """
    Register a new database adapter at runtime.
    
    Args:
        db_type: Database type identifier
        adapter_class: Class implementing DatabaseInterface
    """
    ADAPTER_REGISTRY[db_type.lower()] = adapter_class


def list_supported_databases():
    """Return list of supported database types."""
    return list(ADAPTER_REGISTRY.keys())


def create_db_adapter_with_params(
    db_type: str,
    server: str,
    database: str,
    username: str,
    password: str,
    schema: str = None,
    port: int = None,
    driver: str = None,
    **extra_params
) -> DatabaseInterface:
    """
    Create database adapter with explicit parameters.
    
    This allows creating adapters dynamically without editing config files.
    Useful for switching databases at runtime or connecting to multiple DBs.
    
    Args:
        db_type: Database type ('mssql', 'postgresql', etc.)
        server: Database server hostname
        database: Database name
        username: Database username
        password: Database password
        schema: Database schema (default: depends on DB)
        port: Database port (default: depends on DB)
        driver: Driver name (for ODBC connections)
        **extra_params: Additional database-specific parameters
        
    Returns:
        DatabaseInterface implementation
        
    Example:
        # Connect to MSSQL
        db_mssql = create_db_adapter_with_params(
            db_type='mssql',
            server='localhost',
            database='tpch',
            username='sa',
            password='password',
            schema='dbo'
        )
        
        # Connect to PostgreSQL  
        db_postgres = create_db_adapter_with_params(
            db_type='postgresql',
            server='localhost',
            database='tpch',
            username='postgres',
            password='password',
            schema='public',
            port=51204
        )
    """
    params = {
        'server': server,
        'database': database,
        'username': username,
        'password': password,
    }
    
    # Set defaults based on database type
    if schema is None:
        if db_type.lower() in ['mssql', 'sqlserver']:
            schema = 'dbo'
        elif db_type.lower() in ['postgresql', 'postgres']:
            schema = 'public'
        else:
            schema = 'public'
    
    params['schema'] = schema
    
    if port is not None:
        params['port'] = port
    
    if driver is not None:
        params['driver'] = driver
    
    # Merge extra params
    params.update(extra_params)
    
    return create_db_adapter(db_type, params)


def create_db_adapter_from_config_with_overrides(
    config_path: str = None,
    db_type: str = None,
    **param_overrides
) -> DatabaseInterface:
    """
    Create database adapter from config with optional overrides.
    
    This allows using the config file as a base but dynamically changing
    specific parameters or even the database type.
    
    Args:
        config_path: Path to config file (default: constants.DB_CONFIG)
        db_type: Override the database type from config
        **param_overrides: Override any connection parameter
        
    Returns:
        DatabaseInterface implementation
        
    Example:
        # Use config but connect to different database
        db = create_db_adapter_from_config_with_overrides(
            database='tpch_test'  # Override just the database name
        )
        
        # Use config but switch to PostgreSQL
        db = create_db_adapter_from_config_with_overrides(
            db_type='postgresql',
            port=5432,
            schema='public'
        )
    """
    if config_path is None:
        config_path = constants.ROOT_DIR + constants.DB_CONFIG
    
    # Read configuration
    db_config = configparser.ConfigParser()
    db_config.read(config_path)
    
    # Determine database type
    config_db_type = db_config['SYSTEM']['db_type']
    actual_db_type = db_type if db_type else config_db_type
    
    # Get base parameters from config
    params = {
        'server': db_config[config_db_type]['server'],
        'database': db_config[config_db_type]['database'],
        'username': db_config[config_db_type].get('username', 'sa'),
        'password': db_config[config_db_type].get('password', 'Sql123456'),
        'schema': db_config[config_db_type].get('schema', 'dbo'),
        'driver': db_config[config_db_type].get('driver', 'ODBC Driver 18 for SQL Server'),
    }
    
    # Apply overrides
    params.update(param_overrides)
    
    return create_db_adapter(actual_db_type, params)
