"""
Database adapters package.
"""

# Import adapters with error handling for missing dependencies
try:
    from database.adapters.mssql_adapter import MSSQLAdapter
except ImportError:
    MSSQLAdapter = None

try:
    from database.adapters.postgresql_adapter import PostgreSQLAdapter
except ImportError:
    PostgreSQLAdapter = None

__all__ = ['MSSQLAdapter', 'PostgreSQLAdapter']
