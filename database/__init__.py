"""
Database module for DBA Bandits.

This module provides database abstraction through the DatabaseInterface,
enabling support for multiple database backends.
"""

from database.base import DatabaseInterface, QueryPlanInfo, TableInfo, ColumnInfo, IndexUsage
from database.factory import (
    create_db_adapter, 
    create_db_adapter_from_config, 
    create_db_adapter_with_params,
    create_db_adapter_from_config_with_overrides,
    list_supported_databases,
    register_adapter
)

__all__ = [
    'DatabaseInterface',
    'QueryPlanInfo',
    'TableInfo', 
    'ColumnInfo',
    'IndexUsage',
    'create_db_adapter',
    'create_db_adapter_from_config',
    'create_db_adapter_with_params',
    'create_db_adapter_from_config_with_overrides',
    'list_supported_databases',
    'register_adapter',
]
