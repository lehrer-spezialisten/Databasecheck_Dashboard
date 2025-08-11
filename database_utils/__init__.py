"""
Database utilities package.
Contains shared database connection and configuration code.
"""

from .config import load_environment
from .database import (
    create_mysql_connection,
    close_database_connection,
    test_database_connection,
    table_exists
)
from .email_utils import (
    send_email,
    send_missing_table_notification
)

__all__ = [
    'load_environment',
    'create_mysql_connection',
    'close_database_connection',
    'test_database_connection',
    'table_exists',
    'send_email',
    'send_missing_table_notification'
]
