"""
Database connection utilities.
Provides functions for creating and managing database connections.
"""
import mysql.connector
from mysql.connector import Error
from typing import Optional, Dict, Any

def create_mysql_connection(config: Optional[Dict[str, Any]] = None) -> Optional[mysql.connector.connection.MySQLConnection]:
    """
    Create a MySQL database connection.
    
    Args:
        config: Dictionary containing connection parameters.
               If None, will try to load from environment variables.
               
    Returns:
        MySQLConnection: A database connection object if successful, None otherwise.
    """
    if config is None:
        from .config import get_database_config
        config = get_database_config()
    
    # Prepare connection arguments
    conn_args = {
        'user': config['user'],
        'password': config['password'],
        'host': config['host'],
        'port': int(config['port']),
        'database': config['database']
    }
    
    # Handle SSL configuration
    ssl_disabled = str(config.get('ssl_disabled', 'false')).lower() == 'true'
    
    if ssl_disabled:
        conn_args['ssl_disabled'] = True
    else:
        # Try to use SSL if configured, but don't fail if there are issues
        try:
            # For DigitalOcean Managed Databases, we need to use the CA cert but disable verification
            conn_args.update({
                'ssl_verify_identity': False,
                'ssl_verify_cert': False
            })
            
            # If a CA cert is provided, use it but still skip verification
            if 'ssl_ca' in config and config['ssl_ca']:
                import os
                if os.path.exists(config['ssl_ca']):
                    conn_args['ssl_ca'] = config['ssl_ca']
                else:
                    print(f"Warning: SSL CA certificate not found at {config['ssl_ca']}. Proceeding without SSL.")
                    conn_args['ssl_disabled'] = True
        except Exception as e:
            print(f"Warning: Error configuring SSL, proceeding without SSL. Error: {e}")
            conn_args['ssl_disabled'] = True
    
    try:
        connection = mysql.connector.connect(**conn_args)
        return connection
    except Error as e:
        print(f"Error connecting to database: {e}")
        return None

def close_database_connection(connection) -> None:
    """
    Close the database connection if it exists and is connected.
    
    Args:
        connection: Database connection object to close
    """
    if connection and connection.is_connected():
        connection.close()
        print("Database connection closed.")

def test_database_connection(connection = None) -> bool:
    """
    Test a database connection.
    
    Args:
        connection: Existing database connection. If None, creates a new one.
        
    Returns:
        bool: True if connection test was successful, False otherwise
    """
    close_after = False
    if connection is None:
        connection = create_mysql_connection()
        close_after = True
        if connection is None:
            return False
    
    try:
        if connection.is_connected():
            cursor = connection.cursor()
            cursor.execute("SELECT DATABASE(), VERSION()")
            db_info = cursor.fetchone()
            print(f"[SUCCESS] Connected to database: {db_info[0]}")
            print(f"[SUCCESS] Database version: {db_info[1]}")
            return True
    except Error as e:
        print(f"❌ Database error: {e}")
        return False
    finally:
        if close_after and connection:
            close_database_connection(connection)

def table_exists(connection, table_name: str) -> bool:
    """
    Check if a table exists in the database.
    
    Args:
        connection: Active database connection
        table_name: Name of the table to check
        
    Returns:
        bool: True if table exists, False otherwise
    """
    if not connection or not connection.is_connected():
        print("[ERROR] No active database connection")
        return False
    
    try:
        cursor = connection.cursor()
        cursor.execute("""
            SELECT COUNT(*)
            FROM information_schema.tables 
            WHERE table_schema = DATABASE() 
            AND table_name = %s
        """, (table_name,))
        
        return cursor.fetchone()[0] > 0
    except Error as e:
        print(f"[ERROR] Error checking for table {table_name}: {e}")
        return False
