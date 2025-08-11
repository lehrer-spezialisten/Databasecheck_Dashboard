"""
Configuration utilities for database connections.
Handles loading environment variables and configuration.
"""
import os
from typing import Dict, Optional
from dotenv import load_dotenv

def load_environment(env_file: str = '.env') -> bool:
    """
    Load environment variables from a .env file if it exists.
    
    Args:
        env_file: Path to the .env file
        
    Returns:
        bool: True if .env file was loaded successfully, False otherwise
    """
    try:
        # Check if file exists before trying to load it
        if os.path.exists(env_file):
            return load_dotenv(env_file)
        else:
            print(f"Info: {env_file} not found. Using environment variables from the system.")
            return False
    except Exception as e:
        print(f"Warning: Error loading .env file: {e}. Using environment variables from the system.")
        return False

def get_database_config() -> Dict[str, str]:
    """
    Get database configuration from environment variables with sensible defaults.
    
    Returns:
        Dict containing database configuration with defaults for optional parameters
    """
    # First try to load environment from .env file
    load_environment()
    
    # Get all configuration with defaults
    config = {
        'user': os.getenv('DB_USER', 'root'),
        'password': os.getenv('DB_PASSWORD', ''),
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': os.getenv('DB_PORT', '3306'),
        'database': os.getenv('DB_NAME', 'test'),
        'ssl_ca': os.getenv('DB_SSL_CA', ''),
        'ssl_disabled': os.getenv('DB_SSL_DISABLED', 'true')  # Default to disabled for better compatibility
    }
    
    # Print debug info (will be visible in logs)
    print(f"Database configuration loaded - Host: {config['host']}, Database: {config['database']}, SSL: {'disabled' if config['ssl_disabled'].lower() == 'true' else 'enabled'}")
    
    return config

def check_required_vars(required_vars: list) -> tuple[bool, list[str]]:
    """
    Check if required environment variables are set.
    
    Args:
        required_vars: List of required environment variable names
        
    Returns:
        tuple: (bool: all_vars_present, list: missing_vars)
    """
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    return (len(missing_vars) == 0, missing_vars)
