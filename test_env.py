#!/usr/bin/env python3
"""
Database Connection Tester
Tests connection to MySQL database using environment variables from .env file
"""

import os
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def create_db_connection():
    """Create and return a database connection using environment variables."""
    try:
        connection = mysql.connector.connect(
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            host=os.getenv('DB_HOST'),
            port=int(os.getenv('DB_PORT', '3306')),
            database=os.getenv('DB_NAME'),
            ssl_ca=os.getenv('DB_SSL_CA'),
            ssl_verify_cert=True
        )
        return connection
    except Error as e:
        print(f"Error connecting to database: {e}")
        return None

def test_connection():
    """Test the database connection and print status."""
    print("Testing database connection...")
    conn = create_db_connection()
    
    if conn:
        try:
            if conn.is_connected():
                cursor = conn.cursor()
                cursor.execute("SELECT DATABASE();")
                db_info = cursor.fetchone()
                print(f"✅ Successfully connected to database: {db_info[0]}")
                return True
        except Error as e:
            print(f"❌ Database error: {e}")
            return False
        finally:
            if conn and conn.is_connected():
                cursor.close()
                conn.close()
                print("Connection closed.")
    else:
        print("❌ Failed to create database connection")
        return False

if __name__ == "__main__":
    # Verify required environment variables are set
    required_vars = ['DB_USER', 'DB_PASSWORD', 'DB_HOST', 'DB_NAME']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print("❌ Missing required environment variables:")
        for var in missing_vars:
            print(f" - {var}")
        print("\nPlease check your .env file")
    else:
        test_connection()