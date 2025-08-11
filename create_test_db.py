#!/usr/bin/env python3
"""
Create an empty test SQLite database for testing the DatabaseTableChecker
"""
import sqlite3
import os

# Create test directory if it doesn't exist
os.makedirs("test_data", exist_ok=True)

# Create an empty SQLite database file
conn = sqlite3.connect("test_data/test_db.sqlite")
conn.close()

print("Created empty test database at test_data/test_db.sqlite")
