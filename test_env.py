#!/usr/bin/env python3
"""
Database Connection Tester
Tests connection to MySQL database using environment variables from .env file
"""

import os
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Load environment variables from .env file
load_dotenv()

def send_alert_email(subject: str, body: str) -> bool:
    """Send an email using Gmail SMTP. Requires env vars.
    Uses DEFAULT_ALERT_EMAIL or ALERT_EMAIL as recipient.
    """
    gmail_user = os.getenv('GMAIL_USER')
    gmail_app_password = os.getenv('GMAIL_APP_PASSWORD')
    to_email = os.getenv('DEFAULT_ALERT_EMAIL') or os.getenv('ALERT_EMAIL')

    if not all([gmail_user, gmail_app_password, to_email]):
        print("⚠️ Email not sent: Missing GMAIL_USER, GMAIL_APP_PASSWORD, or DEFAULT_ALERT_EMAIL/ALERT_EMAIL.")
        return False

    try:
        msg = MIMEMultipart()
        msg['From'] = gmail_user
        msg['To'] = to_email
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))

        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(gmail_user, gmail_app_password)
        server.send_message(msg)
        server.quit()
        print(f"📧 Alert email sent to {to_email}")
        return True
    except Exception as e:
        print(f"❌ Failed to send alert email: {e}")
        return False

def create_db_connection():
    """Create and return a database connection using environment variables."""
    try:
        ca = os.getenv('DB_SSL_CA')
        conn_kwargs = dict(
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            host=os.getenv('DB_HOST'),
            port=int(os.getenv('DB_PORT', '3306')),
            database=os.getenv('DB_NAME'),
        )
        # DigitalOcean Managed Databases: use SSL but disable verification.
        # Attach CA if provided and path exists, but keep verification off to avoid strict checks.
        conn_kwargs.update(
            ssl_verify_cert=False,
            ssl_verify_identity=False,
        )
        if ca:
            import os as _os
            if _os.path.exists(ca):
                conn_kwargs['ssl_ca'] = ca
            else:
                print(f"Warning: SSL CA certificate not found at {ca}. Continuing without CA.")
        # Only disable SSL entirely if explicitly requested
        if os.getenv('DB_SSL_DISABLED', 'false').lower() == 'true':
            conn_kwargs['ssl_disabled'] = True

        connection = mysql.connector.connect(**conn_kwargs)
        return connection
    except Error as e:
        print(f"Error connecting to database: {e}")
        # Send alert email on connection error
        subject = "Database Connection Test FAILED"
        body = f"An error occurred while connecting to the database:\n\n{e}\n\nHost: {os.getenv('DB_HOST')}\nDB: {os.getenv('DB_NAME')}\n"
        send_alert_email(subject, body)
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
                # Optional: check for a specific table and alert if missing
                check_table = os.getenv('CHECK_TABLE_NAME')
                if check_table:
                    schema = os.getenv('CHECK_TABLE_SCHEMA') or os.getenv('DB_NAME')
                    print(f"Table check enabled. CHECK_TABLE_NAME='{check_table}', schema='{schema}'")
                    try:
                        cursor.execute(
                            """
                            SELECT COUNT(*) FROM information_schema.tables
                            WHERE table_schema = %s AND table_name = %s
                            """,
                            (schema, check_table)
                        )
                        exists = cursor.fetchone()[0] > 0
                        if not exists:
                            subject = "Database Warning: Table Missing"
                            body = (
                                f"The table '{check_table}' was NOT found in schema '{schema}'.\n"
                                f"Host: {os.getenv('DB_HOST')}\nDB: {os.getenv('DB_NAME')}\n"
                            )
                            send_alert_email(subject, body)
                            print(f"⚠️ Table '{check_table}' not found in schema '{schema}'. Alert sent.")
                        else:
                            print(f"✅ Table '{check_table}' exists in schema '{schema}'.")
                    except Exception as e:
                        print(f"❌ Failed to verify table existence: {e}")
                        send_alert_email(
                            "Database Warning: Table Check Error",
                            f"Failed to verify table '{check_table}' existence due to error: {e}"
                        )
                else:
                    print("Table check skipped: CHECK_TABLE_NAME not set.")
                # Optional: send success email if explicitly requested via env flag
                if os.getenv('SEND_SUCCESS_EMAIL') == '1':
                    send_alert_email(
                        "Database Connection Test SUCCESS",
                        f"Successfully connected to database: {db_info[0]}"
                    )
                return True
        except Error as e:
            print(f"❌ Database error: {e}")
            # Send alert email on database error
            subject = "Database Connection Test FAILED (query)"
            body = f"A database error occurred after connecting:\n\n{e}"
            send_alert_email(subject, body)
            return False
        finally:
            if conn and conn.is_connected():
                cursor.close()
                conn.close()
                print("Connection closed.")
    else:
        print("❌ Failed to create database connection")
        # If we reached here without an Error (unlikely), still send an alert
        subject = "Database Connection Test FAILED"
        body = "Failed to create database connection for unknown reasons."
        send_alert_email(subject, body)
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
        # Send alert if env is incomplete
        send_alert_email(
            "Database Connection Test FAILED (env)",
            "Missing required environment variables: " + ", ".join(missing_vars)
        )
    else:
        test_connection()