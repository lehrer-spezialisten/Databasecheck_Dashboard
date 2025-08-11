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
        # If a CA path is provided, enable SSL verification; otherwise, disable SSL for easier connectivity
        if ca:
            conn_kwargs.update(
                ssl_ca=ca,
                ssl_verify_cert=True,
                ssl_verify_identity=True,
            )
        else:
            # Disable SSL when no CA is provided (useful for quick testing/troubleshooting)
            conn_kwargs.update(ssl_disabled=True)

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