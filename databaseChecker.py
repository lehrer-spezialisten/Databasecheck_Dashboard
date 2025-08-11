#!/usr/bin/env python3
"""
Database Table Existence Checker with Email Alert
Checks if specified tables exist in databases and sends email if not found.
All credentials and configuration loaded from environment variables.
"""

import sqlite3
import psycopg2
import os
import time
import threading
from datetime import datetime, timedelta
from typing import Optional, Dict, List
import logging
import json
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseTableChecker:
    def __init__(self):
        """
        Initialize the checker with configuration from environment variables.
        """
        # Load Gmail credentials from environment
        self.gmail_user = self._get_required_env('GMAIL_USER')
        self.gmail_password = self._get_required_env('GMAIL_APP_PASSWORD')
        self.gmail_sender = self.gmail_user  # Sender is the same as the Gmail user
        
        # Load monitoring settings from environment
        self.check_interval = int(os.getenv('CHECK_INTERVAL', '300'))  # Default: 5 minutes
        self.alert_cooldown = int(os.getenv('ALERT_COOLDOWN', '3600'))  # Default: 1 hour
        
        # Initialize monitoring state
        self.is_running = False
        self.check_thread = None
        self.checks_config = []
        self.last_alert_times = {}  # Track when we last sent alerts to avoid spam
        
        # Load default alert email address
        self.default_alert_email = os.getenv('DEFAULT_ALERT_EMAIL')
        
        # Load database checks from environment
        self._load_checks_from_env()
        
        logger.info(f"Initialized checker with {len(self.checks_config)} checks")
    
    def _get_required_env(self, var_name: str) -> str:
        """
        Get a required environment variable or raise an error.
        
        Args:
            var_name: Name of the environment variable
            
        Returns:
            The environment variable value
            
        Raises:
            ValueError: If the environment variable is not set
        """
        value = os.getenv(var_name)
        if not value:
            raise ValueError(f"Required environment variable {var_name} is not set")
        return value
    
    def _load_checks_from_env(self) -> None:
        """
        Load database checks from environment variables.
        
        Environment variables should follow the pattern:
        DB_CHECK_1_NAME=User Sessions Table
        DB_CHECK_1_TYPE=sqlite
        DB_CHECK_1_DB_PATH=/app/data/sessions.db
        DB_CHECK_1_TABLE_NAME=user_sessions
        DB_CHECK_1_ALERT_EMAIL_ENV=ALERT_EMAIL_ADMIN
        
        For PostgreSQL:
        DB_CHECK_2_NAME=Orders Table
        DB_CHECK_2_TYPE=postgres
        DB_CHECK_2_HOST=localhost
        DB_CHECK_2_PORT=5432
        DB_CHECK_2_DATABASE=myapp
        DB_CHECK_2_USER_ENV=DB_USER
        DB_CHECK_2_PASSWORD_ENV=DB_PASSWORD
        DB_CHECK_2_TABLE_NAME=orders
        DB_CHECK_2_ALERT_EMAIL_ENV=ALERT_EMAIL_DEV
        
        If ALERT_EMAIL_ENV is not specified, uses DEFAULT_ALERT_EMAIL
        """
        check_num = 1
        
        while True:
            prefix = f"DB_CHECK_{check_num}_"
            name = os.getenv(f"{prefix}NAME")
            
            if not name:
                break  # No more checks to load
            
            check_type = os.getenv(f"{prefix}TYPE")
            table_name = os.getenv(f"{prefix}TABLE_NAME")
            
            # Get alert email from environment variable
            alert_email_env = os.getenv(f"{prefix}ALERT_EMAIL_ENV")
            if alert_email_env:
                alert_email = os.getenv(alert_email_env)
                if not alert_email:
                    logger.error(f"Alert email environment variable '{alert_email_env}' not set for check {check_num}")
                    check_num += 1
                    continue
            else:
                # Fall back to default alert email
                alert_email = self.default_alert_email
                if not alert_email:
                    logger.error(f"No alert email configured for check {check_num} and no DEFAULT_ALERT_EMAIL set")
                    check_num += 1
                    continue
            
            if not all([check_type, table_name, alert_email]):
                logger.error(f"Incomplete configuration for DB_CHECK_{check_num}")
                check_num += 1
                continue
            
            check_config = {
                "name": name,
                "type": check_type,
                "table_name": table_name,
                "alert_email": alert_email
            }
            
            if check_type == 'sqlite':
                db_path = os.getenv(f"{prefix}DB_PATH")
                if not db_path:
                    logger.error(f"Missing DB_PATH for SQLite check {check_num}")
                    check_num += 1
                    continue
                check_config["db_path"] = db_path
                
            elif check_type == 'postgres':
                host = os.getenv(f"{prefix}HOST")
                database = os.getenv(f"{prefix}DATABASE")
                port = int(os.getenv(f"{prefix}PORT", "5432"))
                
                # Get user and password from their own environment variables
                user_env = os.getenv(f"{prefix}USER_ENV", f"DB_CHECK_{check_num}_USER")
                password_env = os.getenv(f"{prefix}PASSWORD_ENV", f"DB_CHECK_{check_num}_PASSWORD")
                
                user = os.getenv(user_env)
                password = os.getenv(password_env)
                
                if not all([host, database, user, password]):
                    logger.error(f"Missing PostgreSQL credentials for check {check_num}")
                    logger.error(f"Required: {prefix}HOST, {prefix}DATABASE, {user_env}, {password_env}")
                    check_num += 1
                    continue
                
                check_config.update({
                    "host": host,
                    "port": port,
                    "database": database,
                    "user": user,
                    "password": password
                })
            
            else:
                logger.error(f"Unsupported database type '{check_type}' for check {check_num}")
                check_num += 1
                continue
            
            self.checks_config.append(check_config)
            logger.info(f"Loaded check {check_num}: {name} ({check_type})")
            check_num += 1
    
    def print_env_template(self) -> None:
        """
        Print a template of required environment variables.
        """
        template = """
# Database Table Checker Environment Variables Template
# Copy this to your .env file and fill in the values

# Gmail Configuration (Required)
GMAIL_USER=your.email@gmail.com
# GMAIL_APP_PASSWORD=your_gmail_app_password  # Generate this in your Google Account settings

# Default Alert Email (Optional - used when no specific alert email is set)
DEFAULT_ALERT_EMAIL=alerts@example.com

# Monitoring Settings (Optional)
CHECK_INTERVAL=300          # Check interval in seconds (default: 300 = 5 minutes)
ALERT_COOLDOWN=3600         # Alert cooldown in seconds (default: 3600 = 1 hour)

# Database Check 1 - SQLite Example
DB_CHECK_1_NAME=User Sessions Table
DB_CHECK_1_TYPE=sqlite
DB_CHECK_1_DB_PATH=/app/data/sessions.db
DB_CHECK_1_TABLE_NAME=user_sessions
DB_CHECK_1_ALERT_EMAIL_ENV=ALERT_EMAIL_ADMIN    # References ALERT_EMAIL_ADMIN env var

# Database Check 2 - PostgreSQL Example
DB_CHECK_2_NAME=Orders Table
DB_CHECK_2_TYPE=postgres
DB_CHECK_2_HOST=localhost
DB_CHECK_2_PORT=5432
DB_CHECK_2_DATABASE=myapp
DB_CHECK_2_USER_ENV=POSTGRES_USER      # Name of env var containing username
# DB_CHECK_2_PASSWORD_ENV=POSTGRES_PASS  # Name of env var containing password
DB_CHECK_2_TABLE_NAME=orders
DB_CHECK_2_ALERT_EMAIL_ENV=ALERT_EMAIL_DEV      # References ALERT_EMAIL_DEV env var

# Alert email addresses
ALERT_EMAIL_ADMIN=admin@example.com    # For critical system alerts
ALERT_EMAIL_DEV=dev@example.com        # For development alerts

# Database credentials
POSTGRES_USER=myuser
# POSTGRES_PASS=your_secure_password_here

# You can add more checks by incrementing the number:
# DB_CHECK_3_NAME=...
# DB_CHECK_3_TYPE=...
# etc.
"""
        print(template)
    
    def should_send_alert(self, check_name: str) -> bool:
        """
        Check if enough time has passed since last alert to avoid spam.
        
        Args:
            check_name: Name of the check
            
        Returns:
            True if alert should be sent, False otherwise
        """
        now = datetime.now()
        last_alert = self.last_alert_times.get(check_name)
        
        if not last_alert:
            return True
            
        return (now - last_alert).total_seconds() > self.alert_cooldown
    
    def check_table_sqlite(self, db_path: str, table_name: str) -> bool:
        """
        Check if table exists in SQLite database.
        
        Args:
            db_path: Path to SQLite database file
            table_name: Name of table to check
            
        Returns:
            True if table exists, False otherwise
        """
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name=?
            """, (table_name,))
            
            result = cursor.fetchone()
            conn.close()
            
            return result is not None
            
        except Exception as e:
            logger.error(f"SQLite error for {db_path}: {e}")
            return False
    
    def check_table_postgres(self, host: str, database: str, user: str, 
                           password: str, table_name: str, port: int = 5432) -> bool:
        """
        Check if table exists in PostgreSQL database.
        
        Args:
            host: Database host
            database: Database name
            user: Username
            password: Password
            table_name: Name of table to check
            port: Database port (default 5432)
            
        Returns:
            True if table exists, False otherwise
        """
        try:
            conn = psycopg2.connect(
                host=host,
                database=database,
                user=user,
                password=password,
                port=port
            )
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = %s
                )
            """, (table_name,))
            
            result = cursor.fetchone()[0]
            conn.close()
            
            return result
            
        except Exception as e:
            logger.error(f"PostgreSQL error for {host}:{port}/{database}: {e}")
            return False
    
    def send_email_alert(self, to_email: str, table_name: str, database_info: str, 
                      check_name: str = None) -> bool:
        """
        Send email alert when table is not found.
        
        Args:
            to_email: Recipient email address
            table_name: Name of missing table
            database_info: Database information for context
            check_name: Name of the check (for cooldown tracking)
            
        Returns:
            True if email sent successfully, False otherwise
        """
        # Check cooldown if check_name provided
        if check_name and not self.should_send_alert(check_name):
            logger.info(f"Skipping alert for '{check_name}' - still in cooldown period")
            return False
            
        try:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            subject = f"⚠️ DATABASE ALERT: Table '{table_name}' not found"
            message_body = f"⚠️ ALERT [{timestamp}]:\n\nTable '{table_name}' not found in database {database_info}.\n\nPlease check immediately."
            
            # Create the email message
            msg = MIMEMultipart()
            msg['From'] = self.gmail_sender
            msg['To'] = to_email
            msg['Subject'] = subject
            
            # Attach the message body
            msg.attach(MIMEText(message_body, 'plain'))
            
            # Connect to Gmail SMTP server and send email
            server = smtplib.SMTP('smtp.gmail.com', 587)
            server.starttls()
            server.login(self.gmail_user, self.gmail_password)
            server.send_message(msg)
            server.quit()
            
            # Update last alert time
            if check_name:
                self.last_alert_times[check_name] = datetime.now()
            
            logger.info(f"Email alert sent successfully to {to_email}")
            return True
            
        except Exception as e:
            logger.error(f"Email sending failed: {e}")
            return False
    
    def perform_single_check(self, check_config: Dict) -> None:
        """
        Perform a single database check based on configuration.
        
        Args:
            check_config: Dictionary containing check configuration
        """
        check_name = check_config.get('name', 'Unnamed Check')
        check_type = check_config.get('type')
        table_name = check_config.get('table_name')
        alert_email = check_config.get('alert_email')
        
        table_exists = False
        db_info = ""
        
        try:
            if check_type == 'sqlite':
                db_path = check_config.get('db_path')
                table_exists = self.check_table_sqlite(db_path, table_name)
                db_info = f"SQLite: {db_path}"
                
            elif check_type == 'postgres':
                table_exists = self.check_table_postgres(
                    check_config['host'],
                    check_config['database'],
                    check_config['user'],
                    check_config['password'],
                    table_name,
                    check_config.get('port', 5432)
                )
                db_info = f"PostgreSQL: {check_config['host']}:{check_config.get('port', 5432)}/{check_config['database']}"
                
            if table_exists:
                logger.info(f"✅ [{check_name}] Table '{table_name}' exists")
            else:
                logger.warning(f"❌ [{check_name}] Table '{table_name}' NOT FOUND")
                self.send_email_alert(alert_email, table_name, db_info, check_name)
                
        except Exception as e:
            logger.error(f"Error performing check '{check_name}': {e}")
    
    def run_all_checks(self) -> None:
        """
        Run all configured database checks.
        """
        if not self.checks_config:
            logger.warning("No checks configured")
            return
            
        logger.info(f"Running {len(self.checks_config)} database checks...")
        
        for check_config in self.checks_config:
            self.perform_single_check(check_config)
    
    def monitoring_loop(self) -> None:
        """
        Main monitoring loop that runs checks at specified intervals.
        """
        logger.info(f"Starting monitoring loop with {self.check_interval}s interval")
        
        while self.is_running:
            try:
                self.run_all_checks()
                
                # Sleep in small chunks to allow for responsive stopping
                sleep_time = 0
                while sleep_time < self.check_interval and self.is_running:
                    time.sleep(1)
                    sleep_time += 1
                    
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                time.sleep(10)  # Brief pause before retrying
    
    def start_monitoring(self) -> None:
        """
        Start continuous monitoring in a background thread.
        """
        if self.is_running:
            logger.warning("Monitoring is already running")
            return
            
        if not self.checks_config:
            logger.error("No checks configured. Cannot start monitoring.")
            return
            
        self.is_running = True
        self.check_thread = threading.Thread(target=self.monitoring_loop, daemon=True)
        self.check_thread.start()
        
        logger.info("Monitoring started in background thread")
    
    def stop_monitoring(self) -> None:
        """
        Stop continuous monitoring.
        """
        if not self.is_running:
            logger.warning("Monitoring is not running")
            return
            
        logger.info("Stopping monitoring...")
        self.is_running = False
        
        if self.check_thread and self.check_thread.is_alive():
            self.check_thread.join(timeout=5)
            
        logger.info("Monitoring stopped")
    
    def get_status(self) -> Dict:
        """
        Get current monitoring status.
        
        Returns:
            Dictionary with status information
        """
        return {
            'is_running': self.is_running,
            'check_interval': self.check_interval,
            'alert_cooldown': self.alert_cooldown,
            'configured_checks': len(self.checks_config),
            'checks': [{'name': check['name'], 'type': check['type'], 'table': check['table_name']} 
                      for check in self.checks_config],
            'last_alert_times': {k: v.isoformat() for k, v in self.last_alert_times.items()}
        }

def main():
    """
    Main function to run the database table checker.
    """
    try:
        # Initialize checker (loads all config from environment)
        checker = DatabaseTableChecker()
        
        # Show current configuration
        status = checker.get_status()
        logger.info(f"Checker initialized with {status['configured_checks']} checks:")
        for check in status['checks']:
            logger.info(f"  - {check['name']}: {check['type']} table '{check['table']}'")
        
        # Check if we have any checks configured
        if status['configured_checks'] == 0:
            logger.error("No database checks configured!")
            logger.info("Please set up environment variables. Here's a template:")
            checker.print_env_template()
            return
        
        # Run checks once immediately
        logger.info("Running initial check...")
        checker.run_all_checks()
        
        # Start continuous monitoring
        checker.start_monitoring()
        
        # Keep the program running
        logger.info("Monitoring started. Press Ctrl+C to stop.")
        while True:
            time.sleep(60)  # Print status every minute
            status = checker.get_status()
            logger.info(f"Status: Running={status['is_running']}, "
                       f"Checks={status['configured_checks']}, "
                       f"Interval={status['check_interval']}s")
                       
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        logger.info("Please check your environment variables.")
        return
        
    except KeyboardInterrupt:
        logger.info("Stopping monitoring...")
        checker.stop_monitoring()
        logger.info("Monitoring stopped. Goodbye!")
        
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return

def example_docker_compose():
    """
    Print an example docker-compose.yml configuration.
    """
    compose_yaml = """
# Example docker-compose.yml for the Database Table Checker

version: '3.8'

services:
  db-table-checker:
    build: .
    environment:
      # Gmail Configuration
      GMAIL_USER: ${GMAIL_USER}
      GMAIL_APP_PASSWORD: ${GMAIL_APP_PASSWORD}
      
      # Default Alert Email
      DEFAULT_ALERT_EMAIL: ${DEFAULT_ALERT_EMAIL}
      
      # Monitoring Settings
      CHECK_INTERVAL: 300
      ALERT_COOLDOWN: 3600
      
      # Database Check 1 - SQLite
      DB_CHECK_1_NAME: "User Sessions"
      DB_CHECK_1_TYPE: "sqlite"
      DB_CHECK_1_DB_PATH: "/app/data/sessions.db"
      DB_CHECK_1_TABLE_NAME: "user_sessions"
      DB_CHECK_1_ALERT_EMAIL_ENV: "ALERT_EMAIL_ADMIN"
      
      # Database Check 2 - PostgreSQL
      DB_CHECK_2_NAME: "Orders Table"
      DB_CHECK_2_TYPE: "postgres"
      DB_CHECK_2_HOST: "postgres"
      DB_CHECK_2_PORT: 5432
      DB_CHECK_2_DATABASE: "myapp"
      DB_CHECK_2_USER_ENV: "POSTGRES_USER"
      DB_CHECK_2_PASSWORD_ENV: "POSTGRES_PASSWORD"
      DB_CHECK_2_TABLE_NAME: "orders"
      DB_CHECK_2_ALERT_EMAIL_ENV: "ALERT_EMAIL_DEV"
      
      # Alert email addresses
      ALERT_EMAIL_ADMIN: ${ALERT_EMAIL_ADMIN}
      ALERT_EMAIL_DEV: ${ALERT_EMAIL_DEV}
      
      # Database credentials
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
      
    volumes:
      - ./data:/app/data
    depends_on:
      - postgres
    restart: unless-stopped
      
  postgres:
    image: postgres:15
    environment:
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
      POSTGRES_DB: myapp
    volumes:
      - postgres_data:/var/lib/postgresql/data
    restart: unless-stopped

volumes:
  postgres_data:
"""
    print(compose_yaml)

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--template":
        # Print environment template
        checker = DatabaseTableChecker.__new__(DatabaseTableChecker)
        checker.print_env_template()
    elif len(sys.argv) > 1 and sys.argv[1] == "--docker":
        # Print docker-compose example
        example_docker_compose()
    else:
        # Run the main application
        main()