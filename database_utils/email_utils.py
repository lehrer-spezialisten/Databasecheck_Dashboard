"""
Email utilities for sending notifications.
"""
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os

def send_email(subject: str, body: str, to_email: str, from_email: str, password: str = None) -> bool:
    """
    Send an email using Gmail SMTP.
    
    Args:
        subject: Email subject
        body: Email body (HTML supported)
        to_email: Recipient email address
        from_email: Sender email address (Gmail)
        password: Sender's app password
        
    Returns:
        bool: True if email was sent successfully, False otherwise
    """
    try:
        # Create message
        msg = MIMEMultipart()
        msg['From'] = from_email
        msg['To'] = to_email
        msg['Subject'] = subject
        
        # Attach body
        msg.attach(MIMEText(body, 'html'))
        
        # Connect to Gmail SMTP server
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            # Use APP_PASSWORD if provided, otherwise use the password parameter
            app_password = os.getenv('APP_PASSWORD', password)
            if not app_password:
                raise ValueError("No password provided and APP_PASSWORD not found in environment variables")
                
            server.login(from_email, app_password)
            server.send_message(msg)
            
        print(f"[INFO] Email notification sent to {to_email}")
        return True
        
    except Exception as e:
        print(f"[ERROR] Failed to send email: {e}")
        return False

def send_missing_table_notification(table_name: str, to_email: str, from_email: str, password: str = None) -> None:
    """
    Send a notification email about a missing database table.
    
    Args:
        table_name: Name of the missing table
        to_email: Recipient email address
        from_email: Sender email address (Gmail)
        password: Sender's app password
    """
    subject = f"[Action Required] Missing Database Table: {table_name}"
    
    body = f"""
    <html>
        <body>
            <h2>Database Table Missing</h2>
            <p>The following table was not found in the database:</p>
            <p><strong>Table Name:</strong> {table_name}</p>
            <p>Please investigate and ensure the table is properly created.</p>
            <br>
            <p>This is an automated message. Please do not reply to this email.</p>
        </body>
    </html>
    """
    
    send_email(subject, body, to_email, from_email, password)
