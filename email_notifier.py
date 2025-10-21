#!/usr/bin/env python3
"""
Email Notification System for Broward Lis Pendens Pipeline
=========================================================

Sends email notifications when pipeline completes with summary of results.
Supports both Gmail OAuth2 and SMTP authentication.

Features:
- HTML formatted emails with detailed statistics
- Attachment support for result files
- Environment variable configuration
- Error handling and logging

Author: Blake Jackson
Date: August 7, 2025
"""

import smtplib
import os
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Dict
import pandas as pd

logger = logging.getLogger(__name__)

class EmailNotifier:
    def __init__(self):
        """Initialize email notifier with environment variables"""

        # Email configuration from environment variables
        self.smtp_server = os.environ.get('EMAIL_SMTP_SERVER', 'smtp.gmail.com')
        self.smtp_port = int(os.environ.get('EMAIL_SMTP_PORT', '587'))
        self.sender_email = os.environ.get('EMAIL_SENDER')
        self.sender_password = os.environ.get('EMAIL_PASSWORD')  # App password for Gmail
        self.recipient_emails = os.environ.get('EMAIL_RECIPIENTS', '').split(',')

        # Remove empty emails
        self.recipient_emails = [email.strip() for email in self.recipient_emails if email.strip()]

        # Validation
        if not self.sender_email:
            logger.warning("EMAIL_SENDER not set - email notifications disabled")
        if not self.sender_password:
            logger.warning("EMAIL_PASSWORD not set - email notifications disabled")
        if not self.recipient_emails:
            logger.warning("EMAIL_RECIPIENTS not set - email notifications disabled")

        self.enabled = bool(self.sender_email and self.sender_password and self.recipient_emails)

        if self.enabled:
            logger.info(f"Email notifications enabled - will send to {len(self.recipient_emails)} recipients")
        else:
            logger.warning("Email notifications disabled - missing configuration")

    def generate_summary_html(self, pipeline_results: Dict, csv_file_path: Optional[str] = None) -> str:
        """Generate HTML summary of pipeline results"""

        # Read CSV file for detailed stats if provided
        detailed_stats = {}
        if csv_file_path and os.path.exists(csv_file_path):
            try:
                df = pd.read_csv(csv_file_path)
                total_records = len(df)

                # Count phone numbers found
                phone_columns = [col for col in df.columns if 'Phone_Primary' in col and 'Status' not in col]
                phone_count = 0
                for phone_col in phone_columns:
                    valid_phones = df[df[phone_col].notna() & (df[phone_col] != '') & (df[phone_col] != 'N/A')]
                    phone_count += len(valid_phones)

                # Count addresses
                address_columns = [col for col in df.columns if 'Address' in col and 'Match' not in col]
                address_count = 0
                for addr_col in address_columns:
                    valid_addresses = df[df[addr_col].notna() & (df[addr_col] != '')]
                    address_count += len(valid_addresses)

                detailed_stats = {
                    'total_records': total_records,
                    'phone_numbers_found': phone_count,
                    'addresses_found': address_count,
                    'success_rate': f"{(phone_count / max(1, total_records)) * 100:.1f}%"
                }
            except Exception as e:
                logger.warning(f"Could not read CSV for detailed stats: {e}")

        # Get status icon
        status_icon = "✅" if pipeline_results.get('success', False) else "❌"
        status_text = "SUCCESS" if pipeline_results.get('success', False) else "FAILED"
        status_color = "#28a745" if pipeline_results.get('success', False) else "#dc3545"

        # Calculate duration
        start_time = pipeline_results.get('start_time')
        end_time = pipeline_results.get('end_time')
        duration = "Unknown"
        if start_time and end_time:
            duration = str(end_time - start_time)

        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <style>
                body {{
                    font-family: Arial, sans-serif;
                    margin: 20px;
                    background-color: #f5f5f5;
                }}
                .container {{
                    background-color: white;
                    border-radius: 10px;
                    padding: 30px;
                    box-shadow: 0 2px 10px rgba(0,0,0,0.1);
                    max-width: 700px;
                    margin: 0 auto;
                }}
                .header {{
                    text-align: center;
                    border-bottom: 3px solid #007bff;
                    padding-bottom: 20px;
                    margin-bottom: 30px;
                }}
                .status {{
                    font-size: 24px;
                    font-weight: bold;
                    color: {status_color};
                    margin: 10px 0;
                }}
                .stats-grid {{
                    display: grid;
                    grid-template-columns: 1fr 1fr;
                    gap: 20px;
                    margin: 20px 0;
                }}
                .stat-box {{
                    background-color: #f8f9fa;
                    border: 1px solid #dee2e6;
                    border-radius: 5px;
                    padding: 15px;
                    text-align: center;
                }}
                .stat-number {{
                    font-size: 28px;
                    font-weight: bold;
                    color: #007bff;
                }}
                .stat-label {{
                    font-size: 14px;
                    color: #6c757d;
                    margin-top: 5px;
                }}
                .files-section {{
                    background-color: #e7f3ff;
                    border-left: 4px solid #007bff;
                    padding: 15px;
                    margin: 20px 0;
                }}
                .error-section {{
                    background-color: #fff3cd;
                    border-left: 4px solid #ffc107;
                    padding: 15px;
                    margin: 20px 0;
                }}
                .footer {{
                    text-align: center;
                    margin-top: 30px;
                    padding-top: 20px;
                    border-top: 1px solid #dee2e6;
                    color: #6c757d;
                    font-size: 12px;
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>{status_icon} Broward Lis Pendens Pipeline Report</h1>
                    <div class="status">{status_text}</div>
                    <p>Completed on {datetime.now().strftime('%B %d, %Y at %I:%M %p')}</p>
                </div>

                <div class="stats-grid">
                    <div class="stat-box">
                        <div class="stat-number">{pipeline_results.get('broward_records', 0)}</div>
                        <div class="stat-label">Records Scraped</div>
                    </div>
                    <div class="stat-box">
                        <div class="stat-number">{detailed_stats.get('phone_numbers_found', pipeline_results.get('phone_numbers_found', 0))}</div>
                        <div class="stat-label">Phone Numbers Found</div>
                    </div>
                    <div class="stat-box">
                        <div class="stat-number">{pipeline_results.get('addresses_found', 0)}</div>
                        <div class="stat-label">Addresses Extracted</div>
                    </div>
                    <div class="stat-box">
                        <div class="stat-number">{detailed_stats.get('success_rate', 'N/A')}</div>
                        <div class="stat-label">Success Rate</div>
                    </div>
                </div>

                <div class="stat-box" style="margin: 20px 0;">
                    <div class="stat-label">Pipeline Duration</div>
                    <div class="stat-number" style="font-size: 20px;">{duration}</div>
                </div>
        """

        # Add files created section
        files_created = pipeline_results.get('files_created', [])
        if files_created:
            html += """
                <div class="files-section">
                    <h3>📁 Files Created</h3>
                    <ul>
            """
            for file_path in files_created:
                file_name = Path(file_path).name
                html += f"<li><strong>{file_name}</strong></li>"
            html += """
                    </ul>
                </div>
            """

        # Add errors section if any
        errors = pipeline_results.get('errors', [])
        if errors:
            html += """
                <div class="error-section">
                    <h3>⚠️ Errors Encountered</h3>
                    <ul>
            """
            for error in errors:
                html += f"<li>{error}</li>"
            html += """
                    </ul>
                </div>
            """

        # Add performance metrics if available
        memory_usage = pipeline_results.get('memory_usage', {})
        if memory_usage:
            peak_memory = max(memory_usage.values())
            html += f"""
                <div class="stat-box" style="margin: 20px 0;">
                    <div class="stat-label">Peak Memory Usage</div>
                    <div class="stat-number" style="font-size: 20px;">{peak_memory:.1f} MB</div>
                </div>
            """

        html += """
                <div class="footer">
                    <p>This is an automated notification from the Broward Lis Pendens Pipeline</p>
                    <p>Generated on """ + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + """</p>
                </div>
            </div>
        </body>
        </html>
        """

        return html

    async def send_pipeline_notification(self,
                                       pipeline_results: Dict,
                                       csv_file_path: Optional[str] = None,
                                       attach_files: bool = True,
                                       google_sheets_url: Optional[str] = None) -> bool:
        """Send email notification about pipeline completion"""

        if not self.enabled:
            logger.info("Email notifications disabled - skipping")
            return True

        try:
            # Create message
            msg = MIMEMultipart()
            msg['From'] = self.sender_email
            msg['To'] = ', '.join(self.recipient_emails)

            # Subject: "Broward County Lis Pendens - YYYY-MM-DD"
            current_date = datetime.now().strftime('%Y-%m-%d')
            msg['Subject'] = f"Broward County Lis Pendens - {current_date}"

            # Email body: Only the Google Sheets link
            if google_sheets_url:
                email_body = google_sheets_url
            else:
                email_body = "No spreadsheet available"

            msg.attach(MIMEText(email_body, 'plain'))

            # No attachments - just send the Google Sheets link

            # Send email
            logger.info(f"Sending email notification to {len(self.recipient_emails)} recipients...")

            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.sender_email, self.sender_password)
                server.send_message(msg)

            logger.info("✅ Email notification sent successfully!")
            return True

        except Exception as e:
            logger.error(f"Failed to send email notification: {e}")
            return False

    def test_email_config(self) -> bool:
        """Test email configuration by sending a test email"""

        if not self.enabled:
            print("❌ Email notifications disabled - missing configuration")
            print("Required environment variables:")
            print("  - EMAIL_SENDER (your email address)")
            print("  - EMAIL_PASSWORD (app password for Gmail)")
            print("  - EMAIL_RECIPIENTS (comma-separated list of recipient emails)")
            return False

        try:
            print(f"Testing email configuration...")
            print(f"SMTP Server: {self.smtp_server}:{self.smtp_port}")
            print(f"Sender: {self.sender_email}")
            print(f"Recipients: {len(self.recipient_emails)} addresses")

            # Create test message
            msg = MIMEMultipart()
            msg['From'] = self.sender_email
            msg['To'] = ', '.join(self.recipient_emails)
            msg['Subject'] = "🧪 Broward Pipeline Email Test"

            test_html = f"""
            <!DOCTYPE html>
            <html>
            <body style="font-family: Arial, sans-serif; margin: 20px;">
                <div style="background-color: #e7f3ff; border-left: 4px solid #007bff; padding: 20px; border-radius: 5px;">
                    <h2>🧪 Email Configuration Test</h2>
                    <p>This is a test email to verify your Broward Pipeline email configuration.</p>
                    <p><strong>Test Date:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                    <p><strong>Sender:</strong> {self.sender_email}</p>
                    <p><strong>SMTP Server:</strong> {self.smtp_server}:{self.smtp_port}</p>
                    <p style="color: #28a745; font-weight: bold;">✅ If you receive this email, your configuration is working!</p>
                </div>
            </body>
            </html>
            """

            msg.attach(MIMEText(test_html, 'html'))

            # Send test email
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.sender_email, self.sender_password)
                server.send_message(msg)

            print("✅ Test email sent successfully!")
            print("Check your inbox to confirm receipt.")
            return True

        except Exception as e:
            print(f"❌ Email test failed: {e}")
            print("\nTroubleshooting tips:")
            print("1. Make sure you're using an App Password (not your regular password) for Gmail")
            print("2. Verify your email credentials are correct")
            print("3. Check that 2-factor authentication is enabled and you've generated an App Password")
            print("4. Ensure 'Less secure app access' is enabled if not using App Password")
            return False

# Convenience function for easy import
async def send_pipeline_email(pipeline_results: Dict, csv_file_path: Optional[str] = None, google_sheets_url: Optional[str] = None) -> bool:
    """Convenience function to send pipeline completion email"""
    notifier = EmailNotifier()
    return await notifier.send_pipeline_notification(pipeline_results, csv_file_path, google_sheets_url=google_sheets_url)

# CLI for testing
if __name__ == "__main__":
    import asyncio

    print("Broward Pipeline Email Notifier Test")
    print("=" * 40)

    notifier = EmailNotifier()

    if notifier.test_email_config():
        print("\n" + "=" * 40)
        print("Testing with sample pipeline results...")

        # Test with sample data
        sample_results = {
            'success': True,
            'start_time': datetime.now().replace(hour=13, minute=0, second=0),
            'end_time': datetime.now(),
            'broward_records': 25,
            'phone_numbers_found': 18,
            'addresses_found': 23,
            'files_created': [
                'weekly_output/broward_results_20250807.csv',
                'weekly_output/pipeline_summary_20250807.txt'
            ],
            'errors': [],
            'memory_usage': {'peak': 156.7}
        }

        async def test_notification():
            success = await notifier.send_pipeline_notification(sample_results)
            if success:
                print("✅ Sample notification email sent!")
            else:
                print("❌ Failed to send sample notification")

        asyncio.run(test_notification())
    else:
        print("❌ Email configuration test failed")
