#!/usr/bin/env python3
"""
Final Integration Test - Create Google Sheet and Send Email
"""

from dotenv import load_dotenv
load_dotenv()

import asyncio
import pandas as pd
from google_sheets_integration import GoogleSheetsIntegration
from email_notifier import EmailNotifier
from datetime import datetime

async def main():
    # Load the processed data
    print("Loading data...")
    csv_path = 'weekly_output/broward_lis_pendens_20251001_155828_processed.csv'
    df = pd.read_csv(csv_path)
    print(f'✅ Loaded {len(df)} records')

    # Create worksheet name with correct format: "Broward September 16th"
    current_date = datetime.now()
    day = current_date.day
    
    # Add ordinal suffix (st, nd, rd, th)
    if 4 <= day <= 20 or 24 <= day <= 30:
        suffix = "th"
    else:
        suffix = ["st", "nd", "rd"][day % 10 - 1]
    
    worksheet_name = f"Broward {current_date.strftime('%B')} {day}{suffix}"
    
    print(f'\n📊 Creating Google Sheets worksheet: "{worksheet_name}"')
    
    # Upload to Google Sheets
    sheets = GoogleSheetsIntegration()
    if not sheets.enabled:
        print('❌ Google Sheets not enabled')
        return
    
    print(f'   Spreadsheet ID: {sheets.spreadsheet_id}')
    
    # Upload the CSV
    result = sheets.upload_csv_to_worksheet(csv_path, worksheet_name, clean_column_names=True)
    
    if result:
        print(f'   ✅ Google Sheets upload successful!')
        spreadsheet_url = f'https://docs.google.com/spreadsheets/d/{sheets.spreadsheet_id}'
        print(f'   📊 Spreadsheet: {spreadsheet_url}')
        print(f'   📄 Worksheet: {worksheet_name}')
    else:
        print(f'   ❌ Google Sheets upload failed')
        return

    # Send email notification
    print(f'\n📧 Sending email notification...')
    email = EmailNotifier()
    
    if not email.enabled:
        print('❌ Email not enabled')
        return
    
    # Create email subject with correct format: "Broward-Weekly + date"
    email_subject = f"Broward-Weekly {current_date.strftime('%Y-%m-%d')}"
    
    # Create pipeline results
    pipeline_results = {
        'total_records': len(df),
        'success': True,
        'final_file': csv_path,
        'worksheet_name': worksheet_name
    }
    
    # Send email with Google Sheets link
    result = await email.send_pipeline_notification(
        pipeline_results,
        csv_file_path=None,  # Don't attach file
        attach_files=False,
        google_sheets_url=spreadsheet_url
    )
    
    if result:
        print(f'   ✅ Email sent successfully!')
        print(f'   📧 From: {email.sender_email}')
        print(f'   📧 To: {", ".join(email.recipient_emails)}')
        print(f'   📨 Subject: {email_subject}')
        print(f'   🔗 Link: {spreadsheet_url}')
        print(f'   📄 Worksheet: {worksheet_name}')
    else:
        print(f'   ❌ Email send failed')
        return

    print('\n✅ All tests passed! Email sent and Google Sheet created!')
    print(f'\n📊 Check your email at: {", ".join(email.recipient_emails)}')
    print(f'📊 Check the spreadsheet at: {spreadsheet_url}')

if __name__ == "__main__":
    asyncio.run(main())

