#!/usr/bin/env python3
"""
Test Email and Google Sheets Integration
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

    # Test Google Sheets upload
    print('\n📊 Testing Google Sheets upload...')
    sheets = GoogleSheetsIntegration()
    if sheets.enabled:
        worksheet_name = f"Broward {datetime.now().strftime('%B %d')}"
        print(f'   Uploading to worksheet: {worksheet_name}')

        result = sheets.upload_csv_to_worksheet(csv_path, worksheet_name)
        if result:
            print(f'   ✅ Google Sheets upload successful!')
            print(f'   📊 Spreadsheet: https://docs.google.com/spreadsheets/d/{sheets.spreadsheet_id}')
            print(f'   📄 Worksheet: {worksheet_name}')
        else:
            print(f'   ❌ Google Sheets upload failed')
    else:
        print('   ❌ Google Sheets not enabled')

    # Test email
    print('\n📧 Testing email notification...')
    email = EmailNotifier()
    if email.enabled:
        spreadsheet_url = f'https://docs.google.com/spreadsheets/d/{sheets.spreadsheet_id}'
        worksheet_name = f"Broward {datetime.now().strftime('%B %d')}"

        pipeline_results = {
            'total_records': len(df),
            'success': True,
            'final_file': csv_path
        }

        result = await email.send_pipeline_notification(
            pipeline_results,
            csv_file_path=None,  # Don't attach file
            attach_files=False,
            google_sheets_url=spreadsheet_url
        )
        if result:
            print(f'   ✅ Email sent successfully!')
            print(f'   📧 Sent to: {", ".join(email.recipient_emails)}')
            print(f'   📨 Subject: Broward County Lis Pendens - {datetime.now().strftime("%Y-%m-%d")}')
        else:
            print(f'   ❌ Email send failed')
    else:
        print('   ❌ Email not enabled')

    print('\n✅ Test complete!')

if __name__ == "__main__":
    asyncio.run(main())

