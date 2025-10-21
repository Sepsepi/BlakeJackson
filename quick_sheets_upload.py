#!/usr/bin/env python3
"""
Quick Google Sheets Upload - Bypass the dtype issue
"""

from dotenv import load_dotenv
load_dotenv()

import asyncio
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from email_notifier import EmailNotifier
from datetime import datetime
import os

async def main():
    # Load the processed data
    print("Loading data...")
    csv_path = 'weekly_output/broward_lis_pendens_20251001_155828_processed.csv'
    df = pd.read_csv(csv_path)
    print(f'✅ Loaded {len(df)} records')

    # Create worksheet name with correct format: "Broward October 1st"
    current_date = datetime.now()
    day = current_date.day
    
    # Add ordinal suffix (st, nd, rd, th)
    if 4 <= day <= 20 or 24 <= day <= 30:
        suffix = "th"
    else:
        suffix = ["st", "nd", "rd"][day % 10 - 1]
    
    worksheet_name = f"Broward {current_date.strftime('%B')} {day}{suffix}"
    
    print(f'\n📊 Creating Google Sheets worksheet: "{worksheet_name}"')
    
    # Google Sheets setup
    spreadsheet_id = os.environ.get('GOOGLE_SPREADSHEET_ID')
    service_account_file = os.environ.get('GOOGLE_SERVICE_ACCOUNT_JSON')
    
    if not spreadsheet_id or not service_account_file:
        print('❌ Google Sheets credentials not configured')
        return
    
    try:
        # Authenticate with Google Sheets
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        
        creds = Credentials.from_service_account_file(service_account_file, scopes=scopes)
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_key(spreadsheet_id)
        
        print(f'   ✅ Connected to spreadsheet')
        
        # Create or get worksheet
        try:
            worksheet = spreadsheet.worksheet(worksheet_name)
            print(f'   ℹ️  Worksheet "{worksheet_name}" already exists, will overwrite')
            worksheet.clear()
        except:
            worksheet = spreadsheet.add_worksheet(title=worksheet_name, rows=1000, cols=26)
            print(f'   ✅ Created new worksheet "{worksheet_name}"')
        
        # Convert DataFrame to list of lists (fix for dtype issue)
        print(f'   📤 Uploading {len(df)} records...')
        
        # Get headers
        headers = df.columns.tolist()
        
        # Convert data to strings to avoid dtype issues
        data_rows = []
        for _, row in df.iterrows():
            data_rows.append([str(val) if pd.notna(val) else '' for val in row])
        
        # Combine headers and data
        all_data = [headers] + data_rows
        
        # Upload to Google Sheets
        worksheet.update('A1', all_data)
        
        # Format header row
        worksheet.format('A1:Z1', {
            'backgroundColor': {'red': 0.2, 'green': 0.4, 'blue': 0.8},
            'textFormat': {'foregroundColor': {'red': 1, 'green': 1, 'blue': 1}, 'bold': True}
        })
        
        spreadsheet_url = f'https://docs.google.com/spreadsheets/d/{spreadsheet_id}'
        
        print(f'   ✅ Google Sheets upload successful!')
        print(f'   📊 Spreadsheet: {spreadsheet_url}')
        print(f'   📄 Worksheet: {worksheet_name}')
        
    except Exception as e:
        print(f'   ❌ Google Sheets upload failed: {e}')
        return

    # Send email notification
    print(f'\n📧 Sending email notification...')
    email = EmailNotifier()
    
    if not email.enabled:
        print('❌ Email not enabled')
        return
    
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
        csv_file_path=None,
        attach_files=False,
        google_sheets_url=spreadsheet_url
    )
    
    if result:
        print(f'   ✅ Email sent successfully!')
        print(f'   📧 From: {email.sender_email}')
        print(f'   📧 To: {", ".join(email.recipient_emails)}')
        print(f'   📨 Subject: Broward-Weekly {current_date.strftime("%Y-%m-%d")}')
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

