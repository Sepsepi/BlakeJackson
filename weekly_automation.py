#!/usr/bin/env python3
"""
Weekly Broward Lis Pendens Automation Script
============================================

This script runs the complete Broward Lis Pendens pipeline for weekly automation
with email notifications and Google Sheets integration.

The pipeline now includes:
1. Broward County scraping
2. Name processing
3. Address extraction
4. Phone number lookup (ZabaSearch + Radaris backup)
5. Email notifications
6. Google Sheets upload with date-named worksheets

Usage:
    python weekly_automation.py

Or with custom settings:
    python weekly_automation.py --days-back 14 --batch-size 9
"""

import asyncio
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

# CRITICAL: Load environment variables from .env file
from dotenv import load_dotenv

# Load .env file from the same directory as this script
env_path = Path(__file__).parent / '.env'
load_dotenv(dotenv_path=env_path)

# Verify environment variables are loaded
print(f"🔧 Environment variables loaded from: {env_path}")
print(f"   EMAIL_SENDER: {'✅ SET' if os.environ.get('EMAIL_SENDER') else '❌ NOT SET'}")
print(f"   EMAIL_PASSWORD: {'✅ SET' if os.environ.get('EMAIL_PASSWORD') else '❌ NOT SET'}")
print(f"   GOOGLE_SPREADSHEET_ID: {'✅ SET' if os.environ.get('GOOGLE_SPREADSHEET_ID') else '❌ NOT SET'}")
print(f"   GOOGLE_SERVICE_ACCOUNT_JSON: {'✅ SET' if os.environ.get('GOOGLE_SERVICE_ACCOUNT_JSON') else '❌ NOT SET'}")
print()

# Add current directory to path to import pipeline
sys.path.insert(0, str(Path(__file__).parent))

from pipeline_scheduler import BrowardLisPendensPipeline

async def run_weekly_automation():
    """Run the weekly automation with enhanced error handling"""

    print(f"🗓️ WEEKLY BROWARD LIS PENDENS AUTOMATION")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # Test network connectivity first
    print("🔍 Testing network connectivity...")
    try:
        import subprocess
        import platform

        # Ping test (works on both Windows and Linux)
        ping_cmd = ["ping", "-c", "3" if platform.system() != "Windows" else "-n", "3", "officialrecords.broward.org"]
        result = subprocess.run(ping_cmd, capture_output=True, text=True, timeout=30)

        if result.returncode == 0:
            print("✅ Network connectivity to Broward County website OK")
        else:
            print(f"⚠️ Network connectivity issue detected: {result.stderr}")
    except Exception as e:
        print(f"⚠️ Could not test network connectivity: {e}")

    try:
        # Initialize pipeline with weekly-friendly settings
        pipeline = BrowardLisPendensPipeline(
            output_dir=os.environ.get('BROWARD_OUTPUT_DIR', 'weekly_output'),
            excel_file=os.environ.get('BROWARD_EXCEL_FILE'),
            days_back=int(os.environ.get('BROWARD_DAYS_BACK', '7')),
            batch_size=int(os.environ.get('BROWARD_BATCH_SIZE', '9')),
            headless=True,  # Always headless for automation
            max_retries=5,  # More retries for automation
            skip_scraping=os.environ.get('BROWARD_SKIP_SCRAPING', 'false').lower() == 'true',
            skip_processing=os.environ.get('BROWARD_SKIP_PROCESSING', 'false').lower() == 'true',
            skip_address_extraction=os.environ.get('BROWARD_SKIP_ADDRESS', 'false').lower() == 'true',
            skip_phone_extraction=os.environ.get('BROWARD_SKIP_PHONE', 'false').lower() == 'true'
        )

        # Clean up old files (older than 30 days)
        pipeline._cleanup_old_files(days_old=30)

        # Run the complete pipeline
        results = await pipeline.run_complete_pipeline()

        # Report results
        print(f"\n📊 WEEKLY AUTOMATION RESULTS:")
        print(f"{'='*40}")
        print(f"Success: {'✅ YES' if results['success'] else '❌ NO'}")
        print(f"Duration: {results['end_time'] - results['start_time']}")
        print(f"Broward records: {results.get('broward_records', 0)}")
        print(f"Processed records: {results.get('processed_records', 0)}")
        print(f"Addresses found: {results.get('addresses_found', 0)}")
        print(f"Phone numbers found: {results.get('phone_numbers_found', 0)}")
        print(f"ZabaSearch phones: {results.get('zabasearch_phone_numbers', 0)}")
        print(f"Radaris backup phones: {results.get('radaris_phone_numbers', 0)}")
        print(f"Total phone coverage: {results.get('total_phone_coverage', 0)}")
        print(f"Files created: {len(results.get('files_created', []))}")

        # Email and Google Sheets integration is now handled within the pipeline
        # in step7_email_and_sheets_integration

        if results.get('files_created'):
            print(f"\n📁 Files created:")
            for file_path in results['files_created']:
                print(f"  - {file_path}")

        if results.get('errors'):
            print(f"\n⚠️ Errors encountered:")
            for error in results['errors']:
                print(f"  - {error}")

        print(f"\n🕐 Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        # Return success status
        return results['success']

    except Exception as e:
        print(f"\n💥 CRITICAL ERROR in weekly automation: {e}")
        print(f"Error type: {type(e).__name__}")
        return False

def main():
    """Main entry point for weekly automation"""

    # Set up basic logging for local automation
    log_filename = f'weekly_automation_{datetime.now().strftime("%Y%m%d")}.log'
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_filename)
        ]
    )
    print(f"📝 Logging to: {log_filename}")

    # Run automation
    success = asyncio.run(run_weekly_automation())

    # Print final status
    if success:
        print("🎉 Weekly automation completed successfully!")
    else:
        print("❌ Weekly automation failed!")

    # Exit with proper code for monitoring
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
