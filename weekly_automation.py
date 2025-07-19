#!/usr/bin/env python3
"""
Weekly Broward Lis Pendens Automation Script
============================================

This script demonstrates how to run the Broward Lis Pendens pipeline
for weekly automation with proper error handling and logging.

Usage:
    python weekly_automation.py
    
Or with custom settings:
    python weekly_automation.py --days-back 14 --batch-size 10
"""

import asyncio
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

# Add current directory to path to import pipeline
sys.path.insert(0, str(Path(__file__).parent))

from pipeline_scheduler import BrowardLisPendensPipeline

async def run_weekly_automation():
    """Run the weekly automation with enhanced error handling"""
    
    print(f"🗓️ WEEKLY BROWARD LIS PENDENS AUTOMATION")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    try:
        # Initialize pipeline with weekly-friendly settings
        pipeline = BrowardLisPendensPipeline(
            output_dir=os.environ.get('BROWARD_OUTPUT_DIR', 'weekly_output'),
            excel_file=os.environ.get('BROWARD_EXCEL_FILE'),
            days_back=int(os.environ.get('BROWARD_DAYS_BACK', '7')),
            batch_size=int(os.environ.get('BROWARD_BATCH_SIZE', '15')),
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
        print(f"Files created: {len(results.get('files_created', []))}")
        
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
    
    # Set up basic logging for automation
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(f'weekly_automation_{datetime.now().strftime("%Y%m%d")}.log'),
            logging.StreamHandler()
        ]
    )
    
    # Run automation
    success = asyncio.run(run_weekly_automation())
    
    # Exit with proper code for cron monitoring
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
