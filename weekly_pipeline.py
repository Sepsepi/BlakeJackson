#!/usr/bin/env python3
"""
Example Weekly Cron Job Script for Broward Pipeline
==================================================

This script demonstrates how to set up the complete Broward Lis Pendens
pipeline as a weekly cron job. It integrates all components:

1. Broward County Lis Pendens scraping
2. Name processing and cleaning  
3. Fast address extraction
4. ZabaSearch phone number lookup
5. Excel integration and reporting

Usage Examples:
--------------

# Basic weekly run (last 7 days)
python weekly_pipeline.py

# Custom date range
python weekly_pipeline.py --days-back 14

# With Excel integration
python weekly_pipeline.py --excel-file "C:/path/to/results.xlsx"

# Visible browser mode (for debugging)
python weekly_pipeline.py --visible

# Smaller batches for rate limiting
python weekly_pipeline.py --batch-size 10

Cron Job Setup:
--------------
# Add this to your crontab for weekly Sunday runs at 2 AM:
# 0 2 * * 0 cd /path/to/BlakeJackson && python weekly_pipeline.py >> /var/log/broward_pipeline.log 2>&1

Author: Blake Jackson
Date: July 19, 2025
"""

import sys
import os
import asyncio
from pathlib import Path

# Add current directory to path for imports
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

try:
    from pipeline_scheduler import PipelineScheduler
except ImportError as e:
    print(f"❌ Error importing pipeline scheduler: {e}")
    print("Make sure all required files are in the same directory:")
    print("- pipeline_scheduler.py")
    print("- broward_lis_pendens_scraper.py") 
    print("- lis_pendens_processor.py")
    print("- fast_address_extractor.py")
    print("- zabasearch_batch1_records_1_15.py")
    sys.exit(1)


async def run_weekly_pipeline():
    """
    Run the complete weekly pipeline with standard settings
    """
    print("🗓️  WEEKLY BROWARD LIS PENDENS PIPELINE")
    print("=" * 50)
    print("📅 Running weekly data collection and processing...")
    print("=" * 50)
    
    # Initialize scheduler with weekly settings
    scheduler = PipelineScheduler(
        output_dir="weekly_results",
        excel_file=None,  # Can be customized via command line
        days_back=7,      # Last week
        batch_size=15,    # Optimal batch size for ZabaSearch
        headless=True,    # Headless for cron jobs
        max_retries=3
    )
    
    # Run the complete pipeline
    results = await scheduler.run_complete_pipeline()
    
    # Report results
    if results['success']:
        print("\n🎉 WEEKLY PIPELINE COMPLETED SUCCESSFULLY!")
        print("=" * 50)
        print(f"📊 Records processed: {results['broward_records']}")
        print(f"🏠 Addresses found: {results['addresses_found']}")
        print(f"📞 Phone numbers found: {results['phone_numbers_found']}")
        print(f"⏱️  Total time: {results['end_time'] - results['start_time']}")
        print("\n📁 Files created:")
        for file_path in results['files_created']:
            print(f"   - {file_path}")
            
    else:
        print("\n❌ WEEKLY PIPELINE FAILED!")
        print("=" * 50)
        if results['errors']:
            print("Errors encountered:")
            for error in results['errors']:
                print(f"   - {error}")
                
    return results['success']


def main():
    """Main entry point"""
    try:
        # Run the weekly pipeline
        success = asyncio.run(run_weekly_pipeline())
        
        # Exit with appropriate code for cron job monitoring
        sys.exit(0 if success else 1)
        
    except KeyboardInterrupt:
        print("\n⏹️  Pipeline interrupted by user")
        sys.exit(1)
        
    except Exception as e:
        print(f"\n💥 Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
