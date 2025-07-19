#!/usr/bin/env python3
"""
Broward Lis Pendens Cron Job Runner
===================================

Simple wrapper script for running the Broward County Lis Pendens scraper as a cron job.
Handles different scenarios and provides exit codes for cron monitoring.

Usage:
    python run_broward_cron.py [days] [--verbose]
    
Examples:
    python run_broward_cron.py          # Default: 7 days, quiet mode
    python run_broward_cron.py 14       # 14 days, quiet mode  
    python run_broward_cron.py 7 --verbose  # 7 days, verbose output

Exit Codes:
    0 = Success (data found and processed)
    1 = General failure
    2 = No data found (not necessarily an error)
    3 = Processing failed but raw data available
"""

import sys
import asyncio
import argparse
from pathlib import Path

# Add current directory to path for imports
sys.path.append(str(Path(__file__).parent))

from broward_lis_pendens_scraper import BrowardLisPendensScraper


async def run_cron_job(days_back: int = 7, verbose: bool = False):
    """
    Run the scraper as a cron job with appropriate error handling.
    
    Args:
        days_back: Number of days to search back
        verbose: Whether to show detailed output
        
    Returns:
        int: Exit code (0=success, 1=failure, 2=no data, 3=processing failed)
    """
    try:
        # Create scraper with cleanup enabled
        scraper = BrowardLisPendensScraper(
            headless=True, 
            cleanup_old_files=True
        )
        
        if verbose:
            print(f"Starting Broward Lis Pendens scraper for last {days_back} days...")
        
        # Run the integrated scraper and processor
        result_file = await scraper.scrape_and_process_lis_pendens(days_back)
        
        if result_file:
            # Check if it's a processed file or raw file
            if "_processed.csv" in result_file:
                if verbose:
                    print(f"SUCCESS: Fully processed data saved to {result_file}")
                    
                    # Show brief stats
                    try:
                        analysis = scraper.analyze_results(result_file)
                        print(f"Records: {analysis['total_records']}")
                        print(f"Date range: {analysis['date_range']['earliest']} to {analysis['date_range']['latest']}")
                    except:
                        pass
                        
                return 0  # Success
            else:
                if verbose:
                    print(f"PARTIAL SUCCESS: Raw data saved to {result_file} (processing failed)")
                return 3  # Processing failed but raw data available
        else:
            if verbose:
                print("NO DATA: No results found for the specified date range")
            return 2  # No data found
            
    except Exception as e:
        if verbose:
            print(f"ERROR: {e}")
        return 1  # General failure


def main():
    """Main entry point for cron job."""
    parser = argparse.ArgumentParser(description="Broward Lis Pendens Cron Job Runner")
    parser.add_argument('days', type=int, nargs='?', default=7, 
                       help='Number of days to search back (default: 7)')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Show verbose output')
    
    args = parser.parse_args()
    
    # Validate days parameter
    if args.days not in [1, 7, 14, 30]:
        print(f"ERROR: Invalid days value {args.days}. Must be 1, 7, 14, or 30.")
        sys.exit(1)
    
    # Run the cron job
    exit_code = asyncio.run(run_cron_job(args.days, args.verbose))
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
