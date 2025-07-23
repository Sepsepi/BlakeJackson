#!/usr/bin/env python3
"""
Broward Lis Pendens Complete Pipeline Scheduler
==============================================

Comprehensive automation pipeline that integrates:
1. Broward County Lis Pendens scraping
2. Name processing and cleaning
3. Fast address extraction
4. ZabaSearch phone number lookup with intelligent batching
5. Excel file integration and Google Sheets output

Features:
- Weekly cron job compatible
- Complete error handling and recovery
- Progress tracking and logging
- Excel file modification support
- Command-line interface for automation
- Configurable batch size (defaults to 9)

Author: Blake Jackson
Date: July 20, 2025
"""

import asyncio
import argparse
import logging
import os
import sys
import time
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, List
import pandas as pd
import shutil

# Import all pipeline components
try:
    from broward_lis_pendens_scraper import BrowardLisPendensScraper
    from lis_pendens_processor import process_lis_pendens_csv
    from fast_address_extractor import process_addresses_fast
    from zabasearch_batch1_records_1_15 import ZabaSearchExtractor
    from google_sheets_uploader import upload_to_google_sheets
    from email_notifier import EmailNotifier
    PIPELINE_READY = True
    print("✅ All pipeline components loaded successfully")
except ImportError as e:
    PIPELINE_READY = False
    print(f"❌ Pipeline component import failed: {e}")
    sys.exit(1)


class BrowardLisPendensPipeline:
    """
    Complete automation pipeline for Broward Lis Pendens data processing
    with integrated phone number lookup via ZabaSearch.
    Fully configurable for weekly cron job automation.
    """
    
    def __init__(self, 
                 output_dir: Optional[str] = None,
                 excel_file: Optional[str] = None,
                 days_back: int = 7,
                 batch_size: int = 9,  # Default to 9 as requested
                 headless: bool = True,
                 max_retries: int = 3,
                 skip_scraping: bool = False,
                 skip_processing: bool = False,
                 skip_address_extraction: bool = False,
                 skip_phone_extraction: bool = False):
        """
        Initialize the pipeline scheduler with environment variable support.
        
        Args:
            output_dir: Directory for all pipeline outputs (defaults to current dir/pipeline_output)
            excel_file: Optional Excel file to modify/update
            days_back: Number of days back to search for Lis Pendens
            batch_size: Number of records per ZabaSearch batch (default: 9)
            headless: Run browsers in headless mode
            max_retries: Maximum retry attempts for failed operations
            skip_scraping: Skip Broward scraping step (use existing file)
            skip_processing: Skip name processing step
            skip_address_extraction: Skip address extraction step
            skip_phone_extraction: Skip phone number extraction step
        """
        # Use environment variables or defaults for paths
        if output_dir is None:
            output_dir = os.environ.get('BROWARD_OUTPUT_DIR', 'pipeline_output')
        
        self.output_dir = Path(output_dir).resolve()
        self.output_dir.mkdir(exist_ok=True)
        
        # Excel file can come from environment
        self.excel_file = excel_file or os.environ.get('BROWARD_EXCEL_FILE')
        
        # Configuration with environment variable fallbacks
        self.days_back = int(os.environ.get('BROWARD_DAYS_BACK', str(days_back)))
        self.batch_size = int(os.environ.get('BROWARD_BATCH_SIZE', str(batch_size)))
        self.headless = os.environ.get('BROWARD_HEADLESS', 'true').lower() == 'true' if 'BROWARD_HEADLESS' in os.environ else headless
        self.max_retries = int(os.environ.get('BROWARD_MAX_RETRIES', str(max_retries)))
        
        # Skip flags for partial pipeline execution
        self.skip_scraping = skip_scraping or os.environ.get('BROWARD_SKIP_SCRAPING', 'false').lower() == 'true'
        self.skip_processing = skip_processing or os.environ.get('BROWARD_SKIP_PROCESSING', 'false').lower() == 'true'
        self.skip_address_extraction = skip_address_extraction or os.environ.get('BROWARD_SKIP_ADDRESS', 'false').lower() == 'true'
        self.skip_phone_extraction = skip_phone_extraction or os.environ.get('BROWARD_SKIP_PHONE', 'false').lower() == 'true'
        
        # Render.com specific configuration
        self.is_render_deployment = os.environ.get('RENDER', 'false').lower() == 'true'
        self.webhook_url = os.environ.get('BROWARD_WEBHOOK_URL')  # For status notifications
        self.max_execution_time = int(os.environ.get('BROWARD_MAX_EXECUTION_TIME', '7200'))  # 2 hours default
        self.memory_cleanup_interval = int(os.environ.get('BROWARD_MEMORY_CLEANUP_INTERVAL', '300'))  # 5 minutes
        
        # Pipeline components
        self.broward_scraper = None
        self.zabasearch_extractor = None
        self.email_notifier = EmailNotifier()  # Initialize email notifications
        
        # Results tracking
        self.pipeline_results = {
            'start_time': None,
            'end_time': None,
            'broward_records': 0,
            'processed_records': 0,
            'addresses_found': 0,
            'phone_numbers_found': 0,
            'success': False,
            'files_created': [],
            'errors': [],
            'execution_environment': 'render' if self.is_render_deployment else 'local',
            'memory_usage': {},
            'performance_metrics': {}
        }
        
        # Setup logging
        self.setup_logging()
        
    def log_memory_usage(self, step_name: str):
        """Log current memory usage for monitoring on Render.com"""
        try:
            import psutil
            process = psutil.Process(os.getpid())
            memory_info = process.memory_info()
            memory_mb = memory_info.rss / 1024 / 1024
            
            self.pipeline_results['memory_usage'][step_name] = memory_mb
            self.logger.info(f"🧠 Memory usage at {step_name}: {memory_mb:.1f} MB")
            
            # Alert if memory usage is high (useful for Render.com limits)
            if memory_mb > 800:  # 800MB threshold
                self.logger.warning(f"⚠️ High memory usage detected: {memory_mb:.1f} MB")
                
        except ImportError:
            pass  # psutil not available
        except Exception as e:
            self.logger.debug(f"Memory monitoring error: {e}")
    
    async def send_webhook_notification(self, status: str, message: str):
        """Send status notification via webhook (useful for Render.com monitoring)"""
        if not self.webhook_url:
            return
            
        try:
            import aiohttp
            payload = {
                'status': status,
                'message': message,
                'timestamp': datetime.now().isoformat(),
                'environment': self.pipeline_results['execution_environment'],
                'pipeline_results': self.pipeline_results
            }
            
            timeout = aiohttp.ClientTimeout(total=int(os.environ.get('BROWARD_WEBHOOK_TIMEOUT', '30')))
            async with aiohttp.ClientSession(timeout=timeout) as session:
                await session.post(self.webhook_url, json=payload)
                self.logger.info(f"📤 Webhook notification sent: {status}")
                
        except Exception as e:
            self.logger.warning(f"📤 Webhook notification failed: {e}")
    
    def force_garbage_collection(self):
        """Force garbage collection to free memory (important for Render.com)"""
        try:
            import gc
            collected = gc.collect()
            self.logger.debug(f"🗑️ Garbage collection freed {collected} objects")
        except Exception as e:
            self.logger.debug(f"Garbage collection error: {e}")
        
    def setup_logging(self):
        """Setup comprehensive logging for the pipeline with Render.com optimizations"""
        log_file = self.output_dir / f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        
        # Enhanced logging format for Render.com
        log_format = '%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
        
        # Configure logging with both file and console output
        handlers = [logging.StreamHandler(sys.stdout)]
        
        # Only add file handler if not in Render (to avoid storage issues)
        if not self.is_render_deployment:
            file_handler = logging.FileHandler(log_file, encoding='utf-8')
            handlers.append(file_handler)
        
        logging.basicConfig(
            level=logging.INFO,
            format=log_format,
            handlers=handlers,
            force=True  # Override any existing logging config
        )
        
        self.logger = logging.getLogger(__name__)
        
        if self.is_render_deployment:
            self.logger.info("🌐 RUNNING ON RENDER.COM - Enhanced cloud logging enabled")
            self.logger.info(f"🔧 Render environment variables detected")
        else:
            self.logger.info(f"💻 LOCAL EXECUTION - Log file: {log_file}")
            
        self.logger.info(f"Pipeline scheduler initialized with PID: {os.getpid()}")
        
        # Log system information for debugging
        try:
            import psutil
            memory_info = psutil.virtual_memory()
            self.logger.info(f"💾 System Memory: {memory_info.total // 1024 // 1024} MB total, {memory_info.available // 1024 // 1024} MB available")
        except ImportError:
            self.logger.info("💾 psutil not available - memory monitoring disabled")
        
    async def run_complete_pipeline(self, input_file: Optional[str] = None) -> Dict:
        """
        Execute the complete pipeline from Broward scraping to phone number extraction.
        
        Pipeline Steps:
        1. Broward County Lis Pendens scraping
        2. Name processing and cleaning  
        3. Fast address extraction
        4. ZabaSearch phone number lookup (batched with configurable size)
        5. Excel integration (optional)
        6. Summary report generation
        
        Args:
            input_file: Optional input file to start from (auto-detects if not provided)
        
        Returns:
            Dict containing pipeline results and statistics
        """
        self.pipeline_results['start_time'] = datetime.now()
        self.logger.info("🚀 STARTING COMPLETE BROWARD LIS PENDENS PIPELINE")
        self.logger.info("=" * 70)
        self.logger.info(f"📅 Pipeline started at: {self.pipeline_results['start_time']}")
        self.logger.info(f"📂 Output directory: {self.output_dir}")
        self.logger.info(f"📊 Configuration: {self.days_back} days back, batch size {self.batch_size}")
        self.logger.info(f"🤖 Headless mode: {self.headless}")
        self.logger.info(f"🌐 Environment: {self.pipeline_results['execution_environment']}")
        
        # Log memory at start
        self.log_memory_usage("pipeline_start")
        
        # Send start notification
        await self.send_webhook_notification("started", f"Pipeline started for {self.days_back} days back")
        
        # Track execution start time for timeout monitoring
        execution_start = time.time()
        
        try:
            current_file = input_file
            
            # Step 1: Scrape Broward Lis Pendens data (unless skipped)
            self.logger.info("\n" + "=" * 70)
            self.logger.info("📊 STEP 1: BROWARD COUNTY LIS PENDENS SCRAPING")
            self.logger.info("=" * 70)
            
            # Check execution time
            if time.time() - execution_start > self.max_execution_time:
                raise TimeoutError("Pipeline execution time limit exceeded")
            
            if not self.skip_scraping and not current_file:
                self.logger.info(f"🔍 Scraping Broward County records for last {self.days_back} days...")
                broward_file = await self.step1_scrape_broward()
                if not broward_file:
                    raise Exception("Broward scraping failed - no data retrieved")
                current_file = broward_file
                self.logger.info(f"✅ STEP 1 COMPLETE: Broward data scraped → {broward_file}")
                self.log_memory_usage("step1_complete")
                self.force_garbage_collection()
            elif not current_file:
                # Auto-detect latest Broward file
                self.logger.info("⏭️ Scraping skipped - looking for existing Broward file...")
                current_file = self._find_latest_file("*broward*.csv")
                if not current_file:
                    raise Exception("No Broward file found and scraping skipped")
                self.logger.info(f"📁 Using existing Broward file: {current_file}")
            else:
                self.logger.info(f"📁 Using provided input file: {current_file}")
                
            # Step 2: Process and clean names (unless skipped)
            self.logger.info("\n" + "=" * 70)
            self.logger.info("🔤 STEP 2: NAME PROCESSING AND CLEANING")
            self.logger.info("=" * 70)
            
            # Check execution time
            if time.time() - execution_start > self.max_execution_time:
                raise TimeoutError(f"Pipeline execution time limit exceeded ({self.max_execution_time}s)")
            
            if not self.skip_processing:
                self.logger.info(f"🧹 Processing and cleaning names from: {current_file}")
                processed_file = await self.step2_process_names(current_file)
                if not processed_file:
                    raise Exception("Name processing failed - no processed file created")
                current_file = processed_file
                self.logger.info(f"✅ STEP 2 COMPLETE: Names processed → {processed_file}")
                self.log_memory_usage("step2_complete")
                self.force_garbage_collection()
            else:
                # Auto-detect latest processed file
                self.logger.info("⏭️ Name processing skipped - looking for existing processed file...")
                processed_file = self._find_latest_file("*processed*.csv")
                if processed_file:
                    current_file = processed_file
                    self.logger.info(f"📁 Using existing processed file: {processed_file}")
                else:
                    self.logger.info("⚠️ No processed file found, continuing with current file")
                
            # Step 3: Extract addresses (unless skipped)
            self.logger.info("\n" + "=" * 70)
            self.logger.info("🏠 STEP 3: FAST ADDRESS EXTRACTION")
            self.logger.info("=" * 70)
            
            # Check execution time
            if time.time() - execution_start > self.max_execution_time:
                raise TimeoutError(f"Pipeline execution time limit exceeded ({self.max_execution_time}s)")
            
            # Check if addresses are already present in current file
            addresses_already_present = self._check_addresses_present(current_file)
            
            if not self.skip_address_extraction and not addresses_already_present:
                self.logger.info(f"🔍 Extracting addresses using Broward Property Appraiser...")
                self.logger.info(f"📝 Input file: {current_file}")
                address_file = await self.step3_extract_addresses(current_file)
                if not address_file:
                    raise Exception("Address extraction failed - no addresses found")
                current_file = address_file
                self.logger.info(f"✅ STEP 3 COMPLETE: Addresses extracted → {address_file}")
            elif addresses_already_present:
                self.logger.info("✅ Addresses already present in current file - skipping extraction")
                self.logger.info(f"📁 Continuing with current file: {current_file}")
            else:
                # Auto-detect latest address file
                self.logger.info("⏭️ Address extraction skipped - looking for existing address file...")
                address_file = self._find_latest_file("*with_addresses*.csv")
                if address_file:
                    current_file = address_file
                    self.logger.info(f"📁 Using existing address file: {address_file}")
                else:
                    self.logger.info("⚠️ No address file found, continuing with current file")
                
            # Step 4: Extract phone numbers via ZabaSearch (unless skipped)
            self.logger.info("\n" + "=" * 70)
            self.logger.info(f"📞 STEP 4: ZABASEARCH PHONE NUMBER EXTRACTION (BATCH SIZE: {self.batch_size})")
            self.logger.info("=" * 70)
            
            # Check execution time
            if time.time() - execution_start > self.max_execution_time:
                raise TimeoutError(f"Pipeline execution time limit exceeded ({self.max_execution_time}s)")
            
            if not self.skip_phone_extraction:
                self.logger.info(f"🔍 Starting ZabaSearch phone lookup with batch size: {self.batch_size}")
                self.logger.info(f"📝 Input file: {current_file}")
                self.logger.info("⏰ Note: 10-minute delays between batches for rate limiting")
                final_file = await self.step4_extract_phone_numbers(current_file)
                if not final_file:
                    raise Exception("Phone number extraction failed - ZabaSearch processing error")
                current_file = final_file
                self.logger.info(f"✅ STEP 4 COMPLETE: Phone numbers extracted → {final_file}")
            else:
                self.logger.info("⏭️ Phone extraction skipped")

                
            # Step 5: Handle Excel integration if specified
            self.logger.info("\n" + "=" * 70)
            self.logger.info("📊 STEP 5: EXCEL INTEGRATION")
            self.logger.info("=" * 70)
            
            if self.excel_file:
                self.logger.info(f"📋 Integrating results with Excel file: {self.excel_file}")
                excel_result = await self.step5_excel_integration(current_file)
                if excel_result:
                    self.pipeline_results['files_created'].append(excel_result)
                    self.logger.info(f"✅ STEP 5 COMPLETE: Excel integration → {excel_result}")
                else:
                    self.logger.warning("⚠️ Excel integration failed but continuing...")
            else:
                self.logger.info("⏭️ No Excel file specified - skipping integration")
                    
            # Step 6: Generate summary report
            self.logger.info("\n" + "=" * 70)
            self.logger.info("📋 STEP 6: SUMMARY REPORT GENERATION")
            self.logger.info("=" * 70)
            
            self.logger.info(f"📝 Generating comprehensive summary report...")
            summary_file = await self.step6_generate_summary(current_file)
            if summary_file:
                self.pipeline_results['files_created'].append(summary_file)
                self.logger.info(f"✅ STEP 6 COMPLETE: Summary report → {summary_file}")
            else:
                self.logger.warning("⚠️ Summary generation failed but pipeline completed")
                
            # Step 7: Google Sheets Upload
            self.logger.info("\n" + "=" * 70)
            self.logger.info("📊 STEP 7: GOOGLE SHEETS UPLOAD")
            self.logger.info("=" * 70)
            
            google_sheets_url = await self.step7_google_sheets_upload(current_file)
            if google_sheets_url:
                self.pipeline_results['google_sheets_url'] = google_sheets_url
                self.logger.info(f"✅ STEP 7 COMPLETE: Google Sheets upload → {google_sheets_url}")
            else:
                self.logger.info("⏭️ Google Sheets upload skipped (no credentials or disabled)")
                
            self.pipeline_results['success'] = True
            self.log_memory_usage("pipeline_complete")
            
            # Send success notification
            await self.send_webhook_notification("completed", 
                f"Pipeline completed successfully. Records: {self.pipeline_results['broward_records']}, "
                f"Addresses: {self.pipeline_results['addresses_found']}, "
                f"Phones: {self.pipeline_results['phone_numbers_found']}")
            
            self.logger.info("\n" + "🎉" * 70)
            self.logger.info("✅ COMPLETE PIPELINE EXECUTED SUCCESSFULLY!")
            self.logger.info("🎉" * 70)
            
        except TimeoutError as e:
            self.pipeline_results['errors'].append(f"Execution timeout: {e}")
            self.logger.error(f"\n⏰ PIPELINE TIMED OUT: {e}")
            await self.send_webhook_notification("timeout", str(e))
            self.email_notifier.send_error_email(str(e), "Pipeline Timeout")
            
        except Exception as e:
            self.pipeline_results['errors'].append(str(e))
            self.logger.error(f"\n❌ PIPELINE FAILED AT: {e}")
            self.logger.error("💥" * 70)
            
            # Send error notification
            await self.send_webhook_notification("failed", str(e))
            self.email_notifier.send_error_email(str(e), "Pipeline Execution")
            
        finally:
            self.pipeline_results['end_time'] = datetime.now()
            duration = self.pipeline_results['end_time'] - self.pipeline_results['start_time']
            self.pipeline_results['performance_metrics']['total_duration'] = str(duration)
            
            # Final memory cleanup
            self.force_garbage_collection()
            self.log_memory_usage("pipeline_end")
            
            self.logger.info(f"\n⏱️ Total pipeline duration: {duration}")
            self.logger.info(f"📅 Pipeline ended at: {self.pipeline_results['end_time']}")
            
            # Log final memory usage for Render.com monitoring
            if self.pipeline_results['memory_usage']:
                max_memory = max(self.pipeline_results['memory_usage'].values())
                self.logger.info(f"🧠 Peak memory usage: {max_memory:.1f} MB")
                
            # Final status notification and email
            final_status = "success" if self.pipeline_results['success'] else "failed"
            await self.send_webhook_notification(final_status, 
                f"Pipeline {final_status}. Duration: {duration}. "
                f"Files created: {len(self.pipeline_results['files_created'])}")
            
            # Send completion email with results
            await self.send_completion_email()
            
        return self.pipeline_results
    
    def _find_latest_file(self, pattern: str) -> Optional[str]:
        """Find the most recent file matching the pattern"""
        import glob
        
        # Search in output directory and current directory
        search_paths = [
            str(self.output_dir / pattern),
            pattern,
            f"downloads/{pattern}"  # Legacy downloads folder
        ]
        
        all_files = []
        for search_path in search_paths:
            files = glob.glob(search_path)
            all_files.extend(files)
        
        if not all_files:
            return None
            
        # Return the most recent file
        latest_file = max(all_files, key=os.path.getctime)
        return latest_file if os.path.exists(latest_file) else None
    
    def _cleanup_old_files(self, days_old: int = 30):
        """Clean up files older than specified days"""
        import time
        
        self.logger.info(f"🧹 Cleaning up files older than {days_old} days...")
        
        cutoff_time = time.time() - (days_old * 24 * 60 * 60)
        cleanup_count = 0
        
        # Clean up output directory
        for file_path in self.output_dir.glob("*"):
            if file_path.is_file() and file_path.stat().st_mtime < cutoff_time:
                try:
                    file_path.unlink()
                    cleanup_count += 1
                    self.logger.debug(f"Deleted old file: {file_path}")
                except Exception as e:
                    self.logger.warning(f"Could not delete {file_path}: {e}")
        
        # Clean up downloads directory if it exists
        downloads_dir = Path("downloads")
        if downloads_dir.exists():
            for file_path in downloads_dir.glob("*"):
                if file_path.is_file() and file_path.stat().st_mtime < cutoff_time:
                    try:
                        file_path.unlink()
                        cleanup_count += 1
                        self.logger.debug(f"Deleted old file: {file_path}")
                    except Exception as e:
                        self.logger.warning(f"Could not delete {file_path}: {e}")
        
        self.logger.info(f"✅ Cleanup completed: {cleanup_count} old files removed")
        
    def _check_addresses_present(self, file_path: str) -> bool:
        """
        Check if addresses are already present in the CSV file.
        Returns True if the IndirectName_Address column exists and has data.
        """
        if not file_path or not os.path.exists(file_path):
            return False
            
        try:
            import pandas as pd
            df = pd.read_csv(file_path)
            
            # Check if IndirectName_Address column exists
            if 'IndirectName_Address' not in df.columns:
                return False
            
            # Check if there are any non-null, non-empty addresses
            addresses_present = df['IndirectName_Address'].notna() & (df['IndirectName_Address'] != '')
            address_count = addresses_present.sum()
            total_indirect_records = df['IndirectName'].notna().sum()
            
            if address_count > 0:
                percentage = (address_count / max(1, total_indirect_records)) * 100
                self.logger.info(f"📍 Found {address_count} addresses already present ({percentage:.1f}% of indirect records)")
                return True
            else:
                self.logger.info("📍 No addresses found in current file")
                return False
                
        except Exception as e:
            self.logger.warning(f"⚠️ Error checking for existing addresses: {e}")
            return False
        
    async def step1_scrape_broward(self) -> Optional[str]:
        """Step 1: Scrape Broward County Lis Pendens data"""
        self.logger.info(f"📊 Step 1: Scraping Broward Lis Pendens data ({self.days_back} days back)...")
        
        try:
            self.broward_scraper = BrowardLisPendensScraper(
                download_dir=str(self.output_dir),
                headless=self.headless,
                cleanup_old_files=True
            )
            
            # Run the scraper
            processed_filepath = await self.broward_scraper.scrape_and_process_lis_pendens(self.days_back)
            
            if processed_filepath and os.path.exists(processed_filepath):
                # Get record count
                try:
                    df = pd.read_csv(processed_filepath)
                    self.pipeline_results['broward_records'] = len(df)
                    self.logger.info(f"✅ Broward scraping completed: {len(df)} records found")
                    self.pipeline_results['files_created'].append(processed_filepath)
                    return processed_filepath
                except Exception as e:
                    self.logger.error(f"Error reading scraped file: {e}")
                    
            else:
                self.logger.error("Broward scraping failed - no file created")
                
        except Exception as e:
            self.logger.error(f"Broward scraping error: {e}")
            
        return None
        
    async def step2_process_names(self, input_file: str) -> Optional[str]:
        """Step 2: Process and clean names from Broward data"""
        self.logger.info("🔤 Step 2: Processing and cleaning names...")
        
        try:
            # Process names using lis_pendens_processor
            processed_file = process_lis_pendens_csv(input_file)
            
            if processed_file and os.path.exists(processed_file):
                # Get processed record count
                try:
                    df = pd.read_csv(processed_file)
                    self.pipeline_results['processed_records'] = len(df)
                    self.logger.info(f"✅ Name processing completed: {len(df)} processed records")
                    self.pipeline_results['files_created'].append(processed_file)
                    return processed_file
                except Exception as e:
                    self.logger.error(f"Error reading processed file: {e}")
            else:
                self.logger.error("Name processing failed - no file created")
                
        except Exception as e:
            self.logger.error(f"Name processing error: {e}")
            
        return None
        
    async def step3_extract_addresses(self, input_file: str) -> Optional[str]:
        """Step 3: Extract addresses using fast address extractor"""
        self.logger.info("🏠 Step 3: Extracting addresses...")
        
        try:
            # Extract addresses using fast_address_extractor
            address_file = await process_addresses_fast(
                input_file, 
                max_names=None, 
                headless=self.headless
            )
            
            if address_file and os.path.exists(address_file):
                # Get address count
                try:
                    df = pd.read_csv(address_file)
                    addresses_found = len(df[df['IndirectName_Address'].notna() & (df['IndirectName_Address'] != '')])
                    self.pipeline_results['addresses_found'] = addresses_found
                    self.logger.info(f"✅ Address extraction completed: {addresses_found} addresses found")
                    self.pipeline_results['files_created'].append(address_file)
                    return address_file
                except Exception as e:
                    self.logger.error(f"Error reading address file: {e}")
            else:
                self.logger.error("Address extraction failed - no file created")
                
        except Exception as e:
            self.logger.error(f"Address extraction error: {e}")
            
        return None
        
    async def step4_extract_phone_numbers(self, input_file: str) -> Optional[str]:
        """Step 4: Extract phone numbers using ZabaSearch with intelligent batching"""
        self.logger.info(f"📞 Step 4: Extracting phone numbers (batch size: {self.batch_size})...")
        
        try:
            # Initialize ZabaSearch extractor
            self.logger.info("🔧 Initializing ZabaSearch extractor...")
            self.zabasearch_extractor = ZabaSearchExtractor()
            
            # Read the input file
            self.logger.info(f"📖 Reading input file: {input_file}")
            df = pd.read_csv(input_file)
            self.logger.info(f"📊 Total records in file: {len(df)}")
            
            # Check for different address column names that might exist
            address_columns = ['IndirectName_Address', 'Address', 'address']
            address_col = None
            
            for col in address_columns:
                if col in df.columns:
                    address_col = col
                    break
            
            if address_col is None:
                self.logger.warning("❌ No address column found for phone number extraction")
                return input_file
            
            # Filter for records with valid addresses (Person type only, not Business)
            valid_records = df[
                (df[address_col].notna()) & 
                (df[address_col] != '') & 
                (df.get('IndirectName_Type', 'Person') == 'Person')  # Only process people, not businesses
            ].copy()
            
            if len(valid_records) == 0:
                self.logger.warning("❌ No valid addresses found for phone number extraction")
                return input_file
                
            self.logger.info(f"✅ Found {len(valid_records)} person records with valid addresses for phone lookup")
            
            # Prepare output file
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = str(self.output_dir / f"broward_with_phones_{timestamp}.csv")
            
            # Calculate batches based on actual address count and configured batch size
            total_records_with_addresses = len(valid_records)
            total_batches = (total_records_with_addresses + self.batch_size - 1) // self.batch_size
            
            self.logger.info(f"🎯 ZabaSearch Batch Configuration:")
            self.logger.info(f"   📊 Total person records with addresses: {total_records_with_addresses}")
            self.logger.info(f"   📦 Batch size: {self.batch_size}")
            self.logger.info(f"   🔢 Total batches needed: {total_batches}")
            self.logger.info(f"   ⏰ Delay between batches: 10 minutes")
            self.logger.info(f"   🎯 Processing records in batches of {self.batch_size} for optimal ZabaSearch performance")
            
            # Process each batch of addresses
            all_batch_files = []
            total_phones_found = 0
            
            for batch_num in range(total_batches):
                start_idx = batch_num * self.batch_size
                end_idx = min(start_idx + self.batch_size, total_records_with_addresses)
                
                # Get the current batch of records
                batch_records = valid_records.iloc[start_idx:end_idx].copy()
                actual_batch_size = len(batch_records)
                
                self.logger.info(f"\n🔄 Starting ZabaSearch Batch {batch_num + 1}/{total_batches}")
                self.logger.info(f"   📝 Processing {actual_batch_size} records (indexes {start_idx + 1}-{end_idx})")
                self.logger.info(f"   🏠 Address range: Records {start_idx + 1} to {end_idx} with valid addresses")
                
                # Create batch-specific input and output files
                batch_input_file = str(self.output_dir / f"batch_{batch_num + 1:02d}_input_{timestamp}.csv")
                batch_output_file = str(self.output_dir / f"batch_{batch_num + 1:02d}_output_{timestamp}.csv")
                
                # Save batch input file
                batch_records.to_csv(batch_input_file, index=False)
                self.logger.info(f"   💾 Batch input saved: {batch_input_file}")
                
                try:
                    # Process batch with ZabaSearch
                    self.logger.info(f"   🌐 Launching ZabaSearch browser for batch {batch_num + 1}...")
                    await self.zabasearch_extractor.process_csv_batch(
                        csv_path=batch_input_file,
                        output_path=batch_output_file,
                        start_record=1,  # Always start from 1 since each batch is a separate file
                        end_record=actual_batch_size  # End at the actual batch size
                    )
                    
                    # Check if batch was successful
                    if os.path.exists(batch_output_file):
                        # Read batch results to count successes
                        batch_df = pd.read_csv(batch_output_file)
                        
                        # Count phone numbers found in this batch
                        phone_cols = [col for col in batch_df.columns if 'Phone' in col and 'Primary' in col]
                        batch_phones = 0
                        for phone_col in phone_cols:
                            phone_values = batch_df[batch_df[phone_col].notna() & (batch_df[phone_col] != '') & (batch_df[phone_col] != 'N/A')]
                            batch_phones += len(phone_values)
                        
                        all_batch_files.append(batch_output_file)
                        
                        self.logger.info(f"   ✅ Batch {batch_num + 1} COMPLETED: {batch_phones} phone numbers found in batch file")
                        self.logger.info(f"   📊 Cumulative batch files: {len(all_batch_files)} successful batches")
                        self.logger.info(f"   🔒 Browser closed completely for batch {batch_num + 1}")
                    else:
                        self.logger.warning(f"   ❌ Batch {batch_num + 1} FAILED - no output file created")
                        
                except Exception as e:
                    self.logger.error(f"   💥 Batch {batch_num + 1} ERROR: {e}")
                    
                # Clean up batch input file
                try:
                    if os.path.exists(batch_input_file):
                        os.remove(batch_input_file)
                except Exception as e:
                    self.logger.warning(f"Cleanup warning for {batch_input_file}: {e}")
                    
                # Delay between batches (10 minutes for ZabaSearch rate limiting)
                if batch_num < total_batches - 1:
                    delay = 600  # 10 minutes = 600 seconds
                    self.logger.info(f"\n⏰ BATCH DELAY: Waiting {delay//60} minutes before next ZabaSearch batch...")
                    self.logger.info(f"   🛡️ Rate limiting protection - preventing detection")
                    self.logger.info(f"   ⏳ Next batch ({batch_num + 2}/{total_batches}) will start at: {(datetime.now() + timedelta(seconds=delay)).strftime('%H:%M:%S')}")
                    await asyncio.sleep(delay)
                    self.logger.info(f"   ✅ Delay complete - starting next batch...")
                else:
                    self.logger.info(f"   🏁 Final batch completed - no delay needed")
            
            # Combine all successful batch results
            if all_batch_files:
                self.logger.info(f"\n📋 Combining {len(all_batch_files)} successful batch results...")
                
                # Start with the original dataframe
                final_df = df.copy()
                
                # Add phone columns if they don't exist
                phone_columns = ['DirectName_Phone_Primary', 'DirectName_Phone_Secondary', 'DirectName_Phone_All', 'DirectName_ZabaSearch_Status',
                               'IndirectName_Phone_Primary', 'IndirectName_Phone_Secondary', 'IndirectName_Phone_All', 'IndirectName_ZabaSearch_Status']
                for col in phone_columns:
                    if col not in final_df.columns:
                        final_df[col] = ''
                
                # Process each batch file and merge phone data
                total_phones_merged = 0
                for i, batch_file in enumerate(all_batch_files):
                    try:
                        self.logger.info(f"   📥 Reading batch {i+1}: {batch_file}")
                        batch_df = pd.read_csv(batch_file)
                        
                        # Merge phone data from batch back to final dataframe
                        # The batch files contain phone number columns for the records that were processed
                        phone_cols_in_batch = [col for col in batch_df.columns if 'Phone' in col or 'ZabaSearch_Status' in col]
                        
                        if phone_cols_in_batch:
                            self.logger.info(f"      📞 Found phone columns in batch: {phone_cols_in_batch}")
                            
                            # For each record in batch that has phone data, update the final dataframe
                            for batch_idx, batch_row in batch_df.iterrows():
                                # Find matching record in final_df by comparing key fields
                                name_match = None
                                address_match = None
                                
                                # Try to match by DirectName fields
                                if 'DirectName_Cleaned' in batch_row and pd.notna(batch_row['DirectName_Cleaned']):
                                    name_match = batch_row['DirectName_Cleaned']
                                    if 'DirectName_Address' in batch_row:
                                        address_match = batch_row['DirectName_Address']
                                    
                                    # Find matching row in final_df
                                    mask = (final_df['DirectName_Cleaned'] == name_match) & (final_df['DirectName_Address'] == address_match)
                                    matching_rows = final_df[mask]
                                    
                                    if len(matching_rows) > 0:
                                        # Update phone data for matching rows
                                        for phone_col in phone_cols_in_batch:
                                            if phone_col.startswith('DirectName_') and pd.notna(batch_row[phone_col]):
                                                final_df.loc[mask, phone_col] = batch_row[phone_col]
                                                if 'Phone_Primary' in phone_col and batch_row[phone_col] != '' and batch_row[phone_col] != 'N/A':
                                                    total_phones_merged += 1
                                
                                # Try to match by IndirectName fields  
                                if 'IndirectName_Cleaned' in batch_row and pd.notna(batch_row['IndirectName_Cleaned']):
                                    name_match = batch_row['IndirectName_Cleaned']
                                    if 'IndirectName_Address' in batch_row:
                                        address_match = batch_row['IndirectName_Address']
                                    
                                    # Find matching row in final_df
                                    mask = (final_df['IndirectName_Cleaned'] == name_match) & (final_df['IndirectName_Address'] == address_match)
                                    matching_rows = final_df[mask]
                                    
                                    if len(matching_rows) > 0:
                                        # Update phone data for matching rows
                                        for phone_col in phone_cols_in_batch:
                                            if phone_col.startswith('IndirectName_') and pd.notna(batch_row[phone_col]):
                                                final_df.loc[mask, phone_col] = batch_row[phone_col]
                                                if 'Phone_Primary' in phone_col and batch_row[phone_col] != '' and batch_row[phone_col] != 'N/A':
                                                    total_phones_merged += 1
                            
                            self.logger.info(f"      ✅ Merged phone data from batch {i+1}")
                        else:
                            self.logger.warning(f"      ⚠️ No phone columns found in batch {i+1}")
                            
                    except Exception as e:
                        self.logger.error(f"      ❌ Error processing batch {i+1}: {e}")
                
                # Save final combined results
                final_df.to_csv(output_file, index=False)
                
                # Update pipeline results with actual merged count
                self.pipeline_results['phone_numbers_found'] = total_phones_merged
                
                self.logger.info(f"✅ Phone number extraction completed: {total_phones_merged} phone numbers merged into final dataset")
                self.logger.info(f"📁 Final results saved to: {output_file}")
                self.pipeline_results['files_created'].append(output_file)
                
                # Keep batch files for debugging (don't delete immediately)
                self.logger.info(f"📦 Keeping batch files for verification: {all_batch_files}")
                
                return output_file
            else:
                self.logger.error("❌ No successful batches found - phone extraction failed")
                return input_file
                
        except Exception as e:
            self.logger.error(f"Phone number extraction error: {e}")
            
        return None
    
    def cleanup_batch_files(self, batch_files: List[str]):
        """Clean up batch files after successful processing"""
        self.logger.info(f"🧹 Cleaning up {len(batch_files)} batch files...")
        for batch_file in batch_files:
            try:
                if os.path.exists(batch_file):
                    os.remove(batch_file)
                    self.logger.debug(f"   ✅ Deleted: {batch_file}")
            except Exception as e:
                self.logger.warning(f"   ⚠️ Cleanup warning for {batch_file}: {e}")
        self.logger.info("✅ Batch file cleanup completed")
        
    async def step5_excel_integration(self, input_file: str) -> Optional[str]:
        """Step 5: Integrate results with Excel file if specified"""
        if not self.excel_file:
            return None
            
        self.logger.info(f"📊 Step 5: Integrating with Excel file: {self.excel_file}")
        
        try:
            # Read the pipeline results
            pipeline_df = pd.read_csv(input_file)
            
            # Create Excel output
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            excel_output = str(self.output_dir / f"broward_excel_integration_{timestamp}.xlsx")
            
            # If Excel file exists, try to merge/update
            if os.path.exists(self.excel_file):
                try:
                    # Read existing Excel
                    existing_df = pd.read_excel(self.excel_file)
                    
                    # Create new sheet with pipeline results
                    with pd.ExcelWriter(excel_output, engine='openpyxl') as writer:
                        existing_df.to_excel(writer, sheet_name='Original_Data', index=False)
                        pipeline_df.to_excel(writer, sheet_name=f'Pipeline_Results_{timestamp}', index=False)
                        
                        # Create summary sheet
                        summary_data = {
                            'Metric': ['Total Records', 'Records with Addresses', 'Phone Numbers Found', 'Success Rate'],
                            'Value': [
                                len(pipeline_df),
                                len(pipeline_df[pipeline_df.get('IndirectName_Address', pd.Series()).notna() & (pipeline_df.get('IndirectName_Address', pd.Series()) != '')]),
                                len(pipeline_df[pipeline_df.get('IndirectName_Phone_Primary', pd.Series()).notna() & (pipeline_df.get('IndirectName_Phone_Primary', pd.Series()) != '')]),
                                f"{(len(pipeline_df[pipeline_df.get('IndirectName_Phone_Primary', pd.Series()).notna() & (pipeline_df.get('IndirectName_Phone_Primary', pd.Series()) != '')]) / max(1, len(pipeline_df)) * 100):.1f}%"
                            ]
                        }
                        summary_df = pd.DataFrame(summary_data)
                        summary_df.to_excel(writer, sheet_name='Summary', index=False)
                        
                    self.logger.info(f"✅ Excel integration completed: {excel_output}")
                    return excel_output
                    
                except Exception as e:
                    self.logger.error(f"Excel integration error: {e}")
            else:
                # Create new Excel file with pipeline results
                pipeline_df.to_excel(excel_output, index=False)
                self.logger.info(f"✅ New Excel file created: {excel_output}")
                return excel_output
                
        except Exception as e:
            self.logger.error(f"Excel integration error: {e}")
            
        return None
        
    async def step6_generate_summary(self, input_file: str) -> Optional[str]:
        """Step 6: Generate comprehensive summary report"""
        self.logger.info("📋 Step 6: Generating summary report...")
        
        try:
            # Read final results
            df = pd.read_csv(input_file)
            
            # Create summary file
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            summary_file = str(self.output_dir / f"pipeline_summary_{timestamp}.txt")
            
            # Calculate statistics
            total_records = len(df)
            records_with_addresses = len(df[df.get('IndirectName_Address', pd.Series()).notna() & (df.get('IndirectName_Address', pd.Series()) != '')])
            records_with_phones = len(df[df.get('IndirectName_Phone_Primary', pd.Series()).notna() & (df.get('IndirectName_Phone_Primary', pd.Series()) != '')])
            success_rate = (records_with_phones / max(1, records_with_addresses)) * 100
            
            # Generate summary content
            summary_content = f"""
BROWARD LIS PENDENS PIPELINE SUMMARY REPORT
==========================================
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Pipeline Duration: {self.pipeline_results['end_time'] - self.pipeline_results['start_time']}

PROCESSING STATISTICS:
-- Total Records Scraped: {self.pipeline_results['broward_records']}
-- Records After Processing: {self.pipeline_results['processed_records']}
-- Addresses Found: {self.pipeline_results['addresses_found']}
-- Phone Numbers Found: {self.pipeline_results['phone_numbers_found']}
-- Success Rate: {success_rate:.1f}%

CONFIGURATION:
-- Days Back: {self.days_back}
-- Batch Size: {self.batch_size}
-- Headless Mode: {self.headless}
-- Excel Integration: {'Yes' if self.excel_file else 'No'}

FILES CREATED:
"""
            
            for file_path in self.pipeline_results['files_created']:
                summary_content += f"- {file_path}\n"
                
            if self.pipeline_results['errors']:
                summary_content += "\nERRORS ENCOUNTERED:\n"
                for error in self.pipeline_results['errors']:
                    summary_content += f"- {error}\n"
                    
            summary_content += f"\nPIPELINE STATUS: {'SUCCESS' if self.pipeline_results['success'] else 'FAILED'}\n"
            
            # Write summary
            with open(summary_file, 'w', encoding='utf-8') as f:
                f.write(summary_content)
                
            self.logger.info(f"✅ Summary report generated: {summary_file}")
            print(summary_content)  # Also print to console
            
            return summary_file
            
        except Exception as e:
            self.logger.error(f"Summary generation error: {e}")
            
        return None

    async def step7_google_sheets_upload(self, input_file: str) -> Optional[str]:
        """Step 7: Upload final data to Google Sheets"""
        try:
            if not input_file or not os.path.exists(input_file):
                self.logger.error("Input file not found for Google Sheets upload")
                return None
                
            # Check if Google Sheets credentials are available
            service_account_info = os.environ.get('GOOGLE_SERVICE_ACCOUNT')
            if not service_account_info:
                self.logger.info("📋 No Google service account credentials found (GOOGLE_SERVICE_ACCOUNT env var)")
                self.logger.info("   Set GOOGLE_SERVICE_ACCOUNT environment variable to enable Google Sheets upload")
                return None
                
            # Read the final processed data
            self.logger.info(f"📊 Reading data from: {input_file}")
            df = pd.read_csv(input_file)
            
            if df.empty:
                self.logger.warning("No data to upload to Google Sheets")
                return None
                
            self.logger.info(f"📤 Uploading {len(df)} records to Google Sheets...")
            
            # Extract spreadsheet ID from environment or use your specific one
            spreadsheet_id = os.environ.get('GOOGLE_SHEETS_ID', '1hcUnRsGfk-lraCrRZYJeeBk1QzszoEEZYPdJn9H21tg')
            
            # Get optional settings from environment
            share_email = os.environ.get('GOOGLE_SHEETS_SHARE_EMAIL')
            worksheet_name = os.environ.get('GOOGLE_SHEETS_WORKSHEET_NAME', 'Broward Lis Pendens')
            append_mode = os.environ.get('GOOGLE_SHEETS_APPEND_MODE', 'true').lower() == 'true'
            
            # Upload to Google Sheets
            sheets_url = upload_to_google_sheets(
                df=df,
                spreadsheet_id=spreadsheet_id,
                worksheet_name=worksheet_name,
                append_mode=append_mode,
                share_email=share_email,
                service_account_info=service_account_info
            )
            
            if sheets_url:
                self.logger.info(f"✅ Successfully uploaded to Google Sheets: {sheets_url}")
                if share_email:
                    self.logger.info(f"📧 Spreadsheet shared with: {share_email}")
                return sheets_url
            else:
                self.logger.error("❌ Google Sheets upload failed")
                return None
                
        except Exception as e:
            self.logger.error(f"Google Sheets upload error: {e}")
            return None

    async def send_completion_email(self):
        """Send completion email with pipeline results and Google Sheets link"""
        try:
            # Calculate statistics
            stats = {
                'records_found': self.pipeline_results['broward_records'],
                'records_processed': self.pipeline_results['processed_records'],
                'addresses_found': self.pipeline_results['addresses_found'],
                'phone_numbers_found': self.pipeline_results['phone_numbers_found'],
                'execution_time_minutes': (
                    (self.pipeline_results['end_time'] - self.pipeline_results['start_time']).total_seconds() / 60
                    if self.pipeline_results['end_time'] and self.pipeline_results['start_time'] else 0
                ),
                'files_created': len(self.pipeline_results['files_created']),
                'success': self.pipeline_results['success']
            }
            
            # Find Google Sheets URL if available
            sheets_url = None
            sheets_id = os.getenv('GOOGLE_SHEETS_ID')
            if sheets_id:
                sheets_url = f"https://docs.google.com/spreadsheets/d/{sheets_id}"
            
            # Find latest Excel file
            excel_file = None
            for file_path in self.pipeline_results['files_created']:
                if file_path.endswith('.xlsx'):
                    excel_file = file_path
                    break
            
            # Send email notification
            success = self.email_notifier.send_completion_email(
                stats=stats,
                sheets_url=sheets_url,
                excel_path=excel_file,
                errors=self.pipeline_results['errors'] if self.pipeline_results['errors'] else None
            )
            
            if success:
                self.logger.info("📧 Completion email sent successfully")
            else:
                self.logger.warning("📧 Failed to send completion email")
                
        except Exception as e:
            self.logger.error(f"📧 Email notification error: {e}")

async def main():
    """Main entry point for the pipeline scheduler with weekly automation support"""
    parser = argparse.ArgumentParser(description='Broward Lis Pendens Complete Pipeline Scheduler')
    parser.add_argument('--days-back', type=int, default=7, 
                       help='Number of days back to search (default: 7)')
    parser.add_argument('--batch-size', type=int, default=9,
                       help='ZabaSearch batch size (default: 9)')
    parser.add_argument('--output-dir', type=str,
                       help='Output directory (default: pipeline_output or BROWARD_OUTPUT_DIR env var)')
    parser.add_argument('--excel-file', type=str,
                       help='Excel file to integrate with results (or BROWARD_EXCEL_FILE env var)')
    parser.add_argument('--input-file', type=str,
                       help='Input file to start from (auto-detects if not provided)')
    parser.add_argument('--headless', action='store_true', 
                       help='Run browsers in headless mode (this is the default)')
    parser.add_argument('--visible', action='store_true',
                       help='Run browsers in visible mode (overrides default headless behavior)')
    parser.add_argument('--max-retries', type=int, default=3,
                       help='Maximum retry attempts (default: 3)')
    
    # Skip options for partial pipeline execution
    parser.add_argument('--skip-scraping', action='store_true',
                       help='Skip Broward scraping step (use existing file)')
    parser.add_argument('--skip-processing', action='store_true',
                       help='Skip name processing step')
    parser.add_argument('--skip-address-extraction', action='store_true',
                       help='Skip address extraction step')
    parser.add_argument('--skip-phone-extraction', action='store_true',
                       help='Skip phone number extraction step')
    
    # Weekly automation options
    parser.add_argument('--weekly-mode', action='store_true',
                       help='Enable weekly automation mode with enhanced error recovery')
    parser.add_argument('--cleanup-old-files', action='store_true',
                       help='Clean up old files (older than 30 days)')
    
    args = parser.parse_args()
    
    # Handle headless mode: default to True unless --visible is specified
    headless_mode = not args.visible  # Default headless unless explicitly visible
    
    # Weekly mode adjustments
    if args.weekly_mode:
        # In weekly mode, be more lenient with errors and enable auto-detection
        args.max_retries = max(args.max_retries, 5)
        headless_mode = True  # Always headless for weekly automation
    
    print("🚀 Broward Lis Pendens Complete Pipeline Scheduler")
    print("=" * 50)
    print(f"Mode: {'Weekly Automation' if args.weekly_mode else 'Manual'}")
    print(f"Days back: {args.days_back}")
    print(f"Batch size: {args.batch_size}")
    print(f"Output directory: {args.output_dir or os.environ.get('BROWARD_OUTPUT_DIR', 'pipeline_output')}")
    print(f"Excel integration: {args.excel_file or os.environ.get('BROWARD_EXCEL_FILE', 'None')}")
    print(f"Headless mode: {headless_mode}")
    print(f"Skip options: Scraping={args.skip_scraping}, Processing={args.skip_processing}, Address={args.skip_address_extraction}, Phone={args.skip_phone_extraction}")
    print("=" * 50)
    
    # Initialize and run pipeline
    scheduler = BrowardLisPendensPipeline(
        output_dir=args.output_dir,
        excel_file=args.excel_file,
        days_back=args.days_back,
        batch_size=args.batch_size,
        headless=headless_mode,
        max_retries=args.max_retries,
        skip_scraping=args.skip_scraping,
        skip_processing=args.skip_processing,
        skip_address_extraction=args.skip_address_extraction,
        skip_phone_extraction=args.skip_phone_extraction
    )
    
    # Cleanup old files if requested
    if args.cleanup_old_files:
        scheduler._cleanup_old_files()
    
    # Run the complete pipeline
    results = await scheduler.run_complete_pipeline(input_file=args.input_file)
    
    # Enhanced reporting for weekly mode
    if args.weekly_mode:
        print("\n📊 WEEKLY AUTOMATION SUMMARY:")
        print(f"Success: {results['success']}")
        print(f"Records processed: {results.get('processed_records', 0)}")
        print(f"Addresses found: {results.get('addresses_found', 0)}")
        print(f"Phone numbers found: {results.get('phone_numbers_found', 0)}")
        print(f"Files created: {len(results.get('files_created', []))}")
        if results.get('errors'):
            print(f"Errors: {len(results['errors'])}")
    
    # Exit with appropriate code
    sys.exit(0 if results['success'] else 1)


if __name__ == "__main__":
    asyncio.run(main())
