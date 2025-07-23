#!/usr/bin/env python3
"""
Broward County Lis Pendens Scraper with Integrated Name Processing
===============================================================

This script automates the process of searching, downloading, and processing Lis Pendens records
from the Broward County Official Records website with stealth techniques to avoid detection.

Features:
- Headless browser automation with stealth settings
- Automatic disclaimer acceptance
- Configurable date ranges
- CSV download capability
- Integrated name processing and cleaning
- Human-like delays and behavior simulation
- Robust error handling
- Console-only logging for cron jobs

Author: Blake Jackson
Date: July 18, 2025
"""

import asyncio
import logging
import os
import random
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
from playwright.async_api import async_playwright, Page, BrowserContext

# Import the name processor and address extractor
from lis_pendens_processor import process_lis_pendens_csv
from fast_address_extractor import process_addresses_fast


class BrowardLisPendensScraper:
    """
    A comprehensive scraper for Broward County Lis Pendens records.
    """
    
    def __init__(self, download_dir: str = "downloads", headless: bool = True, cleanup_old_files: bool = True):
        """
        Initialize the scraper.
        
        Args:
            download_dir: Directory to save downloaded files
            headless: Whether to run browser in headless mode
            cleanup_old_files: Whether to clean up old files to prevent accumulation
        """
        self.download_dir = Path(download_dir)
        self.download_dir.mkdir(exist_ok=True)
        self.headless = headless
        self.cleanup_old_files = cleanup_old_files
        self.base_url = "https://officialrecords.broward.org/AcclaimWeb/search/Disclaimer?st=/AcclaimWeb/search/SearchTypeDocType"
        
        # Configure timeouts from environment variables (important for cloud deployment)
        self.navigation_timeout = int(os.environ.get('BROWARD_NAVIGATION_TIMEOUT', '180000'))  # 3 minutes default
        self.page_load_delay = int(os.environ.get('BROWARD_PAGE_LOAD_DELAY', '8000'))  # 8 seconds default
        self.connection_timeout = int(os.environ.get('BROWARD_CONNECTION_TIMEOUT', '30000'))  # 30 seconds default
        
        # Clean up old files if requested (important for cron jobs)
        if self.cleanup_old_files:
            self._cleanup_old_files()
        
        # Setup logging
        self._setup_logging()
        
    def _setup_logging(self):
        """Setup console-only logging for cron jobs."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler()  # Console only, no file logging
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def _cleanup_old_files(self, days_to_keep: int = 7):
        """
        Clean up old CSV files to prevent accumulation during cron jobs.
        
        Args:
            days_to_keep: Number of days of files to keep (default: 7)
        """
        try:
            cutoff_time = time.time() - (days_to_keep * 24 * 60 * 60)
            
            # Clean up old raw CSV files
            for file_path in self.download_dir.glob("broward_lis_pendens_*.csv"):
                if file_path.stat().st_mtime < cutoff_time:
                    file_path.unlink()
                    
            # Clean up old processed CSV files  
            for file_path in self.download_dir.glob("broward_lis_pendens_*_processed.csv"):
                if file_path.stat().st_mtime < cutoff_time:
                    file_path.unlink()
                    
            # Clean up other generated files
            for pattern in ["cleaned_person_names.txt", "business_names.txt", "person_names_report.csv"]:
                for file_path in self.download_dir.glob(pattern):
                    if file_path.stat().st_mtime < cutoff_time:
                        file_path.unlink()
                        
        except Exception as e:
            # Don't fail the entire process if cleanup fails
            print(f"Warning: Could not clean up old files: {e}")
        
    async def create_stealth_browser(self, playwright):
        """
        Create a browser with advanced stealth settings to avoid detection.
        
        Returns:
            Tuple of (browser, context)
        """
        self.logger.info("Creating stealth browser...")
        
        # Advanced browser arguments for stealth and cloud compatibility
        browser_args = [
            '--no-sandbox',
            '--disable-blink-features=AutomationControlled',
            '--disable-dev-shm-usage',
            '--disable-extensions',
            '--disable-plugins',
            '--disable-default-apps',
            '--disable-background-timer-throttling',
            '--disable-backgrounding-occluded-windows',
            '--disable-renderer-backgrounding',
            '--disable-features=TranslateUI',
            '--disable-ipc-flooding-protection',
            '--disable-hang-monitor',
            '--disable-popup-blocking',
            '--disable-prompt-on-repost',
            '--disable-sync',
            '--disable-web-security',
            '--metrics-recording-only',
            '--no-first-run',
            '--safebrowsing-disable-auto-update',
            '--enable-automation',
            '--password-store=basic',
            '--use-mock-keychain',
            '--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            '--disable-gpu',
            '--disable-software-rasterizer',
            '--single-process',
            '--disable-background-networking',
            '--disable-background-media-playback',
            '--disable-background-video-playback'
        ]
        
        browser = await playwright.chromium.launch(
            headless=self.headless,
            args=browser_args
        )
        
        # Create context with stealth settings
        context = await browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            locale='en-US',
            timezone_id='America/New_York',
            extra_http_headers={
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate, br',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Cache-Control': 'max-age=0'
            }
        )
        
        # Set download behavior
        await context.set_extra_http_headers({
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
        })
        
        # Add anti-detection scripts
        await context.add_init_script("""
            // Remove webdriver property
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined,
            });
            
            // Mock plugins
            Object.defineProperty(navigator, 'plugins', {
                get: () => [1, 2, 3, 4, 5],
            });
            
            // Mock languages
            Object.defineProperty(navigator, 'languages', {
                get: () => ['en-US', 'en'],
            });
            
            // Mock chrome object
            window.chrome = {
                runtime: {},
            };
            
            // Mock permissions
            const originalQuery = window.navigator.permissions.query;
            window.navigator.permissions.query = (parameters) => (
                parameters.name === 'notifications' ?
                    Promise.resolve({ state: Cypress.env('notification_permission') || 'denied' }) :
                    originalQuery(parameters)
            );
            
            // Randomize screen properties
            Object.defineProperty(screen, 'availHeight', {
                get: () => 1040 + Math.floor(Math.random() * 10)
            });
            Object.defineProperty(screen, 'availWidth', {
                get: () => 1920 + Math.floor(Math.random() * 10)
            });
        """)
        
        return browser, context
        
    async def human_like_delay(self, min_seconds: float = 1.0, max_seconds: float = 3.0):
        """Add human-like random delays between actions."""
        delay = random.uniform(min_seconds, max_seconds)
        self.logger.debug(f"Waiting {delay:.2f} seconds...")
        await asyncio.sleep(delay)
        
    async def type_like_human(self, page: Page, selector: str, text: str):
        """Type text with human-like delays between keystrokes."""
        self.logger.debug(f"Typing '{text}' into {selector}")
        element = page.locator(selector)
        await element.clear()
        
        for char in text:
            await element.type(char)
            await asyncio.sleep(random.uniform(0.05, 0.15))
            
    async def accept_disclaimer(self, page: Page):
        """Accept the website disclaimer."""
        self.logger.info("Accepting disclaimer...")
        
        try:
            # Wait for disclaimer button and click it
            disclaimer_button = page.get_by_role('button', name='I accept the conditions above.')
            await disclaimer_button.wait_for(state='visible', timeout=10000)
            await self.human_like_delay(1, 2)
            await disclaimer_button.click()
            self.logger.info("Disclaimer accepted successfully")
            return True
        except Exception as e:
            self.logger.warning(f"No disclaimer found or already accepted: {e}")
            return False
            
    async def select_document_type(self, page: Page):
        """Select ONLY LIS PENDENS document type - simplified approach."""
        self.logger.info("Selecting LIS PENDENS document type...")
        
        try:
            # Click the document type dropdown button
            doc_type_button = page.locator('button').filter(has_text='...')
            await doc_type_button.wait_for(state='visible', timeout=10000)
            await self.human_like_delay(1, 2)
            await doc_type_button.click()
            
            # Click on Doc Type List tab to get to the individual document types
            doc_type_list_tab = page.get_by_role('link', name='Doc Type List')
            await doc_type_list_tab.click()
            await self.human_like_delay(1, 2)
            
            # Simply select the LIS PENDENS checkbox - ignore everything else
            lis_pendens_checkbox = page.get_by_role('checkbox', name='LIS PENDENS (LP)')
            await lis_pendens_checkbox.click()
            await self.human_like_delay(1, 2)
            self.logger.info("Selected LIS PENDENS (LP)")
            
            # Click Done button
            done_button = page.get_by_role('button', name='Done')
            await done_button.click()
            
            self.logger.info("LIS PENDENS document type selected successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Error selecting document type: {e}")
            return False
            
    async def set_date_range(self, page: Page, days_back: int = 7):
        """Set the date range using the dropdown - corrected approach."""
        self.logger.info(f"Setting date range to last {days_back} days using dropdown...")
        
        try:
            # Click the date range dropdown - fixed selector
            date_range_dropdown = page.locator('#DateRangeDropDown')
            await date_range_dropdown.wait_for(state='visible', timeout=10000)
            await self.human_like_delay(1, 2)
            await date_range_dropdown.click()
            
            # Select the appropriate option based on days_back
            if days_back <= 1:
                option_text = "Today"
            elif days_back <= 7:
                option_text = "Last 7 Days"
            elif days_back <= 14:
                option_text = "Last 14 Days"
            else:
                option_text = "Last Month"
                
            self.logger.info(f"Selecting '{option_text}' from dropdown")
            
            # Click the option using role-based selector
            date_option = page.get_by_role('option', name=option_text)
            await date_option.click()
            await self.human_like_delay(1, 2)
            
            self.logger.info(f"Date range set successfully using dropdown: {option_text}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error setting date range with dropdown: {e}")
            self.logger.info("Falling back to manual date entry...")
            
            # Fallback to manual date entry if dropdown fails
            try:
                # Calculate dates (ensure within 124-day limit)
                from datetime import datetime, timedelta
                to_date = datetime.now()
                from_date = to_date - timedelta(days=min(days_back, 120))  # Cap at 120 days to be safe
                
                # Format dates as MM/DD/YYYY
                from_date_str = from_date.strftime('%m/%d/%Y')
                to_date_str = to_date.strftime('%m/%d/%Y')
                
                self.logger.info(f"Setting manual date range: {from_date_str} to {to_date_str}")
                
                # Clear and set the "From Record Date" field
                from_date_field = page.locator('#RecordDateFrom')
                await from_date_field.wait_for(state='visible', timeout=10000)
                await from_date_field.fill('')
                await self.human_like_delay(0.5, 1)
                await from_date_field.fill(from_date_str)
                
                # Clear and set the "To Record Date" field  
                to_date_field = page.locator('#RecordDateTo')
                await to_date_field.wait_for(state='visible', timeout=10000)
                await to_date_field.fill('')
                await self.human_like_delay(0.5, 1)
                await to_date_field.fill(to_date_str)
                
                self.logger.info(f"Fallback date range set successfully: {from_date_str} to {to_date_str}")
                return True
                
            except Exception as fallback_error:
                self.logger.error(f"Both dropdown and manual date entry failed: {fallback_error}")
                return False
            
    async def perform_search(self, page: Page):
        """Perform the search for Lis Pendens records."""
        self.logger.info("Performing search...")
        
        try:
            # Click search button
            search_button = page.get_by_role('button', name='Search', exact=True)
            await search_button.wait_for(state='visible', timeout=10000)
            await self.human_like_delay(1, 2)
            await search_button.click()

            # Wait for response - either results or error
            self.logger.info("Waiting for search results...")
            await self.human_like_delay(3, 5)
            
            # Check for error messages first
            try:
                # Check for "maximum limit exceeded" error
                error_dialog = page.locator('text=The number of results exceeded the maximum limit')
                if await error_dialog.is_visible(timeout=3000):
                    self.logger.error("Search returned too many results - need to narrow date range")
                    # Close error dialog
                    try:
                        close_button = page.locator('text=Close').first
                        await close_button.click()
                    except:
                        pass
                    return False
                    
                # Check for "date range cannot span more than 124 days" error
                date_error = page.locator('text=Date range cannot span more than 124 days')
                if await date_error.is_visible(timeout=3000):
                    self.logger.error("Date range exceeds 124-day limit")
                    # Close error dialog
                    try:
                        close_button = page.locator('text=Close').first
                        await close_button.click()
                    except:
                        pass
                    return False
                    
            except:
                pass  # No error dialogs, continue with normal flow
            
            # Check for results
            try:
                # Wait for either results table to appear OR no results message
                await page.wait_for_selector('text=Displaying items', timeout=15000)
                
                # Get result count information
                results_indicator = page.locator('text=Displaying items')
                result_text = await results_indicator.text_content()
                self.logger.info(f"Search completed: {result_text}")
                
                # Check if we have zero results
                if result_text and "0 - 0 of 0" in result_text:
                    self.logger.warning("No LIS PENDENS records found for this date range")
                    return True  # Search was successful, just no results
                
                return True
                
            except Exception as e:
                # If we can't find the results indicator, check for other success indicators
                self.logger.warning(f"Could not find results indicator: {e}")
                
                # Check if we're on a results page by looking for the export link
                try:
                    export_link = page.locator('text=Export to CSV')
                    if await export_link.is_visible():
                        self.logger.info("Search completed - results page loaded")
                        return True
                except:
                    pass
                
                # Check for table headers as another indicator
                try:
                    table_header = page.locator('text=First Direct Name')
                    if await table_header.is_visible():
                        self.logger.info("Search completed - results table visible")
                        return True
                except:
                    pass
                
                self.logger.error("Could not verify search completion")
                return False
            
        except Exception as e:
            self.logger.error(f"Error performing search: {e}")
            return False
            
    async def download_results(self, page: Page) -> Optional[str]:
        """Download the search results as CSV."""
        self.logger.info("Checking for results to download...")
        
        try:
            # First check if there are any results to download
            try:
                results_indicator = page.locator('text=Displaying items')
                result_text = await results_indicator.text_content()
                
                # Check for zero results
                if result_text and "0 - 0 of 0" in result_text:
                    self.logger.warning("No results to download - search returned zero records")
                    return None
                    
                self.logger.info(f"Found results to download: {result_text}")
                    
            except Exception:
                self.logger.warning("Could not verify result count, proceeding with download attempt")
            
            # Check if Export to CSV link is available
            try:
                export_link = page.get_by_role('link', name='Export to CSV')
                await export_link.wait_for(state='visible', timeout=5000)
            except Exception:
                self.logger.warning("Export to CSV link not found - no results to download")
                return None
            
            # Setup download handling
            async with page.expect_download() as download_info:
                # Click Export to CSV link
                await export_link.click()
                
            download = await download_info.value
            
            # Use consistent filename pattern for easier management
            current_date = datetime.now().strftime("%Y%m%d")
            timestamp = datetime.now().strftime("%H%M%S")
            filename = f"broward_lis_pendens_{current_date}_{timestamp}.csv"
            filepath = self.download_dir / filename
            
            # Save the downloaded file
            await download.save_as(filepath)
            
            # Verify the file was actually created and has content
            if filepath.exists() and filepath.stat().st_size > 0:
                self.logger.info(f"File downloaded successfully: {filepath}")
                return str(filepath)
            else:
                self.logger.error("Downloaded file is empty or doesn't exist")
                return None
            
        except Exception as e:
            self.logger.error(f"Error downloading results: {e}")
            return None
            
    async def scrape_raw_data(self, days_back: int = 7) -> Optional[str]:
        """
        Internal method to scrape raw Lis Pendens records.
        
        Args:
            days_back: Number of days back to search (1, 7, 14, or 30)
            
        Returns:
            Path to downloaded raw CSV file or None if failed
        """
        self.logger.info("Starting Broward County Lis Pendens scraping...")
        self.logger.info(f"Target date range: Last {days_back} days")
        
        async with async_playwright() as playwright:
            browser, context = await self.create_stealth_browser(playwright)
            page = await context.new_page()
            
            # Set default timeouts for the page
            page.set_default_navigation_timeout(self.navigation_timeout)
            page.set_default_timeout(self.connection_timeout)
            
            try:
                # First test basic connectivity
                if not await self.test_connectivity(page):
                    raise Exception("Failed basic connectivity test to Broward County website")
                
                # Navigate with retry logic for better reliability
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        self.logger.info(f"Navigation attempt {attempt + 1}/{max_retries}")
                        self.logger.info(f"Navigating to: {self.base_url}")
                        self.logger.info(f"Using navigation timeout: {self.navigation_timeout}ms")
                        
                        await page.goto(self.base_url, wait_until='domcontentloaded', timeout=self.navigation_timeout)
                        self.logger.info("✅ Successfully navigated to Broward County website")
                        break
                        
                    except Exception as nav_error:
                        self.logger.warning(f"❌ Navigation attempt {attempt + 1} failed: {nav_error}")
                        if attempt < max_retries - 1:
                            wait_time = (attempt + 1) * 10  # Exponential backoff
                            self.logger.info(f"⏱️ Waiting {wait_time} seconds before retry...")
                            await asyncio.sleep(wait_time)
                        else:
                            raise nav_error
                            
                await self.human_like_delay(2, 4)
                
                # Accept disclaimer if present
                await self.accept_disclaimer(page)
                await self.human_like_delay(2, 3)
                
                # Select document type
                if not await self.select_document_type(page):
                    raise Exception("Failed to select document type")
                await self.human_like_delay(2, 3)
                
                # Set date range
                if not await self.set_date_range(page, days_back):
                    raise Exception("Failed to set date range")
                await self.human_like_delay(2, 3)
                
                # Perform search
                if not await self.perform_search(page):
                    raise Exception("Failed to perform search")
                await self.human_like_delay(3, 5)
                
                # Download results
                filepath = await self.download_results(page)
                if not filepath:
                    raise Exception("Failed to download results")
                
                self.logger.info("Raw data scraping completed successfully!")
                return filepath
                
            except Exception as e:
                self.logger.error(f"Scraping failed: {e}")
                return None
                
            finally:
                await browser.close()
            
    async def scrape_and_process_lis_pendens(self, days_back: int = 7) -> Optional[str]:
        """
        Main method to scrape and process Lis Pendens records.
        This method is designed to run reliably in cron jobs for weeks without issues.
        
        Args:
            days_back: Number of days back to search (1, 7, 14, or 30)
            
        Returns:
            Path to final processed CSV file or None if failed/no results
        """
        self.logger.info("Starting integrated Broward County Lis Pendens scraping and processing...")
        self.logger.info(f"Target date range: Last {days_back} days")
        
        # First, scrape the raw data
        raw_csv_path = await self.scrape_raw_data(days_back)
        
        if not raw_csv_path:
            self.logger.error("Scraping failed or no results found, cannot proceed with processing")
            return None
        
        # Verify the raw file has actual data
        try:
            import pandas as pd
            test_df = pd.read_csv(raw_csv_path)
            if len(test_df) == 0:
                self.logger.warning("Raw CSV file is empty, no records to process")
                return None
                
            self.logger.info(f"Raw file contains {len(test_df)} records, proceeding with processing")
            
        except Exception as e:
            self.logger.error(f"Could not verify raw data file: {e}")
            return None
        
        # Now process the scraped data
        try:
            self.logger.info("Starting name processing...")
            processed_csv_path = process_lis_pendens_csv(raw_csv_path, silent_mode=True)
            
            if processed_csv_path and os.path.exists(processed_csv_path):
                self.logger.info(f"Name processing completed successfully: {processed_csv_path}")
                
                # Now extract addresses for all person names
                try:
                    self.logger.info("Starting address extraction for all person names...")
                    final_csv_path = await process_addresses_fast(processed_csv_path, max_names=None, headless=True)
                    
                    if final_csv_path and os.path.exists(final_csv_path):
                        self.logger.info(f"Address extraction completed successfully: {final_csv_path}")
                        return final_csv_path
                    else:
                        self.logger.warning("Address extraction failed, returning processed file without addresses")
                        return processed_csv_path
                        
                except Exception as addr_error:
                    self.logger.error(f"Error during address extraction: {addr_error}")
                    self.logger.info("Returning processed file without addresses")
                    return processed_csv_path
                    
            else:
                self.logger.error("Name processing failed but raw data is available")
                return raw_csv_path  # Return raw file if processing fails
                
        except Exception as e:
            self.logger.error(f"Error during name processing: {e}")
            self.logger.info("Returning raw data file as fallback")
            return raw_csv_path  # Return raw file if processing fails
                
    def analyze_results(self, csv_filepath: str) -> dict:
        """
        Analyze the downloaded CSV results.
        
        Args:
            csv_filepath: Path to the CSV file
            
        Returns:
            Dictionary with analysis results
        """
        try:
            df = pd.read_csv(csv_filepath)
            
            analysis = {
                'total_records': len(df),
                'date_range': {
                    'earliest': df['Record Date'].min() if 'Record Date' in df.columns else 'N/A',
                    'latest': df['Record Date'].max() if 'Record Date' in df.columns else 'N/A'
                },
                'top_plaintiffs': df['First Direct Name'].value_counts().head(10).to_dict() if 'First Direct Name' in df.columns else {},
                'case_types': df['Case #'].apply(lambda x: x.split('-')[0] if pd.notna(x) and '-' in str(x) else 'Unknown').value_counts().to_dict() if 'Case #' in df.columns else {},
                'file_info': {
                    'filepath': csv_filepath,
                    'file_size': os.path.getsize(csv_filepath),
                    'columns': list(df.columns)
                }
            }
            
            self.logger.info(f"Analysis completed: {analysis['total_records']} records found")
            return analysis
            
        except Exception as e:
            self.logger.error(f"Error analyzing results: {e}")
            return {}

    async def test_connectivity(self, page: Page):
        """Test basic connectivity to the Broward County website"""
        try:
            self.logger.info("🔍 Testing connectivity to Broward County...")
            
            # Try a simple navigation to the base domain first
            base_domain = "https://officialrecords.broward.org/"
            await page.goto(base_domain, wait_until='domcontentloaded', timeout=30000)
            self.logger.info("✅ Base domain connectivity OK")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Base domain connectivity failed: {e}")
            return False


async def main():
    """Main function to run the integrated scraper and processor - optimized for cron jobs."""
    
    # Use default settings - no user input required for cron jobs
    days_back = 7  # Default to last 7 days
    headless = True  # Run in headless mode by default
    
    # Create scraper with cleanup enabled for cron jobs
    scraper = BrowardLisPendensScraper(headless=headless, cleanup_old_files=True)
    
    # Start the process
    processed_filepath = await scraper.scrape_and_process_lis_pendens(days_back)
    
    if processed_filepath:
        print(f"SUCCESS: Processed data saved to: {processed_filepath}")
        
        # Brief analysis for logging
        try:
            analysis = scraper.analyze_results(processed_filepath)
            if analysis:
                print(f"Records found: {analysis['total_records']}")
                print(f"Date range: {analysis['date_range']['earliest']} to {analysis['date_range']['latest']}")
        except Exception as e:
            print(f"Analysis failed but data was processed: {e}")
            
        return True  # Success
        
    else:
        print("FAILED: No data processed or scraping failed")
        return False  # Failure


if __name__ == "__main__":
    asyncio.run(main())
