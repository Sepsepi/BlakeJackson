#!/usr/bin/env python3
"""
ZabaSearch Batch Phone Number Extractor - Production Cron Job Version
- Dynamic file detection (no hardcoded filenames)
- Complete session management with proper cleanup
- Intelligent batching with rate limiting protection
- Full headless operation for server environments
- Robust error handling for long-running processes
- Progress tracking and resume capability
"""
import asyncio
import pandas as pd
import random
import re
import os
import glob
import time
import argparse
import sys
from datetime import datetime
from playwright.async_api import async_playwright
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote

class ZabaSearchBatchProcessor:
    def __init__(self, headless=True, batch_size=15):
        self.headless = headless
        self.batch_size = batch_size
        self.session_count = 0
        self.total_processed = 0
        self.successful_lookups = 0
        self.failed_lookups = 0
        self.current_batch_successes = 0
        
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/121.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'
        ]
        
        print(f"🤖 ZabaSearch Batch Processor initialized")
        print(f"   • Headless mode: {self.headless}")
        print(f"   • Batch size: {self.batch_size}")
        print(f"   • Session isolation: Enabled")
        print(f"   • Rate limiting protection: Enabled")

    def find_latest_csv_file(self, directory="."):
        """Find the most recent CSV file with addresses"""
        pattern_priorities = [
            "*with_addresses*.csv",
            "*processed*.csv", 
            "*LisPendens*.csv",
            "*.csv"
        ]
        
        for pattern in pattern_priorities:
            files = glob.glob(os.path.join(directory, pattern))
            if files:
                # Sort by modification time, newest first
                files.sort(key=os.path.getmtime, reverse=True)
                
                # Check if it has address columns
                for file_path in files:
                    try:
                        df_check = pd.read_csv(file_path, nrows=1)
                        address_columns = [col for col in df_check.columns if 'address' in col.lower()]
                        if address_columns:
                            print(f"✅ Found input file: {file_path}")
                            print(f"   • Address columns: {address_columns}")
                            return file_path
                    except:
                        continue
        
        raise FileNotFoundError("No suitable CSV file with addresses found")

    def generate_output_filename(self, input_path):
        """Generate timestamped output filename"""
        base_name = os.path.splitext(os.path.basename(input_path))[0]
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"{base_name}_with_phone_numbers_{timestamp}.csv"

    async def create_new_session(self, playwright):
        """Create a completely fresh browser session with full cleanup"""
        print(f"🌐 Creating new session #{self.session_count + 1}")
        
        # Clean browser arguments for headless operation
        launch_args = [
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-dev-shm-usage',
            '--disable-accelerated-2d-canvas',
            '--no-first-run',
            '--no-zygote',
            '--disable-gpu',
            '--disable-background-timer-throttling',
            '--disable-backgrounding-occluded-windows',
            '--disable-renderer-backgrounding',
            '--disable-features=TranslateUI',
            '--disable-ipc-flooding-protection',
            '--disable-blink-features=AutomationControlled',
            '--disable-web-security',
            '--disable-features=VizDisplayCompositor'
        ]
        
        if self.headless:
            launch_args.append('--headless=new')
        
        browser = await playwright.chromium.launch(
            headless=self.headless,
            args=launch_args
        )
        
        # Create context with random user agent and viewport
        user_agent = random.choice(self.user_agents)
        viewport = {
            'width': random.randint(1366, 1920), 
            'height': random.randint(768, 1080)
        }
        
        context = await browser.new_context(
            viewport=viewport,
            user_agent=user_agent,
            locale='en-US',
            timezone_id='America/New_York'
        )
        
        # Anti-detection stealth scripts
        await context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined,
            });
            Object.defineProperty(navigator, 'plugins', {
                get: () => [1, 2, 3, 4, 5],
            });
            Object.defineProperty(navigator, 'languages', {
                get: () => ['en-US', 'en'],
            });
            window.chrome = {
                runtime: {},
            };
        """)
        
        page = await context.new_page()
        
        # Reset batch success counter for new session
        self.current_batch_successes = 0
        self.session_count += 1
        
        print(f"   ✅ Session ready with User-Agent: {user_agent[:50]}...")
        
        return browser, context, page

    async def close_session_completely(self, browser, context, page):
        """Completely close and clean up browser session"""
        try:
            if page:
                await page.close()
            if context:
                await context.close()
            if browser:
                await browser.close()
            
            # Additional cleanup delay
            await asyncio.sleep(2)
            print("   🗑️ Session completely closed and cleaned up")
            
        except Exception as e:
            print(f"   ⚠️ Session cleanup warning: {e}")

    async def smart_delay(self, delay_type="normal"):
        """Intelligent delays for rate limiting protection"""
        delay_ranges = {
            "quick": (0.5, 1.2),
            "normal": (1.5, 3.0), 
            "slow": (3.0, 5.0),
            "typing": (0.1, 0.3),
            "between_searches": (2.0, 4.0),
            "between_batches": (30.0, 60.0),
            "session_break": (60.0, 120.0)
        }
        
        min_delay, max_delay = delay_ranges.get(delay_type, delay_ranges["normal"])
        delay = random.uniform(min_delay, max_delay)
        
        if delay > 10:
            print(f"   ⏳ Waiting {delay:.1f}s for {delay_type}")
        
        await asyncio.sleep(delay)

    def normalize_address(self, address: str) -> str:
        """Normalize address for comparison"""
        if not address:
            return ""
        
        # Convert to uppercase and remove extra spaces
        normalized = re.sub(r'\s+', ' ', address.upper().strip())
        
        # Common address normalizations
        replacements = {
            ' STREET': ' ST',
            ' AVENUE': ' AVE',
            ' DRIVE': ' DR',
            ' COURT': ' CT',
            ' PLACE': ' PL',
            ' ROAD': ' RD',
            ' CIRCLE': ' CIR',
            ' BOULEVARD': ' BLVD',
            ' LANE': ' LN',
            ' TERRACE': ' TER'
        }
        
        for old, new in replacements.items():
            normalized = normalized.replace(old, new)
        
        return normalized

    def addresses_match(self, csv_address: str, zaba_address: str) -> bool:
        """Enhanced address matching with ordinal number handling"""
        if not csv_address or not zaba_address:
            return False
        
        csv_norm = self.normalize_address(csv_address)
        zaba_norm = self.normalize_address(zaba_address)
        
        # Extract components for flexible matching
        csv_parts = csv_norm.split()
        zaba_parts = zaba_norm.split()
        
        if len(csv_parts) < 2 or len(zaba_parts) < 2:
            return False
        
        # Street number must match
        if csv_parts[0] != zaba_parts[0]:
            return False
        
        # Check street name components with ordinal variations
        csv_street = ' '.join(csv_parts[1:])
        zaba_street = ' '.join(zaba_parts[1:])
        
        # Handle ordinal numbers (1ST, 2ND, 3RD, 4TH, etc.)
        ordinal_pattern = r'\b(\d+)(ST|ND|RD|TH)\b'
        
        # Normalize ordinals to just numbers for comparison
        csv_street_norm = re.sub(ordinal_pattern, r'\1', csv_street)
        zaba_street_norm = re.sub(ordinal_pattern, r'\1', zaba_street)
        
        # Direct match after ordinal normalization
        if csv_street_norm == zaba_street_norm:
            return True
        
        # Fuzzy match for minor variations
        similarity = len(set(csv_street_norm.split()) & set(zaba_street_norm.split()))
        total_words = max(len(csv_street_norm.split()), len(zaba_street_norm.split()))
        
        return similarity / total_words >= 0.7 if total_words > 0 else False

    async def accept_terms_if_needed(self, page):
        """Accept ZabaSearch terms if needed"""
        try:
            terms_button = page.locator('button:has-text("I AGREE")')
            if await terms_button.count() > 0:
                await terms_button.click()
                await self.smart_delay("quick")
                print("   ✅ Terms accepted")
                return True
        except:
            pass
        return False

    async def search_zabasearch(self, page, name: str, address: str) -> Dict:
        """Perform ZabaSearch lookup with enhanced error handling"""
        try:
            # Navigate to ZabaSearch
            await page.goto('https://www.zabasearch.com/', 
                          wait_until='domcontentloaded', 
                          timeout=30000)
            await self.smart_delay("quick")
            
            # Accept terms if needed
            await self.accept_terms_if_needed(page)
            
            # Parse name
            if ',' in name:
                parts = name.split(',')
                last_name = parts[0].strip()
                first_name = parts[1].strip().split()[0] if len(parts) > 1 else ""
            else:
                name_parts = name.strip().split()
                first_name = name_parts[0] if name_parts else ""
                last_name = name_parts[-1] if len(name_parts) > 1 else ""
            
            if not first_name or not last_name:
                return {'error': 'Invalid name format'}
            
            print(f"   🔍 Searching: {first_name} {last_name}")
            
            # Fill search form
            await page.fill('input[placeholder*="John"]', first_name)
            await self.smart_delay("typing")
            
            await page.fill('input[placeholder*="Smith"]', last_name)
            await self.smart_delay("typing")
            
            # Select Florida
            await page.select_option('select', value='Florida')
            await self.smart_delay("quick")
            
            # Submit search
            await page.click('button:has-text("Search")')
            await page.wait_for_load_state('domcontentloaded', timeout=30000)
            await self.smart_delay("normal")
            
            # Extract phone numbers and addresses
            phone_numbers = []
            matched_address = None
            
            # Look for phone numbers in various sections
            phone_selectors = [
                'a[href*="phone/"]',
                'text=/\\(\\d{3}\\) \\d{3}-\\d{4}/',
                'text=/\\d{3}-\\d{3}-\\d{4}/',
                '.phone-number',
                '[data-phone]'
            ]
            
            for selector in phone_selectors:
                try:
                    elements = page.locator(selector)
                    count = await elements.count()
                    
                    for i in range(min(count, 20)):  # Limit to prevent infinite loops
                        try:
                            element = elements.nth(i)
                            text = await element.text_content()
                            
                            if text:
                                # Extract phone numbers using regex
                                phone_matches = re.findall(r'\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}', text)
                                phone_numbers.extend(phone_matches)
                        except:
                            continue
                except:
                    continue
            
            # Clean and deduplicate phone numbers
            cleaned_phones = []
            for phone in phone_numbers:
                # Normalize phone number format
                cleaned = re.sub(r'[^\d]', '', phone)
                if len(cleaned) == 10 and cleaned not in cleaned_phones:
                    formatted = f"({cleaned[:3]}) {cleaned[3:6]}-{cleaned[6:]}"
                    cleaned_phones.append(formatted)
            
            # Look for address matches
            try:
                address_elements = page.locator('text=/\\d+[^,]*(?:ST|AVE|DR|CT|WAY|CIR|RD|PL|TER|BLVD|LANE|STREET|AVENUE|DRIVE|COURT|CIRCLE|ROAD|PLACE|TERRACE|BOULEVARD)/i')
                address_count = await address_elements.count()
                
                for i in range(min(address_count, 10)):
                    try:
                        element = address_elements.nth(i)
                        zaba_addr = await element.text_content()
                        
                        if zaba_addr and self.addresses_match(address, zaba_addr.strip()):
                            matched_address = zaba_addr.strip()
                            print(f"   ✅ Address match: {matched_address}")
                            break
                    except:
                        continue
            except:
                pass
            
            result = {
                'phones': cleaned_phones,
                'primary_phone': cleaned_phones[0] if cleaned_phones else None,
                'secondary_phone': cleaned_phones[1] if len(cleaned_phones) > 1 else None,
                'all_phones': cleaned_phones,
                'matched_address': matched_address,
                'total_phones': len(cleaned_phones)
            }
            
            if cleaned_phones:
                print(f"   📞 Found {len(cleaned_phones)} phone(s): {', '.join(cleaned_phones[:3])}")
            else:
                print(f"   📞 No phones found")
                
            return result
            
        except Exception as e:
            print(f"   ❌ Search error: {e}")
            return {'error': str(e)}

    async def process_batch(self, playwright, records_batch, df, output_path):
        """Process a batch of records with complete session isolation"""
        print(f"\n🔄 Processing batch of {len(records_batch)} records...")
        
        # Create completely new session for this batch
        browser, context, page = await self.create_new_session(playwright)
        
        batch_successes = 0
        
        try:
            for i, record in enumerate(records_batch):
                try:
                    print(f"\n📋 Record {i+1}/{len(records_batch)} in current batch")
                    print(f"   • Name: {record['name']}")
                    print(f"   • Address: {record['address']}")
                    
                    # Perform search
                    result = await self.search_zabasearch(page, record['name'], record['address'])
                    
                    if 'error' not in result and result.get('phones'):
                        # Update CSV with results
                        row_idx = record['row_index']
                        prefix = record['column_prefix']
                        
                        df.at[row_idx, f"{prefix}_Phone_Primary"] = result.get('primary_phone', '')
                        df.at[row_idx, f"{prefix}_Phone_Secondary"] = result.get('secondary_phone', '')
                        df.at[row_idx, f"{prefix}_Phone_All"] = ', '.join(result.get('all_phones', []))
                        df.at[row_idx, f"{prefix}_Address_Match"] = result.get('matched_address', '')
                        df.at[row_idx, f"{prefix}_ZabaSearch_Status"] = "Success"
                        
                        batch_successes += 1
                        self.successful_lookups += 1
                        print(f"   ✅ Success #{batch_successes} in batch")
                    else:
                        # Mark as failed
                        row_idx = record['row_index']
                        prefix = record['column_prefix']
                        
                        df.at[row_idx, f"{prefix}_ZabaSearch_Status"] = f"Failed: {result.get('error', 'No phones found')}"
                        self.failed_lookups += 1
                        print(f"   ❌ Failed: {result.get('error', 'No phones found')}")
                    
                    self.total_processed += 1
                    
                    # Delay between searches within batch
                    if i < len(records_batch) - 1:  # Don't delay after last record
                        await self.smart_delay("between_searches")
                    
                    # Save progress every 5 records
                    if (i + 1) % 5 == 0:
                        df.to_csv(output_path, index=False)
                        print(f"   💾 Progress saved ({i + 1} records in batch)")
                        
                except Exception as e:
                    print(f"   💥 Record processing error: {e}")
                    self.failed_lookups += 1
                    continue
            
        finally:
            # Always close session completely
            await self.close_session_completely(browser, context, page)
            
            # Save final batch results
            df.to_csv(output_path, index=False)
            print(f"✅ Batch complete: {batch_successes}/{len(records_batch)} successful")
            
        return batch_successes

    async def process_csv_with_batching(self, csv_path: str, output_path: str, max_records: Optional[int] = None):
        """Main processing function with intelligent batching and session management"""
        print(f"\n🚀 Starting ZabaSearch Batch Processing")
        print(f"   • Input: {csv_path}")
        print(f"   • Output: {output_path}")
        print(f"   • Batch size: {self.batch_size}")
        print(f"   • Headless: {self.headless}")
        
        # Load CSV
        try:
            df = pd.read_csv(csv_path)
            print(f"✅ Loaded {len(df)} rows from CSV")
        except Exception as e:
            print(f"❌ Error loading CSV: {e}")
            return
        
        # Find records that need phone lookups
        records_to_process = []
        
        for index, row in df.iterrows():
            # Check for existing phone data to avoid reprocessing
            existing_columns = [col for col in df.columns if 'Phone_Primary' in col and pd.notna(row.get(col)) and row.get(col) != '']
            
            if existing_columns:
                continue  # Skip if already has phone data
            
            # Look for DirectName and IndirectName with addresses
            for name_type, prefix in [('DirectName_Cleaned', 'DirectName'), ('IndirectName_Cleaned', 'IndirectName')]:
                if (row.get(name_type) and 
                    row.get(f'{prefix}_Address') and
                    str(row.get(f'{prefix}_Type', '')).lower() == 'person'):
                    
                    records_to_process.append({
                        'row_index': index,
                        'name': row[name_type],
                        'address': row[f'{prefix}_Address'],
                        'column_prefix': prefix
                    })
        
        if not records_to_process:
            print("ℹ️ No records need phone number lookup")
            return
        
        if max_records:
            records_to_process = records_to_process[:max_records]
        
        print(f"📊 Found {len(records_to_process)} records to process")
        
        # Add phone columns if they don't exist
        for prefix in ['DirectName', 'IndirectName']:
            for suffix in ['Phone_Primary', 'Phone_Secondary', 'Phone_All', 'Address_Match', 'ZabaSearch_Status']:
                col_name = f"{prefix}_{suffix}"
                if col_name not in df.columns:
                    df[col_name] = ''
        
        # Process in batches
        total_batches = (len(records_to_process) + self.batch_size - 1) // self.batch_size
        total_successes = 0
        
        async with async_playwright() as playwright:
            for batch_num in range(total_batches):
                start_idx = batch_num * self.batch_size
                end_idx = min(start_idx + self.batch_size, len(records_to_process))
                batch_records = records_to_process[start_idx:end_idx]
                
                print(f"\n🎯 Batch {batch_num + 1}/{total_batches}")
                print(f"   📈 Progress: {self.total_processed}/{len(records_to_process)} records")
                print(f"   📞 Success rate: {self.successful_lookups}/{self.total_processed} ({self.successful_lookups/max(self.total_processed,1)*100:.1f}%)")
                
                batch_successes = await self.process_batch(playwright, batch_records, df, output_path)
                total_successes += batch_successes
                
                # Break between batches (except for the last batch)
                if batch_num < total_batches - 1:
                    print(f"   ⏸️ Taking break between batches...")
                    await self.smart_delay("between_batches")
        
        # Final summary
        print(f"\n🎉 Processing Complete!")
        print(f"   📊 Total records processed: {self.total_processed}")
        print(f"   ✅ Successful lookups: {self.successful_lookups}")
        print(f"   ❌ Failed lookups: {self.failed_lookups}")
        print(f"   📈 Success rate: {self.successful_lookups/max(self.total_processed,1)*100:.1f}%")
        print(f"   💾 Results saved to: {output_path}")

def main():
    parser = argparse.ArgumentParser(description='ZabaSearch Batch Phone Number Extractor')
    parser.add_argument('--input', '-i', type=str, help='Input CSV file path (auto-detected if not specified)')
    parser.add_argument('--output', '-o', type=str, help='Output CSV file path (auto-generated if not specified)')
    parser.add_argument('--batch-size', '-b', type=int, default=15, help='Batch size (default: 15)')
    parser.add_argument('--max-records', '-m', type=int, help='Maximum records to process (default: all)')
    parser.add_argument('--visible', '-v', action='store_true', help='Run with visible browser (default: headless)')
    
    args = parser.parse_args()
    
    # Initialize processor
    processor = ZabaSearchBatchProcessor(
        headless=not args.visible,
        batch_size=args.batch_size
    )
    
    try:
        # Determine input file
        input_file = args.input if args.input else processor.find_latest_csv_file()
        
        # Determine output file
        output_file = args.output if args.output else processor.generate_output_filename(input_file)
        
        print(f"🎯 Configuration:")
        print(f"   • Input file: {input_file}")
        print(f"   • Output file: {output_file}")
        print(f"   • Batch size: {args.batch_size}")
        print(f"   • Max records: {args.max_records or 'All'}")
        print(f"   • Browser mode: {'Visible' if args.visible else 'Headless'}")
        
        # Run processing
        asyncio.run(processor.process_csv_with_batching(
            input_file, 
            output_file, 
            args.max_records
        ))
        
    except FileNotFoundError as e:
        print(f"❌ File error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"💥 Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
