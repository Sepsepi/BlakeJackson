"""
ZabaSearch Phone Number Extractor
Cross-references addresses from CSV with ZabaSearch data and extracts phone numbers
"""
import asyncio
import pandas as pd
import random
import re
from playwright.async_api import async_playwright
from typing import Dict, List, Optional, Tuple
import time
from urllib.parse import quote

class ZabaSearchExtractor:
    def __init__(self):
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15'
        ]
        self.firefox_user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:119.0) Gecko/20100101 Firefox/119.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0',
            'Mozilla/5.0 (X11; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0'
        ]
        self.terms_accepted = False
        self.current_browser_type = 'chromium'  # Track current browser
        self.search_count = 0  # Track number of searches
    
    async def create_stealth_browser(self, playwright, browser_type='chromium'):
        """Create a stealth browser with anti-detection measures"""
        print(f"🌐 Creating {browser_type} browser...")
        
        if browser_type == 'firefox':
            browser = await playwright.firefox.launch(
                headless=True,
                args=[
                    '--no-sandbox',
                    '--disable-dev-shm-usage',
                ]
            )
            
            context = await browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent=random.choice(self.firefox_user_agents),
                locale='en-US',
                timezone_id='America/New_York'
            )
            
            # Firefox stealth scripts
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
            """)
            
        else:  # chromium
            browser = await playwright.chromium.launch(
                headless=True,
                args=[
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-accelerated-2d-canvas',
                    '--no-first-run',
                    '--no-zygote',
                    '--single-process',
                    '--disable-gpu',
                    '--disable-background-timer-throttling',
                    '--disable-backgrounding-occluded-windows',
                    '--disable-renderer-backgrounding',
                    '--disable-features=TranslateUI',
                    '--disable-ipc-flooding-protection',
                    '--disable-blink-features=AutomationControlled'
                ]
            )
            
            context = await browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent=random.choice(self.user_agents),
                locale='en-US',
                timezone_id='America/New_York'
            )
            
            # Chrome stealth scripts
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
        
        self.current_browser_type = browser_type
        return browser, context
    
    async def human_delay(self, delay_type="normal"):
        """Add human-like delays"""
        delays = {
            "quick": (1, 3),
            "normal": (2, 5), 
            "slow": (4, 8),
            "typing": (0.1, 0.5)
        }
        min_delay, max_delay = delays.get(delay_type, delays["normal"])
        await asyncio.sleep(random.uniform(min_delay, max_delay))
    
    async def smart_type(self, page, selector, text, delay_type="typing"):
        """Type text with human-like behavior"""
        element = await page.wait_for_selector(selector, timeout=10000)
        await element.fill("")  # Clear the field first
        
        for char in text:
            await element.type(char)
            await asyncio.sleep(random.uniform(0.05, 0.15))
    
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
            ' WAY': ' WAY',
            ' BOULEVARD': ' BLVD',
            ' LANE': ' LN',
            ' TERRACE': ' TER'
        }
        
        for old, new in replacements.items():
            normalized = normalized.replace(old, new)
        
        return normalized
    
    async def detect_blocking(self, page) -> bool:
        """Detect if we're being blocked by ZabaSearch"""
        try:
            # Check for common blocking indicators
            page_content = await page.content()
            page_title = await page.title()
            
            blocking_indicators = [
                'captcha',
                'unusual traffic',
                'blocked',
                'service unavailable',
                'access denied',
                'too many requests'
            ]
            
            content_lower = page_content.lower()
            title_lower = page_title.lower()
            
            for indicator in blocking_indicators:
                if indicator in content_lower or indicator in title_lower:
                    return True
            
            # Check if no person cards found consistently (could be blocking)
            person_cards = await page.query_selector_all('.person')
            if len(person_cards) == 0:
                return True
                
            return False
        except:
            return False

    async def switch_browser(self, playwright, current_browser, current_context):
        """Switch to a different browser type when blocked"""
        try:
            print("🔄 Switching browser to bypass blocking...")
            
            # Close current browser
            await current_context.close()
            await current_browser.close()
            
            # Switch browser type
            new_browser_type = 'firefox' if self.current_browser_type == 'chromium' else 'chromium'
            print(f"🌐 Switching from {self.current_browser_type} to {new_browser_type}")
            
            # Reset terms acceptance for new browser
            self.terms_accepted = False
            self.search_count = 0
            
            # Create new browser
            browser, context = await self.create_stealth_browser(playwright, new_browser_type)
            page = await context.new_page()
            
            # Wait a bit to avoid being detected as the same session
            print("⏳ Cooling down for 30 seconds...")
            await asyncio.sleep(30)
            
            return browser, context, page
            
        except Exception as e:
            print(f"❌ Error switching browser: {e}")
            return None, None, None
    
    def addresses_match(self, csv_address: str, zaba_address: str) -> bool:
        """Check if addresses match with fuzzy matching"""
        if not csv_address or not zaba_address:
            return False
        
        csv_norm = self.normalize_address(csv_address)
        zaba_norm = self.normalize_address(zaba_address)
        
        # Extract street number and name for comparison
        csv_parts = csv_norm.split()
        zaba_parts = zaba_norm.split()
        
        if len(csv_parts) < 2 or len(zaba_parts) < 2:
            return False
        
        # Check if street number matches
        if csv_parts[0] != zaba_parts[0]:
            return False
        
        # Check if street name has significant overlap
        csv_street = ' '.join(csv_parts[1:3])  # First 2 parts of street name
        zaba_street = ' '.join(zaba_parts[1:3])
        
        return csv_street in zaba_norm or zaba_street in csv_norm
    
    async def detect_blocking(self, page) -> bool:
        """Detect if we're being blocked by ZabaSearch"""
        try:
            # Check for common blocking indicators
            page_content = await page.content()
            page_title = await page.title()
            
            blocking_indicators = [
                'captcha',
                'unusual traffic',
                'blocked',
                'service unavailable',
                'access denied',
                'too many requests'
            ]
            
            content_lower = page_content.lower()
            title_lower = page_title.lower()
            
            for indicator in blocking_indicators:
                if indicator in content_lower or indicator in title_lower:
                    return True
            
            # Check if no person cards found consistently (could be blocking)
            person_cards = await page.query_selector_all('.person')
            if len(person_cards) == 0:
                return True
                
            return False
        except:
            return False

    async def switch_browser(self, playwright, current_browser, current_context):
        """Switch to a different browser type when blocked"""
        try:
            print("🔄 Switching browser to bypass blocking...")
            
            # Close current browser
            await current_context.close()
            await current_browser.close()
            
            # Switch browser type
            new_browser_type = 'firefox' if self.current_browser_type == 'chromium' else 'chromium'
            print(f"🌐 Switching from {self.current_browser_type} to {new_browser_type}")
            
            # Reset terms acceptance for new browser
            self.terms_accepted = False
            self.search_count = 0
            
            # Create new browser
            browser, context = await self.create_stealth_browser(playwright, new_browser_type)
            page = await context.new_page()
            
            # Wait a bit to avoid being detected as the same session
            print("⏳ Cooling down for 30 seconds...")
            await asyncio.sleep(30)
            
            return browser, context, page
            
        except Exception as e:
            print(f"❌ Error switching browser: {e}")
            return None, None, None
    
    async def accept_terms_if_needed(self, page):
        """Accept terms and conditions if not already done"""
        if self.terms_accepted:
            return
        
        try:
            # Look for "I AGREE" button
            agree_button = await page.wait_for_selector('text="I AGREE"', timeout=5000)
            if agree_button:
                await agree_button.click()
                self.terms_accepted = True
                await self.human_delay("quick")
                print("  ✓ Accepted terms and conditions")
        except:
            # Terms already accepted or not present
            self.terms_accepted = True
    
    async def search_person(self, page, first_name: str, last_name: str, target_address: str = "", city: str = "", state: str = "Florida") -> Optional[Dict]:
        """Search for a person on ZabaSearch"""
        try:
            print(f"🔍 Searching ZabaSearch: {first_name} {last_name}")
            
            # Navigate to ZabaSearch
            await page.goto('https://www.zabasearch.com', wait_until='domcontentloaded', timeout=60000)
            await asyncio.sleep(5)  # Use fixed delay like in test script
            
            # Accept terms if needed
            await self.accept_terms_if_needed(page)
            
            # Fill search form using the approach that worked in test script
            first_name_box = page.get_by_role("textbox", name="eg. John")
            last_name_box = page.get_by_role("textbox", name="eg. Smith")
            
            await first_name_box.clear()
            await first_name_box.fill(first_name)
            await asyncio.sleep(0.5)
            
            await last_name_box.clear()
            await last_name_box.fill(last_name)
            await asyncio.sleep(1)
            
            # Submit search using Enter key like in test script
            await last_name_box.press("Enter")
            await asyncio.sleep(5)  # Wait for results to load
            
            # Try to extract data directly - no need to wait for specific selectors
            return await self.extract_person_data(page, first_name, last_name, target_address)
            
        except Exception as e:
            print(f"  ❌ Search error: {e}")
            return None
    
    async def extract_person_data(self, page, target_first_name: str, target_last_name: str, target_address: str = "") -> Optional[Dict]:
        """Extract person data from ZabaSearch results page"""
        try:
            print("  📋 Extracting person data...")
            
            # Get all person result containers using the class I found in debug
            person_cards = await page.query_selector_all('.person')
            
            if not person_cards:
                print("  ❌ No person cards found")
                return None
            
            print(f"  ✅ Found {len(person_cards)} person cards")
            
            for i, card in enumerate(person_cards):
                print(f"  🔍 Checking result #{i+1}")
                
                # Get the card text to check if it's the right person
                try:
                    card_text = await card.inner_text()
                    
                    # Check if this card contains our target person
                    if target_first_name.lower() not in card_text.lower() or target_last_name.lower() not in card_text.lower():
                        continue
                    
                    print(f"  ✅ Found matching person in card #{i+1}")
                    
                    # Extract addresses from this card
                    person_addresses = []
                    
                    # Look for address patterns in the text
                    address_lines = card_text.split('\n')
                    for line in address_lines:
                        line = line.strip()
                        # Look for lines that look like addresses - more flexible pattern
                        # Pattern: starts with number, contains street words, may have city/state
                        if (re.search(r'^\d+\s+', line) and  # starts with number
                            re.search(r'(?:ST|DR|AVE|CT|RD|LN|BLVD|WAY|PL|CIR|TER|DRIVE|STREET|AVENUE|COURT|ROAD|LANE|BOULEVARD|PLACE|CIRCLE|TERRACE)', line.upper()) and
                            len(line) > 10):  # reasonable length
                            person_addresses.append(line)
                        # Also look for city, state zip patterns
                        elif re.search(r'[A-Za-z\s]+,\s*[A-Z]{2}\s+\d{5}', line):
                            person_addresses.append(line)
                    
                    print(f"    📍 Found {len(person_addresses)} addresses in this card")
                    
                    # Check if any address matches our target
                    address_match = False
                    if target_address:
                        for addr in person_addresses:
                            if self.addresses_match(target_address, addr):
                                address_match = True
                                print(f"    ✅ Address match found: {addr}")
                                break
                    else:
                        address_match = True  # If no target address, accept any result
                    
                    if not address_match:
                        print(f"    ❌ No address match for result #{i+1}")
                        continue
                    
                    # Extract phone numbers from this card
                    phones = {"primary": None, "secondary": None, "all": []}
                    
                    # Look for phone number patterns in the card text
                    phone_pattern = r'\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}'
                    phone_matches = re.findall(phone_pattern, card_text)
                    
                    if phone_matches:
                        # Clean up phone numbers
                        cleaned_phones = []
                        for phone in phone_matches:
                            # Standardize format to (XXX) XXX-XXXX
                            digits = re.sub(r'\D', '', phone)
                            if len(digits) == 10:
                                formatted = f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
                                if formatted not in cleaned_phones:
                                    cleaned_phones.append(formatted)
                        
                        phones["all"] = cleaned_phones
                        if cleaned_phones:
                            phones["primary"] = cleaned_phones[0]  # First phone as primary
                            if len(cleaned_phones) > 1:
                                phones["secondary"] = cleaned_phones[1]
                        
                        print(f"    📞 Found {len(cleaned_phones)} phone numbers")
                        for phone in cleaned_phones:
                            print(f"      📞 {phone}")
                    
                    # Return the data if we found phone numbers
                    if phones["all"]:
                        return {
                            "name": f"{target_first_name} {target_last_name}",
                            "primary_phone": phones["primary"],
                            "secondary_phone": phones["secondary"], 
                            "all_phones": phones["all"],
                            "matched_address": person_addresses[0] if person_addresses else "",
                            "address_match": address_match,
                            "total_phones": len(phones["all"])
                        }
                    else:
                        print(f"    ❌ No phone numbers found in result #{i+1}")
                        continue
                        
                except Exception as e:
                    print(f"    ❌ Error processing card #{i+1}: {e}")
                    continue
            
            print("  ❌ No matching records with phone numbers found")
            return None
            
        except Exception as e:
            print(f"  ❌ Extraction error: {e}")
            return None
    
    async def process_csv_with_phone_lookup(self, csv_path: str, output_path: str, max_records: Optional[int] = None):
        """Process CSV file and add phone numbers from ZabaSearch"""
        print("📞 ZABASEARCH PHONE EXTRACTOR")
        print("=" * 50)
        
        # Load CSV
        try:
            df = pd.read_csv(csv_path)
            print(f"✓ Loaded {len(df)} records from CSV")
        except Exception as e:
            print(f"❌ Error loading CSV: {e}")
            return
        
        # Find records with addresses from our fast extractor
        records_with_addresses = []
        for _, row in df.iterrows():
            # Check for person names and addresses
            direct_name = row.get('DirectName_Cleaned', '')
            indirect_name = row.get('IndirectName_Cleaned', '')
            direct_address = row.get('DirectName_Address', '')
            indirect_address = row.get('IndirectName_Address', '')
            
            # Check if addresses are valid (not NaN/null)
            if (direct_name and row.get('DirectName_Type') == 'Person' and 
                direct_address and pd.notna(direct_address) and str(direct_address).strip()):
                records_with_addresses.append({
                    'name': direct_name,
                    'address': str(direct_address).strip(),
                    'row_index': row.name,
                    'column_prefix': 'DirectName'
                })
            
            if (indirect_name and row.get('IndirectName_Type') == 'Person' and 
                indirect_address and pd.notna(indirect_address) and str(indirect_address).strip()):
                records_with_addresses.append({
                    'name': indirect_name,
                    'address': str(indirect_address).strip(),
                    'row_index': row.name,
                    'column_prefix': 'IndirectName'
                })
        
        print(f"✓ Found {len(records_with_addresses)} records with person names and addresses")
        
        if max_records:
            records_with_addresses = records_with_addresses[:max_records]
            print(f"✓ Processing first {len(records_with_addresses)} records")
        else:
            print(f"✓ Processing all {len(records_with_addresses)} records")
        
        # Add new columns for phone data
        phone_columns = ['_Phone_Primary', '_Phone_Secondary', '_Phone_All', '_Address_Match', '_ZabaSearch_Status']
        for record in records_with_addresses:
            prefix = record['column_prefix']
            for col in phone_columns:
                col_name = f"{prefix}{col}"
                if col_name not in df.columns:
                    df[col_name] = ''
        
        # Process each record
        async with async_playwright() as playwright:
            browser, context = await self.create_stealth_browser(playwright)
            
            try:
                page = await context.new_page()
                success_count = 0
                
                for i, record in enumerate(records_with_addresses, 1):
                    print(f"\n[{i}/{len(records_with_addresses)}] Processing: {record['name']}")
                    print(f"  📍 Address: {record['address']}")
                    
                    # Proactive browser switching after every 15 searches to avoid reCAPTCHA v3
                    if self.search_count >= 15:
                        print(f"🔄 Proactive browser switch after {self.search_count} searches to avoid reCAPTCHA v3...")
                        result = await self.switch_browser(playwright, browser, context)
                        if result[0] and result[1] and result[2]:
                            browser, context, page = result
                        else:
                            print("❌ Could not switch browser. Continuing with current browser.")
                    
                    # Parse name
                    name_parts = record['name'].split()
                    if len(name_parts) < 2:
                        print("  ❌ Invalid name format")
                        continue
                    
                    first_name = name_parts[0]
                    last_name = name_parts[1]
                    
                    # Extract city from address for better matching
                    city = ""
                    address_str = str(record['address'])
                    if ',' in address_str:
                        parts = address_str.split(',')
                        if len(parts) >= 2:
                            city = parts[1].strip().split()[0]  # First word after comma
                    
                    # Increment search count before searching
                    self.search_count += 1
                    
                    # Search ZabaSearch with address for matching
                    person_data = await self.search_person(page, first_name, last_name, record['address'], city)
                    
                    if not person_data:
                        # Update status
                        row_idx = record['row_index']
                        prefix = record['column_prefix']
                        df.at[row_idx, f"{prefix}_ZabaSearch_Status"] = "No results found"
                        await self.human_delay("normal")
                        continue
                    
                    print(f"  ✓ Found matching person with {person_data['total_phones']} phone(s)")
                    
                    # Update CSV with phone data
                    row_idx = record['row_index']
                    prefix = record['column_prefix']
                    
                    df.at[row_idx, f"{prefix}_Phone_Primary"] = person_data.get('primary_phone', '')
                    df.at[row_idx, f"{prefix}_Phone_Secondary"] = person_data.get('secondary_phone', '')
                    df.at[row_idx, f"{prefix}_Phone_All"] = ', '.join(person_data.get('all_phones', []))
                    df.at[row_idx, f"{prefix}_Address_Match"] = person_data.get('matched_address', '')
                    df.at[row_idx, f"{prefix}_ZabaSearch_Status"] = "Success"
                    
                    success_count += 1
                    print(f"  📞 Primary: {person_data.get('primary_phone', 'None')}")
                    if person_data.get('secondary_phone'):
                        print(f"  📞 Secondary: {person_data.get('secondary_phone')}")
                    print(f"  📞 Total phones: {len(person_data.get('all_phones', []))}")
                    
                    await self.human_delay("normal")
                    
                    # Rate limiting
                    await self.human_delay("slow")
                
                print(f"\n✅ Processing complete!")
                print(f"📊 Successfully found phone numbers for {success_count}/{len(records_with_addresses)} records")
                print(f"📈 Success rate: {success_count/len(records_with_addresses)*100:.1f}%")
                
                # Save results
                df.to_csv(output_path, index=False)
                print(f"💾 Results saved to: {output_path}")
                
            finally:
                await browser.close()

async def main():
    extractor = ZabaSearchExtractor()
    
    csv_path = "LisPendens_BrowardCounty_July7-14_2025_processed_with_addresses_fast.csv"
    output_path = "LisPendens_BrowardCounty_July7-14_2025_with_phone_numbers_all.csv"
    
    # Process ALL records - no limit
    await extractor.process_csv_with_phone_lookup(csv_path, output_path, max_records=None)

if __name__ == "__main__":
    asyncio.run(main())
