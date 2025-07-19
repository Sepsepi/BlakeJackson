"""
ZabaSearch Phone Number Extractor - FIXED VERSION
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
    
    async def create_stealth_browser(self, playwright, browser_type='chromium', proxy=None):
        """Create a stealth browser with anti-detection measures and optional proxy"""
        print(f"🌐 Creating {browser_type} browser...")
        if proxy:
            print(f"🔒 Using proxy: {proxy['server']}")
        
        if browser_type == 'firefox':
            launch_args = [
                '--no-sandbox',
                '--disable-dev-shm-usage',
            ]
            browser = await playwright.firefox.launch(
                headless=False,
                args=launch_args,
                proxy=proxy
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
            launch_args = [
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
            browser = await playwright.chromium.launch(
                headless=False,
                args=launch_args,
                proxy=proxy
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
        
        return browser, context
    
    async def human_delay(self, delay_type="normal"):
        """Add human-like delays - ULTRA FAST VERSION"""
        delays = {
            "quick": (0.2, 0.5),      # Reduced from (0.5, 1.5) - much faster
            "normal": (0.5, 1.0),     # Reduced from (1, 2.5) - much faster
            "slow": (1, 2),           # Reduced from (2, 4) - much faster
            "typing": (0.02, 0.1)     # Reduced from (0.05, 0.3) - much faster
        }
        min_delay, max_delay = delays.get(delay_type, delays["normal"])
        await asyncio.sleep(random.uniform(min_delay, max_delay))
    
    def normalize_address(self, address: str) -> str:
        """Normalize address for comparison with improved ordinal handling"""
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
    
    def addresses_match(self, csv_address: str, zaba_address: str) -> bool:
        """Check if addresses match with improved ordinal number handling"""
        if not csv_address or not zaba_address:
            return False
        
        csv_norm = self.normalize_address(csv_address)
        zaba_norm = self.normalize_address(zaba_address)
        
        print(f"    🔍 Comparing: '{csv_norm}' vs '{zaba_norm}'")
        
        # Extract components for flexible matching
        csv_parts = csv_norm.split()
        zaba_parts = zaba_norm.split()
        
        if len(csv_parts) < 2 or len(zaba_parts) < 2:
            return False
        
        # Check if street number matches
        if csv_parts[0] != zaba_parts[0]:
            return False
        
        # Create variations of street parts to handle ordinal numbers
        def create_ordinal_variations(parts):
            """Create variations with and without ordinal suffixes"""
            variations = []
            for part in parts:
                variations.append(part)
                # If it's a number, add ordinal versions
                if re.match(r'^\d+$', part):
                    num = int(part)
                    if num == 1:
                        variations.extend([f"{part}ST", "1ST"])
                    elif num == 2:
                        variations.extend([f"{part}ND", "2ND"])
                    elif num == 3:
                        variations.extend([f"{part}RD", "3RD"])
                    elif num in [11, 12, 13]:
                        variations.append(f"{part}TH")
                    elif num % 10 == 1:
                        variations.append(f"{part}ST")
                    elif num % 10 == 2:
                        variations.append(f"{part}ND")
                    elif num % 10 == 3:
                        variations.append(f"{part}RD")
                    else:
                        variations.append(f"{part}TH")
                # If it has ordinal suffix, also add base number
                elif re.match(r'^\d+(ST|ND|RD|TH)$', part):
                    base_num = re.sub(r'(ST|ND|RD|TH)$', '', part)
                    variations.append(base_num)
            return variations
        
        # Get key street parts (exclude street number)
        csv_street_parts = csv_parts[1:4] if len(csv_parts) > 3 else csv_parts[1:]
        zaba_street_parts = zaba_parts[1:4] if len(zaba_parts) > 3 else zaba_parts[1:]
        
        # Create variations for both addresses
        csv_variations = create_ordinal_variations(csv_street_parts)
        zaba_variations = create_ordinal_variations(zaba_street_parts)
        
        # Count matches between variations
        matches = 0
        matched_parts = []
        
        for csv_var in csv_variations:
            if csv_var in zaba_variations and csv_var not in matched_parts:
                matches += 1
                matched_parts.append(csv_var)
                if matches >= 2:  # Stop early if we have enough matches
                    break
        
        # Require at least 2 matching parts for positive match
        is_match = matches >= 2
        print(f"    📊 Found {matches} matching parts {matched_parts}, result: {'✅' if is_match else '❌'}")
        return is_match
    
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
            
            # Don't assume no person cards = blocking
            # Sometimes ZabaSearch legitimately has no results for a person
            return False
        except:
            return False

    async def detect_cloudflare_challenge(self, page) -> bool:
        """Detect if we're facing a Cloudflare challenge"""
        try:
            page_title = await page.title()
            page_content = await page.content()
            current_url = page.url
            
            # Check URL for cloudflare challenge indicators
            if 'challenge' in current_url.lower() or 'cloudflare' in current_url.lower():
                return True
            
            cloudflare_indicators = [
                'checking your browser',
                'please wait',
                'verify you are human',
                'cloudflare ray id',
                'cf-browser-verification',
                'challenge-form',
                'cf-challenge'
            ]
            
            content_lower = page_content.lower()
            title_lower = page_title.lower()
            
            # Look for specific Cloudflare text patterns
            for indicator in cloudflare_indicators:
                if indicator in content_lower or indicator in title_lower:
                    return True
            
            # Check for specific Cloudflare elements (but not privacy modal elements)
            cf_selectors = [
                '.cf-challenge-running',
                '#challenge-form',
                '.cf-wrapper',
                '.cf-browser-verification',
                'iframe[src*="challenges.cloudflare.com"]',
                'iframe[src*="cloudflare.com"]'
            ]
            
            for selector in cf_selectors:
                try:
                    element = await page.wait_for_selector(selector, timeout=500)  # Reduced from 1000
                    if element:
                        return True
                except:
                    continue
            
            # Don't consider general checkboxes or privacy modals as Cloudflare
            # Only return True if we find specific Cloudflare indicators
            return False
            
        except:
            return False
    
    async def handle_cloudflare_challenge(self, page):
        """Handle Cloudflare challenge if detected with improved selectors"""
        try:
            print(f"    🛡️ CLOUDFLARE CHALLENGE DETECTED!")
            print(f"    🔍 Looking for verification elements...")
            
            # Enhanced selectors for Cloudflare challenge
            challenge_selectors = [
                # Turnstile iframe selectors (most common)
                'iframe[src*="challenges.cloudflare.com"]',
                'iframe[src*="cloudflare.com"]',
                'iframe[title*="Widget containing checkbox"]',
                'iframe[title*="Widget containing a Cloudflare security challenge"]',
                
                # Direct checkbox selectors
                'input[type="checkbox"][data-ray]',
                'input[type="checkbox"][data-cf-challenge]',
                '.cf-turnstile',
                '.cf-challenge-form',
                '#challenge-form',
                
                # General fallback
                'input[type="checkbox"]'
            ]
            
            challenge_handled = False
            
            for selector in challenge_selectors:
                try:
                    print(f"    🔍 Checking selector: {selector}")
                    
                    if 'iframe' in selector:
                        # Handle iframe-based challenge (Turnstile)
                        try:
                            iframe = await page.wait_for_selector(selector, timeout=2000)
                            if iframe:
                                print(f"    🎯 Found Cloudflare iframe - accessing frame...")
                                iframe_frame = await iframe.content_frame()
                                if iframe_frame:
                                    # Wait a bit for iframe to load
                                    await asyncio.sleep(1)
                                    
                                    # Try multiple checkbox selectors in iframe
                                    iframe_checkbox_selectors = [
                                        'input[type="checkbox"]',
                                        '[role="checkbox"]',
                                        '.cf-turnstile-checkbox',
                                        'input'
                                    ]
                                    
                                    for iframe_selector in iframe_checkbox_selectors:
                                        try:
                                            checkbox = await iframe_frame.wait_for_selector(iframe_selector, timeout=1000)
                                            if checkbox:
                                                print(f"    🎯 Found checkbox in iframe: {iframe_selector}")
                                                await checkbox.click()
                                                print(f"    ✅ Clicked Cloudflare checkbox in iframe!")
                                                challenge_handled = True
                                                break
                                        except:
                                            continue
                                    
                                    if challenge_handled:
                                        break
                        except Exception as iframe_error:
                            print(f"    ⚠️ Iframe handling failed: {iframe_error}")
                            continue
                    else:
                        # Handle direct checkbox
                        try:
                            element = await page.wait_for_selector(selector, timeout=1000)
                            if element:
                                print(f"    🎯 Found element: {selector}")
                                
                                # Check if it's clickable
                                if 'input' in selector:
                                    await element.click()
                                    print(f"    ✅ Clicked checkbox!")
                                    challenge_handled = True
                                    break
                                else:
                                    # Try to find checkbox within the element
                                    checkbox = await element.query_selector('input[type="checkbox"]')
                                    if checkbox:
                                        await checkbox.click()
                                        print(f"    ✅ Clicked checkbox within element!")
                                        challenge_handled = True
                                        break
                        except Exception as direct_error:
                            print(f"    ⚠️ Direct selector failed: {direct_error}")
                            continue
                            
                except Exception as e:
                    print(f"    ⚠️ Selector {selector} failed: {e}")
                    continue
            
            if challenge_handled:
                print(f"    ⏳ Waiting for challenge to complete...")
                await asyncio.sleep(3)  # Reduced initial wait
                
                # Check if challenge is complete
                for i in range(8):  # Reduced wait time
                    try:
                        current_url = page.url
                        if 'zabasearch.com' in current_url and 'challenge' not in current_url.lower():
                            print(f"    ✅ Cloudflare challenge completed!")
                            return True
                        await asyncio.sleep(1)
                    except:
                        break
                        
                print(f"    ⚠️ Challenge handling attempted - continuing...")
                return True
            else:
                print(f"    ❌ Could not find Cloudflare checkbox - skipping...")
                # Don't fail completely, just continue
                return True
                
        except Exception as e:
            print(f"    ❌ Cloudflare challenge handling error: {e}")
            # Don't fail the whole process
            return True

    async def detect_and_handle_popups(self, page):
        """Detect and handle any popups that might appear - ENHANCED"""
        try:
            print(f"    🔍 Quick popup scan...")
            
            # FIRST: Handle privacy/cookie consent modal (I AGREE button)
            print(f"    🍪 Checking for privacy/cookie consent modal...")
            privacy_handled = False
            
            try:
                # Look for "I AGREE" button first
                agree_button = await page.wait_for_selector('text="I AGREE"', timeout=1000)  # Reduced from 2000
                if agree_button:
                    print(f"    🚨 PRIVACY MODAL DETECTED - clicking I AGREE")
                    await agree_button.click()
                    await asyncio.sleep(0.5)  # Reduced from 1
                    print(f"    ✅ Privacy modal closed with I AGREE")
                    privacy_handled = True
            except:
                # Check for other privacy modal patterns
                privacy_selectors = [
                    '[role="dialog"]',
                    '.modal',
                    '[aria-modal="true"]',
                    '.modal-container',
                    '.cky-modal',
                    '.privacy-modal'
                ]
                
                for selector in privacy_selectors:
                    try:
                        modal = await page.wait_for_selector(selector, timeout=500)  # Reduced from 1000
                        if modal:
                            print(f"    🚨 PRIVACY MODAL DETECTED: {selector}")
                            
                            # Try to find and click close buttons
                            close_selectors = [
                                'text="I AGREE"',
                                'text="Accept All"',
                                'text="Accept"',
                                'text="Close"',
                                'text="X"',
                                '.close-button',
                                '[aria-label="Close"]'
                            ]
                            
                            modal_closed = False
                            for close_selector in close_selectors:
                                try:
                                    close_btn = await modal.query_selector(close_selector)
                                    if close_btn:
                                        await close_btn.click()
                                        print(f"    ✅ PRIVACY MODAL CLOSED: {close_selector}")
                                        privacy_handled = True
                                        modal_closed = True
                                        break
                                except:
                                    continue
                            
                            if modal_closed:
                                await asyncio.sleep(0.5)  # Reduced from 1
                                break
                                
                    except:
                        continue
            
            # SECOND: After privacy modal is handled, check for Cloudflare challenge
            if privacy_handled:
                print(f"    ⏳ Waiting for page to settle after privacy modal...")
                await asyncio.sleep(1)  # Reduced from 2
            
            # Now check for actual Cloudflare challenge (only after privacy modal is handled)
            print(f"    🛡️ Checking for Cloudflare challenge...")
            if await self.detect_cloudflare_challenge(page):
                print(f"    🛡️ Cloudflare challenge detected after privacy modal...")
                try:
                    await self.handle_cloudflare_challenge(page)
                except Exception as cf_error:
                    print(f"    ⚠️ Cloudflare handling error: {cf_error}")
                    print(f"    🔄 Continuing despite Cloudflare error...")
                    # Don't crash - just continue
                return
            
            if not privacy_handled:
                print(f"    ✅ No privacy modals or challenges found")
                    
        except Exception as e:
            print(f"    ⚠️ Popup scan error: {e}")
            print(f"    🔄 Continuing despite popup scan error...")
            # Don't fail the whole process for popup detection
            pass
    
    async def accept_terms_if_needed(self, page):
        """Accept terms and conditions if not already done"""
        if self.terms_accepted:
            return
        
        try:
            # Look for "I AGREE" button
            agree_button = await page.wait_for_selector('text="I AGREE"', timeout=3000)  # Reduced from 5000
            if agree_button:
                await agree_button.click()
                self.terms_accepted = True
                await self.human_delay("quick")
                print("  ✓ Accepted terms and conditions")
        except:
            # Terms already accepted or not present
            self.terms_accepted = True
    
    async def search_person(self, page, first_name: str, last_name: str, target_address: str = "", city: str = "", state: str = "Florida") -> Optional[Dict]:
        """Search for a person on ZabaSearch with Cloudflare handling"""
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                print(f"🔍 Searching ZabaSearch: {first_name} {last_name} (Attempt {attempt + 1}/{max_retries})")
                print(f"  🌐 Navigating to ZabaSearch...")
                
                # Navigate to ZabaSearch with timeout
                await page.goto('https://www.zabasearch.com', wait_until='domcontentloaded', timeout=20000)  # Reduced from 30000
                print(f"  ✅ Page loaded successfully")
                await asyncio.sleep(0.5)  # Reduced from 1
                
                # Check for Cloudflare challenge first
                if await self.detect_cloudflare_challenge(page):
                    print(f"  🛡️ Cloudflare challenge detected - handling...")
                    if await self.handle_cloudflare_challenge(page):
                        print(f"  ✅ Cloudflare challenge handled, continuing...")
                        await asyncio.sleep(1)  # Reduced from 2 - Extra wait after challenge
                    else:
                        print(f"  ❌ Failed to handle Cloudflare challenge")
                        if attempt < max_retries - 1:
                            print(f"  🔄 Retrying in 10 seconds...")
                            await asyncio.sleep(10)
                            continue
                        return None
                
                # Check for any other popups
                print(f"  🔍 Checking for other popups...")
                await self.detect_and_handle_popups(page)
                
                # Accept terms if needed
                print(f"  📋 Checking for terms and conditions...")
                await self.accept_terms_if_needed(page)
                
                # Fill search form using the correct selectors from Playwright MCP testing
                print(f"  🔍 Locating search form elements...")
                
                # Fill name fields with correct selectors from Playwright MCP testing
                print(f"  ✏️ Filling first name: {first_name}")
                first_name_box = page.get_by_role("textbox", name="eg. John")
                await first_name_box.clear()
                await first_name_box.fill(first_name)
                await asyncio.sleep(0.1)  # Reduced from 0.3
                
                print(f"  ✏️ Filling last name: {last_name}")
                last_name_box = page.get_by_role("textbox", name="eg. Smith")
                await last_name_box.clear()
                await last_name_box.fill(last_name)
                await asyncio.sleep(0.1)  # Reduced from 0.3
                
                # Fill city and state if provided
                if city:
                    print(f"  🏙️ Filling city: {city}")
                    try:
                        # Use the exact selector from Playwright MCP testing
                        city_box = page.get_by_role("textbox", name="eg. Chicago")
                        await city_box.clear()
                        await city_box.fill(city)
                        await asyncio.sleep(0.1)  # Reduced from 0.3
                        print(f"    ✅ Successfully filled city: {city}")
                    except Exception as e:
                        print(f"    ⚠️ Could not fill city field: {e}")
                
                if state and state.upper() in ["FLORIDA", "FL"]:
                    print(f"  🗺️ Selecting state: Florida")
                    try:
                        # Use the exact selector from Playwright MCP testing  
                        state_dropdown = page.get_by_role("combobox")
                        await state_dropdown.select_option("Florida")
                        await asyncio.sleep(0.1)  # Reduced from 0.3
                        print(f"    ✅ Selected Florida")
                    except Exception as e:
                        print(f"    ⚠️ Could not select state: {e}")
                
                await asyncio.sleep(0.5)  # Reduced from 1
                
                # Submit search using Enter key like in test script
                print(f"  🚀 Submitting search...")
                await last_name_box.press("Enter")
                print(f"  ⏳ Waiting for results to load...")
                await asyncio.sleep(1.5)  # Reduced from 2.5 - Wait for results to load
                
                # Check again for Cloudflare after search
                if await self.detect_cloudflare_challenge(page):
                    print(f"  🛡️ Cloudflare challenge after search - handling...")
                    if await self.handle_cloudflare_challenge(page):
                        await asyncio.sleep(3)  # Reduced from 5 - Longer wait after challenge
                    else:
                        if attempt < max_retries - 1:
                            print(f"  🔄 Retrying after Cloudflare challenge...")
                            await asyncio.sleep(15)
                            continue
                        return None
                
                # Try to extract data directly
                print(f"  📊 Attempting to extract person data...")
                result = await self.extract_person_data(page, first_name, last_name, target_address)
                
                if result:
                    print(f"  ✅ Successfully extracted data for {first_name} {last_name}")
                else:
                    print(f"  ❌ No matching data found for {first_name} {last_name}")
                    
                return result
                
            except Exception as e:
                error_msg = str(e).lower()
                print(f"  ❌ Search error (attempt {attempt + 1}): {e}")
                print(f"  🔍 Error type: {type(e).__name__}")
                
                # Check if it's a connection/socket error (likely Cloudflare)
                if any(term in error_msg for term in ['connection', 'socket', 'timeout', 'closed']):
                    print(f"  🛡️ Detected connection issue - likely Cloudflare blocking")
                    if attempt < max_retries - 1:
                        wait_time = 10 + (attempt * 5)  # Reduced waiting time - was 15 + (attempt * 10)
                        print(f"  ⏳ Waiting {wait_time} seconds before retry...")
                        await asyncio.sleep(wait_time)
                        continue
                
                if attempt == max_retries - 1:
                    print(f"  💥 All retry attempts failed")
                
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
        """Process CSV file and add phone numbers from ZabaSearch with continuous searching"""
        print("📞 ZABASEARCH PHONE EXTRACTOR (CONTINUOUS)")
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

        async with async_playwright() as playwright:
            browser, context = await self.create_stealth_browser(playwright)
            page = await context.new_page()
            success_count = 0
            
            try:
                for i, record in enumerate(records_with_addresses, 1):
                    print(f"\n{'='*60}")
                    print(f"[{i}/{len(records_with_addresses)}] 🔄 PROCESSING RECORD #{i}")
                    print(f"{'='*60}")
                    print(f"  👤 Name: {record['name']}")
                    print(f"  📍 Address: {record['address']}")
                    print(f"  📊 Progress: {i-1} completed, {success_count} successful")

                    # Check for popups that might interrupt the process
                    print(f"  🔍 Pre-search popup check...")
                    await self.detect_and_handle_popups(page)

                    # Parse name
                    name_parts = record['name'].split()
                    if len(name_parts) < 2:
                        print("  ❌ Invalid name format - skipping")
                        continue

                    first_name = name_parts[0]
                    last_name = name_parts[1]
                    print(f"  ✅ Parsed name: '{first_name}' '{last_name}'")

                    # Extract city from address for better matching
                    city = ""
                    address_str = str(record['address'])
                    if ',' in address_str:
                        parts = address_str.split(',')
                        if len(parts) >= 2:
                            # Extract the city name which is between the street and the comma
                            # Address format: "6806 LAKESIDE CIR S DAVIE, FL 33314"
                            # Address format: "9450 NW 8 ST PEMBROKE PINES, FL 33024"
                            street_part = parts[0].strip()  # "6806 LAKESIDE CIR S DAVIE"
                            
                            # Split by common street types to find where street ends and city begins
                            street_types = ['ST', 'AVE', 'DR', 'CT', 'PL', 'RD', 'LN', 'BLVD', 'WAY', 'CIR', 'TER', 'PKWY']
                            
                            # Find the last occurrence of a street type
                            words = street_part.split()
                            city_start_idx = 0
                            
                            for i, word in enumerate(words):
                                if word in street_types:
                                    city_start_idx = i + 1
                                    break
                            
                            # If no street type found, assume last 1-2 words are city
                            if city_start_idx == 0:
                                if len(words) >= 2:
                                    city_start_idx = len(words) - 2  # Take last 2 words as potential city
                                else:
                                    city_start_idx = len(words) - 1  # Take last word
                            
                            # Extract city name (everything after street type)
                            if city_start_idx < len(words):
                                city_words = words[city_start_idx:]
                                
                                # Filter out apartment numbers and unit indicators
                                clean_city_words = []
                                for word in city_words:
                                    # Skip words that look like apartment numbers or unit indicators
                                    if not (word.startswith('#') or word.startswith('APT') or word.startswith('UNIT') or
                                           word.startswith('STE') or word.startswith('SUITE') or 
                                           (word.isdigit() and len(word) <= 4)):
                                        clean_city_words.append(word)
                                
                                city = ' '.join(clean_city_words)
                            
                            # Clean up common directional prefixes that might be included
                            if city.startswith(('N ', 'S ', 'E ', 'W ', 'NE ', 'NW ', 'SE ', 'SW ')):
                                city_parts = city.split(' ', 1)
                                if len(city_parts) > 1:
                                    city = city_parts[1]
                                    
                    print(f"  🏙️ Extracted city: '{city}'")

                    # Search ZabaSearch with address for matching
                    print(f"  🚀 Starting ZabaSearch lookup...")
                    try:
                        person_data = await self.search_person(page, first_name, last_name, record['address'], city)
                    except Exception as search_error:
                        print(f"  💥 CRITICAL ERROR during search: {search_error}")
                        print(f"  🔍 Error type: {type(search_error).__name__}")
                        print(f"  📍 This is likely the cause of the record 16 stop!")
                        
                        # Try to continue after error
                        person_data = None

                    if not person_data:
                        # Update status
                        print(f"  ❌ No results found for {record['name']}")
                        row_idx = record['row_index']
                        prefix = record['column_prefix']
                        df.at[row_idx, f"{prefix}_ZabaSearch_Status"] = "No results found"
                        print(f"  ⏳ Short delay before next search...")
                        await self.human_delay("quick")  # Short delay between searches
                        continue

                    print(f"  🎉 SUCCESS! Found matching person with {person_data['total_phones']} phone(s)")

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
                    print(f"  🏆 Total successful records: {success_count}")

                    # Short delay between searches to be respectful
                    print(f"  ⏳ Brief delay before next search...")
                    await self.human_delay("quick")

                    # Save progress periodically (every 5 successful finds instead of 10)
                    if success_count > 0 and success_count % 5 == 0:
                        df.to_csv(output_path, index=False)
                        print(f"  💾 Progress saved: {success_count} records processed")
                        
            except Exception as e:
                print(f"\n💥 CRITICAL SCRIPT ERROR: {e}")
                print(f"🔍 Error type: {type(e).__name__}")
                print(f"📊 Final status: {success_count} successful records before crash")
                
            finally:
                # Always try to save progress and close browser
                try:
                    df.to_csv(output_path, index=False)
                    print(f"💾 Progress saved due to error: {success_count} records processed")
                except:
                    pass
                
                try:
                    if browser:
                        await browser.close()
                except:
                    pass

            print(f"\n✅ Processing complete!")
            print(f"📊 Successfully found phone numbers for {success_count}/{len(records_with_addresses)} records")
            print(f"📈 Success rate: {success_count/len(records_with_addresses)*100:.1f}%")

            # Save final results
            df.to_csv(output_path, index=False)
            print(f"💾 Final results saved to: {output_path}")

            try:
                if browser:
                    await browser.close()
            except:
                pass

async def main():
    extractor = ZabaSearchExtractor()
    
    csv_path = "LisPendens_BrowardCounty_July7-14_2025_processed_with_addresses_fast.csv"
    output_path = "LisPendens_BrowardCounty_July7-14_2025_with_phone_numbers_fixed.csv"
    
    # Process ALL records - no limit, and resume from where we left off
    print("🔄 RESUMING ZabaSearch extraction with Cloudflare handling...")
    print("🛡️ Enhanced with Cloudflare challenge detection and bypass")
    print("🚀 Will retry failed connections with increasing delays")
    await extractor.process_csv_with_phone_lookup(csv_path, output_path, max_records=None)

if __name__ == "__main__":
    asyncio.run(main())
