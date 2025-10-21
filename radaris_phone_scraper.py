#!/usr/bin/env python3
"""
Radaris Phone Number Scraper
Automated headless browser script to extract phone numbers from Radaris.com

This script:
1. Reads CSV data with names and addresses
2. Searches for each person on Radaris.com
3. Verifies address matches
4. Extracts current phone numbers from profile pages
5. Updates CSV with findings

Author: Auto-generated based on Radaris.com structure analysis
Date: July 26, 2025
"""

import asyncio
import pandas as pd
import re
import logging
import sys
import os
import random
import time
from typing import Dict, List, Optional
from datetime import datetime
from pathlib import Path
from playwright.async_api import async_playwright
import time
import random
from difflib import SequenceMatcher
from difflib import SequenceMatcher

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'radaris_scraper_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class RadarisPhoneScraper:
    def __init__(self, csv_path: str, output_path: str = None):
        """
        Initialize the Radaris Phone Scraper

        Args:
            csv_path (str): Path to input CSV file
            output_path (str, optional): Path for output CSV. Defaults to input_radaris_updated.csv
        """
        self.csv_path = Path(csv_path)
        self.output_path = Path(output_path) if output_path else self.csv_path.parent / f"{self.csv_path.stem}_radaris_updated.csv"
        self.df = None
        self.browser = None
        self.context = None
        self.page = None

        # Columns for results - pipeline integration format
        self.result_columns = [
            'Radaris_Phone_Primary',
            'Radaris_Phone_Secondary', 
            'Radaris_Phone_All',
            'Address_Match',
            'Radaris_Status',
            'Radaris_Profile_URL'
        ]

    async def init_browser(self):
        """Initialize enhanced headless browser with advanced stealth"""
        try:
            self.playwright = await async_playwright().start()
            
            # Advanced browser stealth configuration
            session_id = random.randint(100000, 999999)
            logger.info(f"STEALTH: Creating stealth browser session #{session_id}")

            # Random viewport from common resolutions
            viewports = [
                {'width': 1920, 'height': 1080},
                {'width': 1366, 'height': 768},
                {'width': 1536, 'height': 864},
                {'width': 1440, 'height': 900},
                {'width': 1600, 'height': 900},
                {'width': 1280, 'height': 720}
            ]
            viewport = random.choice(viewports)

            # Random timezone and locale combinations
            locales_timezones = [
                {'locale': 'en-US', 'timezone': 'America/New_York'},
                {'locale': 'en-US', 'timezone': 'America/Chicago'},
                {'locale': 'en-US', 'timezone': 'America/Denver'},
                {'locale': 'en-US', 'timezone': 'America/Los_Angeles'},
                {'locale': 'en-US', 'timezone': 'America/Phoenix'},
                {'locale': 'en-CA', 'timezone': 'America/Toronto'},
            ]
            locale_tz = random.choice(locales_timezones)

            # Enhanced Chrome args with maximum stealth
            launch_args = [
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage',
                '--disable-accelerated-2d-canvas',
                '--no-first-run',
                '--no-zygote',
                '--disable-gpu',
                '--disable-features=VizDisplayCompositor',
                '--disable-background-networking',
                '--disable-background-timer-throttling',
                '--disable-renderer-backgrounding',
                '--disable-backgrounding-occluded-windows',
                '--disable-ipc-flooding-protection',
                '--disable-extensions',
                '--disable-plugins',
                '--disable-translate',
                '--disable-default-apps',
                '--disable-web-security',
                '--disable-features=TranslateUI',
                '--disable-blink-features=AutomationControlled',
                '--no-default-browser-check',
                '--disable-component-extensions-with-background-pages',
                '--disable-background-mode',
                '--disable-client-side-phishing-detection',
                '--disable-sync',
                '--disable-features=Translate',
                '--enable-unsafe-swiftshader',
                '--use-mock-keychain',
                '--disable-popup-blocking',
                '--start-maximized'
            ]

            # User agents for rotation
            user_agents = [
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0',
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15'
            ]
            
            selected_user_agent = random.choice(user_agents)

            self.browser = await self.playwright.chromium.launch(
                headless=True,  # Always headless for automation
                args=launch_args
            )

            self.context = await self.browser.new_context(
                viewport=viewport,
                user_agent=selected_user_agent,
                locale=locale_tz['locale'],
                timezone_id=locale_tz['timezone'],
                screen={'width': viewport['width'], 'height': viewport['height']},
                device_scale_factor=random.choice([1, 1.25, 1.5]),
                has_touch=random.choice([True, False]),
                is_mobile=False,
                permissions=['geolocation'],
                geolocation={'longitude': random.uniform(-80.5, -80.0), 'latitude': random.uniform(25.5, 26.5)},
                java_script_enabled=True,
                bypass_csp=True,
                ignore_https_errors=True,
                extra_http_headers={
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                    'Sec-Ch-Ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
                    'Sec-Ch-Ua-Mobile': '?0',
                    'Sec-Ch-Ua-Platform': '"Windows"',
                    'Sec-Fetch-Dest': 'document',
                    'Sec-Fetch-Mode': 'navigate',
                    'Sec-Fetch-Site': 'none',
                    'Sec-Fetch-User': '?1',
                    'Cache-Control': 'max-age=0'
                }
            )

            # Set default timeouts
            self.context.set_default_timeout(60000)
            self.context.set_default_navigation_timeout(60000)

            # ADVANCED ANTI-DETECTION SCRIPTS
            await self.context.add_init_script("""
                // Remove webdriver traces
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined,
                });

                // Mock plugins with realistic data
                Object.defineProperty(navigator, 'plugins', {
                    get: () => ({
                        length: 3,
                        0: { name: 'Chrome PDF Plugin', description: 'Portable Document Format', filename: 'internal-pdf-viewer' },
                        1: { name: 'Chrome PDF Viewer', description: '', filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai' },
                        2: { name: 'Native Client', description: '', filename: 'internal-nacl-plugin' }
                    }),
                });

                // Realistic language settings
                Object.defineProperty(navigator, 'languages', {
                    get: () => ['en-US', 'en'],
                });

                // Mock Chrome runtime
                window.chrome = {
                    runtime: {
                        onConnect: undefined,
                        onMessage: undefined
                    },
                    app: {
                        isInstalled: false
                    }
                };

                // Canvas fingerprint randomization
                const getImageData = HTMLCanvasElement.prototype.getContext('2d').getImageData;
                HTMLCanvasElement.prototype.getContext('2d').getImageData = function(...args) {
                    const result = getImageData.apply(this, args);
                    // Add tiny noise to canvas
                    for (let i = 0; i < result.data.length; i += 4) {
                        result.data[i] += Math.floor(Math.random() * 3) - 1;
                        result.data[i + 1] += Math.floor(Math.random() * 3) - 1;
                        result.data[i + 2] += Math.floor(Math.random() * 3) - 1;
                    }
                    return result;
                };

                // WebRTC IP leak protection
                const RTCPeerConnection = window.RTCPeerConnection || window.mozRTCPeerConnection || window.webkitRTCPeerConnection;
                if (RTCPeerConnection) {
                    const originalCreateDataChannel = RTCPeerConnection.prototype.createDataChannel;
                    RTCPeerConnection.prototype.createDataChannel = function() {
                        return originalCreateDataChannel.apply(this, arguments);
                    };
                }

                // Audio context fingerprint randomization
                const AudioContext = window.AudioContext || window.webkitAudioContext;
                if (AudioContext) {
                    const originalCreateAnalyser = AudioContext.prototype.createAnalyser;
                    AudioContext.prototype.createAnalyser = function() {
                        const analyser = originalCreateAnalyser.apply(this, arguments);
                        const originalGetByteFrequencyData = analyser.getByteFrequencyData;
                        analyser.getByteFrequencyData = function(array) {
                            originalGetByteFrequencyData.apply(this, arguments);
                            // Add slight noise
                            for (let i = 0; i < array.length; i++) {
                                array[i] += Math.floor(Math.random() * 3) - 1;
                            }
                        };
                        return analyser;
                    };
                }

                // Screen resolution noise
                const originalScreen = window.screen;
                Object.defineProperties(window.screen, {
                    width: { value: originalScreen.width + Math.floor(Math.random() * 3) - 1 },
                    height: { value: originalScreen.height + Math.floor(Math.random() * 3) - 1 },
                    availWidth: { value: originalScreen.availWidth + Math.floor(Math.random() * 3) - 1 },
                    availHeight: { value: originalScreen.availHeight + Math.floor(Math.random() * 3) - 1 }
                });

                // Battery API spoofing
                if (navigator.getBattery) {
                    navigator.getBattery = () => Promise.resolve({
                        charging: true,
                        chargingTime: Infinity,
                        dischargingTime: Infinity,
                        level: Math.random()
                    });
                }

                // Remove automation indicators
                delete window.cdc_adoQpoasnfa76pfcZLmcfl_Array;
                delete window.cdc_adoQpoasnfa76pfcZLmcfl_Promise;
                delete window.cdc_adoQpoasnfa76pfcZLmcfl_Symbol;

                console.log('STEALTH: Advanced Radaris stealth mode activated');
            """)

            self.page = await self.context.new_page()
            
            logger.info(f"STEALTH: Advanced stealth browser initialized (session #{session_id})")
            logger.info(f"DISPLAY: Viewport: {viewport['width']}x{viewport['height']}")
            logger.info(f"LOCALE: Locale: {locale_tz['locale']}, Timezone: {locale_tz['timezone']}")

        except Exception as e:
            logger.error(f"Failed to initialize browser: {e}")
            raise

    async def close_browser(self):
        """Close browser and cleanup"""
        try:
            if self.page:
                await self.page.close()
            if self.context:
                await self.context.close()
            if self.browser:
                await self.browser.close()
            if hasattr(self, 'playwright'):
                await self.playwright.stop()
            logger.info("Browser closed successfully")
        except Exception as e:
            logger.error(f"Error closing browser: {e}")

    def load_data(self):
        """Load CSV data and filter for records needing Radaris backup"""
        try:
            self.df = pd.read_csv(self.csv_path)
            logger.info(f"DATA: Loaded {len(self.df)} total records from {self.csv_path}")

            # Add Radaris result columns if they don't exist
            for col in self.result_columns:
                if col not in self.df.columns:
                    self.df[col] = ''

            # Filter for records that need Radaris processing (empty phone numbers from ZabaSearch)
            original_count = len(self.df)
            
            # Create mask for records that need Radaris backup
            # Look for records where BOTH DirectName and IndirectName phone fields are empty
            need_radaris_mask = pd.Series([False] * len(self.df))
            
            # Check DirectName records (people, not businesses)
            if 'DirectName_Phone_Primary' in self.df.columns and 'DirectName_Address' in self.df.columns:
                direct_mask = (
                    (self.df['DirectName_Type'] == 'Person') &  # Only people
                    (self.df['DirectName_Address'].notna()) &   # Have address
                    (self.df['DirectName_Address'] != '') &
                    (self.df['DirectName_Phone_Primary'].isna() | 
                     (self.df['DirectName_Phone_Primary'] == '') | 
                     (self.df['DirectName_Phone_Primary'] == 'N/A'))
                )
                need_radaris_mask = need_radaris_mask | direct_mask
                logger.info(f"FILTER: {direct_mask.sum()} DirectName records need Radaris backup")
            
            # Check IndirectName records (people, not businesses)
            if 'IndirectName_Phone_Primary' in self.df.columns and 'IndirectName_Address' in self.df.columns:
                indirect_mask = (
                    (self.df['IndirectName_Type'] == 'Person') &  # Only people
                    (self.df['IndirectName_Address'].notna()) &   # Have address
                    (self.df['IndirectName_Address'] != '') &
                    (self.df['IndirectName_Phone_Primary'].isna() | 
                     (self.df['IndirectName_Phone_Primary'] == '') | 
                     (self.df['IndirectName_Phone_Primary'] == 'N/A'))
                )
                need_radaris_mask = need_radaris_mask | indirect_mask
                logger.info(f"FILTER: {indirect_mask.sum()} IndirectName records need Radaris backup")
            
            # Store original dataframe and indices before filtering
            self.original_df = self.df.copy()  # Keep original for final merge
            self.original_indices = self.df.index.tolist()  # Store original row indices
            
            self.df = self.df[need_radaris_mask].copy()
            
            # Store mapping of new index to original index
            self.df['_original_index'] = self.df.index  # This preserves original indices
            self.df = self.df.reset_index(drop=True)  # Reset for clean processing
            
            filtered_count = len(self.df)
            logger.info(f"FILTER: Processing {filtered_count} records needing Radaris backup (out of {original_count} total)")
            
            if filtered_count == 0:
                logger.info("SUCCESS: All records already have phone numbers from ZabaSearch - no Radaris processing needed!")
                return False
            
            # Reset index for clean processing
            self.df = self.df.reset_index(drop=True)
            
            return True

        except Exception as e:
            logger.error(f"Failed to load CSV data: {e}")
            return False

    def normalize_address(self, address: str) -> str:
        """Normalize address for comparison"""
        if not address or pd.isna(address):
            return ""

        # Convert to uppercase and remove extra spaces
        normalized = re.sub(r'\s+', ' ', str(address).upper().strip())

        # Common normalizations
        replacements = {
            ' STREET': ' ST',
            ' AVENUE': ' AVE',
            ' BOULEVARD': ' BLVD',
            ' DRIVE': ' DR',
            ' COURT': ' CT',
            ' CIRCLE': ' CIR',
            ' TERRACE': ' TER',
            ' UNIT ': ' #',
            ' APT ': ' #',
            ' APARTMENT ': ' #',
            'NORTHWEST': 'NW',
            'NORTHEAST': 'NE',
            'SOUTHWEST': 'SW',
            'SOUTHEAST': 'SE',
        }

        for old, new in replacements.items():
            normalized = normalized.replace(old, new)

        # Remove common suffixes and punctuation for comparison
        normalized = re.sub(r'[,\.]', '', normalized)

        return normalized

    def extract_address_components(self, address: str) -> dict:
        """Extract components from address for flexible matching"""
        normalized = self.normalize_address(address)

        # Extract street number
        street_num_match = re.match(r'^(\d+)', normalized)
        street_number = street_num_match.group(1) if street_num_match else ""

        # Extract street name (after number, before city/state)
        street_match = re.search(r'^\d+\s+([^,]+)', normalized)
        street_name = street_match.group(1).strip() if street_match else ""

        # Extract city - FIXED: Better pattern for Florida addresses
        # Pattern handles: "STREET NAME CITY, ZIP" and "STREET NAME CITY, FL ZIP"
        city_pattern = r'(?:STREET|ST|AVENUE|AVE|DRIVE|DR|COURT|CT|PLACE|PL|ROAD|RD|LANE|LN|BOULEVARD|BLVD|WAY|CIRCLE|CIR)\s+([A-Z\s]+?)(?:,|\s+(?:FL|FLORIDA)|\s+\d{5})'
        city_match = re.search(city_pattern, normalized)
        if city_match:
            city = city_match.group(1).strip()
        else:
            # Fallback: look for city before comma or FL/zip
            fallback_match = re.search(r',\s*([A-Z\s]+?)(?:\s*,|\s+FL|\s+\d{5})', normalized)
            city = fallback_match.group(1).strip() if fallback_match else ""

        # Extract state
        state_match = re.search(r'\b([A-Z]{2})\b', normalized)
        state = state_match.group(1) if state_match else ""

        # Extract zip
        zip_match = re.search(r'\b(\d{5}(-\d{4})?)\b', normalized)
        zip_code = zip_match.group(1) if zip_match else ""

        return {
            'street_number': street_number,
            'street_name': street_name,
            'city': city,
            'state': state,
            'zip': zip_code,
            'full_normalized': normalized
        }

    def addresses_match(self, csv_address: str, radaris_address: str) -> bool:
        """
        Enhanced address matching based on Playwright testing discoveries.

        Key improvements:
        - Flexible apartment/unit matching (#14C vs APT C)
        - Better street type normalization (STREET vs St)
        - Ordinal number handling (22ND vs 22)

        Working test cases:
        - "4745 NW 22 STREET # 4295" matches "4745 NW 22Nd St APT 4295"
        - "3330 ATLANTA STREET # 14C" should match "3330 Atlanta St APT C"
        """
        try:
            logger.info(f"Comparing addresses:")
            logger.info(f"CSV: {csv_address}")
            logger.info(f"Radaris: {radaris_address}")

            # Extract street numbers first - must match exactly
            csv_match = re.match(r'^(\d+)', csv_address.strip())
            radaris_match = re.match(r'^(\d+)', radaris_address.strip())

            if not csv_match or not radaris_match:
                logger.info("No street number found in one or both addresses")
                return False

            csv_street_num = csv_match.group(1)
            radaris_street_num = radaris_match.group(1)

            if csv_street_num != radaris_street_num:
                logger.info(f"Street numbers don't match: {csv_street_num} vs {radaris_street_num}")
                return False

            # Extract ZIP codes - must match (main ZIP only, ignore +4)
            csv_zip = re.search(r'(\d{5})(?:-\d{4})?', csv_address)
            radaris_zip = re.search(r'(\d{5})(?:-\d{4})?', radaris_address)

            if csv_zip and radaris_zip:
                if csv_zip.group(1) != radaris_zip.group(1):
                    logger.info(f"ZIP codes don't match: {csv_zip.group(1)} vs {radaris_zip.group(1)}")
                    return False
            elif csv_zip or radaris_zip:
                logger.info("ZIP code present in only one address")
                return False

            # ENHANCED: Normalize for street comparison with better apartment handling
            def normalize_for_comparison(addr: str) -> str:
                # Convert to uppercase
                addr = addr.upper().strip()

                # Remove common variations
                addr = re.sub(r'\s+', ' ', addr)  # Normalize spaces

                # Remove ZIP codes and state names for street comparison
                addr = re.sub(r'\d{5}(?:-\d{4})?', '', addr)  # Remove ZIP
                addr = re.sub(r'\b(?:FL|FLORIDA)\b', '', addr)  # Remove state

                # ENHANCED: Better apartment/unit normalization for flexible matching
                # Handle cases like "#14C" vs "APT C" where letter part might be different
                addr = re.sub(r'\s*#\s*(\d+)([A-Z]?)\s*', r' APT \1\2 ', addr)  # #14C -> APT 14C
                addr = re.sub(r'\bUNIT\b\.?\s*(\d+)([A-Z]?)\s*', r'APT \1\2 ', addr)  # UNIT 14C -> APT 14C
                addr = re.sub(r'\bAPARTMENT\b\.?\s*(\d+)([A-Z]?)\s*', r'APT \1\2 ', addr)  # APARTMENT 14C -> APT 14C

                # Normalize street types
                addr = re.sub(r'\bSTREET\b', 'ST', addr)
                addr = re.sub(r'\bAVENUE\b', 'AVE', addr)
                addr = re.sub(r'\bDRIVE\b', 'DR', addr)
                addr = re.sub(r'\bTERRACE\b', 'TER', addr)
                addr = re.sub(r'\bBOULEVARD\b', 'BLVD', addr)
                addr = re.sub(r'\bCOURT\b', 'CT', addr)
                addr = re.sub(r'\bCIRCLE\b', 'CIR', addr)

                # Normalize ordinal numbers (key for our test cases)
                addr = re.sub(r'\b22ND\b', '22', addr)
                addr = re.sub(r'\b75TH\b', '75', addr)
                addr = re.sub(r'\b(\d+)(?:ST|ND|RD|TH)\b', r'\1', addr)

                # Remove commas and extra spaces
                addr = re.sub(r'[,]', ' ', addr)
                addr = re.sub(r'\s+', ' ', addr)

                return addr.strip()

            csv_normalized = normalize_for_comparison(csv_address)
            radaris_normalized = normalize_for_comparison(radaris_address)

            logger.info(f"Normalized for comparison:")
            logger.info(f"CSV: '{csv_normalized}'")
            logger.info(f"Radaris: '{radaris_normalized}'")

            # ENHANCED: Extract street part with better apartment tolerance
            def extract_street_part(normalized_addr: str) -> str:
                # Remove street number
                no_number = re.sub(r'^\d+\s*', '', normalized_addr)

                # Find potential city names and remove them
                # Common FL cities in our dataset
                cities = ['LAUDERHILL', 'HOLLYWOOD', 'COCONUT CREEK', 'POMPANO BEACH', 'SUNRISE', 'PLANTATION', 'CORAL SPRINGS']
                for city in cities:
                    if city in no_number:
                        no_number = no_number[:no_number.find(city)]
                        break

                return no_number.strip()

            csv_street_part = extract_street_part(csv_normalized)
            radaris_street_part = extract_street_part(radaris_normalized)

            logger.info(f"Street parts:")
            logger.info(f"CSV: '{csv_street_part}'")
            logger.info(f"Radaris: '{radaris_street_part}'")

            # ENHANCED: Calculate similarity with apartment-aware comparison
            similarity = SequenceMatcher(None, csv_street_part, radaris_street_part).ratio()

            logger.info(f"Street similarity: {similarity:.2%}")

            # ENHANCED: If similarity is borderline, check if it's just an apartment difference
            if similarity >= 0.70:  # Lowered threshold for apartment variations
                # Check if the difference is mainly apartment numbers/letters
                csv_no_apt = re.sub(r'\bAPT\s+\w+\s*', '', csv_street_part)
                radaris_no_apt = re.sub(r'\bAPT\s+\w+\s*', '', radaris_street_part)

                street_only_similarity = SequenceMatcher(None, csv_no_apt, radaris_no_apt).ratio()

                if street_only_similarity >= 0.85:  # High similarity without apartment
                    logger.info(f"HIGH MATCH: Street parts very similar ({street_only_similarity:.2%}) - apartment difference acceptable")
                    return True
                elif similarity >= 0.75:  # Lowered from 0.85 to 0.75 for "AVE" vs "AVENUE" cases
                    logger.info("MATCH FOUND!")
                    return True

            logger.info(f"Street names don't match exactly (similarity: {similarity:.2%})")
            return False

        except Exception as e:
            logger.error(f"Error comparing addresses: {e}")
            return False

    async def search_person(self, name: str, city: str, state: str, zip_code: str = '', street_address: str = '') -> dict:
        """Search for a person on Radaris using optimized strategy from testing"""
        try:
            # Navigate to Radaris homepage
            await self.page.goto('https://radaris.com', wait_until='networkidle', timeout=30000)
            await self.random_delay()

            # Fill name field using correct selector
            name_field = self.page.get_by_role('textbox', name='First and Last name')
            await name_field.clear()
            await name_field.fill(name)
            await self.random_delay(0.5, 1.5)

            # OPTIMIZED LOCATION STRATEGY: Based on user request,
            # use ONLY city/state and ignore ZIP codes completely
            location = ""
            if city and state:
                # PRIORITY: City, State ONLY (ignoring ZIP code as requested)
                location = f"{city}, {state}"
                logger.info(f"Searching with city/state: {city}, {state} (ignoring ZIP: {zip_code})")
            elif street_address:
                # FALLBACK: Try with street address if no city/state available
                location = street_address
                logger.info(f"Searching with street address: {street_address}")

            if location:
                location_field = self.page.get_by_role('textbox', name='City, state or zip code')
                await location_field.clear()
                await location_field.fill(location)
                await self.random_delay(0.5, 1.5)

            # Click search button
            search_button = self.page.get_by_role('button', name='Search')
            await search_button.click()

            # Wait for results page
            await self.page.wait_for_load_state('networkidle', timeout=15000)
            await self.random_delay(2, 4)

            # Construct target address for validation
            target_address = f"{street_address} {city} {state} {zip_code}".strip()

            return await self.parse_search_results(target_address)

        except Exception as e:
            logger.error(f"Error searching for {name}: {e}")
            return {
                'status': f'Search Error: {str(e)}',
                'profile_urls': []
            }

    async def extract_phone_with_address_validation(self, target_address: str) -> dict:
        """
        IMPROVED METHOD: Extract phone numbers from search results with proper address validation

        This method:
        1. Finds clickable profile cards in search results
        2. Clicks on each card (max 2 tries)
        3. Checks current and past addresses in the profile
        4. Only extracts phone if address matches
        5. Returns to search results to try next profile if no match
        """
        try:
            # Wait for page to fully load
            await self.page.wait_for_load_state('networkidle', timeout=10000)

            logger.info("NEW APPROACH: Validating addresses before extracting phone numbers")

            # Find clickable profile elements from search results
            profile_elements = []

            # Method 1: Look for "View Profile" buttons (most reliable)
            try:
                view_buttons = self.page.locator('text="View Profile"')
                count = await view_buttons.count()
                logger.info(f"Found {count} 'View Profile' buttons")

                for i in range(min(count, 2)):  # Max 2 tries as requested
                    profile_elements.append({
                        'type': 'view_button',
                        'index': i,
                        'element': view_buttons.nth(i)
                    })
            except Exception as e:
                logger.warning(f"View Profile buttons method failed: {e}")

            # Method 2: Look for person name links (fallback)
            if not profile_elements:
                try:
                    name_links = await self.page.query_selector_all('a[href*="/p/"]')
                    if name_links:
                        logger.info(f"Found {len(name_links)} person name links")
                        for i, link in enumerate(name_links[:2]):  # Max 2 tries
                            profile_elements.append({
                                'type': 'name_link',
                                'index': i,
                                'element': link
                            })
                except Exception as e:
                    logger.warning(f"Name links method failed: {e}")

            if not profile_elements:
                logger.info("No clickable profile elements found")
                return {
                    'status': 'No clickable profiles found',
                    'phone_primary': '',
                    'phone_secondary': '',
                    'phone_all': '',
                    'method': 'none'
                }

            # Try each profile (max 2)
            for i, profile_info in enumerate(profile_elements):
                try:
                    logger.info(f"Trying profile {i+1}/{len(profile_elements)}")

                    # Store current URL to return to search results
                    search_results_url = self.page.url

                    # Click on the profile
                    element = profile_info['element']
                    element_type = profile_info['type']

                    try:
                        if element_type == 'view_button':
                            await element.click()
                        else:
                            await element.click()
                        logger.info(f"Clicked on profile {i+1} ({element_type})")
                    except Exception as click_error:
                        logger.warning(f"Failed to click profile {i+1}: {click_error}")
                        continue

                    # Wait for profile page to load
                    try:
                        await self.page.wait_for_load_state('networkidle', timeout=15000)
                        await self.random_delay(2, 4)
                    except:
                        await self.random_delay(3, 5)

                    # Extract addresses from this profile
                    profile_addresses = await self.extract_addresses_from_profile()

                    # Check if any address matches our target
                    address_match = False
                    matching_address = ""

                    logger.info(f"Profile {i+1} has {len(profile_addresses)} addresses to check")
                    for addr in profile_addresses:
                        logger.info(f"Checking address: {addr}")
                        if self.addresses_match(target_address, addr):
                            address_match = True
                            matching_address = addr
                            logger.info(f"✅ ADDRESS MATCH FOUND: {addr}")
                            break

                    if address_match:
                        # Extract phone numbers since we found a match
                        logger.info("Address matched! Extracting phone numbers...")
                        phones = await self.extract_phone_numbers()

                        if phones.get('primary'):
                            logger.info(f"🎉 SUCCESS: Found phone {phones['primary']} with matching address!")
                            return {
                                'status': 'Success - Phone with Address Match',
                                'phone_primary': phones['primary'],
                                'phone_secondary': phones.get('secondary', ''),
                                'phone_all': phones.get('all', ''),
                                'address_match': 'Yes',
                                'matching_address': matching_address,
                                'profile_url': self.page.url,
                                'method': 'profile_with_address_validation'
                            }
                        else:
                            logger.info("Address matched but no phone numbers found")
                    else:
                        logger.info(f"❌ No address match for profile {i+1}")

                    # Go back to search results for next profile (if not last one)
                    if i < len(profile_elements) - 1:
                        try:
                            await self.page.goto(search_results_url, wait_until='networkidle', timeout=10000)
                            await self.random_delay(1, 2)
                            logger.info("Returned to search results")
                        except Exception as nav_error:
                            logger.warning(f"Failed to return to search results: {nav_error}")
                            break

                except Exception as e:
                    logger.error(f"Error processing profile {i+1}: {e}")
                    continue

            # No matching address found in any profile
            logger.info("No address matches found in any of the checked profiles")
            return {
                'status': 'No address match found',
                'phone_primary': '',
                'phone_secondary': '',
                'phone_all': '',
                'address_match': 'No',
                'method': 'profile_checked_no_match'
            }

        except Exception as e:
            logger.error(f"Error in address validation process: {e}")
            return {
                'status': f'Address validation error: {str(e)}',
                'phone_primary': '',
                'phone_secondary': '',
                'phone_all': '',
                'method': 'error'
            }

    async def parse_search_results(self, target_address: str) -> dict:
        """Parse search results and extract phone with proper address validation"""
        try:
            # Wait for page to fully load
            await self.page.wait_for_load_state('networkidle', timeout=10000)

            page_title = await self.page.title()
            logger.info(f"Search results page title: {page_title}")

            # NEW APPROACH: Always validate addresses before extracting phone numbers
            logger.info("Using address validation approach - clicking on profiles to verify addresses")

            return await self.extract_phone_with_address_validation(target_address)

        except Exception as e:
            logger.error(f"Error parsing search results: {e}")
            return {
                'status': f'Parse Error: {str(e)}',
                'phone_primary': '',
                'phone_secondary': '',
                'phone_all': '',
                'method': 'error'
            }

    async def extract_addresses_from_profile(self) -> list:
        """Extract all addresses from a profile page based on live testing patterns"""
        addresses = []

        try:
            # Wait for page to load completely
            await self.page.wait_for_load_state('networkidle', timeout=10000)

            # Based on live testing, we found addresses in specific locations:
            # 1. Previous Addresses section (most reliable)
            # 2. Current address sections
            # 3. Links to address pages

            # Method 1: Look for "Previous Addresses" section links
            try:
                # Look for address links in the format we found: "4745 Nw 22Nd St, Coconut Creek, FL 33063"
                address_links = await self.page.query_selector_all('a[href*="address"]')
                for link in address_links:
                    text = await link.text_content()
                    if text and ',' in text and re.search(r'\d+', text):
                        # Clean up the address text
                        clean_addr = re.sub(r'\s+', ' ', text.strip())
                        if len(clean_addr) > 15 and clean_addr not in addresses:
                            addresses.append(clean_addr)
                            logger.info(f"Found address from link: {clean_addr}")
            except Exception as e:
                logger.warning(f"Address links method failed: {e}")

            # Method 2: Look for addresses in list items (Previous Addresses section)
            try:
                list_items = await self.page.query_selector_all('li')
                for item in list_items:
                    text = await item.text_content()
                    if text and ',' in text:
                        # Look for address patterns like "4745 Nw 22Nd St, Coconut Creek, FL 33063"
                        address_pattern = r'\d+\s+[A-Za-z\s\d]+,\s*[A-Za-z\s]+,?\s*[A-Z]{2}\s*\d{5}'
                        matches = re.findall(address_pattern, text)
                        for match in matches:
                            clean_addr = re.sub(r'\s+', ' ', match.strip())
                            if len(clean_addr) > 15 and clean_addr not in addresses:
                                addresses.append(clean_addr)
                                logger.info(f"Found address from list: {clean_addr}")
            except Exception as e:
                logger.warning(f"List items method failed: {e}")

            # Method 3: Look in the general page content for address patterns
            try:
                page_content = await self.page.text_content('body')
                if page_content:
                    # More specific address patterns based on our findings
                    address_patterns = [
                        r'\d+\s+[A-Za-z\s\d]+(?:St|Street|Ave|Avenue|Dr|Drive|Blvd|Boulevard|Ct|Court|Ter|Terrace|Cir|Circle)\s*,\s*[A-Za-z\s]+,?\s*[A-Z]{2}\s*\d{5}',
                        r'\d+\s+[A-Za-z\s\d#]+,\s*[A-Za-z\s]+,\s*[A-Z]{2}\s*\d{5}'
                    ]

                    for pattern in address_patterns:
                        matches = re.findall(pattern, page_content, re.IGNORECASE)
                        for match in matches:
                            clean_addr = re.sub(r'\s+', ' ', match.strip())
                            if len(clean_addr) > 15 and clean_addr not in addresses:
                                addresses.append(clean_addr)
                                logger.info(f"Found address from content: {clean_addr}")
            except Exception as e:
                logger.warning(f"Page content method failed: {e}")

        except Exception as e:
            logger.error(f"Error extracting addresses: {e}")

        # Remove duplicates and sort by length (longer addresses tend to be more complete)
        unique_addresses = list(dict.fromkeys(addresses))  # Preserves order while removing duplicates
        unique_addresses.sort(key=len, reverse=True)

        logger.info(f"Extracted {len(unique_addresses)} unique addresses from profile")
        return unique_addresses

    async def extract_profile_info(self, csv_address: str) -> dict:
        """Extract information from a profile page"""
        try:
            # Wait for profile page to load
            await self.page.wait_for_load_state('networkidle', timeout=15000)
            await self.random_delay(2, 3)

            result = {
                'phone_primary': '',
                'phone_secondary': '',
                'phone_all': '',
                'address_match': '',
                'current_address': '',
                'previous_addresses': [],
                'profile_url': self.page.url
            }

            # Extract phone numbers
            phones = await self.extract_phone_numbers()
            result['phone_primary'] = phones.get('primary', '')
            result['phone_secondary'] = phones.get('secondary', '')
            result['phone_all'] = phones.get('all', '')

            # Extract addresses
            addresses = await self.extract_addresses()
            result['current_address'] = addresses.get('current', '')
            result['previous_addresses'] = addresses.get('previous', [])

            # Check address match
            all_addresses = [result['current_address']] + result['previous_addresses']
            best_match = ""

            for addr in all_addresses:
                if addr and self.addresses_match(csv_address, addr):
                    result['address_match'] = addr
                    best_match = addr
                    break

            if not best_match and all_addresses:
                # If no exact match, store the current address for reference
                result['address_match'] = result['current_address']

            return result

        except Exception as e:
            logger.error(f"Error extracting profile info: {e}")
            return {
                'phone_primary': '',
                'phone_secondary': '',
                'phone_all': '',
                'address_match': '',
                'current_address': '',
                'previous_addresses': [],
                'profile_url': self.page.url,
                'error': str(e)
            }

    async def extract_phone_numbers(self) -> dict:
        """Extract phone numbers from profile page, prioritizing the main displayed number"""
        phones = {'primary': '', 'secondary': '', 'all': ''}

        try:
            # Wait for content to load
            await self.page.wait_for_load_state('networkidle', timeout=5000)

            # First, try to get the primary phone from the main PHONE NUMBERS section
            primary_phone = ""
            try:
                # Look for the main phone number display pattern like in the screenshot
                # "(954) 969-9417 +7" format
                phone_section = await self.page.query_selector('div:has-text("PHONE NUMBERS")')
                if phone_section:
                    # Get the parent container that has the phone number
                    parent = await phone_section.query_selector('..')
                    if parent:
                        text = await parent.text_content()
                        # Look for the main phone pattern: (xxx) xxx-xxxx
                        phone_match = re.search(r'\((\d{3})\)\s*(\d{3})-(\d{4})', text)
                        if phone_match:
                            primary_phone = f"({phone_match.group(1)}) {phone_match.group(2)}-{phone_match.group(3)}"
                            logger.info(f"Found primary phone from PHONE NUMBERS section: {primary_phone}")
            except Exception as e:
                logger.warning(f"Primary phone extraction failed: {e}")

            # If no primary phone found, look for phone links and numbers more broadly
            phone_selectors = [
                # Direct phone links
                'a[href*="tel:"]',
                # Elements that commonly contain phones
                '[class*="phone"]',
                '[id*="phone"]',
                'div:has-text("PHONE") + div',
                'div:has-text("Phone") + div',
                'li:has(a[href*="tel:"])',
                'span:has-text("(")',  # Often contains formatted phone numbers
                # Look in contact sections
                'div[class*="contact"]',
                'div[class*="info"]',
                'section:has-text("Contact")',
                # General text that might contain phone numbers
                'p, div, span, li'
            ]

            found_phones = set()  # Use set to avoid duplicates

            for selector in phone_selectors:
                try:
                    elements = self.page.locator(selector)
                    count = await elements.count()

                    for i in range(min(count, 20)):  # Limit to avoid infinite loops
                        try:
                            element = elements.nth(i)

                            # Get text content
                            text = await element.text_content()
                            if text:
                                # Extract phone numbers using comprehensive regex
                                phone_patterns = [
                                    r'\(\d{3}\)\s*\d{3}[-.\s]?\d{4}',  # (555) 123-4567
                                    r'\d{3}[-.\s]\d{3}[-.\s]\d{4}',    # 555-123-4567 or 555.123.4567
                                    r'\d{3}\s\d{3}\s\d{4}',           # 555 123 4567
                                    r'\(\d{3}\)\d{3}-\d{4}'           # (555)123-4567
                                ]

                                for pattern in phone_patterns:
                                    matches = re.findall(pattern, text)
                                    for match in matches:
                                        # Clean the match
                                        digits_only = re.sub(r'[^\d]', '', match)
                                        if len(digits_only) == 10 and not digits_only.startswith('0'):
                                            found_phones.add(digits_only)

                            # Also check href attribute for tel: links
                            href = await element.get_attribute('href')
                            if href and href.startswith('tel:'):
                                phone_digits = re.sub(r'[^\d]', '', href[4:])
                                if len(phone_digits) == 10 and not phone_digits.startswith('0'):
                                    found_phones.add(phone_digits)

                        except Exception as e:
                            continue

                except Exception as e:
                    continue

            # Convert to formatted phone numbers
            formatted_phones = []
            for phone_digits in found_phones:
                if len(phone_digits) == 10:
                    formatted = f"({phone_digits[:3]}) {phone_digits[3:6]}-{phone_digits[6:]}"
                    formatted_phones.append(formatted)

            # Sort phones for consistency
            formatted_phones.sort()

            # Use primary phone if found, otherwise use first available
            if primary_phone:
                phones['primary'] = primary_phone
                # Remove primary from list and add others as secondary
                other_phones = [p for p in formatted_phones if p != primary_phone]
                if other_phones:
                    phones['secondary'] = other_phones[0]
                # Include primary in all phones list
                if primary_phone not in formatted_phones:
                    formatted_phones.insert(0, primary_phone)
                phones['all'] = ', '.join(formatted_phones)
            elif formatted_phones:
                phones['primary'] = formatted_phones[0]
                if len(formatted_phones) > 1:
                    phones['secondary'] = formatted_phones[1]
                phones['all'] = ', '.join(formatted_phones)

            if phones['primary']:
                logger.info(f"Found primary phone: {phones['primary']}")
                logger.info(f"All phones: {phones['all']}")
            else:
                logger.info("No phone numbers found on this profile")

        except Exception as e:
            logger.error(f"Error extracting phone numbers: {e}")

        return phones

    async def extract_addresses(self) -> dict:
        """Extract addresses from profile page"""
        addresses = {'current': '', 'previous': []}

        try:
            # Wait for content to load
            await self.page.wait_for_load_state('networkidle', timeout=5000)

            # More comprehensive address extraction
            address_selectors = [
                # Direct address patterns in text
                'text=/\\d+[^,]*,\\s*[A-Za-z\\s]+,\\s*[A-Z]{2}\\s*\\d{5}/',
                # Common address containers
                'div:has-text("CURRENT ADDRESS")',
                'div:has-text("Current Address")',
                'div:has-text("Previous Addresses")',
                'div:has-text("ADDRESSES")',
                'div:has-text("Address")',
                'li:has-text("Current")',
                'section:has-text("Address")',
                # Address-related classes
                '[class*="address"]',
                '[class*="location"]',
                '[id*="address"]',
                # Links that might contain addresses
                'a[href*="address"]',
                'a:has-text(",")',  # Links with commas (likely addresses)
                # General containers that might have addresses
                'div, p, span, li'
            ]

            found_addresses = set()  # Use set to avoid duplicates

            for selector in address_selectors:
                try:
                    elements = self.page.locator(selector)
                    count = await elements.count()

                    for i in range(min(count, 30)):  # Limit to avoid excessive processing
                        try:
                            element = elements.nth(i)
                            text = await element.text_content()

                            if text and len(text.strip()) > 10:  # Basic length check
                                # Look for address patterns in text
                                address_patterns = [
                                    r'\d+[^,\n]*,\s*[A-Za-z\s]+,\s*[A-Z]{2}\s*\d{5}(?:-\d{4})?',  # Full address
                                    r'\d+[^,\n]*[A-Za-z]+[^,\n]*,\s*[A-Za-z\s]+,?\s*[A-Z]{2}\s*\d{5}',  # Variations
                                    r'\d+\s+[A-Za-z][^,\n]*,\s*[A-Za-z\s]+\s+[A-Z]{2}\s*\d{5}'  # Without comma before state
                                ]

                                for pattern in address_patterns:
                                    matches = re.findall(pattern, text, re.IGNORECASE)
                                    for match in matches:
                                        # Clean and validate the address
                                        cleaned = match.strip()
                                        if len(cleaned) > 15 and ',' in cleaned:  # Basic validation
                                            found_addresses.add(cleaned)

                        except Exception as e:
                            continue

                except Exception as e:
                    continue

            # Convert set to list and clean addresses
            cleaned_addresses = []
            for addr in found_addresses:
                # Additional cleaning
                cleaned = re.sub(r'\s+', ' ', addr.strip())
                # Remove any leading/trailing punctuation
                cleaned = re.sub(r'^[,\.\-\s]+|[,\.\-\s]+$', '', cleaned)

                # Validate it looks like a real address
                if (len(cleaned) > 15 and
                    re.search(r'\d+', cleaned) and  # Has numbers
                    re.search(r'[A-Z]{2}\s*\d{5}', cleaned) and  # Has state and zip
                    ',' in cleaned):  # Has commas

                    if cleaned not in cleaned_addresses:
                        cleaned_addresses.append(cleaned)

            # Sort by length (longer addresses first, likely more complete)
            cleaned_addresses.sort(key=len, reverse=True)

            if cleaned_addresses:
                addresses['current'] = cleaned_addresses[0]
                addresses['previous'] = cleaned_addresses[1:] if len(cleaned_addresses) > 1 else []
                logger.info(f"Found {len(cleaned_addresses)} addresses. Current: {addresses['current']}")
            else:
                logger.info("No addresses found on this profile")

        except Exception as e:
            logger.error(f"Error extracting addresses: {e}")

        return addresses

    async def random_delay(self, min_delay: float = 1.0, max_delay: float = 3.0):
        """Add random delay to avoid detection"""
        delay = random.uniform(min_delay, max_delay)
        await asyncio.sleep(delay)

    async def process_person(self, row_index: int, row: pd.Series) -> dict:
        """Process a single person from the CSV using individual browser session"""
        try:
            # Extract person details
            name = str(row.get('IndirectName_Cleaned', '')).strip() if pd.notna(row.get('IndirectName_Cleaned')) else ''
            address = str(row.get('IndirectName_Address', '')).strip() if pd.notna(row.get('IndirectName_Address')) else ''

            if not name:
                return {
                    'status': 'No name provided',
                    'phone_primary': '',
                    'phone_secondary': '',
                    'phone_all': '',
                    'address_match': '',
                    'profile_url': ''
                }

            # Extract city, state, and ZIP code from address
            city, state, zip_code, street_address = '', '', '', ''
            if address:
                # Use the full address as street_address for now
                street_address = address

                # Extract ZIP code first
                zip_match = re.search(r'\b(\d{5}(-\d{4})?)\b', address)
                zip_code = zip_match.group(1) if zip_match else ''

                # Clean address
                clean_addr = address.strip()

                # Pattern 1: "STREET CITY, FL ZIP" - has state explicitly
                pattern_with_state = re.search(r'([A-Za-z\s]+),\s*([A-Z]{2})\s*\d{5}', clean_addr)
                if pattern_with_state:
                    city_part = pattern_with_state.group(1).strip()
                    state = pattern_with_state.group(2).strip()

                    # Extract city from city_part - it's the last words after street address
                    words = city_part.split()
                    city_words = []
                    # Work backwards to find city name (skip street address parts)
                    for word in reversed(words):
                        if not re.match(r'^\d+', word) and word.upper() not in ['ST', 'STREET', 'AVE', 'AVENUE', 'DR', 'DRIVE', 'CT', 'COURT', 'BLVD', 'BOULEVARD', 'TER', 'TERRACE', 'NW', 'NE', 'SW', 'SE', '#', 'APT', 'UNIT']:
                            city_words.insert(0, word)
                        else:
                            break  # Stop when we hit street address parts
                    city = ' '.join(city_words)

                # Pattern 2: "STREET CITY, ZIP" - no state, assume FL
                elif ',' in clean_addr:
                    parts = clean_addr.split(',')
                    if len(parts) >= 2:
                        city_part = parts[0].strip()
                        zip_part = parts[1].strip()

                        # If zip part is just numbers, assume FL
                        if re.match(r'^\s*\d{5}', zip_part):
                            state = 'FL'

                            # Extract city from city_part
                            words = city_part.split()
                            city_words = []
                            for word in reversed(words):
                                if not re.match(r'^\d+', word) and word.upper() not in ['ST', 'STREET', 'AVE', 'AVENUE', 'DR', 'DRIVE', 'CT', 'COURT', 'BLVD', 'BOULEVARD', 'TER', 'TERRACE', 'NW', 'NE', 'SW', 'SE', '#', 'APT', 'UNIT']:
                                    city_words.insert(0, word)
                                else:
                                    break
                            city = ' '.join(city_words)

                # If still no results, try a more aggressive approach
                if not city:
                    # Remove zip code and work backwards
                    no_zip = re.sub(r'\d{5}(-\d{4})?', '', clean_addr).strip().rstrip(',')
                    words = no_zip.split()
                    if len(words) >= 2:
                        # Take last 1-2 words as potential city
                        city = ' '.join(words[-2:]) if len(words) >= 2 else words[-1]
                        # Clean up city
                        city = re.sub(r'^[,\s]+|[,\s]+$', '', city)
                        if not state:
                            state = 'FL'  # Default for this dataset

            logger.info(f"Processing {row_index + 1}: {name} - {city}, {state} (ZIP: {zip_code}) (from address: {address})")

            # NEW: Use individual browser session for stealth
            return await self.process_person_with_session(name, city, state, zip_code, street_address)

        except Exception as e:
            logger.error(f"Processing failed for {name}: {e}")
            return {
                'status': f'Processing Error: {str(e)}',
                'phone_primary': '',
                'phone_secondary': '',
                'phone_all': '',
                'address_match': 'Error',
                'profile_url': ''
            }

    async def process_person_with_session(self, name: str, city: str, state: str, zip_code: str, street_address: str) -> Dict:
        """
        Process a person with individual browser session for maximum stealth.
        This is the new enhanced version that creates a fresh browser session for each search.
        """
        browser = None
        context = None
        session_page = None
        
        try:
            logger.info(f"\n{'='*80}")
            logger.info(f"Processing with completely isolated session: {name} - {street_address}, {city}, {state}")
            logger.info(f"{'='*80}")
            
            # Use completely isolated session approach
            name_parts = name.split(' ', 1)
            first_name = name_parts[0] if name_parts else name
            last_name = name_parts[1] if len(name_parts) > 1 else ''
            
            result = await self.search_with_isolated_session(first_name, last_name, city, state, street_address)
            
            logger.info(f"Final result: {result}")
            return result
            
        except Exception as e:
            logger.error(f"Process person error for {name}: {e}")
            return {
                'phone_primary': '',
                'phone_secondary': '',
                'phone_all': '',
                'address_match': 'No',
                'status': f'Process Error: {str(e)}',
                'profile_url': '',
                'error_message': str(e)
            }
        finally:
            # Note: cleanup is now handled within search_with_isolated_session
            logger.info("Session cleanup completed")
            
            # Add delay between searches to avoid detection
            await asyncio.sleep(random.uniform(5, 10))

    async def search_person_with_session(self, session_page, name: str, city: str, state: str, zip_code: str, street_address: str) -> Dict:
        """
        Search for a person using individual session with ultra-stealth to avoid CAPTCHAs.
        """
        try:
            logger.info(f"STEALTH: Navigating to Radaris with ultra-stealth mode")
            
            # Navigate with longer timeout and natural user behavior
            await session_page.goto('https://radaris.com', wait_until='networkidle', timeout=45000)
            
            # Natural delay - users don't immediately start typing
            await asyncio.sleep(random.uniform(3, 6))
            
            # Check if we got a CAPTCHA page
            page_content = await session_page.text_content('body')
            if 'verify' in page_content.lower() or 'captcha' in page_content.lower() or 'robot' in page_content.lower():
                logger.warning("CAPTCHA detected - trying to wait it out naturally")
                # Wait a bit and try to continue - sometimes CAPTCHAs auto-resolve
                await asyncio.sleep(random.uniform(10, 15))
                
                # Try to refresh and continue
                await session_page.reload(wait_until='networkidle', timeout=30000)
                await asyncio.sleep(random.uniform(2, 4))
                
                # Check again
                page_content = await session_page.text_content('body')
                if 'verify' in page_content.lower() or 'captcha' in page_content.lower():
                    logger.error("CAPTCHA still present - skipping this search")
                    return {
                        'phone_primary': '',
                        'phone_secondary': '',
                        'phone_all': '',
                        'address_match': 'No',
                        'status': 'CAPTCHA Blocked',
                        'profile_url': session_page.url,
                        'error_message': 'CAPTCHA verification required'
                    }
            
            # Simulate human mouse movement before interacting
            await session_page.mouse.move(random.randint(100, 400), random.randint(100, 300))
            await asyncio.sleep(random.uniform(0.5, 1.5))
            
            # Find and fill name field with human-like typing
            try:
                name_field = session_page.get_by_role('textbox', name='First and Last name')
                
                # Move mouse to field naturally
                name_field_box = await name_field.bounding_box()
                if name_field_box:
                    await session_page.mouse.move(
                        name_field_box['x'] + name_field_box['width'] / 2,
                        name_field_box['y'] + name_field_box['height'] / 2
                    )
                    await asyncio.sleep(random.uniform(0.2, 0.8))
                
                # Clear and type naturally
                await name_field.click()
                await asyncio.sleep(random.uniform(0.3, 0.7))
                await name_field.clear()
                await asyncio.sleep(random.uniform(0.5, 1.2))
                
                # Type with human-like speed
                for char in name:
                    await name_field.type(char)
                    await asyncio.sleep(random.uniform(0.05, 0.15))
                    
                logger.info(f"STEALTH: Entered name: {name}")
                
            except Exception as name_error:
                logger.error(f"Could not fill name field: {name_error}")
                return {
                    'phone_primary': '',
                    'phone_secondary': '',
                    'phone_all': '',
                    'address_match': 'No',
                    'status': 'Name Field Error',
                    'profile_url': session_page.url,
                    'error_message': str(name_error)
                }

            # Natural pause before location
            await asyncio.sleep(random.uniform(1.5, 3))

            # Fill in location if provided
            if city or state:
                try:
                    location_field = session_page.get_by_role('textbox', name='City, state or zip code')
                    
                    # Move mouse to location field
                    location_field_box = await location_field.bounding_box()
                    if location_field_box:
                        await session_page.mouse.move(
                            location_field_box['x'] + location_field_box['width'] / 2,
                            location_field_box['y'] + location_field_box['height'] / 2
                        )
                        await asyncio.sleep(random.uniform(0.2, 0.6))
                    
                    await location_field.click()
                    await asyncio.sleep(random.uniform(0.2, 0.5))
                    await location_field.clear()
                    await asyncio.sleep(random.uniform(0.3, 0.8))

                    location_str = f"{city}, {state}" if city and state else city or state
                    logger.info(f"STEALTH: Searching with location: {location_str}")
                    
                    # Type location naturally
                    for char in location_str:
                        await location_field.type(char)
                        await asyncio.sleep(random.uniform(0.05, 0.12))
                        
                except Exception as loc_error:
                    logger.warning(f"Could not fill location field: {loc_error}")

            # Natural delay before search - humans don't immediately click search
            await asyncio.sleep(random.uniform(2, 4))

            # Submit search with natural mouse movement
            try:
                search_button = session_page.get_by_role('button', name='Search')
                
                # Move to search button naturally
                search_button_box = await search_button.bounding_box()
                if search_button_box:
                    await session_page.mouse.move(
                        search_button_box['x'] + search_button_box['width'] / 2,
                        search_button_box['y'] + search_button_box['height'] / 2
                    )
                    await asyncio.sleep(random.uniform(0.3, 0.8))
                
                await search_button.click()
                logger.info("STEALTH: Search submitted naturally")
                
            except Exception as search_error:
                logger.error(f"Could not click search button: {search_error}")
                return {
                    'phone_primary': '',
                    'phone_secondary': '',
                    'phone_all': '',
                    'address_match': 'No',
                    'status': 'Search Button Error',
                    'profile_url': session_page.url,
                    'error_message': str(search_error)
                }
            
            # Wait for results with patience
            await session_page.wait_for_load_state('networkidle', timeout=20000)
            await asyncio.sleep(random.uniform(3, 5))
            
            # Check page title for results
            page_title = await session_page.title()
            logger.info(f"STEALTH: Results page title: {page_title}")
            
            # Parse results using address validation
            return await self.parse_search_results_with_session(session_page, street_address)
            
        except Exception as e:
            logger.error(f"Search error for {name}: {e}")
            return {
                'phone_primary': '',
                'phone_secondary': '',
                'phone_all': '',
                'address_match': 'No',
                'status': f'Search Error: {str(e)}',
                'profile_url': session_page.url if session_page else '',
                'error_message': str(e)
            }

    async def parse_search_results_with_session(self, session_page, target_address: str) -> Dict:
        """
        Parse search results with individual session page for address validation.
        """
        try:
            await session_page.wait_for_load_state('networkidle', timeout=10000)
            
            logger.info("Using address validation approach - clicking on profiles to verify addresses")
            logger.info("NEW APPROACH: Validating addresses before extracting phone numbers")
            
            # Find all "View Profile" buttons
            try:
                view_buttons = session_page.locator('text="View Profile"')
                button_count = await view_buttons.count()
                logger.info(f"Found {button_count} 'View Profile' buttons")
                
                if button_count == 0:
                    logger.info("No 'View Profile' buttons found - trying name links")
                    # Try to find name links instead
                    name_links = await session_page.query_selector_all('a[href*="/p/"]')
                    logger.info(f"Found {len(name_links)} potential name links")
                    
                    if len(name_links) == 0:
                        return {
                            'phone_primary': '',
                            'phone_secondary': '',
                            'phone_all': '',
                            'address_match': 'No',
                            'status': 'No profiles found in search results',
                            'profile_url': session_page.url
                        }
                    
                    # Use name links instead
                    person_elements = name_links[:2]  # Limit to first 2
                else:
                    person_elements = [await view_buttons.nth(i).element_handle() for i in range(min(button_count, 2))]
                
            except Exception as e:
                logger.error(f"Error finding profile elements: {e}")
                return {
                    'phone_primary': '',
                    'phone_secondary': '',
                    'phone_all': '',
                    'address_match': 'No', 
                    'status': f'Profile search error: {str(e)}',
                    'profile_url': session_page.url
                }
            
            # Check each profile for address matches
            for i, element in enumerate(person_elements):
                try:
                    logger.info(f"Trying profile {i+1}/{len(person_elements)}")
                    
                    # Remember the search results URL to come back to
                    search_results_url = session_page.url
                    
                    # Click on profile
                    await element.click()
                    logger.info(f"Clicked on profile {i+1} (view_button)")
                    
                    # Wait for profile page
                    await session_page.wait_for_load_state('networkidle', timeout=15000)
                    await asyncio.sleep(random.uniform(2, 4))
                    
                    # Check if we found address match and phone
                    result = await self.extract_phone_with_address_validation_session(session_page, target_address)
                    
                    if result.get('address_match') == 'Yes' and result.get('phone_primary'):
                        logger.info(f"SUCCESS: Found phone with address match in profile {i+1}")
                        result.update({
                            'status': 'Success - Phone with Address Match',
                            'profile_url': session_page.url,
                        })
                        return result
                    
                    # Go back to search results for next profile
                    if i < len(person_elements) - 1:
                        try:
                            await session_page.goto(search_results_url, wait_until='networkidle', timeout=10000)
                            await asyncio.sleep(random.uniform(1, 3))
                            logger.info("Returned to search results")
                        except Exception as back_error:
                            logger.warning(f"Could not return to search results: {back_error}")
                            break
                            
                except Exception as profile_error:
                    logger.error(f"Error processing profile {i+1}: {profile_error}")
                    continue
            
            logger.info("No address matches found in any of the checked profiles")
            return {
                'phone_primary': '',
                'phone_secondary': '',
                'phone_all': '',
                'address_match': 'No',
                'status': 'No address matches found',
                'profile_url': session_page.url
            }
            
        except Exception as e:
            logger.error(f"Parse results error: {e}")
            return {
                'phone_primary': '',
                'phone_secondary': '',
                'phone_all': '',
                'address_match': 'No',
                'status': f'Parse error: {str(e)}',
                'profile_url': session_page.url if session_page else ''
            }

    async def extract_phone_with_address_validation_session(self, session_page, target_address: str) -> Dict:
        """
        Extract phone numbers from profile page with address validation using session page.
        """
        try:
            await session_page.wait_for_load_state('networkidle', timeout=10000)
            
            page_title = await session_page.title()
            logger.info(f"Profile page title: {page_title}")
            
            # Extract addresses from profile using session page
            addresses = await self.extract_addresses_from_profile_session(session_page)
            logger.info(f"Extracted {len(addresses)} addresses from profile")
            
            # Check for address matches
            for i, address in enumerate(addresses):
                logger.info(f"Checking address: {address}")
                if self.addresses_match(target_address, address):
                    logger.info(f"ADDRESS MATCH FOUND! Profile address: {address}")
                    
                    # Extract phone numbers
                    phones = await self.extract_phone_numbers_session(session_page)
                    
                    if phones.get('primary'):
                        logger.info(f"SUCCESS: Phone {phones['primary']} found with address match")
                        return {
                            'phone_primary': phones.get('primary', ''),
                            'phone_secondary': phones.get('secondary', ''),  
                            'phone_all': phones.get('all', ''),
                            'address_match': 'Yes',
                            'matching_address': address
                        }
                    else:
                        logger.info("Address match found but no phone number available")
                        return {
                            'phone_primary': '',
                            'phone_secondary': '',
                            'phone_all': '',
                            'address_match': 'Yes',
                            'matching_address': address
                        }
                else:
                    logger.info(f"No address match for profile {i+1}")
            
            logger.info("No address match found in this profile")
            return {
                'phone_primary': '',
                'phone_secondary': '',
                'phone_all': '',
                'address_match': 'No'
            }
            
        except Exception as e:
            logger.error(f"Address validation error: {e}")
            return {
                'phone_primary': '',
                'phone_secondary': '',
                'phone_all': '',
                'address_match': 'Error',
                'error_message': str(e)
            }

    async def extract_addresses_from_profile_session(self, session_page) -> List[str]:
        """Extract addresses from profile using session page."""
        addresses = []
        try:
            # Get page content and search for addresses
            page_content = await session_page.text_content('body')
            
            # Address patterns
            address_patterns = [
                r'\b\d+[A-Za-z]?\s+[A-Za-z\s]+(?:Street|St|Avenue|Ave|Drive|Dr|Court|Ct|Boulevard|Blvd|Lane|Ln|Road|Rd|Terrace|Ter|Place|Pl|Circle|Cir|Way)\b[^,]*,\s*[A-Za-z\s]+,\s*[A-Z]{2}\s+\d{5}(-\d{4})?',
                r'\b\d+[A-Za-z]?\s+[A-Za-z\s\d#-]+,\s*[A-Za-z\s]+,\s*[A-Z]{2}\s+\d{5}(-\d{4})?'
            ]
            
            for pattern in address_patterns:
                matches = re.finditer(pattern, page_content, re.IGNORECASE)
                for match in matches:
                    addr = match.group().strip()
                    if addr and addr not in addresses:
                        addresses.append(addr)
            
            logger.info(f"Found {len(addresses)} unique addresses from profile")
            return addresses
            
        except Exception as e:
            logger.error(f"Error extracting addresses: {e}")
            return []

    async def extract_phone_numbers_session(self, session_page) -> Dict[str, str]:
        """Extract phone numbers from profile using session page."""
        phones = {'primary': '', 'secondary': '', 'all': ''}
        
        try:
            page_content = await session_page.text_content('body')
            
            # Phone number patterns
            phone_patterns = [
                r'\b\d{3}[-.\s]?\d{3}[-.\s]?\d{4}\b',  # Standard US format
                r'\(\d{3}\)\s*\d{3}[-.\s]?\d{4}',     # (xxx) xxx-xxxx
                r'\d{3}\s+\d{3}\s+\d{4}',             # xxx xxx xxxx
            ]
            
            found_phones = []
            for pattern in phone_patterns:
                matches = re.finditer(pattern, page_content)
                for match in matches:
                    phone = re.sub(r'[^\d]', '', match.group())
                    if len(phone) == 10 and phone not in found_phones:
                        found_phones.append(phone)
            
            # Format phones nicely
            formatted_phones = []
            for phone in found_phones:
                formatted = f"{phone[:3]}-{phone[3:6]}-{phone[6:]}"
                formatted_phones.append(formatted)
            
            if formatted_phones:
                phones['primary'] = formatted_phones[0]
                if len(formatted_phones) > 1:
                    phones['secondary'] = formatted_phones[1]
                phones['all'] = ', '.join(formatted_phones)
                
                logger.info(f"Found {len(formatted_phones)} phone numbers: {phones['all']}")
            
            return phones
            
        except Exception as e:
            logger.error(f"Error extracting phone numbers: {e}")
            return phones
            
    async def search_with_isolated_session(self, first_name, last_name, city, state, address=None):
        """Complete session isolation to prevent CAPTCHA persistence"""
        import tempfile
        import shutil
        
        browser = None
        context = None
        page = None
        temp_user_data = None
        
        try:
            # Create completely isolated temporary directory
            session_id = random.randint(100000, 999999)
            temp_user_data = tempfile.mkdtemp(prefix=f"radaris_clean_{session_id}_")
            logger.info(f"ISOLATION: Created fresh session directory: {temp_user_data}")
            
            # Start completely fresh playwright instance
            from playwright.async_api import async_playwright
            playwright = await async_playwright().start()
            
            # Random browser with complete isolation
            browser_types = ['chromium', 'firefox']
            browser_type = random.choice(browser_types)
            
            # Random viewport and timezone first
            viewports = [
                {'width': 1366, 'height': 768}, {'width': 1920, 'height': 1080},
                {'width': 1440, 'height': 900}, {'width': 1536, 'height': 864},
                {'width': 1280, 'height': 720}, {'width': 2560, 'height': 1440}
            ]
            viewport = random.choice(viewports)
            
            timezones = [
                'America/New_York', 'America/Chicago', 'America/Denver',
                'America/Los_Angeles', 'America/Phoenix', 'America/Anchorage'
            ]
            timezone = random.choice(timezones)
            
            user_agents = [
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0',
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'
            ]
            user_agent = random.choice(user_agents)

            if browser_type == 'chromium':
                # Chrome with persistent context for total isolation
                context = await playwright.chromium.launch_persistent_context(
                    temp_user_data,
                    headless=True,
                    viewport=viewport,
                    user_agent=user_agent,
                    timezone_id=timezone,
                    locale='en-US',
                    permissions=[],
                    ignore_https_errors=True,
                    java_script_enabled=True,
                    args=[
                        '--no-first-run',
                        '--no-default-browser-check', 
                        '--disable-background-networking',
                        '--disable-background-timer-throttling',
                        '--disable-backgrounding-occluded-windows',
                        '--disable-renderer-backgrounding',
                        '--disable-component-extensions-with-background-pages',
                        '--disable-extensions',
                        '--disable-plugins',
                        '--disable-sync',
                        '--disable-translate',
                        '--disable-web-security',
                        '--disable-features=VizDisplayCompositor,TranslateUI',
                        '--disable-blink-features=AutomationControlled',
                        '--disable-ipc-flooding-protection',
                        '--disable-dev-shm-usage',
                        '--disable-gpu',
                        '--no-sandbox',
                        '--disable-client-side-phishing-detection',
                        '--disable-component-update',
                        '--disable-domain-reliability',
                        '--disable-background-mode',
                        '--disable-default-apps'
                    ]
                )
                logger.info(f"ISOLATION: Created isolated Chromium persistent context #{session_id}")
            else:
                # Firefox with persistent context for total isolation
                context = await playwright.firefox.launch_persistent_context(
                    temp_user_data,
                    headless=True,
                    viewport=viewport,
                    user_agent=user_agent,
                    timezone_id=timezone,
                    locale='en-US',
                    permissions=[],
                    ignore_https_errors=True,
                    java_script_enabled=True,
                    firefox_user_prefs={
                        'dom.webdriver.enabled': False,
                        'useAutomationExtension': False,
                        'privacy.clearOnShutdown.cache': True,
                        'privacy.clearOnShutdown.cookies': True,
                        'privacy.clearOnShutdown.history': True,
                        'privacy.clearOnShutdown.sessions': True,
                        'browser.cache.disk.enable': False,
                        'browser.cache.memory.enable': False,
                        'network.http.use-cache': False,
                        'browser.privatebrowsing.autostart': True
                    }
                )
                logger.info(f"ISOLATION: Created isolated Firefox persistent context #{session_id}")
            
            
            # No need to create another context since we have persistent context
            logger.info(f"ISOLATION: Fresh context - {viewport['width']}x{viewport['height']}, {timezone}")
            
            # Create page and inject stealth
            page = await context.new_page()
            
            # Inject anti-detection JavaScript
            await page.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
                Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
                window.chrome = { runtime: {} };
                Object.defineProperty(navigator, 'permissions', { get: () => ({
                    query: () => Promise.resolve({ state: 'granted' })
                })});
            """)
            
            logger.info("ISOLATION: Navigating with fresh session...")
            
            # Navigate to Radaris
            await page.goto('https://radaris.com/', wait_until='networkidle', timeout=30000)
            await asyncio.sleep(random.uniform(3, 5))
            
            # Immediate CAPTCHA check
            captcha_selectors = [
                'iframe[src*="recaptcha"]', 'iframe[src*="captcha"]',
                '.g-recaptcha', '.recaptcha', '[data-callback*="captcha"]',
                'div[class*="captcha"]', 'div[id*="captcha"]'
            ]
            
            captcha_detected = False
            for selector in captcha_selectors:
                try:
                    captcha_element = await page.query_selector(selector)
                    if captcha_element:
                        captcha_detected = True
                        logger.warning(f"CAPTCHA detected: {selector}")
                        break
                except:
                    continue
            
            if captcha_detected:
                logger.warning("CAPTCHA: Fresh session still got CAPTCHA - waiting naturally")
                await asyncio.sleep(random.uniform(15, 25))
                
                # Try refresh once
                await page.reload(wait_until='networkidle')
                await asyncio.sleep(random.uniform(3, 6))
                
                # Check again
                captcha_still_there = False
                for selector in captcha_selectors:
                    try:
                        if await page.query_selector(selector):
                            captcha_still_there = True
                            break
                    except:
                        continue
                
                if captcha_still_there:
                    logger.error("CAPTCHA: Still present after refresh - skipping")
                    return {
                        'phone_primary': '', 'phone_secondary': '', 'phone_all': '',
                        'address_match': 'No', 'status': 'CAPTCHA Blocked',
                        'profile_url': 'https://radaris.com/',
                        'error_message': 'CAPTCHA required - fresh session blocked'
                    }
            
            # Perform search
            search_query = f"{first_name} {last_name} {city} {state}".strip()
            logger.info(f"SEARCH: Isolated search for: {search_query}")
            
            # Find search input
            search_input = None
            search_selectors = ['input[name="fn"]', 'input[type="text"]', '.search-input']
            
            for selector in search_selectors:
                try:
                    search_input = await page.query_selector(selector)
                    if search_input:
                        break
                except:
                    continue
            
            if not search_input:
                logger.error("SEARCH: Input field not found")
                return {
                    'phone_primary': '', 'phone_secondary': '', 'phone_all': '',
                    'address_match': 'No', 'status': 'Error',
                    'profile_url': page.url, 'error_message': 'Search input not found'
                }
            
            # Human-like typing using enhanced method
            await self.human_type_text(search_input, search_query)
            
            # Submit with natural interaction
            await search_input.press('Enter')
            await asyncio.sleep(random.uniform(3, 6))
            
            # Check for post-search CAPTCHA
            post_search_captcha = False
            for selector in captcha_selectors:
                try:
                    if await page.query_selector(selector):
                        post_search_captcha = True
                        break
                except:
                    continue
            
            if post_search_captcha:
                logger.warning("CAPTCHA: Appeared after search submission")
                return {
                    'phone_primary': '', 'phone_secondary': '', 'phone_all': '',
                    'address_match': 'No', 'status': 'CAPTCHA Blocked',
                    'profile_url': page.url, 'error_message': 'CAPTCHA after search'
                }
            
            # Process results using original Radaris extraction logic (not the simplified version)
            # Look for profile links
            profile_links = await page.query_selector_all('a[href*="/p/"]')
            logger.info(f"RESULTS: Found {len(profile_links)} potential profiles")
            
            if not profile_links:
                return {
                    'phone_primary': '', 'phone_secondary': '', 'phone_all': '',
                    'address_match': 'No', 'status': 'No Results',
                    'profile_url': page.url, 'error_message': 'No profiles found'
                }
            
            # Click first profile with human-like behavior  
            await profile_links[0].click()
            await asyncio.sleep(random.uniform(3, 5))
            
            # Use the original sophisticated Radaris phone extraction method
            # This is the key - we need to use the existing extraction logic, not a simplified version
            try:
                # Wait for content to load
                await page.wait_for_load_state('networkidle', timeout=5000)
                
                # The original logic looks for the main PHONE NUMBERS section and handles Radaris's specific structure
                phones = {'primary': '', 'secondary': '', 'all': ''}
                
                # First, try to get the primary phone from the main PHONE NUMBERS section
                primary_phone = ""
                try:
                    phone_section = await page.query_selector('div[class*="phone"], .contact-info, div:has-text("PHONE NUMBERS")')
                    if phone_section:
                        # Look for visible phone numbers in various formats
                        phone_text = await phone_section.inner_text() if phone_section else ""
                        
                        # Extract phone numbers using regex
                        import re
                        phone_matches = re.findall(r'(\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4})', phone_text)
                        
                        if phone_matches:
                            primary_phone = phone_matches[0]
                            phones['primary'] = primary_phone
                            phones['all'] = ', '.join(phone_matches)
                            if len(phone_matches) > 1:
                                phones['secondary'] = phone_matches[1]
                except Exception as phone_error:
                    logger.debug(f"Phone section extraction failed: {phone_error}")
                
                # If no phone found in main section, try other strategies
                if not primary_phone:
                    try:
                        # Look for tel: links
                        tel_links = await page.query_selector_all('a[href*="tel:"]')
                        for link in tel_links:
                            href = await link.get_attribute('href')
                            if href:
                                phone = href.replace('tel:', '').strip()
                                if re.match(r'^\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}$', phone):
                                    primary_phone = phone
                                    phones['primary'] = phone
                                    phones['all'] = phone
                                    break
                    except Exception as tel_error:
                        logger.debug(f"Tel link extraction failed: {tel_error}")
                
                # If still no phone, try looking for any phone patterns on the page
                if not primary_phone:
                    try:
                        page_text = await page.inner_text('body')
                        phone_matches = re.findall(r'\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}', page_text)
                        # Filter out common false positives (like dates, IDs, etc.)
                        valid_phones = []
                        for phone in phone_matches:
                            normalized = re.sub(r'[^\d]', '', phone)
                            if len(normalized) == 10 and not normalized.startswith('0') and not normalized.startswith('1'):
                                valid_phones.append(phone)
                        
                        if valid_phones:
                            primary_phone = valid_phones[0]
                            phones['primary'] = primary_phone
                            phones['all'] = ', '.join(valid_phones[:3])  # Limit to first 3
                            if len(valid_phones) > 1:
                                phones['secondary'] = valid_phones[1]
                    except Exception as page_text_error:
                        logger.debug(f"Page text extraction failed: {page_text_error}")
                
                if phones['primary']:
                    # IMPORTANT: We need to do proper address matching here, not just URL checking
                    # Extract addresses from the current profile and check for matches
                    profile_addresses = await self.extract_addresses_from_profile_isolated(page)
                    
                    # Check if any address matches using the original sophisticated logic
                    address_match_result = 'No'
                    matching_address = ""
                    
                    if address:  # Only check if we have an address to match
                        for prof_addr in profile_addresses:
                            if self.addresses_match(address, prof_addr):
                                address_match_result = 'Yes'
                                matching_address = prof_addr
                                logger.info(f"✅ ADDRESS MATCH: {prof_addr}")
                                break
                    
                    logger.info(f"SUCCESS: Found phones: {phones}")
                    return {
                        'phone_primary': phones['primary'],
                        'phone_secondary': phones['secondary'],
                        'phone_all': phones['all'],
                        'address_match': address_match_result,
                        'current_address': matching_address,
                        'status': 'Success',
                        'profile_url': page.url,
                        'error_message': ''
                    }
                else:
                    logger.info("No phone numbers found on profile page")
                    return {
                        'phone_primary': '', 'phone_secondary': '', 'phone_all': '',
                        'address_match': 'No', 'status': 'No Phone Numbers',
                        'profile_url': page.url, 'error_message': 'No phone numbers found'
                    }
                    
            except Exception as result_error:
                logger.error(f"RESULTS: Processing error: {result_error}")
                return {
                    'phone_primary': '', 'phone_secondary': '', 'phone_all': '',
                    'address_match': 'No', 'status': 'Error',
                    'profile_url': page.url, 'error_message': str(result_error)
                }
            
        except Exception as e:
            logger.error(f"ISOLATION: Session error: {e}")
            return {
                'phone_primary': '', 'phone_secondary': '', 'phone_all': '',
                'address_match': 'No', 'status': 'Error',
                'profile_url': '', 'error_message': str(e)
            }
        finally:
            # Complete cleanup
            try:
                if page:
                    await page.close()
                if context:
                    await context.close()
                if 'playwright' in locals():
                    await playwright.stop()
                
                # Delete temp directory completely
                if temp_user_data and os.path.exists(temp_user_data):
                    shutil.rmtree(temp_user_data, ignore_errors=True)
                    logger.info(f"CLEANUP: Deleted session directory: {temp_user_data}")
                    
            except Exception as cleanup_error:
                logger.error(f"CLEANUP: Error: {cleanup_error}")

    async def apply_enhanced_stealth(self, page):
        """Apply enhanced stealth techniques for better session isolation"""
        # Inject comprehensive anti-detection scripts
        await page.add_init_script("""
            // Hide webdriver property
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
            
            // Mock plugins
            Object.defineProperty(navigator, 'plugins', {
                get: () => [1, 2, 3, 4, 5]
            });
            
            // Mock languages
            Object.defineProperty(navigator, 'languages', {
                get: () => ['en-US', 'en']
            });
            
            // Add chrome object
            window.chrome = { 
                runtime: {},
                loadTimes: function() {
                    return {
                        commitLoadTime: Date.now() - 1000,
                        connectionInfo: 'h2',
                        finishDocumentLoadTime: Date.now() - 900,
                        finishLoadTime: Date.now() - 800,
                        firstPaintAfterLoadTime: Date.now() - 700,
                        firstPaintTime: Date.now() - 600,
                        navigationType: 'Navigation',
                        npnNegotiatedProtocol: 'h2',
                        requestTime: Date.now() - 1100,
                        startLoadTime: Date.now() - 1000,
                        wasAlternateProtocolAvailable: false,
                        wasFetchedViaSpdy: true,
                        wasNpnNegotiated: true
                    };
                }
            };
            
            // Mock permissions
            Object.defineProperty(navigator, 'permissions', {
                get: () => ({
                    query: () => Promise.resolve({ state: 'granted' })
                })
            });
            
            // Hide automation indicators
            delete window.cdc_adoQpoasnfa76pfcZLmcfl_Array;
            delete window.cdc_adoQpoasnfa76pfcZLmcfl_Promise;
            delete window.cdc_adoQpoasnfa76pfcZLmcfl_Symbol;
            
            // Mock screen properties
            Object.defineProperty(screen, 'availTop', { get: () => 0 });
            Object.defineProperty(screen, 'availLeft', { get: () => 0 });
            Object.defineProperty(screen, 'availHeight', { get: () => screen.height });
            Object.defineProperty(screen, 'availWidth', { get: () => screen.width });
        """)
    
    async def human_type_text(self, element, text):
        """Type text in a human-like manner with natural variations"""
        await element.click()
        await asyncio.sleep(random.uniform(0.2, 0.5))
        
        # Clear field first
        await element.fill('')
        await asyncio.sleep(random.uniform(0.1, 0.3))
        
        # Type each character with human-like timing
        for i, char in enumerate(text):
            await element.type(char)
            
            # Vary typing speed - slower at beginning, faster in middle, slower at end
            if i < len(text) * 0.2:  # First 20% - slower
                delay = random.uniform(0.08, 0.15)
            elif i < len(text) * 0.8:  # Middle 60% - faster
                delay = random.uniform(0.04, 0.08)
            else:  # Last 20% - slower again
                delay = random.uniform(0.06, 0.12)
            
            # Add occasional longer pauses (like thinking)
            if random.random() < 0.05:  # 5% chance
                delay += random.uniform(0.3, 0.8)
            
            await asyncio.sleep(delay)
        
        # Final pause before submitting
        await asyncio.sleep(random.uniform(0.5, 1.2))

    async def search_person_with_human_input(self, page, person_name, location, address):
        """Search using human-like input patterns"""
        try:
            # Navigate with natural timing
            await page.goto('https://radaris.com/', wait_until='networkidle', timeout=30000)
            await asyncio.sleep(random.uniform(2, 4))
            
            # Check for immediate CAPTCHA
            if await self.check_for_captcha(page):
                return await self.handle_captcha_detection(page)
            
            # Find search input with multiple strategies
            search_input = await self.find_search_input(page)
            if not search_input:
                return self.create_error_result("Search input not found", page.url)
            
            # Construct search query naturally
            search_query = f"{person_name} {location}".strip()
            logger.info(f"SEARCH: Human-like search for: {search_query}")
            
            # Type with human-like patterns
            await self.human_type_text(search_input, search_query)
            
            # Submit with natural interaction
            await search_input.press('Enter')
            await asyncio.sleep(random.uniform(3, 6))
            
            # Check for post-search CAPTCHA
            if await self.check_for_captcha(page):
                return await self.handle_captcha_detection(page)
            
            # Process results using existing logic
            return await self.process_search_results(page, address)
            
        except Exception as e:
            logger.error(f"HUMAN_INPUT: Error during human-like search: {str(e)}")
            return self.create_error_result(str(e), getattr(page, 'url', ''))
    
    async def find_search_input(self, page):
        """Find search input with multiple fallback strategies"""
        search_selectors = [
            'input[name="fn"]',
            'input[placeholder*="first name"]',
            'input[type="text"]',
            '.search-input',
            '#search-input',
            'input[class*="search"]'
        ]
        
        for selector in search_selectors:
            try:
                element = await page.query_selector(selector)
                if element and await element.is_visible():
                    return element
            except:
                continue
        return None
    
    async def check_for_captcha(self, page):
        """Check for CAPTCHA presence"""
        captcha_selectors = [
            'iframe[src*="recaptcha"]',
            'iframe[src*="captcha"]', 
            '.g-recaptcha',
            '.recaptcha',
            '[data-callback*="captcha"]',
            'div[class*="captcha"]',
            'div[id*="captcha"]'
        ]
        
        for selector in captcha_selectors:
            try:
                element = await page.query_selector(selector)
                if element:
                    logger.warning(f"CAPTCHA detected: {selector}")
                    return True
            except:
                continue
        return False
    
    async def handle_captcha_detection(self, page):
        """Handle CAPTCHA detection with natural behavior"""
        logger.warning("CAPTCHA: Detected - attempting natural handling")
        
        # Wait naturally
        await asyncio.sleep(random.uniform(15, 25))
        
        # Try refresh
        await page.reload(wait_until='networkidle')
        await asyncio.sleep(random.uniform(3, 6))
        
        # Check if still there
        if await self.check_for_captcha(page):
            logger.error("CAPTCHA: Still present after refresh")
            return self.create_error_result("CAPTCHA verification required", page.url)
        
        return None  # No CAPTCHA, continue
    
    def create_error_result(self, error_message, url=""):
        """Create standardized error result"""
        return {
            'phone_primary': '',
            'phone_secondary': '', 
            'phone_all': '',
            'address_match': 'No',
            'status': 'Error',
            'profile_url': url,
            'error_message': error_message
        }
    
    async def process_search_results(self, page, address):
        """Process search results to find phone numbers"""
        try:
            # Look for profile links
            profile_links = await page.query_selector_all('a[href*="/p/"]')
            logger.info(f"RESULTS: Found {len(profile_links)} potential profiles")
            
            if not profile_links:
                return self.create_error_result("No profiles found", page.url)
            
            # Click first profile with human-like behavior
            await profile_links[0].click()
            await asyncio.sleep(random.uniform(3, 5))
            
            # Extract phone numbers using multiple strategies
            phones = await self.extract_phone_numbers_advanced(page)
            
            if phones:
                logger.info(f"SUCCESS: Found phones: {phones}")
                return {
                    'phone_primary': phones[0],
                    'phone_secondary': phones[1] if len(phones) > 1 else '',
                    'phone_all': ', '.join(phones),
                    'address_match': self.check_address_match(address, page.url),
                    'status': 'Success',
                    'profile_url': page.url,
                    'error_message': ''
                }
            
            return self.create_error_result("No phone numbers found", page.url)
            
        except Exception as e:
            logger.error(f"RESULTS: Processing error: {e}")
            return self.create_error_result(str(e), page.url)
    
    async def extract_phone_numbers_advanced(self, page):
        """Extract phone numbers using multiple strategies"""
        phones = []
        
        # Strategy 1: Look for phone links
        phone_links = await page.query_selector_all('a[href*="tel:"]')
        for link in phone_links:
            try:
                href = await link.get_attribute('href')
                phone = href.replace('tel:', '').strip()
                if self.is_valid_phone(phone):
                    phones.append(phone)
            except:
                continue
        
        # Strategy 2: Look for phone in text
        phone_elements = await page.query_selector_all('span[class*="phone"], div[class*="phone"], .contact-info')
        for element in phone_elements:
            try:
                text = await element.inner_text()
                phone_matches = re.findall(r'(\d{3}[-.\s]?\d{3}[-.\s]?\d{4})', text)
                for phone in phone_matches:
                    if self.is_valid_phone(phone):
                        phones.append(phone)
            except:
                continue
        
        # Remove duplicates while preserving order
        seen = set()
        unique_phones = []
        for phone in phones:
            normalized = re.sub(r'[^\d]', '', phone)
            if normalized not in seen:
                seen.add(normalized)
                unique_phones.append(phone)
        
        return unique_phones
    
    def is_valid_phone(self, phone):
        """Validate phone number format"""
        normalized = re.sub(r'[^\d]', '', phone)
        return len(normalized) == 10 and normalized[0] != '0' and normalized[0] != '1'
    
    def check_address_match(self, address, url):
        """Check if address matches the profile"""
        if not address:
            return 'Partial'
        
        # Simple address matching logic
        address_parts = address.lower().split()
        url_lower = url.lower()
        
        matches = sum(1 for part in address_parts if len(part) > 3 and part in url_lower)
        if matches >= len(address_parts) * 0.6:
            return 'Yes'
        elif matches > 0:
            return 'Partial'
        return 'No'

    def check_address_match(self, address, url):
        """Check if address matches the profile"""
        if not address:
            return 'Partial'
        
        # Simple address matching logic
        address_parts = address.lower().split()
        url_lower = url.lower()
        
        matches = sum(1 for part in address_parts if len(part) > 3 and part in url_lower)
        if matches >= len(address_parts) * 0.6:
            return 'Yes'
        elif matches > 0:
            return 'Partial'
        return 'No'

    async def extract_addresses_from_profile_isolated(self, page):
        """Extract addresses from profile page for isolated session"""
        addresses = []
        try:
            # Wait for content to load
            await page.wait_for_load_state('networkidle', timeout=5000)
            
            # Use the same selectors as the original method
            address_selectors = [
                '.address, .location',
                'div:has-text("Address")', 
                'div:has-text("Location")',
                'span[class*="address"]',
                'div[class*="address"]',
                '.profile-address'
            ]
            
            for selector in address_selectors:
                try:
                    elements = await page.query_selector_all(selector)
                    for element in elements:
                        text = await element.inner_text()
                        if text and len(text.strip()) > 10:  # Basic filter
                            addresses.append(text.strip())
                except:
                    continue
            
            # Also check page text for addresses
            try:
                page_text = await page.inner_text('body')
                # Look for address patterns in the text
                import re
                address_patterns = re.findall(r'\d+\s+[A-Z][A-Za-z\s]+(?:Street|St|Avenue|Ave|Drive|Dr|Court|Ct|Lane|Ln|Road|Rd|Boulevard|Blvd|Terrace|Ter|Circle|Cir|Way|Place|Pl)\s*[^,\n]*(?:,\s*[A-Z]{2}\s*\d{5})?', page_text)
                addresses.extend(address_patterns)
            except:
                pass
            
            # Remove duplicates and clean up
            unique_addresses = []
            seen = set()
            for addr in addresses:
                addr_clean = addr.strip()
                if addr_clean and addr_clean.lower() not in seen:
                    seen.add(addr_clean.lower())
                    unique_addresses.append(addr_clean)
            
            logger.info(f"Found {len(unique_addresses)} addresses: {unique_addresses}")
            return unique_addresses
            
        except Exception as e:
            logger.error(f"Address extraction error: {e}")
            return []

    async def extract_phone_numbers_isolated(self, page):
        """Extract phone numbers using original sophisticated logic for isolated session"""
        phones = {'primary': '', 'secondary': '', 'all': ''}
        
        try:
            # Wait for content to load
            await page.wait_for_load_state('networkidle', timeout=5000)
            
            # Use the ORIGINAL sophisticated phone extraction logic from the main script
            # This should be identical to the extract_phone_numbers method
            
            # First, try to get the primary phone from the main PHONE NUMBERS section
            primary_phone = ""
            try:
                phone_sections = [
                    'div:has-text("PHONE NUMBERS")',
                    'div[class*="phone"]',
                    '.contact-info',
                    'div:has-text("Phone")'
                ]
                
                for selector in phone_sections:
                    try:
                        phone_section = await page.query_selector(selector)
                        if phone_section:
                            phone_text = await phone_section.inner_text()
                            
                            import re
                            phone_matches = re.findall(r'(\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4})', phone_text)
                            
                            if phone_matches:
                                primary_phone = phone_matches[0]
                                phones['primary'] = primary_phone
                                phones['all'] = ', '.join(phone_matches)
                                if len(phone_matches) > 1:
                                    phones['secondary'] = phone_matches[1]
                                break
                    except:
                        continue
                        
            except Exception as phone_error:
                logger.debug(f"Phone section extraction failed: {phone_error}")
            
            # If no phone found, try tel: links
            if not primary_phone:
                try:
                    tel_links = await page.query_selector_all('a[href*="tel:"]')
                    for link in tel_links:
                        href = await link.get_attribute('href')
                        if href:
                            phone = href.replace('tel:', '').strip()
                            import re
                            if re.match(r'^\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}$', phone):
                                phones['primary'] = phone
                                phones['all'] = phone
                                break
                except:
                    pass
            
            # Last resort: scan page text
            if not phones['primary']:
                try:
                    page_text = await page.inner_text('body')
                    import re
                    phone_matches = re.findall(r'\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}', page_text)
                    
                    valid_phones = []
                    for phone in phone_matches:
                        normalized = re.sub(r'[^\d]', '', phone)
                        if len(normalized) == 10 and not normalized.startswith('0') and not normalized.startswith('1'):
                            valid_phones.append(phone)
                    
                    if valid_phones:
                        phones['primary'] = valid_phones[0]
                        phones['all'] = ', '.join(valid_phones[:3])
                        if len(valid_phones) > 1:
                            phones['secondary'] = valid_phones[1]
                except:
                    pass
            
            return phones
            
        except Exception as e:
            logger.error(f"Phone extraction error: {e}")
            return {'primary': '', 'secondary': '', 'all': ''}

    async def create_stealth_browser(self):
        """Create ultra-stealth browser with complete session isolation to avoid CAPTCHAs."""
        from playwright.async_api import async_playwright
        import tempfile
        import string
        
        try:
            # Create completely isolated temporary session directory
            session_id = random.randint(100000, 999999)
            temp_suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
            temp_session_dir = os.path.join(tempfile.gettempdir(), f"radaris_clean_{session_id}_{temp_suffix}")
            
            # Ensure directory is created
            os.makedirs(temp_session_dir, exist_ok=True)
            logger.info(f"ISOLATION: Created fresh session directory: {temp_session_dir}")
            
            playwright = await async_playwright().start()
            
            # More browser variety to avoid fingerprinting
            browser_configs = [
                {
                    'type': 'chromium',
                    'headless': True,
                    'user_data_dir': temp_session_dir,  # Key isolation
                    'args': [
                        f'--user-data-dir={temp_session_dir}',  # Complete isolation
                        '--no-sandbox',
                        '--disable-setuid-sandbox',
                        '--disable-dev-shm-usage',
                        '--disable-accelerated-2d-canvas',
                        '--disable-accelerated-jpeg-decoding',
                        '--disable-accelerated-mjpeg-decode',
                        '--disable-accelerated-video-decode',
                        '--disable-background-networking',
                        '--disable-background-timer-throttling',
                        '--disable-backgrounding-occluded-windows',
                        '--disable-breakpad',
                        '--disable-client-side-phishing-detection',
                        '--disable-component-extensions-with-background-pages',
                        '--disable-default-apps',
                        '--disable-desktop-notifications',
                        '--disable-domain-reliability',
                        '--disable-extensions',
                        '--disable-features=TranslateUI,VizDisplayCompositor',
                        '--disable-hang-monitor',
                        '--disable-ipc-flooding-protection',
                        '--disable-popup-blocking',
                        '--disable-prompt-on-repost',
                        '--disable-renderer-backgrounding',
                        '--disable-sync',
                        '--disable-translate',
                        '--metrics-recording-only',
                        '--no-first-run',
                        '--safebrowsing-disable-auto-update',
                        '--disable-blink-features=AutomationControlled',
                        '--disable-automation',
                        '--exclude-switches=enable-automation',
                        '--disable-plugins-discovery',
                        '--disable-default-apps',
                        '--disable-background-mode',
                        '--no-default-browser-check',
                        '--disable-web-security',
                        '--allow-running-insecure-content',
                        '--disable-features=VizDisplayCompositor',
                        '--disable-logging'
                    ]
                },
                {
                    'type': 'firefox',
                    'headless': True,
                    'user_data_dir': temp_session_dir,  # Key isolation
                    'firefox_user_prefs': {
                        'dom.webdriver.enabled': False,
                        'useAutomationExtension': False,
                        'general.platform.override': 'Win32',
                        'general.useragent.override': '',
                        'privacy.trackingprotection.enabled': True,
                        'privacy.trackingprotection.socialtracking.enabled': True,
                        'privacy.trackingprotection.fingerprinting.enabled': True,
                        'privacy.resistFingerprinting': True,
                        'dom.battery.enabled': False,
                        'geo.enabled': False,
                        'beacon.enabled': False
                    }
                }
            ]
            
            # Randomly select browser config
            config = random.choice(browser_configs)
            
            if config['type'] == 'chromium':
                browser = await playwright.chromium.launch(
                    headless=config['headless'],
                    args=config['args']
                )
            else:
                browser = await playwright.firefox.launch(
                    headless=config['headless'],
                    firefox_user_prefs=config['firefox_user_prefs']
                )
            
            logger.info(f"ISOLATION: Created isolated {config['type']} session #{session_id}")
            
            # Store session info for cleanup
            browser._temp_session_dir = temp_session_dir
            browser._session_id = session_id
            
            return browser
            
        except Exception as e:
            logger.error(f"Isolated browser creation error: {e}")
            raise

    async def create_stealth_context(self, browser):
        """Create ultra-stealth context to avoid CAPTCHAs with advanced fingerprint randomization."""
        try:
            # Much more diverse viewport pool
            viewports = [
                {'width': 1366, 'height': 768},   # Most common laptop
                {'width': 1920, 'height': 1080},  # Full HD desktop
                {'width': 1440, 'height': 900},   # MacBook Pro 13"
                {'width': 1536, 'height': 864},   # Surface Pro
                {'width': 1280, 'height': 720},   # HD laptop
                {'width': 1600, 'height': 900},   # 16:9 laptop
                {'width': 1680, 'height': 1050},  # Desktop
                {'width': 1920, 'height': 1200},  # Wide desktop
                {'width': 2560, 'height': 1440},  # 2K desktop
                {'width': 1024, 'height': 768},   # Older desktop
            ]
            viewport = random.choice(viewports)
            
            # Realistic user agent pool from real browsers
            user_agents = [
                # Chrome Windows
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                
                # Firefox Windows
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/118.0',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/117.0',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/120.0',
                
                # Edge Windows
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36 Edg/118.0.0.0',
                
                # Chrome Mac
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
                
                # Safari Mac
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Safari/605.1.15',
            ]
            user_agent = random.choice(user_agents)
            
            # More diverse timezones across US to appear more natural
            timezones = [
                'America/New_York',      # Eastern (most common)
                'America/Chicago',       # Central  
                'America/Denver',        # Mountain
                'America/Los_Angeles',   # Pacific
                'America/Phoenix',       # Arizona (no DST)
                'America/Anchorage',     # Alaska
                'Pacific/Honolulu',      # Hawaii
                'America/Detroit',       # Eastern 
                'America/Indianapolis',  # Eastern
                'America/Louisville',    # Eastern
                'America/Menominee',     # Central
                'America/Metlakatla',    # Alaska
                'America/Nome',          # Alaska
                'America/Sitka',         # Alaska
                'America/Yakutat',       # Alaska
            ]
            timezone = random.choice(timezones)
            
            # Vary locales to match different regions
            locale_pools = [
                'en-US',  # Standard US
                'en-CA',  # Canadian English
                'en-GB',  # British English (some US users have this)
            ]
            locale = random.choice(locale_pools)
            
            # Create context with anti-detection settings
            context = await browser.new_context(
                viewport=viewport,
                user_agent=user_agent,
                timezone_id=timezone,
                locale=locale,
                permissions=['geolocation'],
                # Enhanced HTTP headers to look more natural
                extra_http_headers={
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                    'Accept-Language': f'{locale},en;q=0.9',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1',
                    'Sec-Fetch-Dest': 'document',
                    'Sec-Fetch-Mode': 'navigate',
                    'Sec-Fetch-Site': 'none',
                    'Sec-Fetch-User': '?1',
                    'sec-ch-ua': '"Google Chrome";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': '"Windows"',
                    'Cache-Control': 'max-age=0',
                }
            )
            
            logger.info(f"STEALTH: Context created - Viewport: {viewport['width']}x{viewport['height']}")
            logger.info(f"STEALTH: Timezone: {timezone}, Locale: {locale}")
            logger.info(f"STEALTH: User-Agent: {user_agent[:50]}...")
            
            return context
            
        except Exception as e:
            logger.error(f"Context creation error: {e}")
            raise

    async def apply_page_stealth(self, page):
        """Apply ultra-stealth techniques to completely avoid CAPTCHAs."""
        try:
            # Advanced stealth script to hide automation completely
            await page.add_init_script("""
                // Remove webdriver property completely
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined,
                });
                
                // Override automation flags
                delete window.navigator.__proto__.webdriver;
                delete window.navigator.webdriver;
                window.navigator.chrome = {
                    runtime: {},
                    loadTimes: function() {},
                    csi: function() {},
                    app: {}
                };
                
                // Mock plugins to look like real browser
                Object.defineProperty(navigator, 'plugins', {
                    get: () => [
                        {
                            0: {type: "application/x-google-chrome-pdf", suffixes: "pdf", description: "Portable Document Format", enabledPlugin: {}},
                            description: "Portable Document Format",
                            filename: "internal-pdf-viewer",
                            length: 1,
                            name: "Chrome PDF Plugin"
                        },
                        {
                            0: {type: "application/pdf", suffixes: "pdf", description: "", enabledPlugin: {}},
                            description: "",
                            filename: "mhjfbmdgcfjbbpaeojofohoefgiehjai",
                            length: 1,
                            name: "Chrome PDF Viewer"
                        },
                        {
                            0: {type: "application/x-nacl", suffixes: "", description: "Native Client Executable", enabledPlugin: {}},
                            1: {type: "application/x-pnacl", suffixes: "", description: "Portable Native Client Executable", enabledPlugin: {}},
                            description: "",
                            filename: "internal-nacl-plugin",
                            length: 2,
                            name: "Native Client"
                        }
                    ],
                });
                
                // Override permissions API to avoid detection
                const originalQuery = window.navigator.permissions.query;
                window.navigator.permissions.query = (parameters) => (
                    parameters.name === 'notifications' ?
                        Promise.resolve({ state: Notification.permission }) :
                        originalQuery(parameters)
                );
                
                // Mock languages properly
                Object.defineProperty(navigator, 'languages', {
                    get: () => ['en-US', 'en'],
                });
                
                // Hide automation in chrome object
                if (window.chrome) {
                    Object.defineProperty(window.chrome, 'runtime', {
                        get: () => ({
                            onConnect: undefined,
                            onMessage: undefined
                        })
                    });
                }
                
                // Override screen properties to match viewport
                Object.defineProperties(screen, {
                    availHeight: { get: () => window.innerHeight },
                    availWidth: { get: () => window.innerWidth },
                    height: { get: () => window.innerHeight },
                    width: { get: () => window.innerWidth }
                });
                
                // Mock battery API to avoid fingerprinting
                if ('getBattery' in navigator) {
                    navigator.getBattery = () => Promise.resolve({
                        charging: true,
                        chargingTime: 0,
                        dischargingTime: Infinity,
                        level: Math.random() * 0.99 + 0.01
                    });
                }
                
                // Remove automation detection patterns
                ['__webdriver_evaluate', '__selenium_evaluate', '__webdriver_script_function', 
                 '__webdriver_script_func', '__webdriver_script_fn', '__fxdriver_evaluate',
                 '__driver_unwrapped', '__webdriver_unwrapped', '__driver_evaluate',
                 '__selenium_unwrapped', '__fxdriver_unwrapped'].forEach(prop => {
                    delete window[prop];
                });
                
                // Mock geolocation to appear more natural
                if ('geolocation' in navigator) {
                    const mockGeolocation = {
                        getCurrentPosition: (success, error) => {
                            // Simulate different US locations
                            const locations = [
                                {lat: 40.7128, lng: -74.0060}, // NYC
                                {lat: 34.0522, lng: -118.2437}, // LA  
                                {lat: 41.8781, lng: -87.6298}, // Chicago
                                {lat: 29.7604, lng: -95.3698}, // Houston
                                {lat: 39.9526, lng: -75.1652}, // Philadelphia
                            ];
                            const loc = locations[Math.floor(Math.random() * locations.length)];
                            setTimeout(() => success({
                                coords: {
                                    latitude: loc.lat + (Math.random() - 0.5) * 0.01,
                                    longitude: loc.lng + (Math.random() - 0.5) * 0.01,
                                    accuracy: Math.random() * 100 + 50
                                }
                            }), Math.random() * 1000 + 100);
                        }
                    };
                    Object.defineProperty(navigator, 'geolocation', {
                        get: () => mockGeolocation
                    });
                }
                
                // Random timing to avoid detection patterns
                const originalSetTimeout = window.setTimeout;
                window.setTimeout = function(func, delay, ...args) {
                    const jitter = Math.random() * 50; // Add up to 50ms jitter
                    return originalSetTimeout.call(this, func, delay + jitter, ...args);
                };
            """)
            
            # Add mouse movement simulation to appear more human
            await page.evaluate("""
                // Simulate natural mouse movements
                let mouseX = Math.random() * window.innerWidth;
                let mouseY = Math.random() * window.innerHeight;
                
                setInterval(() => {
                    mouseX += (Math.random() - 0.5) * 20;
                    mouseY += (Math.random() - 0.5) * 20;
                    mouseX = Math.max(0, Math.min(window.innerWidth, mouseX));
                    mouseY = Math.max(0, Math.min(window.innerHeight, mouseY));
                    
                    document.dispatchEvent(new MouseEvent('mousemove', {
                        clientX: mouseX,
                        clientY: mouseY,
                        bubbles: true
                    }));
                }, Math.random() * 3000 + 2000); // Every 2-5 seconds
            """)
            
            logger.info("STEALTH: Applied ultra-advanced page stealth to avoid CAPTCHAs")
            
        except Exception as e:
            logger.error(f"Error applying page stealth: {e}")
            raise

    async def process_csv(self, start_row: int = 0, max_rows: int = None):
        """Process the entire CSV file - simplified for ZabaSearch backup"""
        if not self.load_data():
            return False

        total_rows = len(self.df)
        end_row = min(total_rows, start_row + max_rows) if max_rows else total_rows

        logger.info(f"RADARIS: Processing {end_row - start_row} records needing phone numbers")

        try:
            await self.init_browser()

            for i in range(start_row, end_row):
                try:
                    row = self.df.iloc[i]

                    # Process each person in this record that needs phones
                    # Check DirectName first
                    if (hasattr(row, 'DirectName_Type') and row['DirectName_Type'] == 'Person' and
                        pd.notna(row['DirectName_Address']) and row['DirectName_Address'] != '' and
                        (pd.isna(row['DirectName_Phone_Primary']) or row['DirectName_Phone_Primary'] == '' or row['DirectName_Phone_Primary'] == 'N/A')):
                        
                        logger.info(f"Processing DirectName: {row.get('DirectName_Cleaned', 'Unknown')} at {row['DirectName_Address']}")
                        
                        # Use DirectName data for search
                        search_row = row.copy()
                        search_row['IndirectName_Cleaned'] = row['DirectName_Cleaned'] 
                        search_row['IndirectName_Address'] = row['DirectName_Address']
                        
                        result = await self.process_person(i, search_row)
                        
                        # Save to DirectName columns - only if we found a phone number
                        if result['phone_primary'] and result['phone_primary'] != '' and result['phone_primary'] != 'N/A':
                            self.df.at[i, 'DirectName_Phone_Primary'] = result['phone_primary']
                            logger.info(f"SUCCESS: Found DirectName phone: {result['phone_primary']}")
                        if result['phone_secondary'] and result['phone_secondary'] != '' and result['phone_secondary'] != 'N/A':
                            self.df.at[i, 'DirectName_Phone_Secondary'] = result['phone_secondary']
                        if result['phone_all'] and result['phone_all'] != '' and result['phone_all'] != 'N/A':
                            self.df.at[i, 'DirectName_Phone_All'] = result['phone_all']

                    # Check IndirectName
                    if (hasattr(row, 'IndirectName_Type') and row['IndirectName_Type'] == 'Person' and
                        pd.notna(row['IndirectName_Address']) and row['IndirectName_Address'] != '' and
                        (pd.isna(row['IndirectName_Phone_Primary']) or row['IndirectName_Phone_Primary'] == '' or row['IndirectName_Phone_Primary'] == 'N/A')):
                        
                        logger.info(f"Processing IndirectName: {row.get('IndirectName_Cleaned', 'Unknown')} at {row['IndirectName_Address']}")
                        
                        result = await self.process_person(i, row)
                        
                        # Save to IndirectName columns - only if we found a phone number
                        if result['phone_primary'] and result['phone_primary'] != '' and result['phone_primary'] != 'N/A':
                            self.df.at[i, 'IndirectName_Phone_Primary'] = result['phone_primary']
                            logger.info(f"SUCCESS: Found IndirectName phone: {result['phone_primary']}")
                        if result['phone_secondary'] and result['phone_secondary'] != '' and result['phone_secondary'] != 'N/A':
                            self.df.at[i, 'IndirectName_Phone_Secondary'] = result['phone_secondary']
                        if result['phone_all'] and result['phone_all'] != '' and result['phone_all'] != 'N/A':
                            self.df.at[i, 'IndirectName_Phone_All'] = result['phone_all']

                    # Save progress every 10 rows
                    if (i + 1) % 10 == 0:
                        await self.save_progress_simple(i)

                except Exception as e:
                    logger.error(f"Error processing row {i}: {e}")
                    continue

            # Final save
            await self.save_progress_final()
            logger.info(f"RADARIS: Processing complete. Results merged back to original data.")

            return True

        except Exception as e:
            logger.error(f"Critical error in process_csv: {e}")
            return False

        finally:
            await self.close_browser()

    async def save_progress_final(self):
        """Save final results by merging back to original dataframe"""
        try:
            if not hasattr(self, 'original_df'):
                # If no original_df, just save current df
                self.df.to_csv(self.output_path, index=False)
                logger.info(f"Results saved to {self.output_path}")
                return
            
            # Merge processed results back to original dataframe using stored indices
            for i, row in self.df.iterrows():
                if '_original_index' in row:
                    orig_idx = row['_original_index']  # Get the original index
                    
                    # Copy phone results back to original dataframe
                    phone_columns = ['DirectName_Phone_Primary', 'DirectName_Phone_Secondary', 'DirectName_Phone_All',
                                   'IndirectName_Phone_Primary', 'IndirectName_Phone_Secondary', 'IndirectName_Phone_All']
                    
                    for col in phone_columns:
                        if col in self.df.columns and col in self.original_df.columns:
                            # Only update if we found a phone number (not empty/NaN)
                            if not pd.isna(row[col]) and row[col] != '' and row[col] != 'N/A':
                                self.original_df.at[orig_idx, col] = row[col]
                                logger.info(f"MERGE: Updated {col} at index {orig_idx} with: {row[col]}")
            
            # Save the updated original dataframe
            self.original_df.to_csv(self.output_path, index=False)
            logger.info(f"SUCCESS: Final results merged and saved to {self.output_path}")
            
        except Exception as e:
            logger.error(f"ERROR: Failed to merge results: {e}")
            # Fallback - save current df without _original_index column
            df_to_save = self.df.drop(columns=['_original_index'], errors='ignore')
            df_to_save.to_csv(self.output_path, index=False)
            logger.info(f"FALLBACK: Saved processed results to {self.output_path}")

    async def save_progress_simple(self, row_num: int):
        """Simple progress save for debugging"""
        logger.info(f"Progress saved at row {row_num}")

    async def run_sample(self, num_samples: int = 3):
        """Run on a small sample for testing"""
        if not self.load_data():
            return False

        # Get random sample
        sample_indices = random.sample(range(len(self.df)), min(num_samples, len(self.df)))
        logger.info(f"Testing with {len(sample_indices)} random samples: {sample_indices}")

        try:
            await self.init_browser()

            for i in sample_indices:
                row = self.df.iloc[i]
                result = await self.process_person(i, row)

                logger.info(f"Sample {i} result: {result}")

                # Update DataFrame
                self.df.at[i, 'Phone_Primary'] = result['phone_primary']
                self.df.at[i, 'Phone_Secondary'] = result['phone_secondary']
                self.df.at[i, 'Phone_All'] = result['phone_all']
                self.df.at[i, 'Address_Match'] = result['address_match']
                self.df.at[i, 'Search_Status'] = result['status']
                self.df.at[i, 'Source_URL'] = result['profile_url']

                await self.random_delay(3, 6)

            # Save sample results
            sample_output = self.output_path.parent / f"{self.output_path.stem}_sample.csv"
            self.df.to_csv(sample_output, index=False)
            logger.info(f"Sample results saved to {sample_output}")

        finally:
            await self.close_browser()

    async def process_csv_batch(self, input_file: str, output_file: str = None):
        """
        Process CSV batch for pipeline integration - enhanced stealth version
        
        Args:
            input_file (str): Input CSV file path
            output_file (str): Output CSV file path (optional)
            
        Returns:
            tuple: (success: bool, output_file: str)
        """
        logger.info("RADARIS: Starting Radaris batch processing with enhanced stealth...")
        
        try:
            # Update paths
            self.csv_path = Path(input_file)
            if output_file:
                self.output_path = Path(output_file)
            else:
                # Default output with radaris suffix
                self.output_path = self.csv_path.parent / f"{self.csv_path.stem}_radaris_backup.csv"
            
            logger.info(f"INPUT: {self.csv_path}")
            logger.info(f"OUTPUT: {self.output_path}")
            
            # Load and filter data (only records needing Radaris backup)
            if not self.load_data():
                logger.info("SUCCESS: No records need Radaris backup processing!")
                return True, str(self.output_path)
            
            # Check if dataframe has data
            if self.df is None or len(self.df) == 0:
                logger.info("SUCCESS: No records to process!")
                return True, str(self.output_path)
            
            logger.info(f"PROCESSING: Processing {len(self.df)} records that need phone numbers...")
            
            # Initialize enhanced stealth browser
            await self.init_browser()
            
            # Process all filtered records (these are records ZabaSearch couldn't handle)
            success = await self.process_csv()
            
            if success:
                logger.info(f"SUCCESS: Radaris batch processing completed! Results in {self.output_path}")
            else:
                logger.error("ERROR: Radaris batch processing encountered errors")
            
            return success, str(self.output_path)
            
        except Exception as e:
            logger.error(f"ERROR: Error in Radaris batch processing: {e}")
            return False, str(self.output_path) if hasattr(self, 'output_path') else ""
        
        finally:
            try:
                await self.close_browser()
            except:
                pass


async def main():
    """Main function to run the scraper"""
    import sys

    # Check if CSV path provided as command line argument
    if len(sys.argv) > 1:
        CSV_PATH = sys.argv[1]
    else:
        # Updated to use the correct backup file that contains our test cases
        CSV_PATH = r"c:\Users\my notebook\Desktop\BlakeJackson\weekly_output\broward_lis_pendens_20250721_130030_processed_with_addresses_fast_processed_fastpeople_updated.csv_backup_20250724_140527.csv"

    print(f"Using CSV file: {CSV_PATH}")
    scraper = RadarisPhoneScraper(CSV_PATH)

    # Choose what to run:

    # 1. Test with our proven working test cases first
    print("Starting targeted test with known working cases...")
    # Test Earl Cox, Ekaterina Savuskan, Gloria Lapi (rows we know work)
    await scraper.run_sample(num_samples=5)

    # 2. Uncomment to process all records
    # print("Starting full processing...")
    # await scraper.process_csv()

    # 3. Uncomment to process specific range
    # print("Starting partial processing...")
    # await scraper.process_csv(start_row=0, max_rows=10)

if __name__ == "__main__":
    print("Radaris Phone Number Scraper")
    print("=" * 40)
    print(f"Starting at: {datetime.now()}")

    asyncio.run(main())

    print(f"Completed at: {datetime.now()}")
