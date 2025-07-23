#!/usr/bin/env python3
"""
Fast Headless Address Extractor with AI-like Human Behavior
- Shorter, more realistic pauses
- Variable typing speeds
- Mouse movements and scrolling
- Random user agent rotation
- Command line support for cron jobs
"""

import pandas as pd
import asyncio
import time
import random
import argparse
import os
import sys
import re
from typing import Optional
from playwright.async_api import async_playwright

# Realistic user agents to rotate
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/121.0.0.0 Safari/537.36'
]

async def create_smart_browser(playwright, headless=True):
    """Create browser with intelligent stealth settings and timeout configuration"""
    user_agent = random.choice(USER_AGENTS)
    
    # Configure timeouts from environment variables (cloud deployment friendly)
    navigation_timeout = int(os.environ.get('BROWARD_NAVIGATION_TIMEOUT', '60000'))
    
    print(f"[BROWSER] Creating browser with headless={headless}, timeout={navigation_timeout}ms")
    
    browser = await playwright.chromium.launch(
        headless=headless,
        args=[
            '--no-sandbox',
            '--disable-blink-features=AutomationControlled',
            '--disable-dev-shm-usage',
            '--disable-web-security',
            '--disable-features=VizDisplayCompositor',
            f'--user-agent={user_agent}'
        ]
    )
    
    context = await browser.new_context(
        viewport={'width': random.randint(1366, 1920), 'height': random.randint(768, 1080)},
        user_agent=user_agent,
        locale='en-US',
        timezone_id='America/New_York'
    )
    
    # Set default timeout for all page operations
    context.set_default_timeout(navigation_timeout)
    context.set_default_navigation_timeout(navigation_timeout)
    
    return browser, context

async def human_pause(action_type="normal"):
    """Intelligent pauses based on action type"""
    if action_type == "typing":
        # Short pause while typing
        delay = random.uniform(0.1, 0.4)
    elif action_type == "reading":
        # Time to read results
        delay = random.uniform(1.5, 3.0)
    elif action_type == "between_searches":
        # Shorter pause between searches (like a focused user)
        delay = random.uniform(2.5, 5.0)
    elif action_type == "page_load":
        # Wait for page to load
        delay = random.uniform(1.0, 2.5)
    else:
        # Default pause
        delay = random.uniform(0.5, 1.5)
    
    await asyncio.sleep(delay)
    return delay

async def smart_typing(page, selector, text):
    """Human-like typing with variable speeds and occasional corrections"""
    await page.fill(selector, '')
    await human_pause("typing")
    
    # Type with human-like speed variations
    for i, char in enumerate(text):
        await page.type(selector, char, delay=random.randint(80, 200))
        
        # Occasionally pause longer (like thinking)
        if random.random() < 0.1:
            await human_pause("typing")
        
        # Very rarely make a "typo" and correct it
        if random.random() < 0.03 and i > 0:
            await page.press(selector, 'Backspace')
            await human_pause("typing")
            await page.type(selector, char, delay=random.randint(100, 250))

async def add_human_behavior(page):
    """Add subtle human-like behaviors"""
    # Randomly scroll a bit (like someone checking the page)
    if random.random() < 0.3:
        scroll_amount = random.randint(100, 300)
        await page.evaluate(f"window.scrollBy(0, {scroll_amount})")
        await human_pause()
        await page.evaluate(f"window.scrollBy(0, -{scroll_amount})")

async def score_name_match(search_name, found_name, property_name=None):
    """
    Score how well a found name matches our search name
    Check both owner section and property section
    Higher score = better match
    """
    if not search_name or not found_name:
        return 0
    
    search_lower = search_name.lower().strip()
    found_lower = found_name.lower().strip()
    
    # Parse search name
    search_parts = search_lower.split()
    if len(search_parts) < 2:
        return 0
    
    search_first = search_parts[0]
    search_last = search_parts[1]
    
    # Check owner section (found_name)
    has_first_owner = search_first in found_lower
    has_last_owner = search_last in found_lower
    
    # Check property section if provided
    has_first_property = False
    has_last_property = False
    if property_name:
        property_lower = property_name.lower().strip()
        has_first_property = search_first in property_lower
        has_last_property = search_last in property_lower
    
    # Must have both first and last name in either section
    owner_match = has_first_owner and has_last_owner
    property_match = has_first_property and has_last_property
    
    if not (owner_match or property_match):
        return 0  # No match in either section
    
    # Base score for having both names present
    score = 30
    
    # Bonus for matches in both sections
    if owner_match and property_match:
        score += 50  # Found in both owner and property sections
    # Bonus for matches in both sections
    if owner_match and property_match:
        score += 50  # Found in both owner and property sections
    elif owner_match:
        score += 20  # Found in owner section
    elif property_match:
        score += 25  # Found in property section (slightly higher as it's more specific)
    
    # Parse found name more carefully for owner section
    if owner_match:
        if ',' in found_lower:
            # Handle "LAST, FIRST" format
            found_parts = found_lower.split(',')
            found_last_part = found_parts[0].strip()
            found_first_part = found_parts[1].strip() if len(found_parts) > 1 else ""
            
            # Check if our last name matches the main last name
            if search_last == found_last_part:
                score += 30  # Exact last name match
            elif search_last in found_last_part:
                score += 15  # Partial last name match
            
            # Check if our first name is in the first name section
            if search_first in found_first_part:
                # Check for exact first name match
                first_words = found_first_part.split()
                if search_first in first_words:
                    score += 30  # Exact first name match
                else:
                    score += 15  # Partial first name match
        else:
            # Handle "FIRST LAST" format
            found_words = found_lower.split()
            
            # Look for exact word matches
            if search_first in found_words:
                score += 25
            if search_last in found_words:
                score += 25
    
    # Additional scoring for property section matches
    if property_match and property_name:
        property_lower = property_name.lower().strip()
        if ',' in property_lower:
            # Handle "LAST, FIRST" format in property
            prop_parts = property_lower.split(',')
            prop_last_part = prop_parts[0].strip()
            prop_first_part = prop_parts[1].strip() if len(prop_parts) > 1 else ""
            
            if search_last == prop_last_part:
                score += 25  # Exact last name match in property
            if search_first in prop_first_part:
                prop_first_words = prop_first_part.split()
                if search_first in prop_first_words:
                    score += 25  # Exact first name match in property
        else:
            # Handle "FIRST LAST" format in property
            prop_words = property_lower.split()
            if search_first in prop_words:
                score += 20
            if search_last in prop_words:
                score += 20
    
    # Bonus for simpler names (fewer people involved)
    if found_lower.count(' & ') == 0 and found_lower.count(' and ') == 0:
        score += 20  # Single person
    elif found_lower.count(' & ') <= 1 or found_lower.count(' and ') <= 1:
        score += 10  # Two people max
    
    # Bonus for "LAST, FIRST" format (standard format)
    if ',' in found_lower:
        parts_after_comma = found_lower.split(',')[1].strip().split()
        if len(parts_after_comma) == 1:  # Just "LAST, FIRST"
            score += 40
        elif len(parts_after_comma) == 2:  # "LAST, FIRST M"
            score += 20
    
    # Penalty for very complex names
    complexity_indicators = ['h/e', 'etal', 'etals', 'trustee', 'estate']
    for indicator in complexity_indicators:
        if indicator in found_lower:
            score -= 15
    
    # Penalty for names that are much longer (likely multiple people)
    if len(found_lower) > len(search_lower) * 3:
        score -= 10
    
    return max(0, score)  # Don't return negative scores

def convert_to_search_format(name):
    """Convert 'FIRST LAST' or 'FIRST MIDDLE LAST' to 'LAST, FIRST' or 'LAST, FIRST MIDDLE' format"""
    if not name or not name.strip():
        return name
    
    name = name.strip()
    
    # If already in "LAST, FIRST" format, return as-is
    if ',' in name:
        return name
    
    # Split into words
    words = name.split()
    if len(words) < 2:
        return name  # Can't convert if less than 2 words
    
    # Take last word as last name, everything else as first/middle
    last_name = words[-1]
    first_middle = ' '.join(words[:-1])
    
    return f"{last_name}, {first_middle}"

def get_search_variations(name):
    """Get different search variations for a name"""
    if not name or not name.strip():
        return [name]
    
    name = name.strip()
    variations = []
    
    # Always try the original name first
    variations.append(name)
    
    # If it's "FIRST LAST" format, also try "LAST, FIRST"
    if ',' not in name:
        words = name.split()
        if len(words) >= 2:
            last_name = words[-1]
            first_middle = ' '.join(words[:-1])
            converted = f"{last_name}, {first_middle}"
            if converted != name:
                variations.append(converted)
    
    # If it's "LAST, FIRST" format, also try "FIRST LAST"
    elif ',' in name:
        parts = name.split(',')
        if len(parts) == 2:
            last_name = parts[0].strip()
            first_part = parts[1].strip()
            converted = f"{first_part} {last_name}"
            if converted != name:
                variations.append(converted)
    
    return variations

def is_invalid_address(address):
    """Check if address appears to be invalid (like 'School Board', headers, etc.)"""
    if not address or not address.strip():
        return True
    
    address_lower = address.lower().strip()
    
    # Check for obvious invalid addresses
    invalid_patterns = [
        'school board',
        'county',
        'municipal',
        'independent',
        'just value',
        'assessed',
        'homestead',
        'exemption',
        'taxable',
        'portability'
    ]
    
    for pattern in invalid_patterns:
        if pattern in address_lower:
            return True
    
    # Check if it looks like a real address (has common address indicators)
    address_indicators = [
        ' st ', ' ave ', ' dr ', ' ct ', ' way ', ' cir ', ' rd ', ' pl ', ' ter ', ' blvd ', ' ln ', ' lane ',
        ' street', ' avenue', ' drive', ' court', ' circle', ' road', ' place', ' terrace', ' boulevard', ' lane'
    ]
    
    has_address_indicator = any(indicator in address_lower for indicator in address_indicators)
    has_numbers = any(char.isdigit() for char in address)
    
    # If it doesn't have typical address components, it's probably invalid
    if not has_address_indicator or not has_numbers:
        return True
    
    return False

async def search_name_fast(page, name, attempt=1, fallback_name=None):
    """Fast search with smart human behavior and School Board fallback detection"""
    max_attempts = 2
    
    try:
        # Convert name to proper "LAST, FIRST" format for search
        search_name = convert_to_search_format(name)
        print(f"[SEARCH] Searching: {search_name} (converted from: {name}) (attempt {attempt})")
        
        # Navigate if needed or force refresh for clean state
        if 'Record-Search' not in page.url or attempt > 1:
            await page.goto('https://web.bcpa.net/BcpaClient/#/Record-Search', 
                           wait_until='domcontentloaded', timeout=int(os.environ.get('BROWARD_NAVIGATION_TIMEOUT', '60000')))
            await human_pause("page_load")
        
        # Add some human behavior
        await add_human_behavior(page)
        
        # Wait for input field with better detection and multiple strategies
        field_visible = False
        search_selector = None
        
        for strategy in range(4):
            try:
                if strategy == 0:
                    # First try: Navigate to Property Search and force interaction regardless of visibility
                    await page.goto('https://web.bcpa.net/BcpaClient/#/Record-Search', 
                                   wait_until='domcontentloaded', timeout=int(os.environ.get('BROWARD_NAVIGATION_TIMEOUT', '60000')))
                    await human_pause("page_load")
                    
                    # Click Property Search tab to make sure we're on the right tab
                    prop_search_tab = page.locator('tab[role="tab"]:has-text("Property Search")')
                    if await prop_search_tab.count() > 0:
                        await prop_search_tab.click()
                        await asyncio.sleep(1)
                    
                    # Look for the search input regardless of visibility (accessibility software might hide it)
                    search_input = page.locator('#txtField, input[placeholder*="Name"], input[placeholder*="Address"], input[placeholder*="Folio"]')
                    if await search_input.count() > 0:
                        # Force enable the field if it's disabled by accessibility software
                        try:
                            # Remove any hiding attributes that accessibility software might add
                            await page.evaluate("""
                                const inputs = document.querySelectorAll('#txtField, input[placeholder*="Name"], input[placeholder*="Address"], input[placeholder*="Folio"]');
                                inputs.forEach(input => {
                                    input.removeAttribute('data-uw-hidden-control');
                                    input.style.display = 'block';
                                    input.style.visibility = 'visible';
                                    input.disabled = false;
                                    input.readOnly = false;
                                });
                            """)
                            await asyncio.sleep(0.5)
                            
                            # Now try to interact with it
                            await search_input.first.click(force=True)
                            field_visible = True
                            search_selector = '#txtField, input[placeholder*="Name"], input[placeholder*="Address"], input[placeholder*="Folio"]'
                            print("  → Found and activated search field")
                            break
                        except Exception as e:
                            print(f"  → Field activation failed: {e}")
                elif strategy == 1:
                    # Second try: Force interaction with any input field in the Property Search area
                    search_tabpanel = page.locator('tabpanel:has-text("Property Search"), tabpanel[role="tabpanel"]')
                    if await search_tabpanel.count() > 0:
                        inputs_in_tab = search_tabpanel.locator('input, textbox')
                        if await inputs_in_tab.count() > 0:
                            try:
                                # Force enable any input in the tab
                                await page.evaluate("""
                                    const tabpanel = document.querySelector('div[role="tabpanel"]');
                                    if (tabpanel) {
                                        const inputs = tabpanel.querySelectorAll('input, textbox');
                                        inputs.forEach(input => {
                                            input.removeAttribute('data-uw-hidden-control');
                                            input.style.display = 'block';
                                            input.style.visibility = 'visible';
                                            input.disabled = false;
                                            input.readOnly = false;
                                        });
                                    }
                                """)
                                await asyncio.sleep(0.5)
                                
                                await inputs_in_tab.first.click(force=True)
                                field_visible = True
                                search_selector = 'tabpanel input, tabpanel textbox'
                                print("  → Activated input in Property Search tab")
                                break
                            except Exception as e:
                                print(f"  → Tab input activation failed: {e}")
                elif strategy == 2:
                    # Third try: Disable accessibility software and try again
                    print("  → Trying to disable accessibility interference...")
                    try:
                        # Try to disable UserWay accessibility widget
                        await page.evaluate("""
                            // Disable UserWay if present
                            if (window.UserWayWidgetApp) {
                                window.UserWayWidgetApp.disable();
                            }
                            
                            // Remove any accessibility hiding from all inputs
                            const allInputs = document.querySelectorAll('input, textbox');
                            allInputs.forEach(input => {
                                input.removeAttribute('data-uw-hidden-control');
                                input.removeAttribute('data-uw-rm-form');
                                input.style.display = 'block';
                                input.style.visibility = 'visible';
                                input.disabled = false;
                                input.readOnly = false;
                            });
                        """)
                        await asyncio.sleep(1)
                        
                        # Now try to find and use the input
                        all_inputs = page.locator('input, textbox')
                        if await all_inputs.count() > 0:
                            await all_inputs.first.click(force=True)
                            field_visible = True
                            search_selector = 'input, textbox'
                            print("  → Disabled accessibility interference")
                            break
                    except Exception as e:
                        print(f"  → Accessibility disable failed: {e}")
                elif strategy == 3:
                    # Fourth try: Complete reload with accessibility disabled from start
                    print("  → Complete page reload with accessibility disabled...")
                    await page.reload(wait_until='domcontentloaded', timeout=int(os.environ.get('BROWARD_NAVIGATION_TIMEOUT', '60000')))
                    
                    # Immediately disable accessibility features after page load
                    await page.evaluate("""
                        // Disable accessibility features immediately
                        if (window.UserWayWidgetApp) {
                            window.UserWayWidgetApp.disable();
                        }
                        
                        // Override any accessibility hiding
                        const style = document.createElement('style');
                        style.textContent = `
                            input[data-uw-hidden-control] {
                                display: block !important;
                                visibility: visible !important;
                            }
                        `;
                        document.head.appendChild(style);
                    """)
                    await asyncio.sleep(2)
                    
                    # Ensure we're on Property Search tab
                    prop_search_tab = page.locator('tab:has-text("Property Search")')
                    if await prop_search_tab.count() > 0:
                        await prop_search_tab.click()
                        await asyncio.sleep(1)
                    
                    # Force all inputs to be interactive
                    await page.evaluate("""
                        const allInputs = document.querySelectorAll('input, textbox');
                        allInputs.forEach(input => {
                            input.removeAttribute('data-uw-hidden-control');
                            input.removeAttribute('data-uw-rm-form');
                            input.style.display = 'block';
                            input.style.visibility = 'visible';
                            input.disabled = false;
                            input.readOnly = false;
                        });
                    """)
                    
                    # Try to use any input
                    try:
                        all_inputs = page.locator('input, textbox')
                        if await all_inputs.count() > 0:
                            await all_inputs.first.click(force=True)
                            field_visible = True
                            search_selector = 'input, textbox'
                            print("  → Force-enabled inputs after reload")
                            break
                    except Exception as e:
                        print(f"  → Final attempt failed: {e}")
            except Exception as e:
                print(f"    Strategy {strategy} failed: {e}")
                continue
        
        if not field_visible:
            raise Exception("Could not locate search input field after multiple attempts")
        
        # Smart typing - use the converted search name format
        try:
            # First try to clear the field and then fill it
            await page.locator(search_selector).first.fill('')
            await human_pause("typing")
            await page.locator(search_selector).first.fill(search_name)
            await human_pause("typing")
            
            # Alternative: use smart typing character by character if fill doesn't work well
            # await smart_typing(page, search_selector, search_name)
        except Exception as e:
            print(f"  → Typing method failed, trying alternative: {e}")
            try:
                # Fallback to character-by-character typing
                await smart_typing(page, search_selector, search_name)
            except Exception as e2:
                print(f"  → Alternative typing also failed: {e2}")
                raise e2
        
        # Try multiple ways to trigger the search
        search_triggered = False
        
        # Method 1: Press Enter
        try:
            await page.press(search_selector, 'Enter')
            await page.wait_for_timeout(2000)  # Wait for search to start
            search_triggered = True
            print(f"  → Search triggered with Enter key")
        except Exception as e:
            print(f"  → Enter key failed: {e}")
        
        # Method 2: Look for and click search button if Enter didn't work
        if not search_triggered:
            try:
                search_button_selectors = [
                    'button:has-text("Search")',
                    'button[type="submit"]',
                    'input[type="submit"]',
                    'button:has-text("Go")',
                    '.search-button',
                    '[role="button"]:has-text("Search")'
                ]
                
                for btn_selector in search_button_selectors:
                    search_btn = page.locator(btn_selector)
                    if await search_btn.count() > 0:
                        await search_btn.first.click()
                        await page.wait_for_timeout(2000)
                        search_triggered = True
                        print(f"  → Search triggered with button: {btn_selector}")
                        break
            except Exception as e:
                print(f"  → Button search failed: {e}")
        
        # Method 3: Try submitting the form
        if not search_triggered:
            try:
                # Look for a form containing the search field and submit it
                form = page.locator('form').filter(lambda form: form.locator(search_selector).count() > 0)
                if await form.count() > 0:
                    await form.first.evaluate('form => form.submit()')
                    await page.wait_for_timeout(2000)
                    search_triggered = True
                    print(f"  → Search triggered by form submission")
            except Exception as e:
                print(f"  → Form submission failed: {e}")
        
        if not search_triggered:
            print(f"  → WARNING: Could not trigger search, but continuing to check for results")
        
        await human_pause("reading")
        
        # Quick check for results - wait for search results to load
        try:
            # Wait for search results to appear and check what tab we're on
            await page.wait_for_timeout(5000)  # Give more time for search to complete
            
            # FIRST: Check if we're on the Parcel Result tab (direct match found)
            try:
                # Use the correct selector that we found in debug
                parcel_result_tab = page.locator('[role="tab"]:has-text("Parcel Result")')
                
                if await parcel_result_tab.count() > 0:
                    # Check if Parcel Result tab is selected (direct match)
                    is_parcel_selected = await parcel_result_tab.get_attribute('aria-selected')
                    
                    if is_parcel_selected == 'true':
                        print("  → Direct match found, processing Parcel Result")
                        
                        # Extract property information from Parcel Result tab
                        try:
                            # Look for Property Owner(s) using multiple approaches
                            owner_text = None
                            address_text = None
                            
                            # Method 1: Look for table rows with "Property Owner(s):" text
                            owner_row = page.locator('row:has-text("Property Owner(s):"), tr:has-text("Property Owner(s):")')
                            
                            if await owner_row.count() > 0:
                                # Get the cell containing the owner names
                                owner_cells = owner_row.locator('cell, td')
                                
                                if await owner_cells.count() > 1:
                                    owner_cell = owner_cells.nth(1)  # Second cell contains the names
                                    owner_text = await owner_cell.text_content()
                                    if owner_text:
                                        owner_text = owner_text.strip()
                                        print(f"  → Found owner(s): {owner_text}")
                            
                            # Method 2: Look for specific text patterns if Method 1 failed
                            if not owner_text:
                                owner_elements = page.locator('text="Property Owner(s):"')
                                if await owner_elements.count() > 0:
                                    # Try to get the next element or parent element containing the owner names
                                    parent = owner_elements.locator('..').locator('cell, td').nth(1)
                                    if await parent.count() > 0:
                                        owner_text = await parent.text_content()
                                        if owner_text:
                                            owner_text = owner_text.strip()
                                            print(f"  → Found owner(s): {owner_text}")
                            
                            # Get Physical Address using the same approaches
                            address_row = page.locator('row:has-text("Physical Address:"), tr:has-text("Physical Address:")')
                            
                            if await address_row.count() > 0:
                                address_cells = address_row.locator('cell, td')
                                
                                if await address_cells.count() > 1:
                                    address_cell = address_cells.nth(1)  # Second cell
                                    address_text = await address_cell.text_content()
                                    if address_text:
                                        address_text = address_text.strip()
                                        print(f"  → Found address: {address_text}")
                            
                            # Method 2 for address if Method 1 failed
                            if not address_text:
                                address_elements = page.locator('text="Physical Address:"')
                                if await address_elements.count() > 0:
                                    parent = address_elements.locator('..').locator('cell, td').nth(1)
                                    if await parent.count() > 0:
                                        address_text = await parent.text_content()
                                        if address_text:
                                            address_text = address_text.strip()
                                            print(f"  → Found address: {address_text}")
                            
                            # If we have both owner and address, process the match
                            if owner_text and address_text:
                                # Score the match based on owner information
                                score = await score_name_match(name, owner_text, None)
                                print(f"  → Match score: {score}")
                                
                                if score > 30:  # Reasonable threshold for direct matches
                                    # Check for invalid addresses
                                    if is_invalid_address(address_text):
                                        print(f"  ⚠️ Detected invalid address: '{address_text}'")
                                        if fallback_name and pd.notna(fallback_name):
                                            print(f"  🔄 Retrying with fallback name: {fallback_name}")
                                            return await search_name_fast(page, fallback_name, attempt=1, fallback_name=None)
                                        else:
                                            print(f"  ❌ No fallback name available")
                                            return None
                                    
                                    print(f"  [✓] Direct match accepted: {owner_text}")
                                    print(f"  [✓] Address: {address_text}")
                                    return address_text
                                else:
                                    print(f"  [X] Direct match score too low: {score}")
                            else:
                                print(f"  [X] Could not extract owner or address from Parcel Result")
                        except Exception as e:
                            print(f"  [X] Error processing direct match: {e}")
                            
                        # If we got here, the direct match didn't work out, continue to search results
                        # Go back to Property Search to try again or look for Search Results tab
                        property_search_tab = page.locator('[role="tab"]:has-text("Property Search")')
                        if await property_search_tab.count() > 0:
                            await property_search_tab.click()
                            await page.wait_for_timeout(1000)
            except Exception as e:
                print(f"  → Error checking Parcel Result: {e}")
            
            # SECOND: Check if we're on the Search Results tab or need to switch to it
            try:
                # Wait a bit more for search results to fully load
                await page.wait_for_timeout(2000)
                
                # Look for Search Results tab and click it if it exists
                search_results_tab = page.locator('tab[role="tab"]:has-text("Search Results")')
                if await search_results_tab.count() > 0:
                    # Check if Search Results tab is already selected
                    is_selected = await search_results_tab.get_attribute('aria-selected')
                    if is_selected != 'true':
                        await search_results_tab.click()
                        await page.wait_for_timeout(1500)
                        print("  → Clicked Search Results tab")
                    else:
                        print("  → Search Results tab already selected")
                else:
                    print("  → No Search Results tab found, checking current content")
            except Exception as e:
                print(f"  → Error with Search Results tab: {e}")
                # Continue anyway, might already be on the right tab
            
            # Simplified approach: Look for any table with search results pattern immediately
            # Skip complex record count detection and go straight to table analysis
            print("  → Looking for search results in any table structure...")
            
            # Wait longer for search results to load completely
            await page.wait_for_timeout(3000)
            
            # Debug: Let's see what tabs are available
            try:
                all_tabs = page.locator('tab[role="tab"]')
                tab_count = await all_tabs.count()
                print(f"  → Found {tab_count} tabs available")
                
                for i in range(tab_count):
                    tab = all_tabs.nth(i)
                    tab_text = await tab.text_content()
                    is_selected = await tab.get_attribute('aria-selected')
                    print(f"    Tab {i}: '{tab_text}' (selected: {is_selected})")
            except Exception as e:
                print(f"  → Could not debug tabs: {e}")
            
            # Universal table detection and processing approach
            # This handles all possible table structures and finds the best match
            print("  → Analyzing all available tables for search results...")
            
            all_tables = page.locator('table')
            table_count = await all_tables.count()
            print(f"  → Found {table_count} tables to analyze")
            
            all_matches = []  # Store all matches from all tables
            
            for table_idx in range(table_count):
                try:
                    table = all_tables.nth(table_idx)
                    table_ref = await table.get_attribute('ref')
                    
                    print(f"  → Analyzing table {table_idx} (ref: {table_ref})")
                    
                    # Try multiple approaches to find data rows in this table
                    data_rows = None
                    row_count = 0
                    
                    # Approach 1: Modern structure with rowgroups (like e287 table)
                    try:
                        rowgroups = table.locator('rowgroup')
                        rowgroup_count = await rowgroups.count()
                        
                        if rowgroup_count >= 2:
                            # Check if first rowgroup contains headers
                            header_rowgroup = rowgroups.nth(0)
                            header_text = await header_rowgroup.text_content()
                            
                            if header_text and any(header in header_text for header in ['Folio Number', 'Owner Name', 'Site Address']):
                                # This looks like search results - get data from second rowgroup
                                data_rowgroup = rowgroups.nth(1)
                                data_rows = data_rowgroup.locator('row')
                                row_count = await data_rows.count()
                                print(f"    → Found {row_count} data rows using rowgroup structure")
                    except Exception as e:
                        pass  # Try next approach
                    
                    # Approach 2: Traditional table structure with tbody
                    if not data_rows or row_count == 0:
                        try:
                            tbody_rows = table.locator('tbody tr')
                            tbody_count = await tbody_rows.count()
                            
                            if tbody_count > 0:
                                # Check if this looks like search results by examining cell content
                                first_row = tbody_rows.nth(0)
                                cells = first_row.locator('td, cell')
                                cell_count = await cells.count()
                                
                                if cell_count >= 3:
                                    # Check if third cell looks like an address
                                    third_cell_text = await cells.nth(2).text_content()
                                    if third_cell_text and any(indicator in third_cell_text.upper() for indicator in ['ST ', 'AVE ', 'DR ', 'CT ', 'WAY ', 'CIR ', 'RD ', 'PL ', 'TER ', 'BLVD ', 'LANE']):
                                        data_rows = tbody_rows
                                        row_count = tbody_count
                                        print(f"    → Found {row_count} data rows using tbody structure")
                        except Exception as e:
                            pass  # Try next approach
                    
                    # Approach 3: Direct tr elements in table
                    if not data_rows or row_count == 0:
                        try:
                            tr_rows = table.locator('tr')
                            tr_count = await tr_rows.count()
                            
                            if tr_count > 1:  # Skip header row
                                # Check if this looks like search results
                                test_row = tr_rows.nth(1) if tr_count > 1 else tr_rows.nth(0)
                                cells = test_row.locator('td, cell')
                                cell_count = await cells.count()
                                
                                if cell_count >= 3:
                                    third_cell_text = await cells.nth(2).text_content()
                                    if third_cell_text and any(indicator in third_cell_text.upper() for indicator in ['ST ', 'AVE ', 'DR ', 'CT ', 'WAY ', 'CIR ', 'RD ', 'PL ', 'TER ', 'BLVD ', 'LANE']):
                                        data_rows = tr_rows
                                        row_count = tr_count
                                        print(f"    → Found {row_count} data rows using direct tr structure")
                        except Exception as e:
                            pass
                    
                    # Process data rows if we found any
                    if data_rows and row_count > 0:
                        print(f"    → Processing {row_count} rows from table {table_idx}")
                        
                        # Process each row to extract matches
                        for row_idx in range(min(50, row_count)):  # Process up to 50 rows per table
                            try:
                                row = data_rows.nth(row_idx)
                                cells = row.locator('td, cell')  # Support both cell types
                                cell_count = await cells.count()
                                
                                if cell_count >= 3:
                                    # Extract data - assume standard order: Folio, Owner, Address
                                    folio_number = await cells.nth(0).text_content()
                                    owner_name = await cells.nth(1).text_content()
                                    address = await cells.nth(2).text_content()
                                    
                                    if owner_name and address:
                                        owner_name = owner_name.strip()
                                        address = address.strip()
                                        folio_number = folio_number.strip() if folio_number else ""
                                        
                                        # Skip header rows or empty data
                                        if owner_name.lower() in ['owner name', 'owner', 'name'] or address.lower() in ['site address', 'address', 'location']:
                                            continue
                                        
                                        # Score this match based on how well it matches our search
                                        score = await score_name_match(name, owner_name, None)
                                        
                                        if score > 0:  # Only keep matches with some score
                                            match_data = {
                                                'address': address,
                                                'owner': owner_name,
                                                'folio': folio_number,
                                                'table': table_idx,
                                                'row': row_idx,
                                                'score': score
                                            }
                                            all_matches.append(match_data)
                                            print(f"      Row {row_idx}: {owner_name} -> {address} (score: {score})")
                                
                                elif cell_count == 1:
                                    # Sometimes all data is in one cell - try to parse it
                                    cell_text = await cells.nth(0).text_content()
                                    if cell_text and len(cell_text) > 50:  # Likely contains multiple fields
                                        # Try to extract owner and address patterns
                                        text = cell_text.strip()
                                        
                                        # Look for address patterns (numbers + street indicators)
                                        import re
                                        address_pattern = r'\d+[^,]*(?:ST|AVE|DR|CT|WAY|CIR|RD|PL|TER|BLVD|LANE|STREET|AVENUE|DRIVE|COURT|CIRCLE|ROAD|PLACE|TERRACE|BOULEVARD)[^,]*'
                                        addresses = re.findall(address_pattern, text, re.IGNORECASE)
                                        
                                        # Look for owner name patterns (LAST, FIRST format)
                                        name_pattern = r'[A-Z]+,\s*[A-Z][A-Z\s]*'
                                        names = re.findall(name_pattern, text)
                                        
                                        if addresses and names:
                                            for addr in addresses[:3]:  # Limit to first 3 addresses
                                                for owner in names[:3]:  # Limit to first 3 names
                                                    score = await score_name_match(name, owner, None)
                                                    if score > 0:
                                                        match_data = {
                                                            'address': addr.strip(),
                                                            'owner': owner.strip(),
                                                            'folio': 'extracted',
                                                            'table': table_idx,
                                                            'row': row_idx,
                                                            'score': score
                                                        }
                                                        all_matches.append(match_data)
                                                        print(f"      Extracted: {owner.strip()} -> {addr.strip()} (score: {score})")
                            
                            except Exception as e:
                                continue  # Skip problematic rows
                    
                    else:
                        print(f"    → No valid data rows found in table {table_idx}")
                
                except Exception as e:
                    print(f"    → Error processing table {table_idx}: {e}")
                    continue
            
            # Find the best match across all tables
            if all_matches:
                # Sort by score (highest first)
                all_matches.sort(key=lambda x: x['score'], reverse=True)
                
                print(f"  [✓] Found {len(all_matches)} total matches across all tables")
                print(f"  [✓] Top 5 matches:")
                for i, match in enumerate(all_matches[:5]):
                    print(f"    {i+1}. {match['owner']} -> {match['address']} (score: {match['score']}, table: {match['table']})")
                
                best_match = all_matches[0]
                
                # Check for invalid addresses
                if is_invalid_address(best_match['address']):
                    print(f"  ⚠️ Best match has invalid address: '{best_match['address']}'")
                    
                    # Try next best match with valid address
                    for match in all_matches[1:]:
                        if not is_invalid_address(match['address']):
                            best_match = match
                            print(f"  → Using next best match: {match['owner']} -> {match['address']} (score: {match['score']})")
                            break
                    else:
                        # No valid addresses found in any match
                        print(f"  ❌ All matches have invalid addresses")
                        if fallback_name and pd.notna(fallback_name):
                            print(f"  🔄 Retrying with fallback name: {fallback_name}")
                            return await search_name_fast(page, fallback_name, attempt=1, fallback_name=None)
                        else:
                            print(f"  ❌ No fallback name available")
                            return None
                
                print(f"  [✓] Selected best match: {best_match['owner']}")
                print(f"  [✓] Address: {best_match['address']}")
                print(f"  [✓] Score: {best_match['score']} (from table {best_match['table']}, row {best_match['row']})")
                
                return best_match['address']
            
            else:
                print(f"  [X] No valid matches found in any table")
                return None
            
            # Fallback: Check for search results in any table structure
            print("  → Primary search results not found, trying fallback methods...")
            
            # Fallback 1: Look for any table with search results pattern
            all_tables = page.locator('table')
            table_count = await all_tables.count()
            
            for table_idx in range(table_count):
                try:
                    table = all_tables.nth(table_idx)
                    rows = table.locator('tbody tr')
                    row_count = await rows.count()
                    
                    if row_count > 0:
                        print(f"  → Checking table {table_idx} with {row_count} rows...")
                        
                        # Check if this looks like a search results table
                        # Look for the first few rows to see if they have the right structure
                        for row_idx in range(min(5, row_count)):
                            try:
                                row = rows.nth(row_idx)
                                cells = row.locator('td')
                                cell_count = await cells.count()
                                
                                if cell_count >= 3:
                                    # Check if this looks like search results (has address-like content)
                                    address_cell = cells.nth(2)
                                    address_text = await address_cell.text_content()
                                    
                                    if address_text and any(indicator in address_text.upper() for indicator in ['ST ', 'AVE ', 'DR ', 'CT ', 'WAY ', 'CIR ', 'RD ', 'PL ', 'TER ', 'BLVD ']):
                                        print(f"    → Found address-like content in table {table_idx}, row {row_idx}: {address_text.strip()}")
                                        
                                        # This looks like a search results table, process all rows
                                        best_match = None
                                        best_score = -1
                                        
                                        for result_row_idx in range(min(20, row_count)):
                                            try:
                                                result_row = rows.nth(result_row_idx)
                                                result_cells = result_row.locator('td')
                                                result_cell_count = await result_cells.count()
                                                
                                                if result_cell_count >= 3:
                                                    folio_cell = result_cells.nth(0)
                                                    owner_cell = result_cells.nth(1)
                                                    addr_cell = result_cells.nth(2)
                                                    
                                                    # Try to get property name if there's a 4th column
                                                    property_text = None
                                                    if result_cell_count >= 4:
                                                        property_cell = result_cells.nth(3)
                                                        property_text = await property_cell.text_content()
                                                        if property_text:
                                                            property_text = property_text.strip()
                                                    
                                                    folio_text = await folio_cell.text_content()
                                                    owner_text = await owner_cell.text_content()
                                                    addr_text = await addr_cell.text_content()
                                                    
                                                    if owner_text and addr_text and addr_text.strip():
                                                        owner_text = owner_text.strip()
                                                        addr_text = addr_text.strip()
                                                        folio_text = folio_text.strip() if folio_text else ""
                                                        
                                                        # Score this match - check both owner and property sections
                                                        score = await score_name_match(name, owner_text, property_text)
                                                        property_info = f" (property: {property_text})" if property_text else ""
                                                        print(f"      Fallback Row {result_row_idx}: {owner_text} -> {addr_text} (score: {score}){property_info}")
                                                        
                                                        if score > best_score:
                                                            best_score = score
                                                            best_match = {
                                                                'address': addr_text,
                                                                'owner': owner_text,
                                                                'property': property_text,
                                                                'folio': folio_text,
                                                                'row': result_row_idx,
                                                                'score': score
                                                            }
                                            except Exception as e:
                                                continue
                                        
                                        if best_match:
                                            print(f"  ✓ Fallback best match (table {table_idx}, row {best_match['row']}, score {best_match['score']}): {best_match['owner']}")
                                            print(f"  ✓ Address: {best_match['address']}")
                                            
                                            # Check for School Board or other invalid addresses
                                            if is_invalid_address(best_match['address']):
                                                print(f"  ⚠️ Detected invalid address: '{best_match['address']}'")
                                                if fallback_name and pd.notna(fallback_name):
                                                    print(f"  🔄 Retrying with fallback name: {fallback_name}")
                                                    return await search_name_fast(page, fallback_name, attempt=1, fallback_name=None)
                                                else:
                                                    print(f"  ❌ No fallback name available")
                                                    return None
                                            
                                            return best_match['address']
                                        
                                        # Found search results table but no good matches
                                        break
                            except Exception as e:
                                continue
                except Exception as e:
                    continue
            
            print(f"  ❌ No valid search results found")
            return None
            
        except Exception as e:
            print(f"  ❌ Error reading results: {e}")
            return None
            
    except Exception as e:
        print(f"  ❌ Search error: {e}")
        
        # Retry logic with page reload only when necessary
        if attempt < max_attempts:
            print(f"  🔄 Quick retry...")
            await human_pause("between_searches")
            
            # Do page reload to reset state - this ensures we get search functionality back
            try:
                await page.reload(wait_until='domcontentloaded', timeout=int(os.environ.get('BROWARD_NAVIGATION_TIMEOUT', '60000')))
                await human_pause("page_load")
            except:
                pass
                
            return await search_name_fast(page, name, attempt + 1)
        else:
            print(f"  ❌ Failed after {max_attempts} attempts")
            return None

async def process_addresses_fast(csv_path, max_names: Optional[int] = 15, headless=True):
    """Fast processing with smart behavior"""
    print("Fast Address Extractor - AI Human Behavior")
    print("=" * 60)
    
    def extract_short_name(full_name):
        """Extract first and last name only (remove middle names)"""
        if not full_name:
            return None
        
        # Handle "LAST, FIRST MIDDLE" format
        if ',' in full_name:
            parts = full_name.split(',')
            last_name = parts[0].strip()
            first_part = parts[1].strip() if len(parts) > 1 else ""
            
            if first_part:
                # Get just the first word (first name) from the first part
                first_name = first_part.split()[0]
                return f"{first_name} {last_name}"
        else:
            # Handle "FIRST MIDDLE LAST" format
            words = full_name.strip().split()
            if len(words) >= 2:
                # Take first and last words
                return f"{words[0]} {words[-1]}"
        
        return full_name  # Return as-is if can't parse
    
    # Load CSV
    try:
        df = pd.read_csv(csv_path)
        print(f"[✓] Loaded {len(df)} rows")
    except Exception as e:
        print(f"[X] Error loading CSV: {e}")
        return
    
    # Get unique person names from IndirectName_FullCleaned (priority) and DirectName_FullCleaned
    # Also build a mapping of potential fallback names
    person_names = []
    name_fallbacks = {}  # Maps primary name to fallback name(s)
    
    for _, row in df.iterrows():
        primary_name = None
        fallback_name = None
        full_name = None
        
        # Priority: IndirectName_FullCleaned for person names
        if row.get('IndirectName_Type') == 'Person' and row.get('IndirectName_FullCleaned'):
            primary_name = row['IndirectName_FullCleaned']  # This includes middle names
            
            # Use the original IndirectName as fallback if it's different and has more detail
            if row.get('IndirectName') and row['IndirectName'] != primary_name:
                # Clean up the original indirect name for use as fallback
                original_indirect = row['IndirectName'].replace('"', '').strip()
                if original_indirect != primary_name:
                    fallback_name = original_indirect
            
            # Also check if DirectName_FullCleaned has a different form
            if row.get('DirectName_Type') == 'Person' and row.get('DirectName_FullCleaned'):
                if row['DirectName_FullCleaned'] != primary_name:
                    # If we don't have a fallback yet, use DirectName_FullCleaned
                    if not fallback_name:
                        fallback_name = row['DirectName_FullCleaned']
        
        # Fallback: DirectName_FullCleaned for person names
        elif row.get('DirectName_Type') == 'Person' and row.get('DirectName_FullCleaned'):
            primary_name = row['DirectName_FullCleaned']  # This includes middle names
            
            # Use the original DirectName as fallback if it's different and has more detail
            if row.get('DirectName') and pd.notna(row['DirectName']) and row['DirectName'] != primary_name:
                original_direct = str(row['DirectName']).replace('"', '').strip()
                if original_direct != primary_name:
                    fallback_name = original_direct
            
            # Check if IndirectName_FullCleaned exists as potential additional fallback
            if row.get('IndirectName_FullCleaned'):
                if not fallback_name:
                    fallback_name = row['IndirectName_FullCleaned']
        
        if primary_name:
            person_names.append(primary_name)
            if fallback_name and pd.notna(fallback_name):
                name_fallbacks[primary_name] = fallback_name
                print(f"  [DEBUG] {primary_name} -> fallback: {fallback_name}")
    
    unique_names = sorted(list(set(person_names)))
    print(f"[✓] Found {len(unique_names)} unique person names")
    print(f"[✓] Found {len(name_fallbacks)} names with fallback alternatives")
    
    if max_names and len(unique_names) > max_names:
        unique_names = unique_names[:max_names]
        print(f"[✓] Processing first {len(unique_names)} names")
    elif max_names is None:
        print(f"[✓] Processing ALL {len(unique_names)} names")
    
    if not unique_names:
        print("[X] No person names found to process")
        return None
    
    # Process names
    address_map = {}
    
    async with async_playwright() as p:
        browser, context = await create_smart_browser(p, headless=headless)
        page = await context.new_page()
        
        try:
            success_count = 0
            start_time = time.time()
            
            for i, name in enumerate(unique_names, 1):
                print(f"\n[{i}/{len(unique_names)}] {name}")
                
                # Get fallback name if available
                fallback_name = name_fallbacks.get(name)
                if fallback_name and pd.notna(fallback_name):
                    print(f"  [INFO] Fallback name available: {fallback_name}")
                
                address = await search_name_fast(page, name, fallback_name=fallback_name)
                
                if address:
                    address_map[name] = address
                    success_count += 1
                    
                    # Show progress
                    elapsed = time.time() - start_time
                    rate = i / elapsed * 60 if elapsed > 0 else 0
                    print(f"  [SUCCESS] ({success_count}/{i}) - {rate:.1f} searches/min")
                else:
                    print(f"  [FAILED]")
                
                # Smart pause between searches
                if i < len(unique_names):
                    pause_time = await human_pause("between_searches")
                    print(f"  [PAUSE] {pause_time:.1f}s...")
        
        finally:
            try:
                await browser.close()
            except:
                pass
    
    # Add addresses to CSV
    print(f"\n[PROCESSING] Adding {len(address_map)} addresses to CSV...")
    
    # Add new columns if they don't exist
    if 'DirectName_Address' not in df.columns:
        df['DirectName_Address'] = ''
    if 'IndirectName_Address' not in df.columns:
        df['IndirectName_Address'] = ''
    
    # Fill in addresses based on full cleaned names and their original forms
    for index, row in df.iterrows():
        address_found = None
        
        # Try to match using IndirectName_FullCleaned (priority)
        if row.get('IndirectName_FullCleaned'):
            indirect_full_cleaned = row['IndirectName_FullCleaned']
            
            # Check if we found address for the full cleaned name
            if indirect_full_cleaned in address_map:
                address_found = address_map[indirect_full_cleaned]
                df.at[index, 'IndirectName_Address'] = address_found
            # Also check original IndirectName in case it was used as fallback
            elif row.get('IndirectName'):
                original_indirect = row['IndirectName'].replace('"', '').strip()
                if original_indirect in address_map:
                    address_found = address_map[original_indirect]
                    df.at[index, 'IndirectName_Address'] = address_found
        
        # Try DirectName_FullCleaned if we haven't found an address yet
        if not address_found and row.get('DirectName_FullCleaned'):
            direct_full_cleaned = row['DirectName_FullCleaned']
            
            # Check if we found address for the full cleaned name
            if direct_full_cleaned in address_map:
                address_found = address_map[direct_full_cleaned]
                df.at[index, 'DirectName_Address'] = address_found
            # Also check original DirectName in case it was used as fallback
            elif row.get('DirectName') and pd.notna(row['DirectName']):
                original_direct = str(row['DirectName']).replace('"', '').strip()
                if original_direct in address_map:
                    address_found = address_map[original_direct]
                    df.at[index, 'DirectName_Address'] = address_found
    
    # Save updated CSV
    base_name = os.path.splitext(csv_path)[0]
    output_path = f"{base_name}_with_addresses_fast.csv"
    
    df.to_csv(output_path, index=False)
    
    # Final stats
    total_time = time.time() - start_time
    avg_time = total_time / len(unique_names) if unique_names else 0
    
    print(f"\n[COMPLETE]")
    print(f"[✓] Found addresses for {len(address_map)}/{len(unique_names)} people")
    print(f"[✓] Total time: {total_time/60:.1f} minutes")
    print(f"[✓] Average: {avg_time:.1f}s per search")
    print(f"[✓] Success rate: {len(address_map)/len(unique_names)*100:.1f}%")
    print(f"[✓] Updated CSV: {output_path}")
    
    return output_path

def find_latest_processed_file():
    """Find the most recent processed CSV file"""
    downloads_dir = r"c:\Users\my notebook\Desktop\BlakeJackson\downloads"
    
    if not os.path.exists(downloads_dir):
        return None
    
    processed_files = []
    for file in os.listdir(downloads_dir):
        if file.endswith('_processed.csv'):
            file_path = os.path.join(downloads_dir, file)
            processed_files.append((file_path, os.path.getmtime(file_path)))
    
    if not processed_files:
        return None
    
    # Return the most recent file
    latest_file = max(processed_files, key=lambda x: x[1])
    return latest_file[0]

async def main():
    parser = argparse.ArgumentParser(description='Extract addresses for person names from processed LIS PENDENS data')
    parser.add_argument('--csv', help='Path to processed CSV file')
    parser.add_argument('--max-names', type=int, default=15, help='Maximum number of names to process (default: 15)')
    parser.add_argument('--show-browser', action='store_true', help='Show browser (runs in visible mode)')
    
    args = parser.parse_args()
    
    # Determine CSV file to use
    csv_file = args.csv
    if not csv_file:
        csv_file = find_latest_processed_file()
        if csv_file:
            print(f"[✓] Auto-detected latest processed file: {os.path.basename(csv_file)}")
        else:
            print("[X] No processed CSV file found. Please specify --csv path or ensure processed files exist in downloads/")
            return 1
    
    if not os.path.exists(csv_file):
        print(f"[X] CSV file not found: {csv_file}")
        return 1
    
    # Determine headless mode - default to headless unless --show-browser is specified
    headless = not args.show_browser
    
    try:
        output_file = await process_addresses_fast(csv_file, args.max_names, headless)
        if output_file:
            print(f"\n[SUCCESS] Address extraction complete!")
            print(f"[✓] Output file: {output_file}")
            return 0
        else:
            print(f"\n[FAILED] Address extraction failed")
            return 1
    except Exception as e:
        print(f"\n[ERROR] {e}")
        return 1

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
