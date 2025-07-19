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
import sys
from playwright.async_api import async_playwright
import os

# Realistic user agents to rotate
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/121.0.0.0 Safari/537.36'
]

async def create_smart_browser(playwright, headless=True):
    """Create browser with intelligent stealth settings"""
    user_agent = random.choice(USER_AGENTS)
    
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

async def score_name_match(search_name, found_name):
    """
    Score how well a found name matches our search name
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
    
    # Look for our search name within the found name (handles compound names)
    # Check for exact first and last name presence
    has_first = search_first in found_lower
    has_last = search_last in found_lower
    
    if not (has_first and has_last):
        return 0  # No match
    
    # Base score for having both names present
    score = 30
    
    # Parse found name more carefully
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

async def search_name_fast(page, name, attempt=1):
    """Fast search with smart human behavior"""
    max_attempts = 2
    
    try:
        print(f"[SEARCH] Searching: {name} (attempt {attempt})")
        
        # Navigate if needed or force refresh for clean state
        if 'Record-Search' not in page.url or attempt > 1:
            await page.goto('https://web.bcpa.net/BcpaClient/#/Record-Search', 
                           wait_until='domcontentloaded', timeout=20000)
            await human_pause("page_load")
        
        # Add some human behavior
        await add_human_behavior(page)
        
        # Wait for input field with better detection and multiple strategies
        field_visible = False
        for strategy in range(4):
            try:
                if strategy == 0:
                    # First try: wait for the main search textbox
                    search_input = page.locator('input[placeholder*="Name"], input[name*="search"], textbox[name*="Name"], #txtField')
                    await search_input.first.wait_for(state='visible', timeout=8000)
                    field_visible = True
                    search_selector = 'input[placeholder*="Name"], input[name*="search"], textbox[name*="Name"], #txtField'
                    break
                elif strategy == 1:
                    # Second try: look for any input field that might be the search box
                    await page.wait_for_selector('input[type="text"], input:not([type]), textbox', state='visible', timeout=6000)
                    # Try to find the most likely search input
                    inputs = page.locator('input[type="text"], input:not([type]), textbox')
                    input_count = await inputs.count()
                    for i in range(input_count):
                        input_elem = inputs.nth(i)
                        placeholder = await input_elem.get_attribute('placeholder')
                        if placeholder and ('name' in placeholder.lower() or 'address' in placeholder.lower() or 'folio' in placeholder.lower()):
                            field_visible = True
                            search_selector = f'input[type="text"], input:not([type]), textbox'
                            break
                    if field_visible:
                        break
                elif strategy == 2:
                    # Third try: click on search area and try to activate field
                    print("  → Trying to activate search field...")
                    try:
                        await page.click('body')
                        await asyncio.sleep(0.5)
                        # Look for Property Search tab and click it
                        prop_search_tab = page.locator('tab[role="tab"]:has-text("Property Search"), button:has-text("Property Search")')
                        if await prop_search_tab.count() > 0:
                            await prop_search_tab.first.click()
                            await asyncio.sleep(1)
                        
                        await page.wait_for_selector('input[type="text"], textbox', state='visible', timeout=5000)
                        field_visible = True
                        search_selector = 'input[type="text"], textbox'
                        break
                    except:
                        pass
                elif strategy == 3:
                    # Fourth try: reload page and try again
                    print("  → Reloading page to reset search state...")
                    await page.reload(wait_until='domcontentloaded', timeout=15000)
                    await asyncio.sleep(2)
                    await page.wait_for_selector('input[type="text"], textbox', state='visible', timeout=8000)
                    field_visible = True
                    search_selector = 'input[type="text"], textbox'
                    break
            except Exception as e:
                print(f"    Strategy {strategy} failed: {e}")
                continue
        
        if not field_visible:
            raise Exception("Could not locate search input field after multiple attempts")
        
        # Smart typing
        await smart_typing(page, search_selector, name)
        await human_pause("typing")
        
        # Press Enter with slight delay (like a human)
        await page.press(search_selector, 'Enter')
        await human_pause("reading")
        
        # Quick check for results - wait for search results to load
        try:
            # Wait for search results to appear and check the Search Results tab
            await page.wait_for_timeout(2000)  # Give time for search to complete
            
            # Check if we're on the Search Results tab or need to switch to it
            try:
                search_results_tab = page.locator('tab[role="tab"]:has-text("Search Results")')
                if await search_results_tab.count() > 0:
                    # Check if Search Results tab exists and click it
                    await search_results_tab.click()
                    await page.wait_for_timeout(1000)
                    print("  → Switched to Search Results tab")
            except:
                # Tab switching failed, continue with current page
                pass
            
            # Look for the search results table specifically in the Search Results tabpanel
            search_results_panel = page.locator('tabpanel[role="tabpanel"]:has-text("Search Results")')
            search_table_rows = search_results_panel.locator('table tbody tr')
            search_row_count = await search_table_rows.count()
            
            if search_row_count > 0:
                print(f"  [✓] Found {search_row_count} search result rows in Search Results tab")
                
                # Smart matching: analyze all results to find the best match
                best_match = None
                best_score = -1
                
                # Analyze up to first 20 results to find best match
                for row_idx in range(min(20, search_row_count)):
                    try:
                        row = search_table_rows.nth(row_idx)
                        cells = row.locator('td')
                        cell_count = await cells.count()
                        
                        if cell_count >= 3:
                            # Get folio, owner name and address from the Search Results table
                            folio_cell = cells.nth(0)  # Folio Number column
                            owner_cell = cells.nth(1)  # Owner Name column
                            address_cell = cells.nth(2)  # Site Address column
                            
                            folio_number = await folio_cell.text_content()
                            owner_name = await owner_cell.text_content()
                            address = await address_cell.text_content()
                            
                            if owner_name and address and address.strip():
                                owner_name = owner_name.strip()
                                address = address.strip()
                                folio_number = folio_number.strip() if folio_number else ""
                                
                                # Score this match based on how well it matches our search
                                score = await score_name_match(name, owner_name)
                                
                                print(f"    Row {row_idx}: {owner_name} -> {address} (folio: {folio_number}, score: {score})")
                                
                                if score > best_score:
                                    best_score = score
                                    best_match = {
                                        'address': address,
                                        'owner': owner_name,
                                        'folio': folio_number,
                                        'row': row_idx,
                                        'score': score
                                    }
                    
                    except Exception as e:
                        print(f"    Error processing row {row_idx}: {e}")
                        continue
                
                if best_match:
                    print(f"  [✓] Best match (row {best_match['row']}, score {best_match['score']}): {best_match['owner']}")
                    print(f"  [✓] Address: {best_match['address']}")
                    return best_match['address']
                else:
                    print(f"  [X] No valid matches found in {search_row_count} results")
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
                                                    
                                                    folio_text = await folio_cell.text_content()
                                                    owner_text = await owner_cell.text_content()
                                                    addr_text = await addr_cell.text_content()
                                                    
                                                    if owner_text and addr_text and addr_text.strip():
                                                        owner_text = owner_text.strip()
                                                        addr_text = addr_text.strip()
                                                        folio_text = folio_text.strip() if folio_text else ""
                                                        
                                                        # Score this match
                                                        score = await score_name_match(name, owner_text)
                                                        print(f"      Fallback Row {result_row_idx}: {owner_text} -> {addr_text} (score: {score})")
                                                        
                                                        if score > best_score:
                                                            best_score = score
                                                            best_match = {
                                                                'address': addr_text,
                                                                'owner': owner_text,
                                                                'folio': folio_text,
                                                                'row': result_row_idx,
                                                                'score': score
                                                            }
                                            except Exception as e:
                                                continue
                                        
                                        if best_match:
                                            print(f"  ✓ Fallback best match (table {table_idx}, row {best_match['row']}, score {best_match['score']}): {best_match['owner']}")
                                            print(f"  ✓ Address: {best_match['address']}")
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
                await page.reload(wait_until='domcontentloaded', timeout=15000)
                await human_pause("page_load")
            except:
                pass
                
            return await search_name_fast(page, name, attempt + 1)
        else:
            print(f"  ❌ Failed after {max_attempts} attempts")
            return None

async def process_addresses_fast(csv_path, max_names=15, headless=True):
    """Fast processing with smart behavior"""
    print("Fast Address Extractor - AI Human Behavior")
    print("=" * 60)
    
    # Load CSV
    try:
        df = pd.read_csv(csv_path)
        print(f"[✓] Loaded {len(df)} rows")
    except Exception as e:
        print(f"[X] Error loading CSV: {e}")
        return
    
    # Get unique person names from IndirectName_Cleaned (priority) and DirectName_Cleaned
    person_names = []
    
    for _, row in df.iterrows():
        # Priority: IndirectName_Cleaned for person names
        if row.get('IndirectName_Type') == 'Person' and row.get('IndirectName_Cleaned'):
            person_names.append(row['IndirectName_Cleaned'])
        # Fallback: DirectName_Cleaned for person names
        elif row.get('DirectName_Type') == 'Person' and row.get('DirectName_Cleaned'):
            person_names.append(row['DirectName_Cleaned'])
    
    unique_names = sorted(list(set(person_names)))
    print(f"[✓] Found {len(unique_names)} unique person names")
    
    if max_names and len(unique_names) > max_names:
        unique_names = unique_names[:max_names]
        print(f"[✓] Processing first {len(unique_names)} names")
    
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
                
                address = await search_name_fast(page, name)
                
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
    
    # Fill in addresses based on IndirectName_Cleaned primarily
    for index, row in df.iterrows():
        # Priority: IndirectName_Cleaned 
        if row.get('IndirectName_Cleaned') in address_map:
            df.at[index, 'IndirectName_Address'] = address_map[row['IndirectName_Cleaned']]
        # Fallback: DirectName_Cleaned
        elif row.get('DirectName_Cleaned') in address_map:
            df.at[index, 'DirectName_Address'] = address_map[row['DirectName_Cleaned']]
    
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
    parser.add_argument('--max-names', type=int, default=20, help='Maximum number of names to process (default: 20)')
    parser.add_argument('--headless', action='store_true', default=True, help='Run in headless mode (default: True)')
    parser.add_argument('--show-browser', action='store_true', help='Show browser (opposite of headless)')
    
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
    
    # Determine headless mode
    headless = args.headless and not args.show_browser
    
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
