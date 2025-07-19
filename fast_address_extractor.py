#!/usr/bin/env python3
"""
Fast Headless Address Extractor with AI-like Human Behavior
- Shorter, more realistic pauses
- Variable typing speeds
- Mouse movements and scrolling
- Random user agent rotation
"""

import pandas as pd
import asyncio
import time
import random
from playwright.async_api import async_playwright
import os

# Realistic user agents to rotate
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/121.0.0.0 Safari/537.36'
]

async def create_smart_browser(playwright):
    """Create browser with intelligent stealth settings"""
    user_agent = random.choice(USER_AGENTS)
    
    browser = await playwright.chromium.launch(
        headless=False,
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
        print(f"🔍 Searching: {name} (attempt {attempt})")
        
        # Navigate if needed or force refresh for clean state
        if 'Record-Search' not in page.url or attempt > 1:
            await page.goto('https://web.bcpa.net/BcpaClient/#/Record-Search', 
                           wait_until='domcontentloaded', timeout=20000)
            await human_pause("page_load")
        
        # Add some human behavior
        await add_human_behavior(page)
        
        # Wait for input field with better detection and multiple strategies
        field_visible = False
        for strategy in range(3):
            try:
                if strategy == 0:
                    # First try: direct wait
                    await page.wait_for_selector('#txtField', state='visible', timeout=6000)
                    field_visible = True
                    break
                elif strategy == 1:
                    # Second try: click to activate
                    print("  → Text field hidden, attempting to activate...")
                    await page.click('body')
                    await asyncio.sleep(0.3)
                    await page.wait_for_selector('#txtField', state='visible', timeout=4000)
                    field_visible = True
                    break
                elif strategy == 2:
                    # Third try: try different selectors and focus approaches
                    print("  → Trying alternative field activation...")
                    try:
                        # Try clicking on the search area first
                        await page.click('.form-control', timeout=3000)
                        await asyncio.sleep(0.2)
                    except:
                        pass
                    try:
                        # Try focusing the field directly
                        await page.focus('#txtField', timeout=3000)
                        await asyncio.sleep(0.2)
                    except:
                        pass
                    await page.wait_for_selector('#txtField', state='visible', timeout=4000)
                    field_visible = True
                    break
            except:
                continue
        
        if not field_visible:
            raise Exception("Could not make text field visible after multiple attempts")
        
        # Smart typing
        await smart_typing(page, '#txtField', name)
        await human_pause("typing")
        
        # Press Enter with slight delay (like a human)
        await page.press('#txtField', 'Enter')
        await human_pause("reading")
        
        # Quick check for results
        try:
            # Focus on the main search results table first (.table class)
            search_table_rows = page.locator('.table tbody tr')
            search_row_count = await search_table_rows.count()
            
            if search_row_count > 0:
                print(f"  ✓ Found {search_row_count} search result rows")
                
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
                            # Get owner name and address
                            owner_cell = cells.nth(1)  # Owner Name column
                            address_cell = cells.nth(2)  # Site Address column
                            
                            owner_name = await owner_cell.text_content()
                            address = await address_cell.text_content()
                            
                            if owner_name and address and address.strip():
                                owner_name = owner_name.strip()
                                address = address.strip()
                                
                                # Score this match based on how well it matches our search
                                score = await score_name_match(name, owner_name)
                                
                                print(f"    Row {row_idx}: {owner_name} -> {address} (score: {score})")
                                
                                if score > best_score:
                                    best_score = score
                                    best_match = {
                                        'address': address,
                                        'owner': owner_name,
                                        'row': row_idx,
                                        'score': score
                                    }
                    
                    except Exception as e:
                        print(f"    Error processing row {row_idx}: {e}")
                        continue
                
                if best_match:
                    print(f"  ✓ Best match (row {best_match['row']}, score {best_match['score']}): {best_match['owner']}")
                    print(f"  ✓ Address: {best_match['address']}")
                    return best_match['address']
                else:
                    print(f"  ❌ No valid matches found in {search_row_count} results")
                    return None
            
            # Fallback: Check all table rows and find the first one with 3 cells
            all_table_rows = page.locator('table tbody tr')
            all_row_count = await all_table_rows.count()
            
            if all_row_count > 0:
                print(f"  → Fallback: Checking {all_row_count} total table rows...")
                
                # First, look specifically for .table tbody tr results (search results)
                search_table_rows = page.locator('.table tbody tr')
                search_row_count = await search_table_rows.count()
                
                if search_row_count > 0:
                    print(f"  → Found {search_row_count} search result rows in fallback")
                    
                    # Use smart matching on search results
                    best_match = None
                    best_score = -1
                    
                    for row_idx in range(min(10, search_row_count)):
                        try:
                            row = search_table_rows.nth(row_idx)
                            cells = row.locator('td')
                            cell_count = await cells.count()
                            
                            if cell_count >= 3:
                                owner_cell = cells.nth(1)
                                address_cell = cells.nth(2)
                                
                                owner_name = await owner_cell.text_content()
                                address = await address_cell.text_content()
                                
                                if owner_name and address and address.strip():
                                    owner_name = owner_name.strip()
                                    address = address.strip()
                                    
                                    # Score this match
                                    score = await score_name_match(name, owner_name)
                                    print(f"    Fallback Row {row_idx}: {owner_name} -> {address} (score: {score})")
                                    
                                    if score > best_score:
                                        best_score = score
                                        best_match = {
                                            'address': address,
                                            'owner': owner_name,
                                            'row': row_idx,
                                            'score': score
                                        }
                        except Exception as e:
                            print(f"    Error processing fallback row {row_idx}: {e}")
                            continue
                    
                    if best_match:
                        print(f"  ✓ Fallback best match (row {best_match['row']}, score {best_match['score']}): {best_match['owner']}")
                        print(f"  ✓ Address: {best_match['address']}")
                        return best_match['address']
                
                # Last resort: check first few general table rows
                for row_idx in range(min(10, all_row_count)):  # Check first 10 rows
                    try:
                        row = all_table_rows.nth(row_idx)
                        cells = row.locator('td')
                        cell_count = await cells.count()
                        
                        if cell_count >= 3:
                            address_cell = cells.nth(2)
                            address = await address_cell.text_content()
                            
                            if address and address.strip() and any(indicator in address.upper() for indicator in ['ST ', 'AVE ', 'DR ', 'CT ', 'WAY ', 'CIR ', 'RD ', 'PL ']):
                                address = address.strip()
                                print(f"  ✓ Found address in row {row_idx}: {address}")
                                return address
                    except Exception as e:
                        continue
                
                print(f"  ❌ Could not find valid address in any table row")
                return None
            else:
                print(f"  ⚠️ No table rows found")
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

async def process_addresses_fast(csv_path, max_names=15):
    """Fast processing with smart behavior"""
    print("🚀 FAST ADDRESS EXTRACTOR (AI HUMAN BEHAVIOR)")
    print("=" * 60)
    
    # Load CSV
    try:
        df = pd.read_csv(csv_path)
        print(f"✓ Loaded {len(df)} rows")
    except Exception as e:
        print(f"❌ Error loading CSV: {e}")
        return
    
    # Get unique person names
    person_names = []
    
    for _, row in df.iterrows():
        if row.get('DirectName_Type') == 'Person' and row.get('DirectName_Cleaned'):
            person_names.append(row['DirectName_Cleaned'])
        if row.get('IndirectName_Type') == 'Person' and row.get('IndirectName_Cleaned'):
            person_names.append(row['IndirectName_Cleaned'])
    
    unique_names = sorted(list(set(person_names)))
    print(f"✓ Found {len(unique_names)} unique names")
    
    if max_names:
        unique_names = unique_names[:max_names]
        print(f"✓ Processing {len(unique_names)} names")
    
    # Process names
    address_map = {}
    
    async with async_playwright() as p:
        browser, context = await create_smart_browser(p)
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
                    print(f"  ✅ SUCCESS ({success_count}/{i}) - {rate:.1f} searches/min")
                else:
                    print(f"  ❌ FAILED")
                
                # Smart pause between searches
                if i < len(unique_names):
                    pause_time = await human_pause("between_searches")
                    print(f"  ⏳ Pausing {pause_time:.1f}s...")
        
        finally:
            try:
                await browser.close()
            except:
                pass
    
    # Add addresses to CSV
    print(f"\n📝 Adding {len(address_map)} addresses to CSV...")
    
    # Add new columns if they don't exist
    if 'DirectName_Address' not in df.columns:
        df['DirectName_Address'] = ''
    if 'IndirectName_Address' not in df.columns:
        df['IndirectName_Address'] = ''
    
    # Fill in addresses
    for index, row in df.iterrows():
        if row.get('DirectName_Cleaned') in address_map:
            df.at[index, 'DirectName_Address'] = address_map[row['DirectName_Cleaned']]
        
        if row.get('IndirectName_Cleaned') in address_map:
            df.at[index, 'IndirectName_Address'] = address_map[row['IndirectName_Cleaned']]
    
    # Save updated CSV
    base_name = os.path.splitext(csv_path)[0]
    output_path = f"{base_name}_with_addresses_fast.csv"
    
    df.to_csv(output_path, index=False)
    
    # Final stats
    total_time = time.time() - start_time
    avg_time = total_time / len(unique_names) if unique_names else 0
    
    print(f"\n🎉 COMPLETE!")
    print(f"✓ Found addresses for {len(address_map)}/{len(unique_names)} people")
    print(f"✓ Total time: {total_time/60:.1f} minutes")
    print(f"✓ Average: {avg_time:.1f}s per search")
    print(f"✓ Success rate: {len(address_map)/len(unique_names)*100:.1f}%")
    print(f"✓ Updated CSV: {output_path}")
    
    return output_path

async def main():
    csv_file = r"c:\Users\my notebook\Desktop\BlakeJackson\LisPendens_BrowardCounty_July7-14_2025_processed.csv"
    
    if not os.path.exists(csv_file):
        print(f"❌ CSV file not found: {csv_file}")
        return
    
    # Ask for number of names
    try:
        user_input = input("\nEnter max number of names to process (or press Enter for 20): ").strip()
        max_names = int(user_input) if user_input else 20
    except:
        max_names = 20
    
    await process_addresses_fast(csv_file, max_names)

if __name__ == "__main__":
    asyncio.run(main())
