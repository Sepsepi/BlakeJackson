import pandas as pd
import asyncio
import time
import random
from playwright.async_api import async_playwright
import os
from urllib.parse import quote

async def create_stealth_browser(playwright):
    """
    Create a browser with stealth settings to avoid detection
    """
    browser = await playwright.chromium.launch(
        headless=True,
        args=[
            '--no-sandbox',
            '--disable-blink-features=AutomationControlled',
            '--disable-dev-shm-usage',
            '--disable-extensions',
            '--disable-plugins',
            '--disable-default-apps',
            '--disable-background-timer-throttling',
            '--disable-backgrounding-occluded-windows',
            '--disable-renderer-backgrounding',
            '--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        ]
    )
    
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
        }
    )
    
    # Remove automation detection
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

async def human_like_delay(min_seconds=1, max_seconds=3):
    """
    Add human-like random delays between actions
    """
    delay = random.uniform(min_seconds, max_seconds)
    await asyncio.sleep(delay)

async def type_like_human(page, selector, text):
    """
    Type text with human-like delays between keystrokes
    """
    element = page.locator(selector)
    await element.clear()
    
    for char in text:
        await element.type(char)
        await asyncio.sleep(random.uniform(0.05, 0.15))

async def search_property_address(page, person_name):
    """
    Search for a person's property address on the Broward County Property Appraiser website
    """
    try:
        print(f"Searching for: {person_name}")
        
        # Navigate to the property search page
        await page.goto('https://web.bcpa.net/BcpaClient/#/Record-Search', 
                       wait_until='domcontentloaded', timeout=60000)
        
        await human_like_delay(3, 6)
        
        # Try multiple selectors for the search input (using correct selector first)
        search_input_selectors = [
            '#txtField',  # This is the correct selector based on our debug
            'input[placeholder*="Name"]',
            'input[type="text"]',
            'input.form-control',
            '#searchInput',
            '[data-testid="search-input"]'
        ]
        
        search_input = None
        for selector in search_input_selectors:
            try:
                search_input = page.locator(selector).first
                await search_input.wait_for(state='visible', timeout=5000)
                break
            except:
                continue
        
        if not search_input:
            print(f"  → Could not find search input field")
            return None
        
        await human_like_delay(1, 2)
        
        # Clear and type the person's name using the correct selector
        await type_like_human(page, '#txtField', person_name)
        
        await human_like_delay(1, 2)
        
        # Press Enter instead of clicking search button (more reliable)
        await page.press('#txtField', 'Enter')
        
        await human_like_delay(3, 5)
        
        # Debug: Check current page state
        current_url = page.url
        print(f"  → Current URL: {current_url}")
        
        # Check for results table first (most reliable indicator)
        try:
            results_table = page.locator('table tbody tr')
            row_count = await results_table.count()
            print(f"  → Found {row_count} table rows")
            
            if row_count > 0:
                # We have results! Process them
                print(f"  → Processing search results...")
                
                # Look for the cleanest match (name without suffixes)
                best_address = None
                target_name_parts = person_name.lower().split()
                target_first = target_name_parts[0]
                target_last = target_name_parts[1] if len(target_name_parts) > 1 else ""
                
                for i in range(min(row_count, 10)):  # Check first 10 results
                    row = results_table.nth(i)
                    
                    # Get all cells in the row
                    cells = row.locator('td')
                    cell_count = await cells.count()
                    
                    if cell_count >= 3:
                        # Get owner name and address from the row
                        owner_cell = cells.nth(1)  # Owner Name column
                        address_cell = cells.nth(2)  # Site Address column
                        
                        owner_name = await owner_cell.text_content()
                        address = await address_cell.text_content()
                        
                        if owner_name and address:
                            owner_name = owner_name.strip()
                            address = address.strip()
                            
                            print(f"  → Row {i}: {owner_name} -> {address}")
                            
                            # Check if this is a clean match (just LAST, FIRST without suffixes)
                            owner_lower = owner_name.lower()
                            
                            # Look for exact first and last name match without extra suffixes
                            if (target_first in owner_lower and target_last in owner_lower and
                                not any(suffix in owner_lower for suffix in ['jr', 'sr', 'ii', 'iii', 'iv'])):
                                
                                print(f"  → Found clean match: {owner_name} at {address}")
                                best_address = address
                                break
                
                if not best_address and row_count > 0:
                    # If no clean match found, take the first result
                    first_row = results_table.nth(0)
                    cells = first_row.locator('td')
                    if await cells.count() >= 3:
                        address_cell = cells.nth(2)
                        best_address = await address_cell.text_content()
                        if best_address:
                            best_address = best_address.strip()
                            print(f"  → Using first result: {best_address}")
                
                return best_address
            
            else:
                print(f"  → No results found in table")
                return None
                
        except Exception as e:
            print(f"  → Error processing results: {e}")
        
        # Original logic as fallback
        # Check which tab we're on and extract address accordingly
        search_results_tab = page.locator('a[href="#/Record-Search/Search-Results"]')
        parcel_result_tab = page.locator('a[href="#/Record-Search/Parcel-Result"]')
        
        # Check if we have search results or went directly to parcel result
        
        if 'Search-Results' in current_url or await search_results_tab.is_visible():
            # We're on search results page - look for the cleanest match
            print(f"  → Multiple results found, looking for best match...")
            
            # Wait for the results table
            results_table = page.locator('table')
            await results_table.wait_for(state='visible', timeout=10000)
            
            # Get all rows in the results table
            rows = page.locator('table tbody tr')
            row_count = await rows.count()
            
            if row_count == 0:
                print(f"  → No results found for {person_name}")
                return None
            
            # Look for the cleanest match (name without suffixes)
            best_address = None
            target_name_parts = person_name.lower().split()
            target_first = target_name_parts[0]
            target_last = target_name_parts[1] if len(target_name_parts) > 1 else ""
            
            for i in range(min(row_count, 10)):  # Check first 10 results
                row = rows.nth(i)
                
                # Get owner name and address from the row
                owner_cell = row.locator('td').nth(1)  # Owner Name column
                address_cell = row.locator('td').nth(2)  # Site Address column
                
                owner_name = await owner_cell.text_content()
                address = await address_cell.text_content()
                
                if owner_name and address:
                    owner_name = owner_name.strip()
                    address = address.strip()
                    
                    # Check if this is a clean match (just LAST, FIRST without suffixes)
                    owner_lower = owner_name.lower()
                    
                    # Look for exact first and last name match without extra suffixes
                    if (target_first in owner_lower and target_last in owner_lower and
                        not any(suffix in owner_lower for suffix in ['jr', 'sr', 'ii', 'iii', 'iv'])):
                        
                        print(f"  → Found clean match: {owner_name} at {address}")
                        best_address = address
                        break
            
            if not best_address and row_count > 0:
                # If no clean match found, take the first result
                first_row = rows.nth(0)
                address_cell = first_row.locator('td').nth(2)
                best_address = await address_cell.text_content()
                if best_address:
                    best_address = best_address.strip()
                    print(f"  → Using first result: {best_address}")
            
            return best_address
            
        elif 'Parcel-Result' in current_url or await parcel_result_tab.is_visible():
            # We went directly to parcel result (exact match)
            print(f"  → Direct match found, extracting address...")
            
            # Look for the physical address in the parcel result page
            address_elements = [
                'span:has-text("Physical Address:") + span',
                'span:has-text("Site Address:") + span',
                'td:has-text("Physical Address:") + td',
                'td:has-text("Site Address:") + td'
            ]
            
            for selector in address_elements:
                try:
                    address_element = page.locator(selector).first
                    if await address_element.is_visible():
                        address = await address_element.text_content()
                        if address and address.strip():
                            address = address.strip()
                            print(f"  → Found address: {address}")
                            return address
                except:
                    continue
            
            print(f"  → Could not extract address from parcel result page")
            return None
        
        else:
            # Check for "No record found" message
            no_results = page.locator('text=No record found')
            if await no_results.is_visible():
                print(f"  → No record found for {person_name}")
                return None
            
            print(f"  → Unexpected page state for {person_name}")
            return None
    
    except Exception as e:
        print(f"  → Error searching for {person_name}: {str(e)}")
        return None

async def process_names_with_addresses(csv_file_path, max_names=None):
    """
    Process the CSV file and add addresses for person names
    """
    print("BROWARD COUNTY PROPERTY ADDRESS EXTRACTOR")
    print("=" * 60)
    
    # Read the processed CSV file
    if not os.path.exists(csv_file_path):
        print(f"ERROR: File not found: {csv_file_path}")
        return
    
    try:
        df = pd.read_csv(csv_file_path)
        print(f"✓ Loaded {len(df)} rows from CSV file")
    except Exception as e:
        print(f"ERROR: Could not read CSV file: {e}")
        return
    
    # Check if we have the required columns
    required_columns = ['DirectName_Cleaned', 'IndirectName_Cleaned', 'DirectName_Type', 'IndirectName_Type']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        print(f"ERROR: Missing required columns: {missing_columns}")
        print("Please run the name processor first to create cleaned names.")
        return
    
    # Add address columns if they don't exist
    if 'DirectName_Address' not in df.columns:
        df['DirectName_Address'] = ''
    if 'IndirectName_Address' not in df.columns:
        df['IndirectName_Address'] = ''
    
    # Collect unique person names to search
    person_names = set()
    
    # Collect DirectName persons
    direct_persons = df[(df['DirectName_Type'] == 'Person') & (df['DirectName_Cleaned'] != '')]
    for _, row in direct_persons.iterrows():
        person_names.add(row['DirectName_Cleaned'])
    
    # Collect IndirectName persons
    indirect_persons = df[(df['IndirectName_Type'] == 'Person') & (df['IndirectName_Cleaned'] != '')]
    for _, row in indirect_persons.iterrows():
        person_names.add(row['IndirectName_Cleaned'])
    
    person_names = sorted(list(person_names))
    
    if max_names:
        person_names = person_names[:max_names]
    
    print(f"✓ Found {len(person_names)} unique person names to search")
    
    if not person_names:
        print("No person names found to search.")
        return
    
    # Create address lookup dictionary
    address_lookup = {}
    
    async with async_playwright() as playwright:
        print("✓ Starting browser automation...")
        browser, context = await create_stealth_browser(playwright)
        page = await context.new_page()
        
        try:
            # Process each unique person name
            for i, person_name in enumerate(person_names, 1):
                print(f"\n[{i}/{len(person_names)}] Processing: {person_name}")
                
                # Add random delay between searches to avoid rate limiting
                if i > 1:
                    delay = random.uniform(3, 7)
                    print(f"  → Waiting {delay:.1f}s before next search...")
                    await asyncio.sleep(delay)
                
                # Search for the address
                address = await search_property_address(page, person_name)
                
                if address:
                    address_lookup[person_name] = address
                    print(f"  ✓ Address found: {address}")
                else:
                    print(f"  ✗ No address found")
                
                # Every 10 searches, take a longer break
                if i % 10 == 0:
                    print(f"  → Taking extended break after {i} searches...")
                    await asyncio.sleep(random.uniform(10, 20))
        
        finally:
            await browser.close()
    
    # Update the DataFrame with found addresses
    print(f"\n✓ Updating CSV with {len(address_lookup)} found addresses...")
    
    for index, row in df.iterrows():
        # Update DirectName address
        if (row['DirectName_Type'] == 'Person' and 
            row['DirectName_Cleaned'] and 
            row['DirectName_Cleaned'] in address_lookup):
            df.at[index, 'DirectName_Address'] = address_lookup[row['DirectName_Cleaned']]
        
        # Update IndirectName address
        if (row['IndirectName_Type'] == 'Person' and 
            row['IndirectName_Cleaned'] and 
            row['IndirectName_Cleaned'] in address_lookup):
            df.at[index, 'IndirectName_Address'] = address_lookup[row['IndirectName_Cleaned']]
    
    # Save the updated CSV
    base_dir = os.path.dirname(csv_file_path)
    base_name = os.path.splitext(os.path.basename(csv_file_path))[0]
    output_file = os.path.join(base_dir, f"{base_name}_with_addresses.csv")
    
    try:
        df.to_csv(output_file, index=False)
        print(f"✓ Updated CSV saved: {output_file}")
    except Exception as e:
        print(f"ERROR: Could not save updated CSV: {e}")
        return
    
    # Generate summary
    direct_addresses_found = len(df[(df['DirectName_Type'] == 'Person') & (df['DirectName_Address'] != '')])
    indirect_addresses_found = len(df[(df['IndirectName_Type'] == 'Person') & (df['IndirectName_Address'] != '')])
    
    print("\n" + "=" * 60)
    print("ADDRESS EXTRACTION SUMMARY")
    print("=" * 60)
    print(f"Unique names searched: {len(person_names)}")
    print(f"Addresses found: {len(address_lookup)}")
    print(f"DirectName addresses: {direct_addresses_found}")
    print(f"IndirectName addresses: {indirect_addresses_found}")
    print(f"Success rate: {len(address_lookup)/len(person_names)*100:.1f}%")
    
    print("\n" + "=" * 60)
    print("NEW COLUMNS ADDED")
    print("=" * 60)
    print("- DirectName_Address: Property address for DirectName persons")
    print("- IndirectName_Address: Property address for IndirectName persons")
    
    print(f"\n✓ Process complete! Check: {output_file}")
    
    return output_file

async def main():
    """
    Main function
    """
    # Default file path - use the processed file
    csv_file = r"c:\Users\my notebook\Desktop\BlakeJackson\LisPendens_BrowardCounty_July7-14_2025_processed.csv"
    
    print("BROWARD COUNTY PROPERTY ADDRESS EXTRACTOR")
    print("=" * 60)
    print("This script will:")
    print("1. Read cleaned person names from the processed CSV")
    print("2. Search Broward County Property Appraiser website")
    print("3. Extract property addresses for each person")
    print("4. Add addresses to the CSV file")
    print("5. Use stealth techniques to avoid detection")
    print("=" * 60)
    
    # Check if file exists
    if not os.path.exists(csv_file):
        print(f"Processed CSV file not found: {csv_file}")
        print("Please run the name processor first.")
        return
    
    # Ask user for number of names to process (for testing)
    try:
        max_names_input = input("\nEnter max number of names to process (or press Enter for all): ").strip()
        max_names = int(max_names_input) if max_names_input else None
    except ValueError:
        max_names = None
    
    # Process the file
    output_file = await process_names_with_addresses(csv_file, max_names)
    
    if output_file:
        print(f"\nSuccess! Address extraction complete.")
        print(f"Output file: {output_file}")
    else:
        print("\nAddress extraction failed. Please check the error messages above.")

if __name__ == "__main__":
    asyncio.run(main())
