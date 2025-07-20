#!/usr/bin/env python3
"""
Test script to verify browser closure and anti-detection effectiveness.
This script will test the enhanced anti-detection measures and verify that
browsers are properly closing between batches.
"""

import asyncio
import psutil
import time
import subprocess
from datetime import datetime
from zabasearch_batch1_records_1_15 import ZabaSearchExtractor

async def test_browser_closure():
    """Test browser closure and process cleanup"""
    print("🧪 TESTING BROWSER CLOSURE AND ANTI-DETECTION")
    print("=" * 60)
    
    extractor = ZabaSearchExtractor(headless=True)
    
    # Test multiple browser sessions to simulate batches
    for session in range(3):
        print(f"\n🔄 Testing Session {session + 1}/3")
        print(f"📊 Starting browser processes check...")
        
        # Get initial process count
        initial_chrome_processes = count_browser_processes()
        print(f"  📈 Initial browser processes: {initial_chrome_processes}")
        
        # Start browser session
        from playwright.async_api import async_playwright
        async with async_playwright() as playwright:
            print(f"  🌐 Creating stealth browser...")
            browser, context = await extractor.create_stealth_browser(playwright)
            
            # Count processes after browser start
            after_start_processes = count_browser_processes()
            print(f"  📈 Browser processes after start: {after_start_processes}")
            print(f"  📊 New processes created: {after_start_processes - initial_chrome_processes}")
            
            # Create a page and navigate to test the session
            page = await context.new_page()
            print(f"  🌐 Testing navigation to ZabaSearch...")
            
            try:
                await page.goto('https://www.zabasearch.com', timeout=10000)
                print(f"  ✅ Navigation successful")
                
                # Check for Cloudflare or blocking
                page_title = await page.title()
                page_content = await page.content()
                
                if 'cloudflare' in page_content.lower() or 'challenge' in page_content.lower():
                    print(f"  🛡️ Cloudflare challenge detected")
                elif 'blocked' in page_content.lower() or 'unusual traffic' in page_content.lower():
                    print(f"  ❌ BLOCKING DETECTED!")
                else:
                    print(f"  ✅ No blocking detected")
                    
                print(f"  📋 Page title: {page_title}")
                
            except Exception as e:
                print(f"  ⚠️ Navigation error: {e}")
            
            # Enhanced cleanup test
            print(f"  🔄 Testing enhanced browser cleanup...")
            
            # Close page
            await page.close()
            print(f"    ✅ Page closed")
            
            # Close context
            await context.close()
            print(f"    ✅ Context closed")
            
            # Close browser
            await browser.close()
            print(f"    ✅ Browser closed")
            
            # Wait for processes to terminate
            await asyncio.sleep(3)
            
            # Force garbage collection
            import gc
            gc.collect()
            print(f"    🗑️ Garbage collection completed")
        
        # Count processes after cleanup
        after_cleanup_processes = count_browser_processes()
        print(f"  📈 Browser processes after cleanup: {after_cleanup_processes}")
        print(f"  📊 Processes cleaned up: {after_start_processes - after_cleanup_processes}")
        
        if after_cleanup_processes <= initial_chrome_processes:
            print(f"  ✅ BROWSER CLEANUP SUCCESSFUL")
        else:
            print(f"  ❌ BROWSER CLEANUP INCOMPLETE - {after_cleanup_processes - initial_chrome_processes} processes still running")
        
        # Wait between sessions to simulate batch delay
        if session < 2:
            print(f"  ⏳ Waiting 10 seconds before next session...")
            await asyncio.sleep(10)
    
    print(f"\n🏁 BROWSER CLOSURE TEST COMPLETE")

def count_browser_processes():
    """Count running browser processes"""
    chrome_count = 0
    firefox_count = 0
    playwright_count = 0
    
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            name = proc.info['name'].lower() if proc.info['name'] else ""
            cmdline = ' '.join(proc.info['cmdline']) if proc.info['cmdline'] else ""
            
            if 'chrome' in name or 'chromium' in name:
                chrome_count += 1
            elif 'firefox' in name:
                firefox_count += 1
            elif 'playwright' in cmdline:
                playwright_count += 1
                
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    
    total = chrome_count + firefox_count + playwright_count
    print(f"    🌐 Chrome/Chromium: {chrome_count}, Firefox: {firefox_count}, Playwright: {playwright_count}")
    return total

async def test_anti_detection_effectiveness():
    """Test anti-detection by checking what fingerprint data is exposed"""
    print(f"\n🛡️ TESTING ANTI-DETECTION EFFECTIVENESS")
    print("=" * 60)
    
    extractor = ZabaSearchExtractor(headless=True)
    
    from playwright.async_api import async_playwright
    async with async_playwright() as playwright:
        browser, context = await extractor.create_stealth_browser(playwright)
        page = await context.new_page()
        
        print(f"🔍 Testing fingerprint concealment...")
        
        # Test anti-detection measures
        try:
            # Check webdriver property
            webdriver_result = await page.evaluate("navigator.webdriver")
            print(f"  🔍 navigator.webdriver: {webdriver_result}")
            
            # Check user agent
            user_agent = await page.evaluate("navigator.userAgent")
            print(f"  🔍 User Agent: {user_agent[:50]}...")
            
            # Check plugins
            plugins_length = await page.evaluate("navigator.plugins.length")
            print(f"  🔍 Plugin count: {plugins_length}")
            
            # Check languages
            languages = await page.evaluate("navigator.languages")
            print(f"  🔍 Languages: {languages}")
            
            # Check screen resolution
            screen_width = await page.evaluate("screen.width")
            screen_height = await page.evaluate("screen.height")
            print(f"  🔍 Screen resolution: {screen_width}x{screen_height}")
            
            # Check timezone
            timezone = await page.evaluate("Intl.DateTimeFormat().resolvedOptions().timeZone")
            print(f"  🔍 Timezone: {timezone}")
            
            # Check Chrome object
            chrome_runtime = await page.evaluate("typeof window.chrome")
            print(f"  🔍 Chrome object: {chrome_runtime}")
            
            # Test canvas fingerprinting protection
            canvas_data = await page.evaluate("""
                const canvas = document.createElement('canvas');
                const ctx = canvas.getContext('2d');
                ctx.textBaseline = 'top';
                ctx.font = '14px Arial';
                ctx.fillText('Canvas fingerprint test', 2, 2);
                return canvas.toDataURL().slice(0, 50);
            """)
            print(f"  🔍 Canvas fingerprint (first 50 chars): {canvas_data}")
            
            print(f"  ✅ Fingerprint analysis complete")
            
        except Exception as e:
            print(f"  ⚠️ Fingerprint test error: {e}")
        
        finally:
            await browser.close()

async def main():
    """Run all tests"""
    print(f"🚀 STARTING COMPREHENSIVE BROWSER AND ANTI-DETECTION TESTS")
    print(f"📅 Test started at: {datetime.now()}")
    print("=" * 70)
    
    # Test 1: Browser closure
    await test_browser_closure()
    
    # Test 2: Anti-detection effectiveness
    await test_anti_detection_effectiveness()
    
    print(f"\n✅ ALL TESTS COMPLETE")
    print(f"📅 Test ended at: {datetime.now()}")

if __name__ == "__main__":
    asyncio.run(main())
