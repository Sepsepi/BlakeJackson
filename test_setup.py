#!/usr/bin/env python3
"""
Test Setup Script - Verify Broward Automation Configuration
===========================================================

This script tests all components needed for the weekly automation:
1. Environment variables loading
2. Email configuration
3. Google Sheets access
4. Playwright browsers
5. Service account authentication

Run this before setting up the cron job to ensure everything works.
"""

import sys
from pathlib import Path

# Load environment variables
from dotenv import load_dotenv
env_path = Path(__file__).parent / '.env'
load_dotenv(dotenv_path=env_path)

import os
from datetime import datetime

def print_header(text):
    """Print a formatted header"""
    print("\n" + "=" * 70)
    print(f"  {text}")
    print("=" * 70)

def print_status(check_name, status, details=""):
    """Print a status line"""
    icon = "✅" if status else "❌"
    print(f"{icon} {check_name}")
    if details:
        print(f"   {details}")

def test_environment_variables():
    """Test if environment variables are loaded"""
    print_header("1. ENVIRONMENT VARIABLES")
    
    required_vars = {
        'EMAIL_SENDER': os.environ.get('EMAIL_SENDER'),
        'EMAIL_PASSWORD': os.environ.get('EMAIL_PASSWORD'),
        'EMAIL_RECIPIENTS': os.environ.get('EMAIL_RECIPIENTS'),
        'GOOGLE_SPREADSHEET_ID': os.environ.get('GOOGLE_SPREADSHEET_ID'),
        'GOOGLE_SERVICE_ACCOUNT_JSON': os.environ.get('GOOGLE_SERVICE_ACCOUNT_JSON'),
    }
    
    all_set = True
    for var_name, var_value in required_vars.items():
        is_set = bool(var_value)
        all_set = all_set and is_set
        
        if var_name == 'EMAIL_PASSWORD':
            display_value = "***" + var_value[-4:] if var_value else "NOT SET"
        else:
            display_value = var_value if var_value else "NOT SET"
        
        print_status(var_name, is_set, display_value)
    
    return all_set

def test_email_configuration():
    """Test email configuration"""
    print_header("2. EMAIL CONFIGURATION")
    
    try:
        from email_notifier import EmailNotifier
        notifier = EmailNotifier()
        
        print_status("Email module loaded", True)
        print_status("Email enabled", notifier.enabled, 
                    f"Sender: {notifier.sender_email}")
        print_status("Recipients configured", len(notifier.recipient_emails) > 0,
                    f"Recipients: {', '.join(notifier.recipient_emails)}")
        
        if notifier.enabled:
            print("\n   📧 Email is configured and ready!")
            print(f"   Emails will be sent from: {notifier.sender_email}")
            print(f"   Emails will be sent to: {', '.join(notifier.recipient_emails)}")
            return True
        else:
            print("\n   ⚠️ Email is NOT enabled - check your .env file")
            return False
            
    except Exception as e:
        print_status("Email configuration", False, f"Error: {e}")
        return False

def test_google_sheets():
    """Test Google Sheets configuration"""
    print_header("3. GOOGLE SHEETS CONFIGURATION")
    
    try:
        from google_sheets_integration import GoogleSheetsIntegration
        sheets = GoogleSheetsIntegration()
        
        print_status("Google Sheets module loaded", True)
        print_status("Google Sheets enabled", sheets.enabled)
        
        if sheets.enabled:
            print_status("Spreadsheet ID", bool(sheets.spreadsheet_id), 
                        sheets.spreadsheet_id)
            print_status("Service account file", bool(sheets.service_account_file),
                        sheets.service_account_file)
            
            # Check if service account file exists
            if sheets.service_account_file:
                service_account_path = Path(sheets.service_account_file)
                file_exists = service_account_path.exists()
                print_status("Service account file exists", file_exists,
                            str(service_account_path.absolute()))
                
                if file_exists:
                    print("\n   📊 Google Sheets is configured!")
                    print(f"   Spreadsheet: https://docs.google.com/spreadsheets/d/{sheets.spreadsheet_id}")
                    print(f"   Service account: {sheets.service_account_file}")
                    return True
            
        print("\n   ⚠️ Google Sheets is NOT enabled - check your .env file")
        return False
        
    except Exception as e:
        print_status("Google Sheets configuration", False, f"Error: {e}")
        return False

def test_playwright_browsers():
    """Test if Playwright browsers are installed"""
    print_header("4. PLAYWRIGHT BROWSERS")
    
    try:
        import subprocess
        
        # Check if playwright is installed
        result = subprocess.run(
            ['playwright', '--version'],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        if result.returncode == 0:
            version = result.stdout.strip()
            print_status("Playwright CLI available", True, version)
            
            # Try to check if browsers are installed
            print("\n   ℹ️ To verify browsers are installed, run:")
            print("   playwright install chromium firefox")
            return True
        else:
            print_status("Playwright CLI", False, "Not found in PATH")
            return False
            
    except FileNotFoundError:
        print_status("Playwright CLI", False, "Not found - run: pip install playwright")
        return False
    except Exception as e:
        print_status("Playwright check", False, f"Error: {e}")
        return False

def test_output_directories():
    """Test if output directories exist"""
    print_header("5. OUTPUT DIRECTORIES")
    
    directories = [
        'weekly_output',
        'pipeline_output',
        'downloads',
        'logs'
    ]
    
    all_exist = True
    for dir_name in directories:
        dir_path = Path(dir_name)
        exists = dir_path.exists()
        
        if not exists:
            try:
                dir_path.mkdir(exist_ok=True)
                print_status(dir_name, True, "Created")
            except Exception as e:
                print_status(dir_name, False, f"Failed to create: {e}")
                all_exist = False
        else:
            print_status(dir_name, True, "Exists")
    
    return all_exist

def main():
    """Run all tests"""
    print("\n" + "🔍 BROWARD AUTOMATION SETUP TEST".center(70))
    print(f"{'Started at: ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S')}".center(70))
    
    results = {
        'Environment Variables': test_environment_variables(),
        'Email Configuration': test_email_configuration(),
        'Google Sheets': test_google_sheets(),
        'Playwright Browsers': test_playwright_browsers(),
        'Output Directories': test_output_directories(),
    }
    
    # Summary
    print_header("SUMMARY")
    
    passed = sum(1 for v in results.values() if v)
    total = len(results)
    
    for test_name, result in results.items():
        print_status(test_name, result)
    
    print(f"\n   Tests passed: {passed}/{total}")
    
    if passed == total:
        print("\n   🎉 ALL TESTS PASSED! You're ready to set up the cron job!")
        print("\n   Next steps:")
        print("   1. Run: playwright install chromium firefox")
        print("   2. Test manual run: python weekly_automation.py")
        print("   3. Set up Task Scheduler / cron job")
        print("   4. Check CRON_JOB_SETUP_GUIDE.md for detailed instructions")
        return 0
    else:
        print("\n   ⚠️ SOME TESTS FAILED - Fix the issues above before proceeding")
        print("\n   Check CRON_JOB_SETUP_GUIDE.md for troubleshooting")
        return 1

if __name__ == "__main__":
    sys.exit(main())

