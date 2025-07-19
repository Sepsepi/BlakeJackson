#!/usr/bin/env python3
"""
Pipeline Test Script
===================

Quick test script to verify all pipeline components are working correctly
before setting up automation.

This script runs a minimal test of each pipeline stage:
1. Tests imports and component availability
2. Runs a small test batch (3 days back, visible browser)
3. Verifies output files and data quality

Usage:
    python test_pipeline.py

Author: Blake Jackson
Date: July 19, 2025
"""

import sys
import os
import asyncio
from pathlib import Path

# Add current directory to path for imports
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

def test_imports():
    """Test that all required components can be imported"""
    print("🔍 Testing component imports...")
    
    components = {
        'Broward Scraper': 'broward_lis_pendens_scraper',
        'Name Processor': 'lis_pendens_processor', 
        'Address Extractor': 'fast_address_extractor',
        'ZabaSearch Extractor': 'zabasearch_batch1_records_1_15',
        'Pipeline Scheduler': 'pipeline_scheduler'
    }
    
    results = {}
    
    for name, module in components.items():
        try:
            __import__(module)
            results[name] = "✅ OK"
            print(f"  {name}: ✅ OK")
        except ImportError as e:
            results[name] = f"❌ FAILED: {e}"
            print(f"  {name}: ❌ FAILED: {e}")
    
    all_good = all("✅" in status for status in results.values())
    
    if all_good:
        print("🎉 All components imported successfully!")
    else:
        print("❌ Some components failed to import. Check file locations and dependencies.")
        
    return all_good, results

async def test_pipeline():
    """Run a minimal pipeline test"""
    print("\n🧪 Running pipeline test (3 days back, small batch)...")
    
    try:
        from pipeline_scheduler import PipelineScheduler
        
        # Create test scheduler with minimal settings
        scheduler = PipelineScheduler(
            output_dir="test_output",
            excel_file=None,
            days_back=3,        # Minimal date range
            batch_size=5,       # Small batch
            headless=False,     # Visible for debugging
            max_retries=2
        )
        
        print("⚙️  Pipeline scheduler created successfully")
        
        # Run the pipeline
        results = await scheduler.run_complete_pipeline()
        
        # Analyze results
        if results['success']:
            print("✅ Pipeline test completed successfully!")
            print(f"   Records found: {results['broward_records']}")
            print(f"   Addresses found: {results['addresses_found']}")
            print(f"   Phone numbers found: {results['phone_numbers_found']}")
            print(f"   Files created: {len(results['files_created'])}")
        else:
            print("❌ Pipeline test failed")
            if results['errors']:
                print("   Errors:")
                for error in results['errors']:
                    print(f"     - {error}")
                    
        return results['success']
        
    except Exception as e:
        print(f"❌ Pipeline test failed with exception: {e}")
        return False

def check_requirements():
    """Check if required Python packages are available"""
    print("\n📦 Checking required packages...")
    
    packages = {
        'pandas': 'Data processing',
        'playwright': 'Browser automation',
        'asyncio': 'Async operations',
        'pathlib': 'File path handling',
        'logging': 'Logging system'
    }
    
    missing = []
    
    for package, description in packages.items():
        try:
            __import__(package)
            print(f"  {package}: ✅ Available ({description})")
        except ImportError:
            print(f"  {package}: ❌ Missing ({description})")
            missing.append(package)
    
    if missing:
        print(f"\n⚠️  Missing packages: {', '.join(missing)}")
        print("Install with: pip install " + " ".join(missing))
        return False
    else:
        print("🎉 All required packages available!")
        return True

async def main():
    """Main test function"""
    print("🚀 BROWARD PIPELINE TEST SUITE")
    print("=" * 50)
    
    # Test 1: Check imports
    imports_ok, import_results = test_imports()
    
    # Test 2: Check packages  
    packages_ok = check_requirements()
    
    if not imports_ok or not packages_ok:
        print("\n❌ Pre-flight checks failed. Please fix issues before running pipeline.")
        return False
    
    # Test 3: Confirm test run
    print("\n🔔 READY FOR PIPELINE TEST")
    print("=" * 30)
    print("This will run a minimal pipeline test:")
    print("  - 3 days back (small dataset)")
    print("  - Visible browser (for monitoring)")
    print("  - Batch size of 5 records")
    print("  - Output to 'test_output' directory")
    print("")
    
    response = input("Continue with pipeline test? (y/N): ").strip().lower()
    
    if response != 'y':
        print("Test cancelled by user.")
        return False
    
    # Test 4: Run pipeline
    pipeline_ok = await test_pipeline()
    
    # Final results
    print("\n📊 TEST SUMMARY")
    print("=" * 20)
    print(f"Component imports: {'✅ PASS' if imports_ok else '❌ FAIL'}")
    print(f"Package check: {'✅ PASS' if packages_ok else '❌ FAIL'}")
    print(f"Pipeline test: {'✅ PASS' if pipeline_ok else '❌ FAIL'}")
    
    if imports_ok and packages_ok and pipeline_ok:
        print("\n🎉 ALL TESTS PASSED!")
        print("Pipeline is ready for production use.")
        print("\nNext steps:")
        print("1. Review output files in 'test_output' directory")
        print("2. Run weekly_pipeline.py for full automation") 
        print("3. Set up Windows Task Scheduler with run_weekly_pipeline.bat")
        return True
    else:
        print("\n❌ SOME TESTS FAILED")
        print("Please review errors above before using pipeline.")
        return False

if __name__ == "__main__":
    try:
        success = asyncio.run(main())
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n⏹️  Test interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n💥 Test failed with unexpected error: {e}")
        sys.exit(1)
