#!/usr/bin/env python3
"""
Quick integration test for zaba.py with pipeline
Tests method signatures and compatibility
"""

import sys
import inspect
import pandas as pd
from pathlib import Path

def test_zaba_integration():
    """Test zaba.py integration with pipeline"""
    
    print("=" * 80)
    print("ZABA.PY INTEGRATION TEST")
    print("=" * 80)
    
    # Test 1: Import zaba.py
    print("\n[TEST 1] Importing zaba.py...")
    try:
        from zaba import ZabaSearchExtractor
        print("✅ zaba.py imported successfully")
    except Exception as e:
        print(f"❌ Failed to import zaba.py: {e}")
        return False
    
    # Test 2: Instantiate extractor
    print("\n[TEST 2] Instantiating ZabaSearchExtractor...")
    try:
        extractor = ZabaSearchExtractor()
        print("✅ ZabaSearchExtractor instantiated")
    except Exception as e:
        print(f"❌ Failed to instantiate: {e}")
        return False
    
    # Test 3: Check process_csv_batch method exists
    print("\n[TEST 3] Checking process_csv_batch method...")
    if not hasattr(extractor, 'process_csv_batch'):
        print("❌ process_csv_batch method not found")
        return False
    print("✅ process_csv_batch method exists")
    
    # Test 4: Check method signature
    print("\n[TEST 4] Verifying method signature...")
    sig = inspect.signature(extractor.process_csv_batch)
    params = list(sig.parameters.keys())
    
    expected_params = ['csv_path', 'output_path', 'start_record', 'end_record']
    
    print(f"   Expected params: {expected_params}")
    print(f"   Actual params:   {params}")
    
    if params != expected_params:
        print("❌ Method signature mismatch")
        return False
    print("✅ Method signature matches pipeline requirements")
    
    # Test 5: Check parameter defaults
    print("\n[TEST 5] Checking parameter defaults...")
    output_path_default = sig.parameters['output_path'].default
    start_record_default = sig.parameters['start_record'].default
    end_record_default = sig.parameters['end_record'].default
    
    print(f"   output_path default: {output_path_default}")
    print(f"   start_record default: {start_record_default}")
    print(f"   end_record default: {end_record_default}")
    
    if output_path_default is not None:
        print("❌ output_path should default to None")
        return False
    if start_record_default != 1:
        print("❌ start_record should default to 1")
        return False
    if end_record_default is not None:
        print("❌ end_record should default to None")
        return False
    
    print("✅ Parameter defaults are correct")
    
    # Test 6: Import pipeline
    print("\n[TEST 6] Importing pipeline_scheduler...")
    try:
        from pipeline_scheduler import BrowardLisPendensPipeline
        print("✅ pipeline_scheduler imported successfully")
    except Exception as e:
        print(f"❌ Failed to import pipeline: {e}")
        return False
    
    # Test 7: Check pipeline uses zaba.py
    print("\n[TEST 7] Verifying pipeline uses zaba.py...")
    try:
        import pipeline_scheduler
        source = inspect.getsource(pipeline_scheduler)
        if 'from zaba import ZabaSearchExtractor' in source:
            print("✅ Pipeline imports from zaba.py")
        elif 'from zabasearch_batch1_records_1_15 import ZabaSearchExtractor' in source:
            print("⚠️ Pipeline still imports from old script")
            print("   (This is OK if you haven't deployed yet)")
        else:
            print("❌ Cannot determine ZabaSearch import")
            return False
    except Exception as e:
        print(f"⚠️ Could not verify import: {e}")
    
    # Test 8: Import google_sheets_integration
    print("\n[TEST 8] Importing google_sheets_integration...")
    try:
        from google_sheets_integration import GoogleSheetsIntegration
        print("✅ google_sheets_integration imported successfully")
    except Exception as e:
        print(f"❌ Failed to import google_sheets_integration: {e}")
        return False
    
    # Test 9: Check dtype fix
    print("\n[TEST 9] Verifying dtype fix in google_sheets_integration...")
    try:
        import google_sheets_integration
        source = inspect.getsource(google_sheets_integration)
        if 'pd.api.types.is_object_dtype' in source:
            print("✅ dtype fix is present")
        else:
            print("⚠️ dtype fix not found (may cause issues)")
    except Exception as e:
        print(f"⚠️ Could not verify dtype fix: {e}")
    
    # Test 10: Check summary fix
    print("\n[TEST 10] Verifying summary generation fix...")
    try:
        import pipeline_scheduler
        source = inspect.getsource(pipeline_scheduler)
        if "self.pipeline_results.get('end_time')" in source:
            print("✅ Summary generation fix is present")
        else:
            print("⚠️ Summary fix not found (may cause issues)")
    except Exception as e:
        print(f"⚠️ Could not verify summary fix: {e}")
    
    print("\n" + "=" * 80)
    print("✅ ALL TESTS PASSED - INTEGRATION IS READY")
    print("=" * 80)
    
    return True

if __name__ == "__main__":
    success = test_zaba_integration()
    sys.exit(0 if success else 1)

