#!/usr/bin/env python3
"""Verify server deployment"""

import inspect
from zaba import ZabaSearchExtractor

print("=" * 80)
print("SERVER DEPLOYMENT VERIFICATION")
print("=" * 80)

# Test 1: Import
print("\n[TEST 1] Importing zaba.py...")
try:
    extractor = ZabaSearchExtractor()
    print("✅ zaba.py imported and instantiated successfully")
except Exception as e:
    print(f"❌ Failed: {e}")
    exit(1)

# Test 2: Check method exists
print("\n[TEST 2] Checking process_csv_batch method...")
if hasattr(extractor, 'process_csv_batch'):
    print("✅ process_csv_batch method exists")
else:
    print("❌ process_csv_batch method NOT FOUND")
    exit(1)

# Test 3: Check signature
print("\n[TEST 3] Verifying method signature...")
sig = inspect.signature(extractor.process_csv_batch)
params = list(sig.parameters.keys())
expected = ['csv_path', 'output_path', 'start_record', 'end_record']

if params == expected:
    print(f"✅ Method signature correct: {params}")
else:
    print(f"❌ Signature mismatch!")
    print(f"   Expected: {expected}")
    print(f"   Got: {params}")
    exit(1)

# Test 4: Import pipeline
print("\n[TEST 4] Importing pipeline_scheduler...")
try:
    from pipeline_scheduler import BrowardLisPendensPipeline
    print("✅ pipeline_scheduler imported successfully")
except Exception as e:
    print(f"❌ Failed: {e}")
    exit(1)

# Test 5: Import google_sheets_integration
print("\n[TEST 5] Importing google_sheets_integration...")
try:
    from google_sheets_integration import GoogleSheetsIntegration
    print("✅ google_sheets_integration imported successfully")
except Exception as e:
    print(f"❌ Failed: {e}")
    exit(1)

print("\n" + "=" * 80)
print("✅ ALL DEPLOYMENT VERIFICATION TESTS PASSED")
print("=" * 80)
print("\nServer is ready for production!")

