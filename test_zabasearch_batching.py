#!/usr/bin/env python3
"""
Test script to verify ZabaSearch batching logic in the pipeline
"""
import asyncio
import pandas as pd
import os
from datetime import datetime

async def test_zabasearch_batch_logic():
    """Test the ZabaSearch batching without actually running the browser"""
    print("🧪 TESTING ZABASEARCH BATCH LOGIC")
    print("=" * 50)
    
    # Create test data that simulates the Broward pipeline output
    test_data = {
        'Lis Pendens File Number': [f'LP{i+100}' for i in range(32)],  # 32 records
        'DirectName_Cleaned': [f'Person {i+1}' for i in range(32)],
        'DirectName_Type': ['Person'] * 32,
        'DirectName_Address': [f'{100+i} Test St City{i%3+1}, FL 3300{i%10}' for i in range(32)],
        'IndirectName_Cleaned': [''] * 32,
        'IndirectName_Type': [''] * 32,
        'IndirectName_Address': [''] * 32
    }
    
    # Create test CSV
    df = pd.DataFrame(test_data)
    test_csv = 'test_zabasearch_input.csv'
    df.to_csv(test_csv, index=False)
    print(f"✅ Created test CSV: {test_csv} with {len(df)} records")
    
    # Simulate the pipeline's batching logic
    batch_size = 20
    valid_records = []
    
    # Find records with addresses (same logic as pipeline)
    for _, row in df.iterrows():
        direct_name = row.get('DirectName_Cleaned', '')
        direct_address = row.get('DirectName_Address', '')
        
        if (direct_name and row.get('DirectName_Type') == 'Person' and 
            direct_address and pd.notna(direct_address) and str(direct_address).strip()):
            valid_records.append({
                'name': direct_name,
                'address': str(direct_address).strip(),
                'row_index': row.name
            })
    
    print(f"✅ Found {len(valid_records)} valid records with addresses")
    
    # Calculate batches
    total_records = len(valid_records)
    total_batches = (total_records + batch_size - 1) // batch_size
    
    print(f"📊 Batch calculation:")
    print(f"   - Total records: {total_records}")
    print(f"   - Batch size: {batch_size}")
    print(f"   - Total batches: {total_batches}")
    
    # Simulate batch processing
    for batch_num in range(total_batches):
        start_idx = batch_num * batch_size + 1  # 1-based indexing
        end_idx = min(start_idx + batch_size - 1, total_records)
        
        print(f"\n🔄 Batch {batch_num + 1}/{total_batches}:")
        print(f"   - Records: {start_idx}-{end_idx}")
        print(f"   - Batch size: {end_idx - start_idx + 1}")
        
        # Simulate ZabaSearch processing time
        await asyncio.sleep(0.5)  # Quick simulation
        
        # Simulate delay between batches (but faster for testing)
        if batch_num < total_batches - 1:
            print(f"   ⏳ In real pipeline: would wait 5 minutes before next batch")
            print(f"   ⏳ Test delay: 1 second...")
            await asyncio.sleep(1)
    
    print(f"\n✅ ZabaSearch batch simulation completed!")
    print(f"📈 Would process {total_records} records in {total_batches} batches of {batch_size}")
    print(f"🕐 Total time with 5-minute delays: ~{(total_batches-1)*5} minutes")
    
    # Clean up
    if os.path.exists(test_csv):
        os.remove(test_csv)
        print(f"🧹 Cleaned up test file: {test_csv}")

if __name__ == "__main__":
    asyncio.run(test_zabasearch_batch_logic())
