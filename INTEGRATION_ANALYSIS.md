# Integration Analysis: zaba.py → Pipeline
**Date:** October 1, 2025  
**Objective:** Identify all changes needed to integrate `zaba.py` into the pipeline

---

## 🎯 EXECUTIVE SUMMARY

### **CRITICAL ISSUE FOUND:**

**`zaba.py` is INCOMPATIBLE with `pipeline_scheduler.py`** - Different method signatures!

### **The Problem:**

**Pipeline calls:**
```python
# pipeline_scheduler.py line 778
await self.zabasearch_extractor.process_csv_batch(
    csv_path=batch_input_file,
    output_path=batch_output_file,
    start_record=1,
    end_record=actual_batch_size
)
```

**Current script has:**
```python
# zabasearch_batch1_records_1_15.py line 1290
async def process_csv_batch(self, csv_path: str, output_path: Optional[str] = None, 
                           start_record: int = 1, end_record: Optional[int] = None):
```

**New script has:**
```python
# zaba.py line 1897
async def process_csv_with_sessions(self, csv_path: str):
    # NO output_path, start_record, end_record parameters!
```

---

## 🔍 DETAILED ANALYSIS

### **1. METHOD SIGNATURE MISMATCH**

#### **Pipeline Expects:**
- Method name: `process_csv_batch()`
- Parameters: `csv_path`, `output_path`, `start_record`, `end_record`
- Behavior: Process specific range of records, save to output_path

#### **zaba.py Provides:**
- Method name: `process_csv_with_sessions()`
- Parameters: `csv_path` only
- Behavior: Process ALL records, save to same file (in-place update)

**Impact:** 🔴 **CRITICAL** - Pipeline will crash with `AttributeError: 'ZabaSearchExtractor' object has no attribute 'process_csv_batch'`

---

### **2. BATCH PROCESSING LOGIC DIFFERENCE**

#### **Pipeline's Batching Strategy:**
```python
# pipeline_scheduler.py creates batches EXTERNALLY
for batch_num in range(total_batches):
    # Create batch-specific input file with subset of records
    batch_records = valid_records.iloc[start_idx:end_idx].copy()
    batch_records.to_csv(batch_input_file, index=False)
    
    # Call ZabaSearch to process this batch
    await self.zabasearch_extractor.process_csv_batch(
        csv_path=batch_input_file,      # Small file with 9 records
        output_path=batch_output_file,  # Separate output file
        start_record=1,
        end_record=actual_batch_size
    )
    
    # Merge batch results back into main file
```

**Pipeline's Approach:**
- Creates separate CSV files for each batch (9 records each)
- Calls ZabaSearch once per batch file
- Merges all batch results back into main file

#### **zaba.py's Batching Strategy:**
```python
# zaba.py does batching INTERNALLY
async def process_csv_with_sessions(self, csv_path: str):
    # Load ENTIRE CSV
    df = pd.read_csv(csv_path)
    
    # Find ALL records with addresses
    records_with_addresses = []  # Could be 100+ records
    
    # Process 1 record per session (internal loop)
    for session_num in range(total_sessions):
        session_records = remaining_records[session_start:session_end]
        # Create browser, process 1 record, close browser
        
    # Save results back to SAME file (in-place)
    df.to_csv(csv_path, index=False)
```

**zaba.py's Approach:**
- Loads entire CSV file (all records)
- Processes 1 record per browser session (internal loop)
- Saves results back to same file (in-place update)

**Impact:** 🟡 **MODERATE** - Different batching philosophies, but both work

---

### **3. OUTPUT FILE HANDLING**

#### **Pipeline Expects:**
```python
# Separate output file for each batch
batch_output_file = str(self.output_dir / f"batch_{batch_num + 1:02d}_output_{timestamp}.csv")

await self.zabasearch_extractor.process_csv_batch(
    csv_path=batch_input_file,
    output_path=batch_output_file  # Expects separate output file
)

# Check if batch was successful
if os.path.exists(batch_output_file):
    batch_df = pd.read_csv(batch_output_file)
    # Merge batch results back
```

#### **zaba.py Does:**
```python
# In-place update (saves to same file)
async def process_csv_with_sessions(self, csv_path: str):
    df = pd.read_csv(csv_path)
    # ... process records ...
    df.to_csv(csv_path, index=False)  # Overwrites input file
```

**Impact:** 🔴 **CRITICAL** - Pipeline expects separate output files, zaba.py overwrites input

---

### **4. CITY/STATE COLUMN USAGE**

#### **Pipeline Provides:**
```python
# pipeline_scheduler.py doesn't extract city/state from addresses
# It only has: DirectName_Address, IndirectName_Address
# NO DirectName_City, DirectName_State columns
```

#### **zaba.py Expects:**
```python
# zaba.py line 1917-1918
city_col = f"{prefix}_City"
state_col = f"{prefix}_State"

city = row.get(city_col, '')
state = row.get(state_col, '')

# Uses city and state for ZabaSearch search
final_city = record['city'] if record['city'] else 'HALLANDALE BEACH'
final_state = record['state'] if record['state'] else 'Florida'
```

**Impact:** 🟡 **MODERATE** - zaba.py will use defaults (HALLANDALE BEACH, Florida) if columns missing

---

### **5. PROXY HANDLING**

#### **Current Script:**
```python
# zabasearch_batch1_records_1_15.py - OPTIONAL proxy
if PROXY_AVAILABLE and is_proxy_enabled():
    proxy_dict = get_proxy_for_zabasearch()
    if proxy_dict:
        proxy_config = {...}
# Continues even if no proxy
```

#### **zaba.py:**
```python
# zaba.py line 2016-2031 - MANDATORY proxy
try:
    from proxy_manager import proxy_manager
    proxy = proxy_manager.get_proxy_for_zabasearch()
    if proxy:
        print(f"🔒 Using proxy: {proxy['server']}")
    else:
        print("❌ No proxy available - aborting session")
        return  # ABORTS if no proxy
except ImportError:
    print("❌ Proxy manager not available - aborting session")
    return  # ABORTS if no proxy
```

**Impact:** 🔴 **CRITICAL** - zaba.py will abort entire processing if proxy unavailable

---

### **6. CSV FORMAT HANDLER DEPENDENCY**

#### **zaba.py Imports:**
```python
# zaba.py line 26-31
try:
    from csv_format_handler import CSVFormatHandler
    print("✅ Enhanced CSV Format Handler loaded for intelligent address processing")
except ImportError as e:
    print(f"⚠️ CSV Format Handler not available: {e}")
    CSVFormatHandler = None
```

**Impact:** 🟢 **MINOR** - Optional import, gracefully handles missing module

---

## 📋 REQUIRED CHANGES

### **OPTION 1: MODIFY `zaba.py` TO MATCH PIPELINE (RECOMMENDED)**

Add `process_csv_batch()` method to `zaba.py` that matches pipeline's signature:

```python
async def process_csv_batch(self, csv_path: str, output_path: Optional[str] = None, 
                           start_record: int = 1, end_record: Optional[int] = None):
    """
    Process CSV batch for pipeline scheduler compatibility.
    This method wraps process_csv_with_sessions() to match pipeline's interface.
    """
    # Set output path to same as input if not provided
    if not output_path:
        output_path = csv_path
    
    # Load CSV
    df = pd.read_csv(csv_path)
    
    # Process the specified range (pipeline provides this)
    total_records = len(df)
    if end_record is None:
        end_record = total_records
    
    # Ensure bounds are valid
    start_record = max(1, start_record)
    end_record = min(end_record, total_records)
    
    print(f"✓ Processing records {start_record} to {end_record} of {total_records}")
    
    # Extract records for processing (same logic as current script)
    records_to_process = []
    for idx in range(start_record - 1, end_record):
        row = df.iloc[idx]
        
        # Process BOTH DirectName and IndirectName records
        for prefix in ['IndirectName', 'DirectName']:
            name_col = f"{prefix}_Cleaned"
            address_col = f"{prefix}_Address"
            city_col = f"{prefix}_City"  # May not exist
            state_col = f"{prefix}_State"  # May not exist
            type_col = f"{prefix}_Type"
            
            name = row.get(name_col, '')
            address = row.get(address_col, '')
            city = row.get(city_col, '') if city_col in df.columns else ''
            state = row.get(state_col, 'Florida') if state_col in df.columns else 'Florida'
            record_type = row.get(type_col, '')
            
            # Check if valid Person record with address
            if (name and address and pd.notna(name) and pd.notna(address) and
                str(name).strip() and str(address).strip() and
                record_type == 'Person'):
                
                # Check if already has phone
                phone_col = f"{prefix}_Phone_Primary"
                if phone_col in df.columns and pd.notna(row.get(phone_col)) and str(row.get(phone_col)).strip():
                    print(f"  ⏭️ Skipping {name} - already has phone number")
                    break
                
                records_to_process.append({
                    'name': str(name).strip(),
                    'address': str(address).strip(),
                    'city': str(city).strip() if city else '',
                    'state': str(state).strip() if state else 'Florida',
                    'row_index': idx,
                    'column_prefix': prefix
                })
                break
    
    print(f"✓ Found {len(records_to_process)} records to process")
    
    # Add phone columns if they don't exist
    phone_columns = ['_Phone_Primary', '_Phone_Secondary', '_Phone_All', '_Address_Match']
    for record in records_to_process:
        prefix = record['column_prefix']
        for col in phone_columns:
            col_name = f"{prefix}{col}"
            if col_name not in df.columns:
                df[col_name] = ''
    
    # Process each record (1 per session) - SAME AS process_csv_with_sessions
    total_success = 0
    
    for record_num, record in enumerate(records_to_process, 1):
        print(f"\n{'='*80}")
        print(f"🔄 RECORD #{record_num}/{len(records_to_process)}")
        print(f"{'='*80}")
        
        # Get proxy (MANDATORY)
        proxy = None
        try:
            from proxy_manager import proxy_manager
            proxy = proxy_manager.get_proxy_for_zabasearch()
            if not proxy:
                print("❌ No proxy available - aborting batch")
                break  # Abort entire batch if no proxy
        except Exception as e:
            print(f"❌ Proxy error: {e} - aborting batch")
            break
        
        # Create browser session for this record
        async with async_playwright() as playwright:
            browser, context = await self.create_stealth_browser(playwright, browser_type='firefox', proxy=proxy)
            page = await context.new_page()
            
            try:
                # Parse name
                name_parts = record['name'].split()
                if len(name_parts) < 2:
                    continue
                
                first_name = name_parts[0]
                last_name = name_parts[1]
                
                # Search ZabaSearch
                person_data = await self.search_person(
                    page, first_name, last_name, 
                    record['address'], 
                    record.get('city', ''), 
                    record.get('state', 'Florida')
                )
                
                if person_data:
                    # Update CSV with phone data
                    row_idx = record['row_index']
                    prefix = record['column_prefix']
                    
                    df.loc[row_idx, f"{prefix}_Phone_Primary"] = str(person_data.get('primary_phone', ''))
                    df.loc[row_idx, f"{prefix}_Phone_Secondary"] = str(person_data.get('secondary_phone', ''))
                    df.loc[row_idx, f"{prefix}_Phone_All"] = str(', '.join(person_data.get('all_phones', [])))
                    df.loc[row_idx, f"{prefix}_Address_Match"] = str(person_data.get('matched_address', ''))
                    
                    total_success += 1
                    
            finally:
                # Cleanup
                await page.close()
                await context.close()
                await browser.close()
                gc.collect()
    
    # Save to output file (NOT in-place)
    df.to_csv(output_path, index=False)
    print(f"💾 Batch results saved to: {output_path}")
    print(f"✅ Successfully processed {total_success}/{len(records_to_process)} records")
```

**Changes to zaba.py:**
1. ✅ Add `process_csv_batch()` method with pipeline-compatible signature
2. ✅ Save to `output_path` instead of overwriting input
3. ✅ Handle missing City/State columns gracefully
4. ✅ Keep mandatory proxy check (abort batch if no proxy)
5. ✅ Keep all bandwidth optimizations and Firefox-first approach

---

### **OPTION 2: MODIFY PIPELINE TO USE `zaba.py` AS-IS (NOT RECOMMENDED)**

Would require rewriting pipeline's entire batching logic - too risky and complex.

---

## ✅ FINAL RECOMMENDATION

### **ACTION PLAN:**

1. **Add `process_csv_batch()` method to `zaba.py`** (see code above)
2. **Keep `process_csv_with_sessions()` for standalone use**
3. **Test locally first:**
   ```bash
   python zaba.py --input test_file.csv --show-browser
   ```
4. **Deploy to server:**
   ```bash
   scp -i "C:/Users/my notebook/.ssh/vultr_new" zaba.py root@45.76.254.12:/root/cron-job/
   ```
5. **Update pipeline import:**
   ```python
   # pipeline_scheduler.py line 64
   from zaba import ZabaSearchExtractor  # Change from zabasearch_batch1_records_1_15
   ```
6. **Test full pipeline run**

---

## 📊 COMPATIBILITY MATRIX

| Feature | Current Script | zaba.py | Compatible? | Fix Needed? |
|---------|---------------|---------|-------------|-------------|
| Method name | `process_csv_batch()` | `process_csv_with_sessions()` | ❌ NO | ✅ Add method |
| Parameters | 4 params | 1 param | ❌ NO | ✅ Add method |
| Output handling | Separate file | In-place | ❌ NO | ✅ Add method |
| City/State columns | Not used | Used (optional) | ✅ YES | ✅ Graceful fallback |
| Proxy handling | Optional | Mandatory | ⚠️ PARTIAL | ✅ Keep mandatory |
| Bandwidth optimization | None | 85-95% | ✅ YES | ✅ Keep |
| Firefox-first | No | Yes | ✅ YES | ✅ Keep |
| CSV Format Handler | Not used | Optional | ✅ YES | ✅ Keep |

---

## 🎯 SUMMARY

**To integrate `zaba.py` into the pipeline:**

1. ✅ **Add `process_csv_batch()` method** - 100 lines of code
2. ✅ **Keep all optimizations** - Bandwidth, Firefox, stealth
3. ✅ **Handle missing columns** - City/State graceful fallback
4. ✅ **Keep mandatory proxy** - Better than wasting time
5. ✅ **Test thoroughly** - Local first, then server

**Estimated Time:** 30 minutes to implement + 1 hour testing

**Risk Level:** 🟢 LOW (adding method, not changing existing code)

