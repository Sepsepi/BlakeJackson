import pandas as pd
import re
from collections import Counter
import os
import asyncio
import sys

# Import our address extractor
try:
    from fast_address_extractor import process_addresses_fast
    ADDRESS_EXTRACTOR_AVAILABLE = True
    print("✅ Address extractor module loaded successfully")
except ImportError as e:
    ADDRESS_EXTRACTOR_AVAILABLE = False
    print(f"⚠️ Address extractor not available: {e}")
    print("   Address extraction will be skipped")

def is_person_name(name):
    """
    Determine if a name represents a person rather than a company/organization
    """
    if pd.isna(name):
        return False
        
    # Common business/organization indicators
    business_indicators = [
        'LLC', 'INC', 'CORP', 'LTD', 'COMPANY', 'ASSOCIATION', 'ASSN', 'BANK', 'TRUST',
        'MORTGAGE', 'FINANCIAL', 'CREDIT UNION', 'HOLDINGS', 'FUND', 'CAPITAL',
        'PROPERTIES', 'REALTY', 'INVESTMENTS', 'GROUP', 'SERVICES', 'MANAGEMENT',
        'CONDOMINIUM', 'CONDO', 'HOMEOWNERS', 'MASTER', 'COMMUNITY', 'VILLAGE',
        'ESTATES', 'GARDENS', 'CLUB', 'CENTER', 'PRODUCTS', 'AVIATION', 'ASSETS',
        'LENDING', 'LOAN', 'FEDERAL', 'NATIONAL', 'SAVINGS', 'FINANCING', 'FSB',
        'TOWNHOMES', 'TOWNHOUSES', 'APARTMENTS', 'CONDOS', 'RANCHES', 'POINT',
        'COLONNADES', 'MANORS', 'VILLAS', 'IMPERIAL', 'ROYAL', 'CYPRESS',
        'INVERRARY', 'PELICAN', 'PLANTATION', 'RESIDENCE', 'MAINLANDS', 'TERNBRIDGE'
    ]
    
    # Check if any business indicator is in the name
    name_upper = str(name).upper()
    for indicator in business_indicators:
        if indicator in name_upper:
            return False
    
    # If it contains a comma, it's likely in "LAST,FIRST" format (person)
    if ',' in str(name):
        return True
    
    # Additional checks for person names
    words = name_upper.split()
    if len(words) >= 2:
        # Check if it looks like a person name (no obvious business words)
        return True
    
    return False

def clean_person_name(name):
    """
    Clean a person's name by removing suffixes and formatting properly
    """
    if pd.isna(name):
        return None
        
    # Remove quotes if present
    name = str(name).strip('"')
    
    # Common suffixes to remove
    suffixes_to_remove = [
        'JR', 'SR', 'II', 'III', 'IV', 'V', 'VI', 'VII', 'VIII', 'IX', 'X',
        'ESQ', 'MD', 'DDS', 'PHD', 'DO', 'DVM'
    ]
    
    # Split by comma if present (usually LAST,FIRST format)
    if ',' in name:
        parts = name.split(',')
        if len(parts) >= 2:
            last_name = parts[0].strip()
            first_part = parts[1].strip()
            
            # Clean the first part (may contain first name + middle initial/name + suffix)
            first_words = first_part.split()
            
            # Remove suffixes from the end
            while first_words and first_words[-1].upper().rstrip('.') in suffixes_to_remove:
                first_words.pop()
            
            # Take only first name (first word) and ignore middle initials/names
            if first_words:
                first_name = first_words[0]
                # Remove periods from initials
                first_name = first_name.rstrip('.')
                
                # Capitalize properly
                first_name = first_name.capitalize()
                last_name = last_name.capitalize()
                
                return f"{first_name} {last_name}"
    
    # If no comma, try to parse as "FIRST MIDDLE LAST" or "FIRST LAST"
    else:
        words = name.split()
        if len(words) >= 2:
            # Remove suffixes from the end
            while words and words[-1].upper().rstrip('.') in suffixes_to_remove:
                words.pop()
            
            if len(words) >= 2:
                # Take first and last word, ignore middle names/initials
                first_name = words[0].rstrip('.').capitalize()
                last_name = words[-1].rstrip('.').capitalize()
                return f"{first_name} {last_name}"
    
    # If we can't parse it properly, return None
    return None

def process_lis_pendens_csv(input_file_path, silent_mode=False):
    """
    Complete processing of Lis Pendens CSV file - adds cleaned names and generates reports
    
    Args:
        input_file_path: Path to the input CSV file
        silent_mode: If True, suppress most console output (for cron jobs)
    """
    if not silent_mode:
        print(f"Processing CSV file: {input_file_path}")
        print("=" * 60)
    
    # Check if file exists
    if not os.path.exists(input_file_path):
        if not silent_mode:
            print(f"ERROR: File not found: {input_file_path}")
        return None
    
    # Read the original CSV file
    try:
        df = pd.read_csv(input_file_path)
        if not silent_mode:
            print(f"✓ Successfully loaded {len(df)} rows from CSV file")
    except Exception as e:
        if not silent_mode:
            print(f"ERROR: Could not read CSV file: {e}")
        return None
    
    # Create new columns for cleaned names
    df['DirectName_Cleaned'] = ''
    df['IndirectName_Cleaned'] = ''
    df['DirectName_Type'] = ''
    df['IndirectName_Type'] = ''
    
    if not silent_mode:
        print("✓ Processing names and adding cleaned columns...")
    
    # Counters for statistics
    direct_persons = 0
    direct_businesses = 0
    indirect_persons = 0
    indirect_businesses = 0
    
    # Collect all person names for reporting
    all_person_names = []
    
    # Process each row
    for index, row in df.iterrows():
        # Process DirectName
        if pd.notna(row['DirectName']):
            direct_name = str(row['DirectName'])
            if is_person_name(direct_name):
                cleaned = clean_person_name(direct_name)
                if cleaned:
                    df.at[index, 'DirectName_Cleaned'] = cleaned
                    df.at[index, 'DirectName_Type'] = 'Person'
                    all_person_names.append(cleaned)
                    direct_persons += 1
                else:
                    df.at[index, 'DirectName_Cleaned'] = ''
                    df.at[index, 'DirectName_Type'] = 'Person (unparseable)'
                    direct_persons += 1
            else:
                df.at[index, 'DirectName_Cleaned'] = ''
                df.at[index, 'DirectName_Type'] = 'Business/Organization'
                direct_businesses += 1
        
        # Process IndirectName
        if pd.notna(row['IndirectName']):
            indirect_name = str(row['IndirectName'])
            if is_person_name(indirect_name):
                cleaned = clean_person_name(indirect_name)
                if cleaned:
                    df.at[index, 'IndirectName_Cleaned'] = cleaned
                    df.at[index, 'IndirectName_Type'] = 'Person'
                    all_person_names.append(cleaned)
                    indirect_persons += 1
                else:
                    df.at[index, 'IndirectName_Cleaned'] = ''
                    df.at[index, 'IndirectName_Type'] = 'Person (unparseable)'
                    indirect_persons += 1
            else:
                df.at[index, 'IndirectName_Cleaned'] = ''
                df.at[index, 'IndirectName_Type'] = 'Business/Organization'
                indirect_businesses += 1
    
    # Generate output file paths
    base_dir = os.path.dirname(input_file_path)
    base_name = os.path.splitext(os.path.basename(input_file_path))[0]
    
    output_csv = os.path.join(base_dir, f"{base_name}_processed.csv")
    person_names_file = os.path.join(base_dir, "cleaned_person_names.txt")
    business_names_file = os.path.join(base_dir, "business_names.txt")
    report_file = os.path.join(base_dir, "person_names_report.csv")
    
    # Save the updated CSV
    try:
        df.to_csv(output_csv, index=False)
        if not silent_mode:
            print(f"✓ Main processed file saved: {output_csv}")
    except Exception as e:
        if not silent_mode:
            print(f"ERROR: Could not save processed CSV: {e}")
        return None
    
    # Generate statistics
    if not silent_mode:
        print("\n" + "=" * 60)
        print("PROCESSING SUMMARY")
        print("=" * 60)
        print(f"Total rows processed: {len(df)}")
        print(f"DirectName column:")
        print(f"  - Persons: {direct_persons}")
        print(f"  - Businesses: {direct_businesses}")
        print(f"IndirectName column:")
        print(f"  - Persons: {indirect_persons}")
        print(f"  - Businesses: {indirect_businesses}")
    
    # Process unique names
    unique_person_names = sorted(list(set(all_person_names)))
    name_counts = Counter(all_person_names)
    
    if not silent_mode:
        print(f"\nUnique person names found: {len(unique_person_names)}")
        print(f"Total person name occurrences: {len(all_person_names)}")
    
    # Save individual person names file
    try:
        with open(person_names_file, 'w', encoding='utf-8') as f:
            f.write("Cleaned Person Names\n")
            f.write("===================\n\n")
            for name in unique_person_names:
                f.write(f"{name}\n")
        if not silent_mode:
            print(f"✓ Person names list saved: {person_names_file}")
    except Exception as e:
        if not silent_mode:
            print(f"WARNING: Could not save person names file: {e}")
    
    # Save business names file
    try:
        business_names = []
        for _, row in df.iterrows():
            if row['DirectName_Type'] == 'Business/Organization':
                business_names.append(str(row['DirectName']).strip('"'))
            if row['IndirectName_Type'] == 'Business/Organization':
                business_names.append(str(row['IndirectName']).strip('"'))
        
        unique_business_names = sorted(list(set(business_names)))
        
        with open(business_names_file, 'w', encoding='utf-8') as f:
            f.write("Business/Organization Names\n")
            f.write("==========================\n\n")
            for name in unique_business_names:
                f.write(f"{name}\n")
        if not silent_mode:
            print(f"✓ Business names list saved: {business_names_file}")
    except Exception as e:
        if not silent_mode:
            print(f"WARNING: Could not save business names file: {e}")
    
    # Create detailed person names report
    try:
        person_data = []
        for name in unique_person_names:
            count = name_counts[name]
            person_data.append({'Name': name, 'Frequency': count})
        
        person_df = pd.DataFrame(person_data).sort_values(['Frequency', 'Name'], ascending=[False, True])
        person_df.to_csv(report_file, index=False)
        if not silent_mode:
            print(f"✓ Person names report saved: {report_file}")
    except Exception as e:
        if not silent_mode:
            print(f"WARNING: Could not save person names report: {e}")
    
    # Show examples and summary only if not in silent mode
    if not silent_mode:
        # Show examples
        print("\n" + "=" * 60)
        print("CLEANING EXAMPLES")
        print("=" * 60)
        
        # Show examples of DirectName cleaning
        direct_examples = df[(df['DirectName_Type'] == 'Person') & (df['DirectName_Cleaned'] != '')].head(5)
        if not direct_examples.empty:
            print("DirectName examples:")
            for _, row in direct_examples.iterrows():
                print(f"  {row['DirectName']} → {row['DirectName_Cleaned']}")
        
        # Show examples of IndirectName cleaning
        indirect_examples = df[(df['IndirectName_Type'] == 'Person') & (df['IndirectName_Cleaned'] != '')].head(5)
        if not indirect_examples.empty:
            print("\nIndirectName examples:")
            for _, row in indirect_examples.iterrows():
                print(f"  {row['IndirectName']} → {row['IndirectName_Cleaned']}")
        
        # Show most frequent names
        if name_counts:
            print(f"\nMost frequent person names:")
            for name, count in name_counts.most_common(10):
                print(f"  {count:2d}x {name}")
        
        print("\n" + "=" * 60)
        print("FILES CREATED")
        print("=" * 60)
        print(f"1. {output_csv}")
        print(f"   - Main file with original data + cleaned name columns")
        print(f"2. {person_names_file}")
        print(f"   - List of {len(unique_person_names)} unique cleaned person names")
        print(f"3. {business_names_file}")
        print(f"   - List of business/organization names")
        print(f"4. {report_file}")
        print(f"   - Detailed person names report with frequencies")
        
        print("\n" + "=" * 60)
        print("NEW COLUMNS ADDED TO MAIN FILE")
        print("=" * 60)
        print("- DirectName_Cleaned: Cleaned person names from DirectName column")
        print("- IndirectName_Cleaned: Cleaned person names from IndirectName column")
        print("- DirectName_Type: Type classification (Person/Business)")
        print("- IndirectName_Type: Type classification (Person/Business)")
        
        print("\n✓ Processing complete!")
    
    return output_csv

def main():
    """
    Main function to process the Lis Pendens CSV file and extract addresses
    """
    # Look for any CSV file with 'broward' and 'lis_pendens' in the name in the current directory
    current_dir = os.getcwd()
    csv_files = []
    
    # Search for relevant CSV files
    for file in os.listdir(current_dir):
        if (file.lower().endswith('.csv') and 
            ('broward' in file.lower() or 'lis_pendens' in file.lower() or 'lispend' in file.lower())):
            csv_files.append(os.path.join(current_dir, file))
    
    # Default file path if no files found
    default_csv_file = r"c:\Users\my notebook\Desktop\BlakeJackson\LisPendens_BrowardCounty_July7-14_2025.csv"
    
    print("LIS PENDENS COMPLETE PROCESSOR WITH ADDRESS EXTRACTION")
    print("=" * 80)
    print("This script will:")
    print("1. Extract person names from DirectName and IndirectName columns")
    print("2. Clean names by removing suffixes (JR, M, III, etc.)")
    print("3. Convert format from 'LAST,FIRST M' to 'First Last'")
    print("4. Separate person names from business names")
    print("5. Add cleaned names to the original file")
    print("6. Generate summary reports")
    if ADDRESS_EXTRACTOR_AVAILABLE:
        print("7. 🏠 Extract property addresses for person names")
        print("8. 🎯 Create final CSV with names AND addresses")
    else:
        print("7. ⚠️ Address extraction skipped (module not available)")
    print("=" * 80)
    
    # Determine which file to use
    csv_file = None
    
    if csv_files:
        if len(csv_files) == 1:
            csv_file = csv_files[0]
            print(f"Found CSV file: {csv_file}")
        else:
            print(f"Found {len(csv_files)} potential CSV files:")
            for i, file in enumerate(csv_files):
                print(f"  {i+1}. {os.path.basename(file)}")
            
            try:
                choice = input(f"Select file (1-{len(csv_files)}) or press Enter for most recent: ").strip()
                if choice:
                    csv_file = csv_files[int(choice) - 1]
                else:
                    # Use the most recent file
                    csv_file = max(csv_files, key=os.path.getmtime)
                    print(f"Using most recent file: {os.path.basename(csv_file)}")
            except (ValueError, IndexError):
                csv_file = csv_files[0]
                print(f"Invalid choice, using: {os.path.basename(csv_file)}")
    
    # If no files found, check default location or prompt user
    if not csv_file:
        if os.path.exists(default_csv_file):
            csv_file = default_csv_file
            print(f"Using default file: {csv_file}")
        else:
            print("No relevant CSV files found in current directory.")
            csv_file = input("Enter the full path to your CSV file: ").strip()
            if csv_file.startswith('"') and csv_file.endswith('"'):
                csv_file = csv_file[1:-1]  # Remove quotes if present
    
    # Step 1: Process the names
    print(f"\n🔄 STEP 1: Processing LIS PENDENS names...")
    output_file = process_lis_pendens_csv(csv_file, silent_mode=False)
    
    if not output_file:
        print("\n❌ Name processing failed. Please check the error messages above.")
        return
    
    print(f"\n✅ STEP 1 COMPLETE: Names processed successfully!")
    print(f"📄 Processed file: {output_file}")
    
    # Step 2: Extract addresses if module is available
    if ADDRESS_EXTRACTOR_AVAILABLE:
        print(f"\n🔄 STEP 2: Extracting property addresses...")
        
        # Ask user about address extraction options
        print("\nAddress extraction options:")
        print("1. Extract addresses for ALL person names (recommended)")
        print("2. Extract addresses for first 15 names only (quick test)")
        print("3. Skip address extraction")
        
        choice = input("Choose option (1-3) [default: 1]: ").strip()
        
        if choice == "3":
            print("⏭️ Address extraction skipped by user choice")
        else:
            max_names = None if choice == "1" else 15
            max_desc = "ALL" if choice == "1" else "15"
            
            print(f"🚀 Starting address extraction for {max_desc} person names...")
            print("⏳ This may take several minutes depending on the number of names...")
            
            try:
                # Run the address extractor
                if choice == "1":
                    # Extract all names - don't pass max_names parameter, use default
                    final_output = asyncio.run(process_addresses_fast(
                        output_file, 
                        max_names=None,  # Process all names
                        headless=True  # Always run headless for automatic processing
                    ))
                else:
                    # Extract limited number of names
                    final_output = asyncio.run(process_addresses_fast(
                        output_file, 
                        max_names=15, 
                        headless=True  # Always run headless for automatic processing
                    ))
                
                if final_output:
                    print(f"\n✅ STEP 2 COMPLETE: Address extraction successful!")
                    print(f"🏠 Final file with addresses: {final_output}")
                    
                    # Show summary stats
                    try:
                        df = pd.read_csv(final_output)
                        direct_found = df['DirectName_Address'].notna().sum()
                        indirect_found = df['IndirectName_Address'].notna().sum()
                        total_found = (df['DirectName_Address'].notna() | df['IndirectName_Address'].notna()).sum()
                        total_persons = len(df[df['IndirectName_Type'] == 'Person']) + len(df[df['DirectName_Type'] == 'Person'])
                        
                        print(f"\n📊 ADDRESS EXTRACTION SUMMARY:")
                        print(f"   💼 DirectName addresses found: {direct_found}")
                        print(f"   👤 IndirectName addresses found: {indirect_found}")
                        print(f"   📍 Total addresses found: {total_found}")
                        print(f"   📈 Success rate: {total_found/total_persons*100:.1f}% ({total_found}/{total_persons} people)")
                        
                    except Exception as e:
                        print(f"⚠️ Could not generate summary stats: {e}")
                        
                else:
                    print(f"\n❌ STEP 2 FAILED: Address extraction encountered errors")
                    print(f"📄 You can still use the processed names file: {output_file}")
                    
            except Exception as e:
                print(f"\n❌ STEP 2 ERROR: {e}")
                print(f"📄 You can still use the processed names file: {output_file}")
    else:
        print(f"\n⏭️ STEP 2 SKIPPED: Address extractor module not available")
        print(f"📄 Final output: {output_file}")
    
    print(f"\n🎉 COMPLETE! All processing finished.")
    if ADDRESS_EXTRACTOR_AVAILABLE and 'final_output' in locals() and final_output:
        print(f"🏆 Your final file with names AND addresses: {os.path.basename(final_output)}")
    else:
        print(f"🏆 Your processed names file: {os.path.basename(output_file)}")

if __name__ == "__main__":
    main()
