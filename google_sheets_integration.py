#!/usr/bin/env python3
"""
Google Sheets Integration for Broward Lis Pendens Pipeline
==========================================================

Automatically uploads pipeline results to Google Sheets with date-named worksheets.
Supports OAuth2 authentication and service account authentication.

Features:
- Creates new worksheet with current date name
- Uploads CSV data to Google Sheets
- Handles authentication via service account JSON
- Environment variable configuration
- Error handling and logging

Author: Blake Jackson
Date: August 7, 2025
"""

import os
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, List
import pandas as pd

try:
    import gspread
    from google.oauth2.service_account import Credentials
    GSPREAD_AVAILABLE = True
except ImportError:
    GSPREAD_AVAILABLE = False

logger = logging.getLogger(__name__)

class GoogleSheetsIntegration:
    def __init__(self):
        """Initialize Google Sheets integration with environment variables"""

        # Configuration from environment variables
        self.spreadsheet_id = os.environ.get('GOOGLE_SPREADSHEET_ID')
        self.service_account_file = os.environ.get('GOOGLE_SERVICE_ACCOUNT_JSON')
        self.spreadsheet_name = os.environ.get('GOOGLE_SPREADSHEET_NAME', 'Broward Lis Pendens Data')

        self.client = None
        self.spreadsheet = None

        # Check if Google Sheets integration is available
        if not GSPREAD_AVAILABLE:
            logger.warning("Google Sheets integration disabled - gspread not installed")
            logger.info("Install with: pip install gspread google-auth google-auth-oauthlib")
            self.enabled = False
            return

        # Validation
        if not self.service_account_file:
            logger.warning("GOOGLE_SERVICE_ACCOUNT_JSON not set - Google Sheets integration disabled")
            self.enabled = False
            return

        if not os.path.exists(self.service_account_file):
            logger.warning(f"Service account file not found: {self.service_account_file}")
            self.enabled = False
            return

        if not (self.spreadsheet_id or self.spreadsheet_name):
            logger.warning("Neither GOOGLE_SPREADSHEET_ID nor GOOGLE_SPREADSHEET_NAME set")
            self.enabled = False
            return

        # Initialize Google Sheets client
        try:
            self._initialize_client()
            self.enabled = True
            logger.info("Google Sheets integration enabled successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Google Sheets client: {e}")
            self.enabled = False

    def _initialize_client(self):
        """Initialize the Google Sheets client"""

        # Define the scope
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]

        # Load credentials
        credentials = Credentials.from_service_account_file(
            self.service_account_file,
            scopes=scopes
        )

        # Create client
        self.client = gspread.authorize(credentials)

        # Open spreadsheet
        if self.spreadsheet_id:
            logger.info(f"Opening spreadsheet by ID: {self.spreadsheet_id}")
            self.spreadsheet = self.client.open_by_key(self.spreadsheet_id)
        else:
            logger.info(f"Opening spreadsheet by name: {self.spreadsheet_name}")
            self.spreadsheet = self.client.open(self.spreadsheet_name)

        logger.info(f"Connected to spreadsheet: {self.spreadsheet.title}")

    def create_dated_worksheet(self, date_str: Optional[str] = None, include_time: bool = True) -> str:
        """Create a new worksheet with current date as name"""

        if not self.enabled:
            logger.warning("Google Sheets integration disabled")
            return ""

        try:
            # Generate worksheet name with current date
            if not date_str:
                now = datetime.now()
                if include_time:
                    date_str = now.strftime('%B %d - %H%M')  # e.g. "August 10 - 1430"
                else:
                    date_str = now.strftime('%B %d')  # e.g. "August 10"

            logger.info(f"Creating worksheet: {date_str}")

            # Check if worksheet already exists
            try:
                existing_worksheet = self.spreadsheet.worksheet(date_str)
                logger.warning(f"Worksheet '{date_str}' already exists - will clear and reuse")
                existing_worksheet.clear()
                return date_str
            except gspread.WorksheetNotFound:
                # Worksheet doesn't exist, create it
                pass

            # Create new worksheet
            worksheet = self.spreadsheet.add_worksheet(
                title=date_str,
                rows=1000,  # Initial size
                cols=50     # Should be enough for our data
            )

            logger.info(f"✅ Created worksheet: {date_str}")
            return date_str

        except Exception as e:
            logger.error(f"Failed to create worksheet: {e}")
            return ""

    def upload_csv_to_worksheet(self,
                               csv_file_path: str,
                               worksheet_name: Optional[str] = None,
                               clean_column_names: bool = True) -> bool:
        """Upload CSV data to a specific worksheet"""

        if not self.enabled:
            logger.warning("Google Sheets integration disabled")
            return False

        if not os.path.exists(csv_file_path):
            logger.error(f"CSV file not found: {csv_file_path}")
            return False

        try:
            # Read CSV file
            logger.info(f"Reading CSV file: {csv_file_path}")
            df = pd.read_csv(csv_file_path)
            logger.info(f"Loaded {len(df)} records with {len(df.columns)} columns")

            # Clean column names if requested
            if clean_column_names:
                df = self._clean_dataframe_for_sheets(df)

            # Create worksheet if name provided
            if worksheet_name:
                actual_worksheet_name = self.create_dated_worksheet(worksheet_name)
                if not actual_worksheet_name:
                    return False
            else:
                # Create worksheet with current date
                actual_worksheet_name = self.create_dated_worksheet()
                if not actual_worksheet_name:
                    return False

            # Get the worksheet
            worksheet = self.spreadsheet.worksheet(actual_worksheet_name)

            # Clear existing content
            worksheet.clear()

            # Prepare data for upload (convert to list of lists)
            # Include headers
            headers = df.columns.tolist()

            # Convert data to strings to avoid dtype issues
            data_rows = []
            for _, row in df.iterrows():
                data_rows.append([str(val) if pd.notna(val) else '' for val in row])

            # Combine headers and data
            data_to_upload = [headers] + data_rows

            logger.info(f"Uploading {len(data_to_upload)} rows to worksheet '{actual_worksheet_name}'...")

            # Upload data in batches to avoid timeout
            batch_size = 100
            total_rows = len(data_to_upload)

            for i in range(0, total_rows, batch_size):
                batch_end = min(i + batch_size, total_rows)
                batch_data = data_to_upload[i:batch_end]

                # Calculate range
                start_row = i + 1
                end_row = batch_end
                end_col = len(df.columns)

                range_name = f"A{start_row}:{chr(64 + end_col)}{end_row}"

                logger.info(f"Uploading batch {i//batch_size + 1}: rows {start_row}-{end_row}")

                # Update range
                worksheet.update(range_name, batch_data)

            # Format header row
            self._format_header_row(worksheet, len(df.columns))

            logger.info(f"✅ Successfully uploaded {len(df)} records to worksheet '{actual_worksheet_name}'")

            # Get the spreadsheet URL for easy access
            spreadsheet_url = f"https://docs.google.com/spreadsheets/d/{self.spreadsheet.id}/edit#gid={worksheet.id}"
            logger.info(f"📊 View results: {spreadsheet_url}")

            return True

        except Exception as e:
            logger.error(f"Failed to upload CSV to worksheet: {e}")
            return False

    def _clean_dataframe_for_sheets(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean DataFrame for Google Sheets upload - remove vendor references"""

        logger.info("Cleaning column names and data for Google Sheets...")

        # Create a copy to avoid modifying original
        clean_df = df.copy()

        # Column name mappings to remove vendor references
        column_mappings = {}

        for col in clean_df.columns:
            new_col = col

            # Remove vendor prefixes/references
            if 'Radaris_' in col:
                new_col = col.replace('Radaris_', '')
            elif 'ZabaSearch_' in col:
                new_col = col.replace('ZabaSearch_', '')
            elif col.startswith('DirectName_'):
                new_col = col.replace('DirectName_', '')
            elif col.startswith('IndirectName_'):
                new_col = col.replace('IndirectName_', '')

            # Clean up common patterns
            new_col = new_col.replace('_Status', '_Search_Status')
            new_col = new_col.replace('_Profile_URL', '_Source_URL')

            # Store mapping if changed
            if new_col != col:
                column_mappings[col] = new_col

        # Apply column name changes
        if column_mappings:
            logger.info(f"Renaming {len(column_mappings)} columns to remove vendor references")
            clean_df = clean_df.rename(columns=column_mappings)

            for old_col, new_col in column_mappings.items():
                logger.debug(f"  {old_col} → {new_col}")

        # Remove unnecessary columns that might contain vendor references or useless data
        columns_to_remove = []
        for col in clean_df.columns:
            # Remove columns that are clearly debugging/internal
            if any(pattern in col.lower() for pattern in [
                'profile_url', 'debug', 'test', '_all', 'secondary'
            ]):
                # Keep only primary phone numbers as requested
                if 'secondary' in col.lower() or '_all' in col.lower():
                    columns_to_remove.append(col)
                    logger.debug(f"  Removing non-primary column: {col}")

        # Remove identified columns
        if columns_to_remove:
            clean_df = clean_df.drop(columns=columns_to_remove)
            logger.info(f"Removed {len(columns_to_remove)} non-essential columns")

        # Clean up data values
        for col in clean_df.columns:
            # Check if column has object dtype (string columns)
            try:
                if pd.api.types.is_object_dtype(clean_df[col]):  # String columns
                    # Remove any remaining vendor references in data
                    clean_df[col] = clean_df[col].astype(str).str.replace('Radaris', 'Phone Search', regex=False)
                    clean_df[col] = clean_df[col].str.replace('ZabaSearch', 'Phone Search', regex=False)
            except Exception as e:
                logger.warning(f"Could not clean column {col}: {e}")
                continue

        logger.info(f"Final dataset: {len(clean_df)} records with {len(clean_df.columns)} columns")

        return clean_df

    def _format_header_row(self, worksheet, num_cols: int):
        """Format the header row to make it stand out"""

        try:
            # Format header row (row 1)
            header_range = f"A1:{chr(64 + num_cols)}1"

            # Apply formatting
            worksheet.format(header_range, {
                "backgroundColor": {
                    "red": 0.0,
                    "green": 0.5,
                    "blue": 1.0
                },
                "textFormat": {
                    "bold": True,
                    "foregroundColor": {
                        "red": 1.0,
                        "green": 1.0,
                        "blue": 1.0
                    }
                },
                "horizontalAlignment": "CENTER"
            })

            logger.info("Applied header formatting")

        except Exception as e:
            logger.warning(f"Could not format header row: {e}")

    async def upload_pipeline_results(self,
                                    csv_file_path: str,
                                    pipeline_results: Dict,
                                    custom_sheet_name: Optional[str] = None) -> Dict:
        """Upload pipeline results with metadata to Google Sheets"""

        if not self.enabled:
            logger.warning("Google Sheets integration disabled")
            return {
                'success': False,
                'spreadsheet_url': None,
                'worksheet_name': None
            }

        try:
            # Determine sheet name
            if custom_sheet_name:
                sheet_name = custom_sheet_name
            else:
                # Generate name with date and summary
                now = datetime.now()
                phone_count = pipeline_results.get('phone_numbers_found', 0)
                records_count = pipeline_results.get('broward_records', 0)
                success_indicator = "SUCCESS" if pipeline_results.get('success', False) else "FAILED"

                sheet_name = f"{now.strftime('%Y_%m_%d')}_{success_indicator}_{phone_count}phones_{records_count}records"

            logger.info(f"Uploading results to sheet: {sheet_name}")

            # Upload main data
            success = self.upload_csv_to_worksheet(csv_file_path, sheet_name, clean_column_names=True)

            if success:
                # Add metadata sheet with pipeline information
                await self._add_metadata_sheet(sheet_name, pipeline_results, csv_file_path)

                # Get spreadsheet URL
                spreadsheet_url = f"https://docs.google.com/spreadsheets/d/{self.spreadsheet.id}"

                return {
                    'success': True,
                    'spreadsheet_url': spreadsheet_url,
                    'worksheet_name': sheet_name
                }
            else:
                return {
                    'success': False,
                    'spreadsheet_url': None,
                    'worksheet_name': sheet_name
                }

        except Exception as e:
            logger.error(f"Failed to upload pipeline results: {e}")
            return {
                'success': False,
                'spreadsheet_url': None,
                'worksheet_name': None
            }

    async def _add_metadata_sheet(self,
                                base_sheet_name: str,
                                pipeline_results: Dict,
                                csv_file_path: str):
        """Add a metadata sheet with pipeline information"""

        try:
            metadata_sheet_name = f"{base_sheet_name}_SUMMARY"

            # Create metadata worksheet
            try:
                metadata_worksheet = self.spreadsheet.add_worksheet(
                    title=metadata_sheet_name,
                    rows=50,
                    cols=10
                )
            except Exception as e:
                logger.warning(f"Could not create metadata sheet: {e}")
                return

            # Prepare metadata
            start_time = pipeline_results.get('start_time')
            end_time = pipeline_results.get('end_time')
            duration = "Unknown"
            if start_time and end_time:
                duration = str(end_time - start_time)

            metadata = [
                ["Broward Lis Pendens Pipeline Summary"],
                [""],
                ["Generated", datetime.now().strftime('%Y-%m-%d %H:%M:%S')],
                ["Status", "SUCCESS" if pipeline_results.get('success', False) else "FAILED"],
                ["Duration", duration],
                [""],
                ["RESULTS SUMMARY"],
                ["Total Records Scraped", pipeline_results.get('broward_records', 0)],
                ["Records Processed", pipeline_results.get('processed_records', 0)],
                ["Addresses Found", pipeline_results.get('addresses_found', 0)],
                ["Phone Numbers Found", pipeline_results.get('phone_numbers_found', 0)],
                [""],
                ["FILES CREATED"],
            ]

            # Add files created
            files_created = pipeline_results.get('files_created', [])
            for file_path in files_created:
                file_name = Path(file_path).name
                metadata.append([file_name])

            # Add errors if any
            errors = pipeline_results.get('errors', [])
            if errors:
                metadata.extend([
                    [""],
                    ["ERRORS ENCOUNTERED"],
                ])
                for error in errors[:10]:  # Limit to first 10 errors
                    metadata.append([str(error)[:100]])  # Truncate long errors

            # Upload metadata
            metadata_worksheet.update('A1', metadata)

            # Format the summary sheet
            self._format_summary_sheet(metadata_worksheet)

            logger.info(f"Added summary sheet: {metadata_sheet_name}")

        except Exception as e:
            logger.warning(f"Could not add metadata sheet: {e}")

    def _format_summary_sheet(self, worksheet):
        """Format the summary sheet for better readability"""

        try:
            # Format title
            worksheet.format('A1', {
                "textFormat": {"bold": True, "fontSize": 16},
                "horizontalAlignment": "CENTER"
            })

            # Format section headers
            worksheet.format('A7', {"textFormat": {"bold": True}})  # RESULTS SUMMARY
            worksheet.format('A13', {"textFormat": {"bold": True}})  # FILES CREATED

            logger.info("Applied summary sheet formatting")

        except Exception as e:
            logger.warning(f"Could not format summary sheet: {e}")

    def list_worksheets(self) -> List[str]:
        """List all worksheets in the spreadsheet"""

        if not self.enabled:
            return []

        try:
            worksheets = self.spreadsheet.worksheets()
            return [worksheet.title for worksheet in worksheets]

        except Exception as e:
            logger.error(f"Failed to list worksheets: {e}")
            return []

    def test_connection(self) -> bool:
        """Test Google Sheets connection"""

        if not self.enabled:
            print("❌ Google Sheets integration disabled")
            print("Required configuration:")
            print("  - GOOGLE_SERVICE_ACCOUNT_JSON (path to service account JSON file)")
            print("  - GOOGLE_SPREADSHEET_ID or GOOGLE_SPREADSHEET_NAME")
            return False

        try:
            print(f"Testing Google Sheets connection...")
            print(f"Spreadsheet: {self.spreadsheet.title}")
            print(f"Service Account File: {self.service_account_file}")

            # List existing worksheets
            worksheets = self.list_worksheets()
            print(f"Current worksheets: {len(worksheets)}")
            for worksheet in worksheets[:5]:  # Show first 5
                print(f"  - {worksheet}")

            # Create a test worksheet
            test_name = f"TEST_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            test_sheet_name = self.create_dated_worksheet(test_name, include_time=False)

            if test_sheet_name:
                # Add some test data
                test_worksheet = self.spreadsheet.worksheet(test_sheet_name)
                test_data = [
                    ["Test Column 1", "Test Column 2", "Test Column 3"],
                    ["Test Data", "Success", datetime.now().strftime('%Y-%m-%d %H:%M:%S')],
                    ["Connection", "Working", "✅"]
                ]
                test_worksheet.update('A1', test_data)
                self._format_header_row(test_worksheet, 3)

                print(f"✅ Test worksheet created: {test_sheet_name}")

                # Get URL
                spreadsheet_url = f"https://docs.google.com/spreadsheets/d/{self.spreadsheet.id}"
                print(f"📊 View spreadsheet: {spreadsheet_url}")

                # Optionally delete test worksheet
                try:
                    self.spreadsheet.del_worksheet(test_worksheet)
                    print(f"🗑️ Test worksheet deleted")
                except:
                    print(f"⚠️ Test worksheet not deleted - please remove manually")

                return True
            else:
                print("❌ Failed to create test worksheet")
                return False

        except Exception as e:
            print(f"❌ Google Sheets connection test failed: {e}")
            print("\nTroubleshooting tips:")
            print("1. Ensure your service account JSON file exists and is valid")
            print("2. Verify the service account has access to the spreadsheet")
            print("3. Check that the spreadsheet ID or name is correct")
            print("4. Make sure the Google Sheets API is enabled in your Google Cloud project")
            return False

# Convenience function for easy import
async def upload_to_google_sheets(csv_file_path: str, pipeline_results: Dict) -> Dict:
    """Convenience function to upload pipeline results to Google Sheets"""
    sheets_integration = GoogleSheetsIntegration()
    return await sheets_integration.upload_pipeline_results(csv_file_path, pipeline_results)

# CLI for testing
if __name__ == "__main__":
    import asyncio

    print("Broward Pipeline Google Sheets Integration Test")
    print("=" * 50)

    sheets = GoogleSheetsIntegration()

    if sheets.test_connection():
        print("\n" + "=" * 50)
        print("Testing with sample data...")

        # Create sample CSV for testing
        sample_data = {
            'Name': ['John Doe', 'Jane Smith', 'Bob Johnson'],
            'Phone_Primary': ['(555) 123-4567', '(555) 987-6543', '(555) 456-7890'],
            'Address': ['123 Main St, Miami, FL', '456 Oak Ave, Tampa, FL', '789 Pine Rd, Orlando, FL'],
            'Search_Status': ['Success', 'Success', 'No Results']
        }

        sample_df = pd.DataFrame(sample_data)
        test_csv_path = 'test_sample_data.csv'
        sample_df.to_csv(test_csv_path, index=False)

        sample_results = {
            'success': True,
            'start_time': datetime.now().replace(minute=0, second=0),
            'end_time': datetime.now(),
            'broward_records': 3,
            'phone_numbers_found': 2,
            'addresses_found': 3,
            'files_created': [test_csv_path],
            'errors': []
        }

        async def test_upload():
            result = await sheets.upload_pipeline_results(test_csv_path, sample_results, "TEST_UPLOAD")

            # Clean up
            try:
                os.remove(test_csv_path)
            except:
                pass

            if result['success']:
                print("✅ Sample data uploaded successfully!")
                if result['spreadsheet_url']:
                    print(f"🔗 View at: {result['spreadsheet_url']}")
            else:
                print("❌ Failed to upload sample data")

        asyncio.run(test_upload())
    else:
        print("❌ Google Sheets connection test failed")
