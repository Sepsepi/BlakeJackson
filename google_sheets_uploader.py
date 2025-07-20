#!/usr/bin/env python3
"""
Google Sheets Integration for Broward Lis Pendens Pipeline
========================================================

This module handles uploading processed Broward Lis Pendens data to Google Sheets
using service account authentication for cloud deployment.

Features:
- Service account authentication (secure for cloud)
- Automatic spreadsheet creation
- Data formatting and uploading
- Error handling and retry logic
- Support for large datasets

Author: Blake Jackson
Date: July 20, 2025
"""

import json
import logging
import os
import time
from datetime import datetime
from typing import Optional, Dict, Any, List
import pandas as pd

try:
    import gspread
    from google.oauth2.service_account import Credentials
    from google.auth.exceptions import GoogleAuthError
    GOOGLE_SHEETS_AVAILABLE = True
    SpreadsheetNotFound = gspread.SpreadsheetNotFound
    WorksheetNotFound = gspread.WorksheetNotFound
except ImportError:
    GOOGLE_SHEETS_AVAILABLE = False
    gspread = None
    Credentials = None
    GoogleAuthError = Exception
    SpreadsheetNotFound = Exception
    WorksheetNotFound = Exception


class GoogleSheetsUploader:
    """
    Handles uploading data to Google Sheets using service account authentication.
    """
    
    def __init__(self, service_account_info: Optional[str] = None):
        """
        Initialize Google Sheets uploader.
        
        Args:
            service_account_info: JSON string of service account credentials
                                 If None, will try to get from GOOGLE_SERVICE_ACCOUNT environment variable
        """
        self.logger = logging.getLogger(__name__)
        self.client = None
        
        if not GOOGLE_SHEETS_AVAILABLE:
            self.logger.warning("Google Sheets libraries not available. Install with: pip install gspread google-auth")
            return
            
        # Get service account credentials
        if service_account_info is None:
            service_account_info = os.environ.get('GOOGLE_SERVICE_ACCOUNT')
            
        if not service_account_info:
            self.logger.warning("No Google service account credentials provided. Google Sheets upload will be skipped.")
            return
            
        try:
            # Parse service account JSON
            if isinstance(service_account_info, str):
                service_account_data = json.loads(service_account_info)
            else:
                service_account_data = service_account_info
                
            # Set up credentials and client
            scopes = [
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive'
            ]
            
            credentials = Credentials.from_service_account_info(
                service_account_data, 
                scopes=scopes
            )
            
            self.client = gspread.authorize(credentials)
            self.logger.info("✅ Google Sheets client initialized successfully")
            
        except (json.JSONDecodeError, GoogleAuthError, Exception) as e:
            self.logger.error(f"❌ Failed to initialize Google Sheets client: {e}")
            self.client = None
    
    def upload_dataframe(self, 
                        df: pd.DataFrame,
                        spreadsheet_id: Optional[str] = None,
                        spreadsheet_name: Optional[str] = None,
                        worksheet_name: str = "Sheet1",
                        append_mode: bool = False,
                        share_email: Optional[str] = None,
                        max_retries: int = 3) -> Optional[str]:
        """
        Upload a pandas DataFrame to Google Sheets.
        
        Args:
            df: DataFrame to upload
            spreadsheet_id: ID of existing spreadsheet (from URL)
            spreadsheet_name: Name of the spreadsheet to create/update (if no ID provided)
            worksheet_name: Name of the worksheet within the spreadsheet
            append_mode: If True, append to existing data instead of replacing
            share_email: Email to share the spreadsheet with (optional)
            max_retries: Maximum number of retry attempts
            
        Returns:
            URL of the created/updated spreadsheet, or None if failed
        """
        if not self.client:
            self.logger.warning("Google Sheets client not available. Skipping upload.")
            return None
            
        if df.empty:
            self.logger.warning("DataFrame is empty. Skipping Google Sheets upload.")
            return None
            
        for attempt in range(max_retries):
            try:
                self.logger.info(f"📊 Uploading data to Google Sheets (attempt {attempt + 1}/{max_retries})...")
                
                # Open spreadsheet by ID or name
                if spreadsheet_id:
                    try:
                        spreadsheet = self.client.open_by_key(spreadsheet_id)
                        self.logger.info(f"📝 Found existing spreadsheet by ID: {spreadsheet_id}")
                    except SpreadsheetNotFound:
                        self.logger.error(f"❌ Spreadsheet with ID {spreadsheet_id} not found. Make sure the service account has access to it.")
                        return None
                    except Exception as e:
                        self.logger.error(f"❌ Failed to access spreadsheet {spreadsheet_id}: {e}")
                        return None
                else:
                    # Fallback to name-based lookup or creation
                    try:
                        spreadsheet = self.client.open(spreadsheet_name or "Broward Lis Pendens")
                        self.logger.info(f"📝 Found existing spreadsheet: {spreadsheet_name}")
                    except SpreadsheetNotFound:
                        spreadsheet = self.client.create(spreadsheet_name or "Broward Lis Pendens")
                        self.logger.info(f"📄 Created new spreadsheet: {spreadsheet_name}")
                        
                        # Share with specified email if provided
                        if share_email:
                            spreadsheet.share(share_email, perm_type='user', role='writer')
                            self.logger.info(f"📧 Shared spreadsheet with {share_email}")
                
                # Get or create worksheet
                try:
                    worksheet = spreadsheet.worksheet(worksheet_name)
                    self.logger.info(f"📋 Found existing worksheet: {worksheet_name}")
                except WorksheetNotFound:
                    worksheet = spreadsheet.add_worksheet(title=worksheet_name, rows=1000, cols=26)
                    self.logger.info(f"📋 Created new worksheet: {worksheet_name}")
                
                # Prepare data for upload
                data_to_upload = self._prepare_data_for_upload(df)
                
                if append_mode:
                    # Append mode: find the next empty row
                    existing_data = worksheet.get_all_values()
                    next_row = len(existing_data) + 1
                    
                    if len(existing_data) == 0:
                        # Empty sheet, upload with headers
                        worksheet.update('A1', data_to_upload)
                        self.logger.info(f"📤 Uploaded {len(data_to_upload)} rows (with headers) to empty sheet")
                    else:
                        # Append data without headers
                        data_without_headers = data_to_upload[1:]  # Skip header row
                        if data_without_headers:
                            start_cell = f'A{next_row}'
                            worksheet.update(start_cell, data_without_headers)
                            self.logger.info(f"📤 Appended {len(data_without_headers)} rows starting at row {next_row}")
                        else:
                            self.logger.info("📤 No data to append (only headers)")
                else:
                    # Replace mode: clear and upload all data
                    worksheet.clear()
                    worksheet.update('A1', data_to_upload)
                    self.logger.info(f"📤 Replaced all data with {len(data_to_upload)} rows")
                
                # Format the spreadsheet
                self._format_spreadsheet(worksheet, len(df.columns))
                
                spreadsheet_url = spreadsheet.url
                self.logger.info(f"✅ Successfully uploaded to Google Sheets: {spreadsheet_url}")
                
                return spreadsheet_url
                
            except Exception as e:
                self.logger.error(f"❌ Attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 5
                    self.logger.info(f"⏳ Waiting {wait_time} seconds before retry...")
                    time.sleep(wait_time)
                else:
                    self.logger.error(f"❌ All {max_retries} attempts failed. Google Sheets upload skipped.")
                    
        return None
    
    def _prepare_data_for_upload(self, df: pd.DataFrame) -> List[List[str]]:
        """
        Prepare DataFrame for Google Sheets upload.
        
        Args:
            df: DataFrame to prepare
            
        Returns:
            List of lists suitable for Google Sheets upload
        """
        # Convert DataFrame to list of lists
        data = []
        
        # Add header row
        headers = df.columns.tolist()
        data.append(headers)
        
        # Add data rows
        for _, row in df.iterrows():
            # Convert all values to strings and handle NaN/None
            row_data = []
            for value in row:
                if pd.isna(value) or value is None:
                    row_data.append("")
                else:
                    row_data.append(str(value))
            data.append(row_data)
            
        return data
    
    def _get_column_letter(self, col_num: int) -> str:
        """
        Convert column number to Excel-style letter (A, B, C, ..., AA, AB, etc.)
        
        Args:
            col_num: Column number (1-based)
            
        Returns:
            Column letter
        """
        result = ""
        while col_num > 0:
            col_num -= 1
            result = chr(65 + (col_num % 26)) + result
            col_num //= 26
        return result
    
    def _format_spreadsheet(self, worksheet, num_columns: int):
        """
        Apply basic formatting to the spreadsheet.
        
        Args:
            worksheet: Google Sheets worksheet object
            num_columns: Number of columns in the data
        """
        try:
            # Format header row (bold, frozen)
            header_range = f'A1:{self._get_column_letter(num_columns)}1'
            
            # Make header bold
            worksheet.format(header_range, {
                'textFormat': {'bold': True},
                'backgroundColor': {'red': 0.9, 'green': 0.9, 'blue': 0.9}
            })
            
            # Freeze header row
            worksheet.freeze(rows=1)
            
            self.logger.info("✅ Applied formatting to spreadsheet")
            
        except Exception as e:
            self.logger.warning(f"⚠️ Could not apply formatting: {e}")


def upload_to_google_sheets(df: pd.DataFrame,
                          spreadsheet_id: Optional[str] = None,
                          spreadsheet_name: Optional[str] = None,
                          worksheet_name: str = "Broward Lis Pendens",
                          append_mode: bool = True,
                          share_email: Optional[str] = None,
                          service_account_info: Optional[str] = None) -> Optional[str]:
    """
    Convenience function to upload DataFrame to Google Sheets.
    
    Args:
        df: DataFrame to upload
        spreadsheet_id: ID of existing spreadsheet (from URL, preferred)
        spreadsheet_name: Name of the spreadsheet (fallback if no ID)
        worksheet_name: Name of the worksheet
        append_mode: If True, append to existing data instead of replacing
        share_email: Email to share with
        service_account_info: Service account JSON string
        
    Returns:
        URL of the spreadsheet or None if failed
    """
    uploader = GoogleSheetsUploader(service_account_info)
    return uploader.upload_dataframe(
        df=df,
        spreadsheet_id=spreadsheet_id,
        spreadsheet_name=spreadsheet_name,
        worksheet_name=worksheet_name,
        append_mode=append_mode,
        share_email=share_email
    )


# Example usage
if __name__ == "__main__":
    # Example of how to use this module
    logging.basicConfig(level=logging.INFO)
    
    # Create sample data
    sample_data = pd.DataFrame({
        'Name': ['John Doe', 'Jane Smith'],
        'Address': ['123 Main St', '456 Oak Ave'],
        'Phone': ['555-1234', '555-5678']
    })
    
    # Upload to Google Sheets
    # Note: You need to set GOOGLE_SERVICE_ACCOUNT environment variable
    url = upload_to_google_sheets(
        df=sample_data,
        spreadsheet_name="Test Broward Data",
        share_email="your-email@gmail.com"  # Optional
    )
    
    if url:
        print(f"✅ Data uploaded successfully: {url}")
    else:
        print("❌ Upload failed")
