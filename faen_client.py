#!/usr/bin/env python3
"""
FAEN API Client for authentication and data retrieval
"""

import json
import time
import requests
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional, Any, Union
from urllib.parse import urljoin

from console_utils import print_section, print_info, print_success, print_error, print_data, print_warning
from mrae import MRAEClient


class FaenApiClient:
    """Client for interacting with the FAEN API"""
    
    def __init__(self, base_url: str, username: str, password: str):
        """
        Initialize the FAEN API client
        
        Args:
            base_url: Base URL of the FAEN API
            username: Username for authentication
            password: Password for authentication
        """
        self.base_url = base_url.rstrip('/')
        self.username = username
        self.password = password
        self.access_token: Optional[str] = None
        self.token_type: str = "Bearer"
        self.session = requests.Session()
    
    def authenticate(self) -> bool:
        """
        Authenticate with the FAEN API using OAuth2 password flow
        
        Returns:
            True if authentication successful, False otherwise
        """
        print_section("🔐 Authentication")
        print_info(f"Authenticating as: {self.username}")
        
        token_url = urljoin(self.base_url + '/', 'token')
        print_data("Token URL", token_url, 1)
        
        # Prepare form data for OAuth2 password flow
        auth_data = {
            'username': self.username,
            'password': self.password,
            'grant_type': 'password'
        }
        
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        
        try:
            print_info("Sending authentication request...")
            response = self.session.post(
                token_url,
                data=auth_data,
                headers=headers
            )
            response.raise_for_status()
            
            token_data = response.json()
            self.access_token = token_data['access_token']
            self.token_type = token_data.get('token_type', 'Bearer')
            
            # Set authorization header for future requests
            self.session.headers.update({
                'Authorization': f'{self.token_type} {self.access_token}'
            })
            
            print_success(f"✓ Authentication successful!")
            print_data("Token type", self.token_type, 1)
            print_data("Token preview", f"{self.access_token[:20]}...{self.access_token[-10:]}", 1)
            return True
            
        except requests.exceptions.RequestException as e:
            print_error(f"Authentication failed: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print_data("Response status", str(e.response.status_code), 1)
                print_data("Response content", e.response.text[:200], 1)
            return False
    
    def query_consumption(self, 
                         query: Dict[str, Any], 
                         limit: int = 100, 
                         sort: Optional[str] = None,
                         eumed: bool = False) -> List[Dict[str, Any]]:
        """
        Query consumption data using POST request with automatic chunking for large date ranges
        
        Args:
            query: MongoDB query document
            limit: Maximum number of results to return per chunk
            sort: Sort key (e.g., "+datetime")
            eumed: Whether to return EUMED-compliant JSON-LD format
            
        Returns:
            List of consumption data records
        """
        if not self.access_token:
            if not self.authenticate():
                raise Exception("Authentication required before making API calls")
        
        print_section("📊 Querying Consumption Data")
        consumption_url = urljoin(self.base_url + '/', 'consumption/query')
        print_data("Endpoint", consumption_url, 1)
        
        # Check if this is a large date range query that needs chunking
        datetime_range = query.get('datetime', {})
        if isinstance(datetime_range, dict) and '$gte' in datetime_range and '$lt' in datetime_range:
            # Extract start and end dates from query
            start_date_str = datetime_range['$gte']['$date']
            end_date_str = datetime_range['$lt']['$date']
            
            try:
                start_date = datetime.fromisoformat(start_date_str.replace('Z', '+00:00'))
                end_date = datetime.fromisoformat(end_date_str.replace('Z', '+00:00'))
                
                # If date range is more than 10 days, use automatic chunking
                date_diff = end_date - start_date
                if date_diff.days > 10:
                    return self._query_consumption_chunked(
                        consumption_url, query, limit, sort, eumed, 
                        start_date, end_date
                    )
            except (ValueError, TypeError):
                # If date parsing fails, proceed with single request
                print_warning("⚠ Could not parse date range, proceeding with single request")
        
        # Original single request logic
        request_body = {
            'query': query,
            'limit': limit,
            'eumed': eumed
        }
        
        if sort:
            request_body['sort'] = sort
            print_data("Sort order", sort, 1)
        
        print_data("Limit", str(limit), 1)
        print_data("EUMED format", str(eumed), 1)
        
        headers = {
            'Content-Type': 'application/json'
        }
        
        try:
            print_info("Sending query request...")
            response = self.session.post(
                consumption_url,
                json=request_body,
                headers=headers
            )
            response.raise_for_status()
            
            data = response.json()
            record_count = len(data) if isinstance(data, list) else 1
            print_success(f"✓ Retrieved {record_count} consumption records")
            return data
            
        except requests.exceptions.RequestException as e:
            print_error(f"Failed to query consumption data: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print_data("Response status", str(e.response.status_code), 1)
                print_data("Response content", e.response.text[:200], 1)
            raise
    
    def _query_consumption_chunked(self,
                                  consumption_url: str,
                                  base_query: Dict[str, Any],
                                  limit: int,
                                  sort: Optional[str],
                                  eumed: bool,
                                  start_date: datetime,
                                  end_date: datetime) -> List[Dict[str, Any]]:
        """
        Execute chunked consumption queries for large date ranges (10-day chunks)
        
        Args:
            consumption_url: API endpoint URL
            base_query: Base MongoDB query document
            limit: Maximum number of results to return per chunk  
            sort: Sort key (e.g., "+datetime")
            eumed: Whether to return EUMED-compliant JSON-LD format
            start_date: Overall start date for the range
            end_date: Overall end date for the range
            
        Returns:
            Combined list of consumption data records from all chunks
        """
        all_records = []
        current_date = start_date
        chunk_size_days = 10
        total_chunks = 0
        
        print_info(f"🔄 Large date range detected ({(end_date - start_date).days} days)")
        print_info(f"🔄 Using automatic 10-day chunking...")
        
        headers = {
            'Content-Type': 'application/json'
        }
        
        while current_date < end_date:
            # Calculate chunk end date (10 days or remaining time, whichever is smaller)
            chunk_end_date = min(current_date + timedelta(days=chunk_size_days), end_date)
            total_chunks += 1
            
            # Display the original end date for the last chunk to avoid confusion
            if chunk_end_date == end_date:
                # For the last chunk, show the original user-specified end date (exclusive)
                original_end_user_date = (end_date - timedelta(days=1)).strftime('%Y-%m-%d')
                display_text = f"{current_date.strftime('%Y-%m-%d')} to {original_end_user_date} (exclusive)"
            else:
                display_text = f"{current_date.strftime('%Y-%m-%d')} to {chunk_end_date.strftime('%Y-%m-%d')}"
            
            print_data(f"Chunk {total_chunks}", display_text, 1)
            
            # Create chunk-specific query
            chunk_query = base_query.copy()
            chunk_query['datetime'] = {
                '$gte': {'$date': current_date.isoformat()},
                '$lt': {'$date': chunk_end_date.isoformat()}
            }
            
            request_body = {
                'query': chunk_query,
                'limit': limit,
                'eumed': eumed
            }
            
            if sort:
                request_body['sort'] = sort
            
            try:
                print_info(f"  📡 Requesting chunk {total_chunks}...")
                response = self.session.post(
                    consumption_url,
                    json=request_body,
                    headers=headers
                )
                response.raise_for_status()
                
                chunk_data = response.json()
                chunk_record_count = len(chunk_data) if isinstance(chunk_data, list) else 1
                
                if chunk_record_count > 0:
                    all_records.extend(chunk_data)
                    print_success(f"  ✓ Retrieved {chunk_record_count} records")
                else:
                    print_info(f"  ✓ No data for this period")
                
            except requests.exceptions.RequestException as e:
                print_error(f"  ✗ Failed to retrieve chunk {total_chunks}: {e}")
                if hasattr(e, 'response') and e.response is not None:
                    print_data("    Response status", str(e.response.status_code), 1)
                    print_data("    Response content", e.response.text[:200], 1)
                
                # For robustness, continue with next chunk instead of failing completely
                print_warning("  → Continuing with next chunk...")
            
            # Move to next chunk
            current_date = chunk_end_date
            
            # Small delay to avoid overwhelming the API
            time.sleep(0.1)
        
        total_records = len(all_records)
        print_success(f"✓ Completed chunked query: {total_records} total records from {total_chunks} chunks")
        
        return all_records
    
    def query_generation(self, 
                        query: Dict[str, Any], 
                        limit: int = 100, 
                        sort: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Query generation data using GET request with automatic chunking for large date ranges
        
        Args:
            query: MongoDB query document
            limit: Maximum number of results to return per chunk
            sort: Sort key (e.g., "+datetime")
            
        Returns:
            List of generation data records
        """
        if not self.access_token:
            if not self.authenticate():
                raise Exception("Authentication required before making API calls")
        
        print_section("⚡ Querying Generation Data")
        generation_url = urljoin(self.base_url + '/', 'generation/')
        print_data("Endpoint", generation_url, 1)
        
        # Check if this is a large date range query that needs chunking
        datetime_range = query.get('datetime', {})
        if isinstance(datetime_range, dict) and '$gte' in datetime_range and '$lt' in datetime_range:
            from datetime import datetime, timedelta
            
            # Extract start and end dates from query
            start_date_str = datetime_range['$gte']['$date']
            end_date_str = datetime_range['$lt']['$date']
            
            try:
                start_date = datetime.fromisoformat(start_date_str.replace('Z', '+00:00'))
                end_date = datetime.fromisoformat(end_date_str.replace('Z', '+00:00'))
                
                # If date range is more than 10 days, use automatic chunking
                date_diff = end_date - start_date
                if date_diff.days > 10:
                    return self._query_generation_chunked(
                        generation_url, query, limit, sort,
                        start_date, end_date
                    )
            except (ValueError, TypeError):
                # If date parsing fails, proceed with single request
                print_warning("⚠ Could not parse date range, proceeding with single request")
        
        # Original single request logic
        params = {
            'query': json.dumps(query),
            'limit': limit
        }
        
        if sort:
            params['sort'] = sort
            print_data("Sort order", sort, 1)
        
        print_data("Limit", str(limit), 1)
        
        try:
            print_info("Sending query request...")
            response = self.session.get(
                generation_url,
                params=params
            )
            response.raise_for_status()
            
            data = response.json()
            record_count = len(data) if isinstance(data, list) else 1
            print_success(f"✓ Retrieved {record_count} generation records")
            return data
            
        except requests.exceptions.RequestException as e:
            print_error(f"Failed to query generation data: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print_data("Response status", str(e.response.status_code), 1)
                print_data("Response content", e.response.text[:200], 1)
            raise
    
    def _query_generation_chunked(self,
                                  generation_url: str,
                                  base_query: Dict[str, Any],
                                  limit: int,
                                  sort: Optional[str],
                                  start_date: datetime,
                                  end_date: datetime) -> List[Dict[str, Any]]:
        """
        Execute chunked generation queries for large date ranges (10-day chunks)
        
        Args:
            generation_url: API endpoint URL
            base_query: Base MongoDB query document
            limit: Maximum number of results to return per chunk  
            sort: Sort key (e.g., "+datetime")
            start_date: Overall start date for the range
            end_date: Overall end date for the range
            
        Returns:
            Combined list of generation data records from all chunks
        """
        all_records = []
        current_date = start_date
        chunk_size_days = 10
        total_chunks = 0
        
        print_info(f"🔄 Large date range detected ({(end_date - start_date).days} days)")
        print_info(f"🔄 Using automatic 10-day chunking...")
        
        while current_date < end_date:
            # Calculate chunk end date (10 days or remaining time, whichever is smaller)
            chunk_end_date = min(current_date + timedelta(days=chunk_size_days), end_date)
            total_chunks += 1
            
            # Display the original end date for the last chunk to avoid confusion
            if chunk_end_date == end_date:
                # For the last chunk, show the original user-specified end date (exclusive)
                original_end_user_date = (end_date - timedelta(days=1)).strftime('%Y-%m-%d')
                display_text = f"{current_date.strftime('%Y-%m-%d')} to {original_end_user_date} (exclusive)"
            else:
                display_text = f"{current_date.strftime('%Y-%m-%d')} to {chunk_end_date.strftime('%Y-%m-%d')}"
            
            print_data(f"Chunk {total_chunks}", display_text, 1)
            
            # Create chunk-specific query
            chunk_query = base_query.copy()
            chunk_query['datetime'] = {
                '$gte': {'$date': current_date.isoformat()},
                '$lt': {'$date': chunk_end_date.isoformat()}
            }
            
            # Prepare URL parameters
            params = {
                'query': json.dumps(chunk_query),
                'limit': limit
            }
            
            if sort:
                params['sort'] = sort
            
            try:
                print_info(f"  📡 Requesting chunk {total_chunks}...")
                response = self.session.get(
                    generation_url,
                    params=params
                )
                response.raise_for_status()
                
                chunk_data = response.json()
                chunk_record_count = len(chunk_data) if isinstance(chunk_data, list) else 1
                
                if chunk_record_count > 0:
                    all_records.extend(chunk_data)
                    print_success(f"  ✓ Retrieved {chunk_record_count} records")
                else:
                    print_info(f"  ✓ No data for this period")
                
            except requests.exceptions.RequestException as e:
                print_error(f"  ✗ Failed to retrieve chunk {total_chunks}: {e}")
                if hasattr(e, 'response') and e.response is not None:
                    print_data("    Response status", str(e.response.status_code), 1)
                    print_data("    Response content", e.response.text[:200], 1)
                
                # For robustness, continue with next chunk instead of failing completely
                print_warning("  → Continuing with next chunk...")
            
            # Move to next chunk
            current_date = chunk_end_date
            
            # Small delay to avoid overwhelming the API
            time.sleep(0.1)
        
        total_records = len(all_records)
        print_success(f"✓ Completed chunked query: {total_records} total records from {total_chunks} chunks")
        
        return all_records
    
    def query_weather(self, 
                     query: Dict[str, Any], 
                     limit: int = 100, 
                     sort: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Query weather data using GET request with automatic chunking for large date ranges
        
        Args:
            query: MongoDB query document
            limit: Maximum number of results to return per chunk
            sort: Sort key (e.g., "+datetime")
            
        Returns:
            List of weather data records
        """
        if not self.access_token:
            if not self.authenticate():
                raise Exception("Authentication required before making API calls")
        
        print_section("🌤️ Querying Weather Data")
        weather_url = urljoin(self.base_url + '/', 'weather/')
        print_data("Endpoint", weather_url, 1)
        
        # Check if this is a large date range query that needs chunking
        datetime_range = query.get('datetime_utc', {})
        if isinstance(datetime_range, dict) and '$gte' in datetime_range and '$lt' in datetime_range:
            # Extract start and end dates from query
            start_date_str = datetime_range['$gte']['$date']
            end_date_str = datetime_range['$lt']['$date']
            
            try:
                start_date = datetime.fromisoformat(start_date_str.replace('Z', '+00:00'))
                end_date = datetime.fromisoformat(end_date_str.replace('Z', '+00:00'))
                
                # If date range is more than 10 days, use automatic chunking
                date_diff = end_date - start_date
                if date_diff.days > 10:
                    return self._query_weather_chunked(
                        weather_url, query, limit, sort,
                        start_date, end_date
                    )
            except (ValueError, TypeError):
                # If date parsing fails, proceed with single request
                print_warning("⚠ Could not parse date range, proceeding with single request")
        
        # Original single request logic
        params = {
            'query': json.dumps(query),
            'limit': limit
        }
        
        if sort:
            params['sort'] = sort
            print_data("Sort order", sort, 1)
        
        print_data("Limit", str(limit), 1)
        
        try:
            print_info("Sending query request...")
            response = self.session.get(
                weather_url,
                params=params
            )
            response.raise_for_status()
            
            data = response.json()
            record_count = len(data) if isinstance(data, list) else 1
            print_success(f"✓ Retrieved {record_count} weather records")
            return data
            
        except requests.exceptions.RequestException as e:
            print_error(f"Failed to query weather data: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print_data("Response status", str(e.response.status_code), 1)
                print_data("Response content", e.response.text[:200], 1)
            raise
    
    def _query_weather_chunked(self,
                                weather_url: str,
                                base_query: Dict[str, Any],
                                limit: int,
                                sort: Optional[str],
                                start_date: datetime,
                                end_date: datetime) -> List[Dict[str, Any]]:
        """
        Execute chunked weather queries for large date ranges (10-day chunks)
        
        Args:
            weather_url: API endpoint URL
            base_query: Base MongoDB query document
            limit: Maximum number of results to return per chunk  
            sort: Sort key (e.g., "+datetime")
            start_date: Overall start date for the range
            end_date: Overall end date for the range
            
        Returns:
            Combined list of weather data records from all chunks
        """
        all_records = []
        current_date = start_date
        chunk_size_days = 10
        total_chunks = 0
        
        print_info(f"🔄 Large date range detected ({(end_date - start_date).days} days)")
        print_info(f"🔄 Using automatic 10-day chunking...")
        
        while current_date < end_date:
            # Calculate chunk end date (10 days or remaining time, whichever is smaller)
            chunk_end_date = min(current_date + timedelta(days=chunk_size_days), end_date)
            total_chunks += 1
            
            # Display the original end date for the last chunk to avoid confusion
            if chunk_end_date == end_date:
                # For the last chunk, show the original user-specified end date (exclusive)
                original_end_user_date = (end_date - timedelta(days=1)).strftime('%Y-%m-%d')
                display_text = f"{current_date.strftime('%Y-%m-%d')} to {original_end_user_date} (exclusive)"
            else:
                display_text = f"{current_date.strftime('%Y-%m-%d')} to {chunk_end_date.strftime('%Y-%m-%d')}"
            
            print_data(f"Chunk {total_chunks}", display_text, 1)
            
            # Create chunk-specific query
            chunk_query = base_query.copy()
            chunk_query['datetime_utc'] = {
                '$gte': {'$date': current_date.isoformat()},
                '$lt': {'$date': chunk_end_date.isoformat()}
            }
            
            # Prepare URL parameters
            params = {
                'query': json.dumps(chunk_query),
                'limit': limit
            }
            
            if sort:
                params['sort'] = sort
            
            try:
                print_info(f"  📡 Requesting chunk {total_chunks}...")
                response = self.session.get(
                    weather_url,
                    params=params
                )
                response.raise_for_status()
                
                chunk_data = response.json()
                chunk_record_count = len(chunk_data) if isinstance(chunk_data, list) else 1
                
                if chunk_record_count > 0:
                    all_records.extend(chunk_data)
                    print_success(f"  ✓ Retrieved {chunk_record_count} records")
                else:
                    print_info(f"  ✓ No data for this period")
                
            except requests.exceptions.RequestException as e:
                print_error(f"  ✗ Failed to retrieve chunk {total_chunks}: {e}")
                if hasattr(e, 'response') and e.response is not None:
                    print_data("    Response status", str(e.response.status_code), 1)
                    print_data("    Response content", e.response.text[:200], 1)
                
                # For robustness, continue with next chunk instead of failing completely
                print_warning("  → Continuing with next chunk...")
            
            # Move to next chunk
            current_date = chunk_end_date
            
            # Small delay to avoid overwhelming the API
            time.sleep(0.1)
        
        total_records = len(all_records)
        print_success(f"✓ Completed chunked query: {total_records} total records from {total_chunks} chunks")
        
        return all_records
    
    def get_current_user(self) -> Dict[str, Any]:
        """
        Get current user information
        
        Returns:
            User information dictionary
        """
        if not self.access_token:
            if not self.authenticate():
                raise Exception("Authentication required before making API calls")
        
        print_section("👤 User Information")
        user_url = urljoin(self.base_url + '/', 'users/me/')
        print_data("Endpoint", user_url, 1)
        
        try:
            print_info("Fetching user information...")
            response = self.session.get(user_url)
            response.raise_for_status()
            user_data = response.json()
            
            print_success("✓ User information retrieved")
            print_data("Username", user_data.get('username', 'Unknown'), 1)
            print_data("Email", user_data.get('email', 'Not provided'), 1)
            print_data("Full name", user_data.get('full_name', 'Not provided'), 1)
            print_data("Disabled", str(user_data.get('disabled', False)), 1)
            
            return user_data
            
        except requests.exceptions.RequestException as e:
            print_error(f"Failed to get user info: {e}")
            raise

    def get_mrae_client(self):
        """
        Get MRAE client instance for querying charging infrastructure data
        
        Returns:
            MRAEClient instance configured with this session
        """
        if not self.access_token:
            if not self.authenticate():
                raise Exception("Authentication required before making API calls")
        
        return MRAEClient(self.session, self.base_url)
    
    def query_mrae(self, 
                   start_date: Optional[str] = None,
                   end_date: Optional[str] = None,
                   location: Optional[str] = None,
                   limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Query MRAE charging infrastructure data using GET request
        
        Delegates to MRAEClient for implementation.
        
        Args:
            start_date: Start date in YYYY-MM-DD format (optional)
            end_date: End date in YYYY-MM-DD format (optional)
            location: Location filter (e.g., "MRA-E") (optional)
            limit: Maximum number of results to return (optional)
            
        Returns:
            List of MRAE charging infrastructure records
        """
        mrae_client = self.get_mrae_client()
        return mrae_client.query_mrae(start_date, end_date, location, limit)
    
    def get_mrae_stats(self) -> Dict[str, Any]:
        """
        Get MRAE dataset statistics
        
        Delegates to MRAEClient for implementation.
        
        Returns:
            Dictionary with MRAE statistics
        """
        mrae_client = self.get_mrae_client()
        return mrae_client.get_mrae_stats()
    
    def get_mrae_monthly_summary(self, year: int) -> List[Dict[str, Any]]:
        """
        Get MRAE monthly summary for a specific year
        
        Delegates to MRAEClient for implementation.
        
        Args:
            year: Year to retrieve data for
            
        Returns:
            List of MRAE records for all months in the year
        """
        mrae_client = self.get_mrae_client()
        return mrae_client.get_mrae_monthly_summary(year)


def create_full_day_query(start_date: Union[date, datetime], end_date: Union[date, datetime]) -> Dict[str, Any]:
    """
    Create a MongoDB query for full days (00:00:00 to 00:00:00 next day)
    
    Args:
        start_date: Start date (date or datetime object) - inclusive
        end_date: End date (date or datetime object) - exclusive
        
    Returns:
        MongoDB query document with full day ranges
    """
    # Convert to date objects if datetime objects are passed
    if isinstance(start_date, datetime):
        start_date = start_date.date()
    if isinstance(end_date, datetime):
        end_date = end_date.date()
    
    # Create datetime objects for start of start_date and start of day after end_date
    start_datetime = datetime.combine(start_date, datetime.min.time())
    end_datetime = datetime.combine(end_date + timedelta(days=1), datetime.min.time())
    
    return {
        "datetime": {
            "$gte": {"$date": start_datetime.isoformat()},
            "$lt": {"$date": end_datetime.isoformat()}  # Use $lt instead of $lte for cleaner boundaries
        }
    }


def create_weather_query(start_date: Union[date, datetime], end_date: Union[date, datetime]) -> Dict[str, Any]:
    """
    Create a MongoDB query for weather data using datetime_utc field
    
    Args:
        start_date: Start date (date or datetime object) - inclusive
        end_date: End date (date or datetime object) - exclusive
        
    Returns:
        MongoDB query document for weather data
    """
    # Convert to date objects if datetime objects are passed
    if isinstance(start_date, datetime):
        start_date = start_date.date()
    if isinstance(end_date, datetime):
        end_date = end_date.date()
    
    # Create datetime objects for start of start_date and start of day after end_date
    start_datetime = datetime.combine(start_date, datetime.min.time())
    end_datetime = datetime.combine(end_date + timedelta(days=1), datetime.min.time())
    
    return {
        "datetime_utc": {
            "$gte": {"$date": start_datetime.isoformat()},
            "$lt": {"$date": end_datetime.isoformat()}
        }
    }


def create_date_range_query(start_date: str, end_date: str) -> Dict[str, Any]:
    """
    Create a MongoDB query for a date range (legacy function - kept for compatibility)
    
    Args:
        start_date: Start date in ISO format (e.g., "2022-07-13T16:00:00+0200")
        end_date: End date in ISO format
        
    Returns:
        MongoDB query document
    """
    return {
        "datetime": {
            "$gte": {"$date": start_date},
            "$lte": {"$date": end_date}
        }
    }