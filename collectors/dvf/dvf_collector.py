"""DVF (Demandes de Valeurs Foncières) data collector.

This module implements a collector for French property transaction data from
data.gouv.fr. It downloads yearly property transaction files and stores them
in Google Cloud Storage with proper organization and idempotency checks.
"""

import re
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Tuple
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup

from collectors.base_collector import BaseCollector
from utils.utils import (
    NetworkError, StorageError, ValidationError,
    setup_logging, download_file_with_retry, upload_to_gcs,
    file_exists_in_gcs, get_file_metadata
)


class DVFCollector(BaseCollector):
    """Collector for DVF (Demandes de Valeurs Foncières) property transaction data."""
    
    def __init__(self):
        """Initialize the DVF collector."""
        super().__init__('dvf')
        
        # DVF-specific configuration
        self.base_url = self.collector_config.get('base_url', 'https://files.data.gouv.fr/geo-dvf/latest/csv/')
        self.download_subdirs = self.collector_config.get('download_subdirs', False)
        self.years_to_collect = self.collector_config.get('years', None)  # None = all available
        
        # File patterns
        self.main_file_pattern = 'full.csv.gz'
        self.year_pattern = re.compile(r'^(20\d{2})/?$')
        
        self.logger.info(f"Initialized DVF collector with base URL: {self.base_url}")
    
    def collect(self) -> Dict[str, Any]:
        """Main collection method for DVF data.
        
        Returns:
            Dictionary with collection results and metadata
        """
        results = {
            'files_collected': 0,
            'files_skipped': 0,
            'total_size_bytes': 0,
            'years_processed': [],
            'errors': []
        }
        
        try:
            # Get available years
            available_years = self._get_available_years()
            self.logger.info(f"Found available years: {available_years}")
            
            # Filter years if configured
            if self.years_to_collect:
                years_to_process = [year for year in available_years if year in self.years_to_collect]
                self.logger.info(f"Filtering to configured years: {years_to_process}")
            else:
                years_to_process = available_years
            
            # Process each year
            for year in sorted(years_to_process):
                try:
                    year_result = self._process_year(year)
                    results['files_collected'] += year_result['files_collected']
                    results['files_skipped'] += year_result['files_skipped']
                    results['total_size_bytes'] += year_result['total_size_bytes']
                    results['years_processed'].append(year)
                    
                    if year_result.get('errors'):
                        results['errors'].extend(year_result['errors'])
                        
                except Exception as e:
                    error_msg = f"Error processing year {year}: {e}"
                    self.logger.error(error_msg)
                    results['errors'].append({
                        'year': year,
                        'error': str(e),
                        'type': type(e).__name__
                    })
                    
        except Exception as e:
            error_msg = f"Error in DVF collection: {e}"
            self.logger.error(error_msg)
            results['errors'].append({
                'error': str(e),
                'type': type(e).__name__
            })
        
        return results
    
    def _get_available_years(self) -> List[str]:
        """Get list of available years from the DVF data source.
        
        Returns:
            List of available years as strings
            
        Raises:
            NetworkError: If unable to fetch directory listing
            ValidationError: If no valid years found
        """
        try:
            self.logger.info(f"Fetching available years from {self.base_url}")
            
            response = requests.get(self.base_url, timeout=self.timeout)
            response.raise_for_status()
            
            # Parse HTML directory listing
            soup = BeautifulSoup(response.content, 'html.parser')
            years = []
            
            # Look for links that match year pattern
            for link in soup.find_all('a', href=True):
                href = link['href']
                match = self.year_pattern.match(href)
                if match:
                    year = match.group(1)
                    years.append(year)
            
            if not years:
                raise ValidationError("No valid years found in directory listing")
            
            self.logger.info(f"Found {len(years)} available years: {sorted(years)}")
            return sorted(years)
            
        except requests.exceptions.RequestException as e:
            raise NetworkError(f"Failed to fetch directory listing from {self.base_url}: {e}")
        except Exception as e:
            raise ValidationError(f"Failed to parse directory listing: {e}")
    
    def _process_year(self, year: str) -> Dict[str, Any]:
        """Process data collection for a specific year.
        
        Args:
            year: Year to process (e.g., "2024")
            
        Returns:
            Dictionary with processing results for the year
        """
        results = {
            'files_collected': 0,
            'files_skipped': 0,
            'total_size_bytes': 0,
            'errors': []
        }
        
        self.logger.info(f"Processing year {year}")
        
        try:
            # Process main file (full.csv.gz)
            main_result = self._process_main_file(year)
            results['files_collected'] += main_result['files_collected']
            results['files_skipped'] += main_result['files_skipped']
            results['total_size_bytes'] += main_result['total_size_bytes']
            
            if main_result.get('errors'):
                results['errors'].extend(main_result['errors'])
            
            # Process subdirectories if configured
            if self.download_subdirs:
                for subdir in ['communes', 'departements']:
                    try:
                        subdir_result = self._process_subdirectory(year, subdir)
                        results['files_collected'] += subdir_result['files_collected']
                        results['files_skipped'] += subdir_result['files_skipped']
                        results['total_size_bytes'] += subdir_result['total_size_bytes']
                        
                        if subdir_result.get('errors'):
                            results['errors'].extend(subdir_result['errors'])
                            
                    except Exception as e:
                        error_msg = f"Error processing subdirectory {subdir} for year {year}: {e}"
                        self.logger.warning(error_msg)
                        results['errors'].append({
                            'year': year,
                            'subdirectory': subdir,
                            'error': str(e),
                            'type': type(e).__name__
                        })
        
        except Exception as e:
            error_msg = f"Error processing year {year}: {e}"
            self.logger.error(error_msg)
            results['errors'].append({
                'year': year,
                'error': str(e),
                'type': type(e).__name__
            })
        
        return results
    
    def _process_main_file(self, year: str) -> Dict[str, Any]:
        """Process the main full.csv.gz file for a year.
        
        Args:
            year: Year to process
            
        Returns:
            Dictionary with processing results
        """
        results = {
            'files_collected': 0,
            'files_skipped': 0,
            'total_size_bytes': 0,
            'errors': []
        }
        
        try:
            # Construct URLs and paths
            year_url = urljoin(self.base_url, f"{year}/")
            file_url = urljoin(year_url, self.main_file_pattern)
            gcs_path = f"{self.raw_path}/{year}/{self.main_file_pattern}"
            
            self.logger.info(f"Processing main file: {file_url}")
            
            # Get remote file metadata
            remote_metadata = self._get_remote_file_metadata(file_url)
            
            # Check if we need to download
            should_download, reason = self._should_download_file(gcs_path, remote_metadata)
            
            if not should_download:
                self.logger.info(f"Skipping {file_url}: {reason}")
                results['files_skipped'] = 1
                return results
            
            # Download and upload file
            local_path = f"/tmp/dvf_{year}_{self.main_file_pattern}"
            
            # Download file
            success = download_file_with_retry(
                url=file_url,
                local_path=local_path,
                timeout=self.timeout,
                chunk_size=self.config.get('processing_config.chunk_size_bytes', 8192)
            )
            
            if not success:
                raise NetworkError(f"Failed to download {file_url}")
            
            # Upload to GCS
            upload_success = upload_to_gcs(
                local_path=local_path,
                gcs_path=gcs_path,
                check_existing=False  # We already checked above
            )
            
            if not upload_success:
                raise StorageError(f"Failed to upload {local_path} to {gcs_path}")
            
            # Clean up local file
            import os
            os.remove(local_path)
            
            results['files_collected'] = 1
            results['total_size_bytes'] = remote_metadata.get('size', 0)
            
            self.logger.info(f"Successfully processed main file for year {year}")
            
        except Exception as e:
            error_msg = f"Error processing main file for year {year}: {e}"
            self.logger.error(error_msg)
            results['errors'].append({
                'year': year,
                'file': self.main_file_pattern,
                'error': str(e),
                'type': type(e).__name__
            })
        
        return results
    
    def _process_subdirectory(self, year: str, subdir: str) -> Dict[str, Any]:
        """Process files in a subdirectory (communes or departements).
        
        Args:
            year: Year to process
            subdir: Subdirectory name ('communes' or 'departements')
            
        Returns:
            Dictionary with processing results
        """
        results = {
            'files_collected': 0,
            'files_skipped': 0,
            'total_size_bytes': 0,
            'errors': []
        }
        
        try:
            # Construct subdirectory URL
            subdir_url = urljoin(self.base_url, f"{year}/{subdir}/")
            
            self.logger.info(f"Processing subdirectory: {subdir_url}")
            
            # Get files in subdirectory
            files = self._get_files_in_directory(subdir_url)
            
            if not files:
                self.logger.info(f"No files found in {subdir_url}")
                return results
            
            # Process each file
            for filename, file_info in files.items():
                try:
                    if filename.endswith('.csv.gz') or filename.endswith('.csv'):
                        file_result = self._process_subdir_file(year, subdir, filename, file_info)
                        results['files_collected'] += file_result['files_collected']
                        results['files_skipped'] += file_result['files_skipped']
                        results['total_size_bytes'] += file_result['total_size_bytes']
                        
                        if file_result.get('errors'):
                            results['errors'].extend(file_result['errors'])
                    else:
                        self.logger.debug(f"Skipping non-CSV file: {filename}")
                        
                except Exception as e:
                    error_msg = f"Error processing file {filename} in {subdir}: {e}"
                    self.logger.warning(error_msg)
                    results['errors'].append({
                        'year': year,
                        'subdirectory': subdir,
                        'file': filename,
                        'error': str(e),
                        'type': type(e).__name__
                    })
        
        except Exception as e:
            error_msg = f"Error processing subdirectory {subdir} for year {year}: {e}"
            self.logger.error(error_msg)
            results['errors'].append({
                'year': year,
                'subdirectory': subdir,
                'error': str(e),
                'type': type(e).__name__
            })
        
        return results
    
    def _process_subdir_file(self, year: str, subdir: str, filename: str, file_info: Dict) -> Dict[str, Any]:
        """Process a single file from a subdirectory.
        
        Args:
            year: Year being processed
            subdir: Subdirectory name
            filename: Name of the file
            file_info: File metadata from directory listing
            
        Returns:
            Dictionary with processing results
        """
        results = {
            'files_collected': 0,
            'files_skipped': 0,
            'total_size_bytes': 0,
            'errors': []
        }
        
        try:
            # Construct URLs and paths
            file_url = urljoin(self.base_url, f"{year}/{subdir}/{filename}")
            gcs_path = f"{self.raw_path}/{year}/{subdir}/{filename}"
            
            # Check if we need to download
            should_download, reason = self._should_download_file(gcs_path, file_info)
            
            if not should_download:
                self.logger.debug(f"Skipping {filename}: {reason}")
                results['files_skipped'] = 1
                return results
            
            # Download and upload file
            local_path = f"/tmp/dvf_{year}_{subdir}_{filename}"
            
            # Download file
            success = download_file_with_retry(
                url=file_url,
                local_path=local_path,
                timeout=self.timeout
            )
            
            if not success:
                raise NetworkError(f"Failed to download {file_url}")
            
            # Upload to GCS
            upload_success = upload_to_gcs(
                local_path=local_path,
                gcs_path=gcs_path,
                check_existing=False
            )
            
            if not upload_success:
                raise StorageError(f"Failed to upload {local_path} to {gcs_path}")
            
            # Clean up
            import os
            os.remove(local_path)
            
            results['files_collected'] = 1
            results['total_size_bytes'] = file_info.get('size', 0)
            
        except Exception as e:
            error_msg = f"Error processing file {filename}: {e}"
            self.logger.error(error_msg)
            results['errors'].append({
                'year': year,
                'subdirectory': subdir,
                'file': filename,
                'error': str(e),
                'type': type(e).__name__
            })
        
        return results
    
    def _get_remote_file_metadata(self, url: str) -> Dict[str, Any]:
        """Get metadata for a remote file using HEAD request.
        
        Args:
            url: URL of the file
            
        Returns:
            Dictionary with file metadata
        """
        try:
            response = requests.head(url, timeout=self.timeout)
            response.raise_for_status()
            
            metadata = {}
            
            # Get content length
            if 'content-length' in response.headers:
                metadata['size'] = int(response.headers['content-length'])
            
            # Get last modified
            if 'last-modified' in response.headers:
                metadata['last_modified'] = response.headers['last-modified']
            
            # Get ETag
            if 'etag' in response.headers:
                metadata['etag'] = response.headers['etag']
            
            return metadata
            
        except requests.exceptions.RequestException as e:
            raise NetworkError(f"Failed to get metadata for {url}: {e}")
    
    def _get_files_in_directory(self, url: str) -> Dict[str, Dict]:
        """Get list of files in a directory from HTML listing.
        
        Args:
            url: Directory URL
            
        Returns:
            Dictionary mapping filename to file info
        """
        try:
            response = requests.get(url, timeout=self.timeout)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            files = {}
            
            # Parse directory listing
            for link in soup.find_all('a', href=True):
                href = link['href']
                # Skip parent directory and subdirectories
                if href in ['./', '../'] or href.endswith('/'):
                    continue
                
                # Extract file info from the listing
                file_info = {'name': href}
                
                # Try to extract size and date from the text
                link_text = link.parent.get_text() if link.parent else ""
                
                # Simple pattern matching for size and date
                # This may need adjustment based on actual HTML format
                parts = link_text.split()
                for i, part in enumerate(parts):
                    if part.isdigit() and len(part) > 3:  # Likely a file size
                        file_info['size'] = int(part)
                    elif '-' in part and len(part) >= 8:  # Likely a date
                        file_info['date'] = part
                
                files[href] = file_info
            
            return files
            
        except requests.exceptions.RequestException as e:
            raise NetworkError(f"Failed to get directory listing for {url}: {e}")
    
    def _should_download_file(self, gcs_path: str, remote_metadata: Dict) -> Tuple[bool, str]:
        """Determine if a file should be downloaded based on metadata comparison.
        
        Args:
            gcs_path: GCS path of the file
            remote_metadata: Metadata of the remote file
            
        Returns:
            Tuple of (should_download, reason)
        """
        try:
            # Check if file exists in GCS
            if not file_exists_in_gcs(gcs_path):
                return True, "File does not exist in GCS"
            
            # Get GCS file metadata
            gcs_metadata = get_file_metadata(gcs_path)
            if not gcs_metadata:
                return True, "Cannot get GCS file metadata"
            
            # Compare file sizes
            remote_size = remote_metadata.get('size')
            gcs_size = gcs_metadata.get('size')
            
            if remote_size and gcs_size and remote_size != gcs_size:
                return True, f"Size mismatch: remote={remote_size}, gcs={gcs_size}"
            
            # If sizes match and we have both, file is up to date
            if remote_size and gcs_size:
                return False, "File exists with matching size"
            
            # If we can't compare sizes, download to be safe
            return True, "Cannot compare file sizes"
            
        except Exception as e:
            self.logger.warning(f"Error checking file metadata for {gcs_path}: {e}")
            return True, f"Error checking metadata: {e}"
    
    def validate_data(self, data: Any) -> bool:
        """Validate collected DVF data.
        
        Args:
            data: Data to validate (collection results)
            
        Returns:
            True if data is valid, False otherwise
        """
        if not isinstance(data, dict):
            return False
        
        # Check required fields
        required_fields = ['files_collected', 'files_skipped', 'total_size_bytes', 'years_processed']
        for field in required_fields:
            if field not in data:
                return False
        
        # Validate data types
        if not isinstance(data['files_collected'], int) or data['files_collected'] < 0:
            return False
        
        if not isinstance(data['files_skipped'], int) or data['files_skipped'] < 0:
            return False
        
        if not isinstance(data['total_size_bytes'], int) or data['total_size_bytes'] < 0:
            return False
        
        if not isinstance(data['years_processed'], list):
            return False
        
        return True


# Cloud Function entry point
def dvf_collector_main(request=None):
    """Cloud Function entry point for DVF data collection.
    
    Args:
        request: HTTP request object (unused)
        
    Returns:
        HTTP response with collection results
    """
    try:
        collector = DVFCollector()
        result = collector.run()
        
        return {
            'statusCode': 200,
            'body': result
        }
        
    except Exception as e:
        logging.error(f"DVF collector failed: {e}")
        return {
            'statusCode': 500,
            'body': {
                'error': str(e),
                'type': type(e).__name__
            }
        }


if __name__ == "__main__":
    # For local testing
    collector = DVFCollector()
    result = collector.run()
    print(f"DVF collection result: {result}")