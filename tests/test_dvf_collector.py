"""Unit tests for DVF collector."""

import unittest
from unittest.mock import Mock, patch, mock_open, MagicMock
import tempfile
import os
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup

from collectors.dvf.dvf_collector import DVFCollector
from utils.utils import NetworkError, StorageError, ValidationError


class TestDVFCollector(unittest.TestCase):
    """Test DVF collector functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Mock config to avoid loading actual config
        self.config_patch = patch('collectors.base_collector.get_config')
        self.mock_config = self.config_patch.start()
        
        # Mock GCS client
        self.gcs_patch = patch('collectors.base_collector.get_gcs_client')
        self.mock_gcs_client = self.gcs_patch.start()
        
        # Setup mock config responses
        mock_config_obj = Mock()
        mock_config_obj.get.side_effect = self._mock_config_get
        mock_config_obj.get_required.side_effect = self._mock_config_get_required
        self.mock_config.return_value = mock_config_obj
        
        # Create collector instance
        self.collector = DVFCollector()
    
    def tearDown(self):
        """Clean up test fixtures."""
        self.config_patch.stop()
        self.gcs_patch.stop()
    
    def _mock_config_get(self, key, default=None):
        """Mock config.get() method."""
        config_map = {
            'data_sources.dvf': {
                'base_url': 'https://files.data.gouv.fr/geo-dvf/latest/csv/',
                'download_subdirs': False,
                'years': None
            },
            'data_sources.dvf.base_url': 'https://files.data.gouv.fr/geo-dvf/latest/csv/',
            'data_sources.dvf.download_subdirs': False,
            'data_sources.dvf.years': None,
            'processing_config.batch_size': 1000,
            'processing_config.max_retries': 3,
            'processing_config.retry_delay_seconds': 30,
            'processing_config.timeout_seconds': 300,
            'processing_config.chunk_size_bytes': 8192,
            'logging_config.level': 'INFO',
            'logging_config.enable_cloud_logging': False,
            'logging_config.format': 'text',
            'features.enable_idempotency_check': True,
            'features.enable_file_comparison': True,
            'gcs_config.bucket_name': 'test-bucket'
        }
        return config_map.get(key, default)
    
    def _mock_config_get_required(self, key):
        """Mock config.get_required() method."""
        result = self._mock_config_get(key)
        if result is None:
            raise ValueError(f"Required config key not found: {key}")
        return result
    
    @patch('requests.get')
    def test_get_available_years_success(self, mock_get):
        """Test successful parsing of available years."""
        # Mock HTML response with year directories
        html_content = '''
        <html><body>
        <a href="../">../</a>
        <a href="2020/">2020/</a>
        <a href="2021/">2021/</a>
        <a href="2022/">2022/</a>
        <a href="2023/">2023/</a>
        <a href="2024/">2024/</a>
        </body></html>
        '''
        
        mock_response = Mock()
        mock_response.content = html_content.encode()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        years = self.collector._get_available_years()
        
        expected_years = ['2020', '2021', '2022', '2023', '2024']
        self.assertEqual(years, expected_years)
        mock_get.assert_called_once_with(
            'https://files.data.gouv.fr/geo-dvf/latest/csv/', 
            timeout=300
        )
    
    @patch('requests.get')
    def test_get_available_years_network_error(self, mock_get):
        """Test network error handling in year parsing."""
        mock_get.side_effect = requests.exceptions.ConnectionError("Network error")
        
        with self.assertRaises(NetworkError):
            self.collector._get_available_years()
    
    @patch('requests.get')
    def test_get_available_years_no_years_found(self, mock_get):
        """Test validation error when no years found."""
        html_content = '<html><body><a href="../">../</a></body></html>'
        
        mock_response = Mock()
        mock_response.content = html_content.encode()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        with self.assertRaises(ValidationError):
            self.collector._get_available_years()
    
    @patch('requests.head')
    def test_get_remote_file_metadata_success(self, mock_head):
        """Test successful remote file metadata retrieval."""
        mock_response = Mock()
        mock_response.headers = {
            'content-length': '91646818',
            'last-modified': 'Tue, 08 Apr 2025 14:40:00 GMT',
            'etag': '"abc123"'
        }
        mock_response.raise_for_status.return_value = None
        mock_head.return_value = mock_response
        
        metadata = self.collector._get_remote_file_metadata('http://example.com/file.csv.gz')
        
        expected_metadata = {
            'size': 91646818,
            'last_modified': 'Tue, 08 Apr 2025 14:40:00 GMT',
            'etag': '"abc123"'
        }
        self.assertEqual(metadata, expected_metadata)
    
    @patch('requests.head')
    def test_get_remote_file_metadata_network_error(self, mock_head):
        """Test network error in metadata retrieval."""
        mock_head.side_effect = requests.exceptions.RequestException("Request failed")
        
        with self.assertRaises(NetworkError):
            self.collector._get_remote_file_metadata('http://example.com/file.csv.gz')
    
    @patch('collectors.dvf.dvf_collector.file_exists_in_gcs')
    def test_should_download_file_not_exists(self, mock_file_exists):
        """Test download decision when file doesn't exist."""
        mock_file_exists.return_value = False
        
        should_download, reason = self.collector._should_download_file(
            'raw/dvf/2024/full.csv.gz', 
            {'size': 91646818}
        )
        
        self.assertTrue(should_download)
        self.assertEqual(reason, "File does not exist in GCS")
    
    @patch('collectors.dvf.dvf_collector.get_file_metadata')
    @patch('collectors.dvf.dvf_collector.file_exists_in_gcs')
    def test_should_download_file_size_mismatch(self, mock_file_exists, mock_get_metadata):
        """Test download decision when file sizes don't match."""
        mock_file_exists.return_value = True
        mock_get_metadata.return_value = {'size': 50000000}  # Different size
        
        should_download, reason = self.collector._should_download_file(
            'raw/dvf/2024/full.csv.gz',
            {'size': 91646818}
        )
        
        self.assertTrue(should_download)
        self.assertIn("Size mismatch", reason)
    
    @patch('collectors.dvf.dvf_collector.get_file_metadata')
    @patch('collectors.dvf.dvf_collector.file_exists_in_gcs')
    def test_should_download_file_same_size(self, mock_file_exists, mock_get_metadata):
        """Test download decision when file sizes match."""
        mock_file_exists.return_value = True
        mock_get_metadata.return_value = {'size': 91646818}
        
        should_download, reason = self.collector._should_download_file(
            'raw/dvf/2024/full.csv.gz',
            {'size': 91646818}
        )
        
        self.assertFalse(should_download)
        self.assertEqual(reason, "File exists with matching size")
    
    @patch('requests.get')
    def test_get_files_in_directory_success(self, mock_get):
        """Test successful directory file listing."""
        html_content = '''
        <html><body>
        <a href="../">../</a>
        <a href="file1.csv.gz">file1.csv.gz</a>
        <a href="file2.csv">file2.csv</a>
        <a href="subdir/">subdir/</a>
        </body></html>
        '''
        
        mock_response = Mock()
        mock_response.content = html_content.encode()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        files = self.collector._get_files_in_directory('http://example.com/dir/')
        
        expected_files = {
            'file1.csv.gz': {'name': 'file1.csv.gz'},
            'file2.csv': {'name': 'file2.csv'}
        }
        self.assertEqual(files, expected_files)
    
    @patch('collectors.dvf.dvf_collector.upload_to_gcs')
    @patch('collectors.dvf.dvf_collector.download_file_with_retry')
    @patch('os.remove')
    def test_process_main_file_success(self, mock_remove, mock_download, mock_upload):
        """Test successful main file processing."""
        # Mock successful download and upload
        mock_download.return_value = True
        mock_upload.return_value = True
        
        # Mock file metadata and download decision
        with patch.object(self.collector, '_get_remote_file_metadata') as mock_metadata, \
             patch.object(self.collector, '_should_download_file') as mock_should_download:
            
            mock_metadata.return_value = {'size': 91646818}
            mock_should_download.return_value = (True, "New file")
            
            result = self.collector._process_main_file('2024')
            
            expected_result = {
                'files_collected': 1,
                'files_skipped': 0,
                'total_size_bytes': 91646818,
                'errors': []
            }
            self.assertEqual(result, expected_result)
            
            # Verify calls
            mock_download.assert_called_once()
            mock_upload.assert_called_once()
            mock_remove.assert_called_once()
    
    @patch.object(DVFCollector, '_get_remote_file_metadata')
    @patch.object(DVFCollector, '_should_download_file')
    def test_process_main_file_skip(self, mock_should_download, mock_metadata):
        """Test skipping main file when not needed."""
        mock_metadata.return_value = {'size': 91646818}
        mock_should_download.return_value = (False, "File exists with matching size")
        
        result = self.collector._process_main_file('2024')
        
        expected_result = {
            'files_collected': 0,
            'files_skipped': 1,
            'total_size_bytes': 0,
            'errors': []
        }
        self.assertEqual(result, expected_result)
    
    @patch('collectors.dvf.dvf_collector.download_file_with_retry')
    def test_process_main_file_download_failure(self, mock_download):
        """Test main file processing with download failure."""
        mock_download.return_value = False
        
        with patch.object(self.collector, '_get_remote_file_metadata') as mock_metadata, \
             patch.object(self.collector, '_should_download_file') as mock_should_download:
            
            mock_metadata.return_value = {'size': 91646818}
            mock_should_download.return_value = (True, "New file")
            
            result = self.collector._process_main_file('2024')
            
            self.assertEqual(result['files_collected'], 0)
            self.assertEqual(result['files_skipped'], 0)
            self.assertTrue(len(result['errors']) > 0)
            self.assertIn('NetworkError', result['errors'][0]['type'])
    
    @patch.object(DVFCollector, '_get_available_years')
    @patch.object(DVFCollector, '_process_year')
    def test_collect_success(self, mock_process_year, mock_get_years):
        """Test successful complete collection."""
        mock_get_years.return_value = ['2023', '2024']
        mock_process_year.side_effect = [
            {
                'files_collected': 1,
                'files_skipped': 0,
                'total_size_bytes': 91646818,
                'errors': []
            },
            {
                'files_collected': 1,
                'files_skipped': 1,
                'total_size_bytes': 89234567,
                'errors': []
            }
        ]
        
        result = self.collector.collect()
        
        expected_result = {
            'files_collected': 2,
            'files_skipped': 1,
            'total_size_bytes': 180881385,
            'years_processed': ['2023', '2024'],
            'errors': []
        }
        self.assertEqual(result, expected_result)
        
        # Verify process_year was called for each year
        self.assertEqual(mock_process_year.call_count, 2)
        mock_process_year.assert_any_call('2023')
        mock_process_year.assert_any_call('2024')
    
    @patch.object(DVFCollector, '_get_available_years')
    def test_collect_with_year_filter(self, mock_get_years):
        """Test collection with year filtering."""
        # Set up collector with year filter
        self.collector.years_to_collect = ['2024']
        mock_get_years.return_value = ['2022', '2023', '2024']
        
        with patch.object(self.collector, '_process_year') as mock_process_year:
            mock_process_year.return_value = {
                'files_collected': 1,
                'files_skipped': 0,
                'total_size_bytes': 91646818,
                'errors': []
            }
            
            result = self.collector.collect()
            
            # Should only process 2024
            self.assertEqual(result['years_processed'], ['2024'])
            mock_process_year.assert_called_once_with('2024')
    
    @patch.object(DVFCollector, '_get_available_years')
    def test_collect_year_processing_error(self, mock_get_years):
        """Test collection handling year processing errors."""
        mock_get_years.return_value = ['2024']
        
        with patch.object(self.collector, '_process_year') as mock_process_year:
            mock_process_year.side_effect = Exception("Processing error")
            
            result = self.collector.collect()
            
            self.assertEqual(result['files_collected'], 0)
            self.assertEqual(result['years_processed'], [])
            self.assertTrue(len(result['errors']) > 0)
            self.assertEqual(result['errors'][0]['year'], '2024')
    
    def test_validate_data_valid(self):
        """Test data validation with valid data."""
        valid_data = {
            'files_collected': 2,
            'files_skipped': 1,
            'total_size_bytes': 180881385,
            'years_processed': ['2023', '2024']
        }
        
        self.assertTrue(self.collector.validate_data(valid_data))
    
    def test_validate_data_invalid_missing_field(self):
        """Test data validation with missing required field."""
        invalid_data = {
            'files_collected': 2,
            'files_skipped': 1,
            'total_size_bytes': 180881385
            # Missing 'years_processed'
        }
        
        self.assertFalse(self.collector.validate_data(invalid_data))
    
    def test_validate_data_invalid_type(self):
        """Test data validation with invalid data type."""
        invalid_data = {
            'files_collected': "2",  # Should be int
            'files_skipped': 1,
            'total_size_bytes': 180881385,
            'years_processed': ['2023', '2024']
        }
        
        self.assertFalse(self.collector.validate_data(invalid_data))
    
    def test_validate_data_negative_values(self):
        """Test data validation with negative values."""
        invalid_data = {
            'files_collected': -1,  # Should be non-negative
            'files_skipped': 1,
            'total_size_bytes': 180881385,
            'years_processed': ['2023', '2024']
        }
        
        self.assertFalse(self.collector.validate_data(invalid_data))
    
    @patch.object(DVFCollector, '_get_files_in_directory')
    @patch.object(DVFCollector, '_process_subdir_file')
    def test_process_subdirectory_success(self, mock_process_file, mock_get_files):
        """Test successful subdirectory processing."""
        mock_get_files.return_value = {
            'file1.csv.gz': {'name': 'file1.csv.gz', 'size': 1000000},
            'file2.csv': {'name': 'file2.csv', 'size': 500000},
            'readme.txt': {'name': 'readme.txt', 'size': 1000}  # Should be skipped
        }
        
        mock_process_file.side_effect = [
            {
                'files_collected': 1,
                'files_skipped': 0,
                'total_size_bytes': 1000000,
                'errors': []
            },
            {
                'files_collected': 0,
                'files_skipped': 1,
                'total_size_bytes': 0,
                'errors': []
            }
        ]
        
        result = self.collector._process_subdirectory('2024', 'communes')
        
        expected_result = {
            'files_collected': 1,
            'files_skipped': 1,
            'total_size_bytes': 1000000,
            'errors': []
        }
        self.assertEqual(result, expected_result)
        
        # Should only process CSV files
        self.assertEqual(mock_process_file.call_count, 2)
    
    @patch.object(DVFCollector, '_get_files_in_directory')
    def test_process_subdirectory_no_files(self, mock_get_files):
        """Test subdirectory processing with no files."""
        mock_get_files.return_value = {}
        
        result = self.collector._process_subdirectory('2024', 'communes')
        
        expected_result = {
            'files_collected': 0,
            'files_skipped': 0,
            'total_size_bytes': 0,
            'errors': []
        }
        self.assertEqual(result, expected_result)


class TestDVFCollectorIntegration(unittest.TestCase):
    """Integration tests for DVF collector."""
    
    @patch('collectors.base_collector.get_config')
    @patch('collectors.base_collector.get_gcs_client')
    def test_collector_initialization(self, mock_gcs_client, mock_config):
        """Test collector initialization."""
        # Setup mock config
        mock_config_obj = Mock()
        mock_config_obj.get.side_effect = lambda key, default=None: {
            'data_sources.dvf': {'base_url': 'https://test.example.com/'},
            'data_sources.dvf.base_url': 'https://test.example.com/',
            'processing_config.batch_size': 1000,
            'processing_config.max_retries': 3,
            'processing_config.retry_delay_seconds': 30,
            'processing_config.timeout_seconds': 300,
            'logging_config.level': 'INFO',
            'logging_config.enable_cloud_logging': False,
            'logging_config.format': 'text'
        }.get(key, default)
        mock_config.return_value = mock_config_obj
        
        collector = DVFCollector()
        
        self.assertEqual(collector.collector_name, 'dvf')
        self.assertEqual(collector.base_url, 'https://test.example.com/')
        self.assertFalse(collector.download_subdirs)
        self.assertIsNone(collector.years_to_collect)
    
    @patch('collectors.dvf.dvf_collector.DVFCollector')
    def test_cloud_function_entry_point_success(self, mock_collector_class):
        """Test Cloud Function entry point with success."""
        from collectors.dvf.dvf_collector import dvf_collector_main
        
        # Mock collector instance and run method
        mock_collector = Mock()
        mock_collector.run.return_value = {
            'status': 'completed',
            'files_collected': 2
        }
        mock_collector_class.return_value = mock_collector
        
        result = dvf_collector_main()
        
        expected_result = {
            'statusCode': 200,
            'body': {
                'status': 'completed',
                'files_collected': 2
            }
        }
        self.assertEqual(result, expected_result)
    
    @patch('collectors.dvf.dvf_collector.DVFCollector')
    def test_cloud_function_entry_point_error(self, mock_collector_class):
        """Test Cloud Function entry point with error."""
        from collectors.dvf.dvf_collector import dvf_collector_main
        
        # Mock collector instance to raise exception
        mock_collector_class.side_effect = Exception("Collector initialization failed")
        
        result = dvf_collector_main()
        
        self.assertEqual(result['statusCode'], 500)
        self.assertIn('error', result['body'])
        self.assertEqual(result['body']['error'], "Collector initialization failed")


if __name__ == '__main__':
    unittest.main()