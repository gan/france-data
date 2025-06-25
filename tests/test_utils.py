"""Unit tests for core utility functions."""

import unittest
from unittest.mock import Mock, patch, mock_open, MagicMock
import os
import tempfile
import json
import hashlib
import base64
from pathlib import Path

import pytest
import requests
from google.cloud import storage
from google.cloud.exceptions import NotFound, Forbidden

from utils.utils import (
    setup_logging,
    download_file_with_retry,
    upload_to_gcs,
    file_exists_in_gcs,
    get_file_metadata,
    validate_environment,
    FranceDataError,
    NetworkError,
    StorageError,
    ConfigurationError,
    ValidationError,
    _compare_files_gcs
)


class TestSetupLogging(unittest.TestCase):
    """Test logging setup functionality."""
    
    @patch('utils.utils.cloud_logging.Client')
    def test_setup_logging_json_format(self, mock_cloud_client):
        """Test JSON format logging setup."""
        logger = setup_logging("test.logger", level="INFO", log_format="json")
        
        self.assertEqual(logger.name, "test.logger")
        self.assertEqual(logger.level, 20)  # INFO level
        self.assertFalse(logger.propagate)
        self.assertTrue(len(logger.handlers) > 0)
    
    @patch('utils.utils.cloud_logging.Client')
    def test_setup_logging_text_format(self, mock_cloud_client):
        """Test text format logging setup."""
        logger = setup_logging("test.logger", level="DEBUG", log_format="text")
        
        self.assertEqual(logger.name, "test.logger")
        self.assertEqual(logger.level, 10)  # DEBUG level
    
    @patch('utils.utils.cloud_logging.Client')
    def test_setup_logging_cloud_disabled(self, mock_cloud_client):
        """Test logging setup with cloud logging disabled."""
        logger = setup_logging("test.logger", enable_cloud_logging=False)
        
        mock_cloud_client.assert_not_called()
        self.assertEqual(logger.name, "test.logger")
    
    @patch('utils.utils.cloud_logging.Client')
    def test_setup_logging_cloud_error(self, mock_cloud_client):
        """Test graceful handling of cloud logging errors."""
        mock_cloud_client.return_value.setup_logging.side_effect = Exception("Cloud error")
        
        # Should not raise exception
        logger = setup_logging("test.logger")
        self.assertEqual(logger.name, "test.logger")


class TestDownloadFileWithRetry(unittest.TestCase):
    """Test file download functionality."""
    
    @patch('requests.get')
    def test_download_success(self, mock_get):
        """Test successful file download."""
        # Mock successful response
        mock_response = Mock()
        mock_response.headers = {'content-length': '12'}  # Match actual content length
        mock_response.iter_content.return_value = [b'test', b'data', b'here']
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        with tempfile.TemporaryDirectory() as tmpdir:
            local_path = os.path.join(tmpdir, "test.txt")
            
            result = download_file_with_retry("http://example.com/file", local_path)
            
            self.assertTrue(result)
            self.assertTrue(os.path.exists(local_path))
            
            with open(local_path, 'rb') as f:
                content = f.read()
                self.assertEqual(content, b'testdatahere')
    
    @patch('requests.get')
    def test_download_network_error(self, mock_get):
        """Test network error handling."""
        mock_get.side_effect = requests.exceptions.ConnectionError("Network error")
        
        with tempfile.TemporaryDirectory() as tmpdir:
            local_path = os.path.join(tmpdir, "test.txt")
            
            with self.assertRaises(NetworkError):
                download_file_with_retry("http://example.com/file", local_path)
    
    @patch('requests.get')
    def test_download_size_validation_error(self, mock_get):
        """Test file size validation error."""
        mock_response = Mock()
        mock_response.headers = {'content-length': '20'}  # Expected 20 bytes
        mock_response.iter_content.return_value = [b'short']  # Only 5 bytes
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        with tempfile.TemporaryDirectory() as tmpdir:
            local_path = os.path.join(tmpdir, "test.txt")
            
            with self.assertRaises(FranceDataError):  # ValidationError is wrapped in FranceDataError
                download_file_with_retry("http://example.com/file", local_path)
            
            # File should be cleaned up on validation error
            self.assertFalse(os.path.exists(local_path))
    
    @patch('requests.get')
    def test_download_with_headers(self, mock_get):
        """Test download with custom headers."""
        mock_response = Mock()
        mock_response.headers = {}
        mock_response.iter_content.return_value = [b'data']
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        with tempfile.TemporaryDirectory() as tmpdir:
            local_path = os.path.join(tmpdir, "test.txt")
            headers = {'Authorization': 'Bearer token'}
            
            result = download_file_with_retry(
                "http://example.com/file", 
                local_path, 
                headers=headers
            )
            
            self.assertTrue(result)
            mock_get.assert_called_with(
                "http://example.com/file",
                stream=True,
                timeout=300,
                headers=headers
            )


class TestGCSOperations(unittest.TestCase):
    """Test GCS operation utilities."""
    
    @patch('utils.utils.get_config')
    @patch('utils.utils.storage.Client')
    def test_upload_to_gcs_success(self, mock_storage_client, mock_config):
        """Test successful GCS upload."""
        # Mock config
        mock_config.return_value.get_required.return_value = "test-bucket"
        
        # Mock GCS client
        mock_client = Mock()
        mock_bucket = Mock()
        mock_blob = Mock()
        mock_blob.exists.return_value = False
        mock_bucket.blob.return_value = mock_blob
        mock_client.bucket.return_value = mock_bucket
        mock_storage_client.return_value = mock_client
        
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
            f.write("test content")
            temp_path = f.name
        
        try:
            result = upload_to_gcs(temp_path, "test/path.txt")
            
            self.assertTrue(result)
            mock_blob.upload_from_filename.assert_called_once_with(temp_path)
        finally:
            os.unlink(temp_path)
    
    @patch('utils.utils.get_config')
    @patch('utils.utils.storage.Client')
    def test_upload_to_gcs_file_exists_same_content(self, mock_storage_client, mock_config):
        """Test upload when file exists with same content."""
        mock_config.return_value.get_required.return_value = "test-bucket"
        
        mock_client = Mock()
        mock_bucket = Mock()
        mock_blob = Mock()
        mock_blob.exists.return_value = True
        mock_bucket.blob.return_value = mock_blob
        mock_client.bucket.return_value = mock_bucket
        mock_storage_client.return_value = mock_client
        
        # Mock file comparison to return match
        with patch('utils.utils._compare_files_gcs') as mock_compare:
            mock_compare.return_value = (True, "Files match")
            
            with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
                f.write("test content")
                temp_path = f.name
            
            try:
                result = upload_to_gcs(temp_path, "test/path.txt")
                
                self.assertTrue(result)
                # Should not upload since files match
                mock_blob.upload_from_filename.assert_not_called()
            finally:
                os.unlink(temp_path)
    
    @patch('utils.utils.get_config')
    @patch('utils.utils.storage.Client')
    def test_file_exists_in_gcs(self, mock_storage_client, mock_config):
        """Test checking file existence in GCS."""
        mock_config.return_value.get_required.return_value = "test-bucket"
        
        mock_client = Mock()
        mock_bucket = Mock()
        mock_blob = Mock()
        mock_blob.exists.return_value = True
        mock_bucket.blob.return_value = mock_blob
        mock_client.bucket.return_value = mock_bucket
        mock_storage_client.return_value = mock_client
        
        result = file_exists_in_gcs("test/path.txt")
        
        self.assertTrue(result)
        mock_bucket.blob.assert_called_once_with("test/path.txt")
        mock_blob.exists.assert_called_once()
    
    @patch('utils.utils.get_config')
    @patch('utils.utils.storage.Client')
    def test_get_file_metadata(self, mock_storage_client, mock_config):
        """Test getting file metadata from GCS."""
        mock_config.return_value.get_required.return_value = "test-bucket"
        
        mock_client = Mock()
        mock_bucket = Mock()
        mock_blob = Mock()
        mock_blob.exists.return_value = True
        mock_blob.size = 1024
        mock_blob.time_created = Mock()
        mock_blob.time_created.isoformat.return_value = "2023-01-01T00:00:00Z"
        mock_blob.updated = Mock()
        mock_blob.updated.isoformat.return_value = "2023-01-02T00:00:00Z"
        mock_blob.etag = "test-etag"
        mock_blob.md5_hash = "test-md5"
        mock_blob.content_type = "text/plain"
        mock_blob.generation = 1
        mock_blob.metageneration = 1
        mock_bucket.blob.return_value = mock_blob
        mock_client.bucket.return_value = mock_bucket
        mock_storage_client.return_value = mock_client
        
        result = get_file_metadata("test/path.txt")
        
        self.assertIsNotNone(result)
        self.assertEqual(result['size'], 1024)
        self.assertEqual(result['etag'], "test-etag")
        self.assertEqual(result['md5_hash'], "test-md5")
        mock_blob.reload.assert_called_once()
    
    @patch('utils.utils.get_config')
    @patch('utils.utils.storage.Client')
    def test_get_file_metadata_not_exists(self, mock_storage_client, mock_config):
        """Test getting metadata for non-existent file."""
        mock_config.return_value.get_required.return_value = "test-bucket"
        
        mock_client = Mock()
        mock_bucket = Mock()
        mock_blob = Mock()
        mock_blob.exists.return_value = False
        mock_bucket.blob.return_value = mock_blob
        mock_client.bucket.return_value = mock_bucket
        mock_storage_client.return_value = mock_client
        
        result = get_file_metadata("test/path.txt")
        
        self.assertIsNone(result)


class TestCompareFilesGCS(unittest.TestCase):
    """Test GCS file comparison functionality."""
    
    def test_compare_files_match(self):
        """Test comparing files that match."""
        mock_blob = Mock()
        mock_blob.size = 10
        mock_blob.md5_hash = base64.b64encode(b'test_hash_bytes').decode()
        
        with patch('os.path.exists') as mock_exists, \
             patch('os.path.getsize') as mock_getsize, \
             patch('builtins.open', mock_open(read_data=b'test_content')) as mock_file, \
             patch('hashlib.md5') as mock_md5:
            
            mock_exists.return_value = True
            mock_getsize.return_value = 10
            mock_md5.return_value.hexdigest.return_value = 'test_hash_bytes'.encode().hex()
            
            matches, reason = _compare_files_gcs(mock_blob, "test/path.txt")
            
            self.assertTrue(matches)
            self.assertEqual(reason, "Files match")
    
    def test_compare_files_size_mismatch(self):
        """Test comparing files with different sizes."""
        mock_blob = Mock()
        mock_blob.size = 10
        
        with patch('os.path.exists') as mock_exists, \
             patch('os.path.getsize') as mock_getsize:
            
            mock_exists.return_value = True
            mock_getsize.return_value = 20  # Different size
            
            matches, reason = _compare_files_gcs(mock_blob, "test/path.txt")
            
            self.assertFalse(matches)
            self.assertIn("Size mismatch", reason)
    
    def test_compare_files_local_not_exists(self):
        """Test comparing when local file doesn't exist."""
        mock_blob = Mock()
        
        with patch('os.path.exists') as mock_exists:
            mock_exists.return_value = False
            
            matches, reason = _compare_files_gcs(mock_blob, "test/path.txt")
            
            self.assertFalse(matches)
            self.assertEqual(reason, "Local file does not exist")


class TestValidateEnvironment(unittest.TestCase):
    """Test environment validation functionality."""
    
    @patch('utils.utils.get_config')
    @patch('utils.utils.storage.Client')
    def test_validate_environment_success(self, mock_storage_client, mock_config):
        """Test successful environment validation."""
        # Mock config
        mock_config_obj = Mock()
        mock_config_obj.get_required.return_value = "test-bucket"
        mock_config_obj.get.side_effect = lambda key: {
            'data_sources.dvf': {'name': 'DVF'},
            'data_sources.sirene': {'name': 'SIRENE'},
            'data_sources.insee_contours': {'name': 'INSEE'},
            'data_sources.plu': {'name': 'PLU'}
        }.get(key)
        mock_config.return_value = mock_config_obj
        
        # Mock GCS client
        mock_client = Mock()
        mock_client.list_buckets.return_value = []
        mock_storage_client.return_value = mock_client
        
        result = validate_environment()
        
        self.assertTrue(result['valid'])
        self.assertEqual(len(result['errors']), 0)
        self.assertTrue(result['checks']['gcs_bucket_configured'])
        self.assertTrue(result['checks']['gcs_credentials'])
    
    @patch('utils.utils.get_config')
    def test_validate_environment_config_error(self, mock_config):
        """Test validation with configuration errors."""
        mock_config.side_effect = Exception("Config error")
        
        with self.assertRaises(ConfigurationError):
            validate_environment()
    
    @patch('utils.utils.get_config')
    @patch('utils.utils.storage.Client')
    def test_validate_environment_gcs_error(self, mock_storage_client, mock_config):
        """Test validation with GCS credential errors."""
        mock_config_obj = Mock()
        mock_config_obj.get_required.return_value = "test-bucket"
        mock_config_obj.get.return_value = None
        mock_config.return_value = mock_config_obj
        
        mock_client = Mock()
        mock_client.list_buckets.side_effect = Exception("GCS error")
        mock_storage_client.return_value = mock_client
        
        result = validate_environment()
        
        self.assertFalse(result['valid'])
        self.assertTrue(any("GCS credentials" in error for error in result['errors']))
        self.assertFalse(result['checks']['gcs_credentials'])


class TestCustomExceptions(unittest.TestCase):
    """Test custom exception classes."""
    
    def test_exception_hierarchy(self):
        """Test exception inheritance hierarchy."""
        self.assertTrue(issubclass(NetworkError, FranceDataError))
        self.assertTrue(issubclass(StorageError, FranceDataError))
        self.assertTrue(issubclass(ConfigurationError, FranceDataError))
        self.assertTrue(issubclass(ValidationError, FranceDataError))
    
    def test_exception_messages(self):
        """Test exception message handling."""
        error = NetworkError("Network failed")
        self.assertEqual(str(error), "Network failed")
        
        error = StorageError("Storage failed")
        self.assertEqual(str(error), "Storage failed")


if __name__ == '__main__':
    unittest.main()