"""Core utility functions for France data collection project.

This module provides consolidated utility functions for:
- HTTP downloads with retry logic
- GCS operations with idempotency
- Logging setup
- Custom exceptions for robust error handling
"""

import logging
import os
import hashlib
import base64
from datetime import timezone
from typing import Optional, Dict, Any, Tuple
from pathlib import Path
import requests
from tenacity import retry, stop_after_attempt, wait_exponential
from google.cloud import storage, logging as cloud_logging
from google.cloud.exceptions import NotFound, Forbidden, ServiceUnavailable

from config.config_loader import get_config


# Custom Exceptions
class FranceDataError(Exception):
    """Base exception for France data collection errors."""
    pass


class NetworkError(FranceDataError):
    """Exception for network-related errors."""
    pass


class StorageError(FranceDataError):
    """Exception for GCS storage-related errors."""
    pass


class ConfigurationError(FranceDataError):
    """Exception for configuration-related errors."""
    pass


class ValidationError(FranceDataError):
    """Exception for data validation errors."""
    pass


def setup_logging(
    name: str,
    level: str = "INFO", 
    enable_cloud_logging: bool = True,
    log_format: str = "json"
) -> logging.Logger:
    """Setup centralized logging configuration.
    
    Args:
        name: Logger name (usually module or collector name)
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        enable_cloud_logging: Whether to enable Google Cloud Logging
        log_format: Log format ("json" or "text")
        
    Returns:
        Configured logger instance
        
    Raises:
        ConfigurationError: If logging setup fails
    """
    try:
        logger = logging.getLogger(name)
        logger.setLevel(getattr(logging, level.upper()))
        
        # Clear existing handlers to avoid duplicates
        logger.handlers.clear()
        
        # Setup Cloud Logging if enabled
        if enable_cloud_logging:
            try:
                client = cloud_logging.Client()
                client.setup_logging()
            except Exception as e:
                # Fallback to console logging if cloud logging fails
                print(f"Warning: Could not setup Cloud Logging: {e}")
        
        # Setup console handler
        console_handler = logging.StreamHandler()
        
        if log_format.lower() == "json":
            # JSON formatter for structured logging
            import json
            from datetime import datetime
            
            class JsonFormatter(logging.Formatter):
                def format(self, record):
                    log_entry = {
                        'timestamp': datetime.now(timezone.utc).isoformat(),
                        'level': record.levelname,
                        'logger': record.name,
                        'message': record.getMessage(),
                        'module': record.module,
                        'function': record.funcName,
                        'line': record.lineno
                    }
                    if record.exc_info:
                        log_entry['exception'] = self.formatException(record.exc_info)
                    return json.dumps(log_entry)
            
            formatter = JsonFormatter()
        else:
            # Text formatter
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
        
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        # Prevent propagation to root logger to avoid duplicate logs
        logger.propagate = False
        
        return logger
        
    except Exception as e:
        raise ConfigurationError(f"Failed to setup logging for {name}: {e}")


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    reraise=True
)
def download_file_with_retry(
    url: str,
    local_path: str,
    timeout: int = 300,
    chunk_size: int = 8192,
    headers: Optional[Dict[str, str]] = None
) -> bool:
    """Download a file with exponential backoff retry logic.
    
    Args:
        url: URL to download from
        local_path: Local path to save file
        timeout: Request timeout in seconds
        chunk_size: Download chunk size in bytes
        headers: Optional HTTP headers
        
    Returns:
        True if successful
        
    Raises:
        NetworkError: If download fails after all retries
        ValidationError: If file validation fails
    """
    logger = logging.getLogger(__name__)
    
    try:
        logger.info(f"Downloading {url} to {local_path}")
        
        # Create directory if needed
        Path(local_path).parent.mkdir(parents=True, exist_ok=True)
        
        # Make request with streaming
        response = requests.get(
            url, 
            stream=True, 
            timeout=timeout,
            headers=headers or {}
        )
        response.raise_for_status()
        
        # Download in chunks
        total_size = int(response.headers.get('content-length', 0))
        downloaded_size = 0
        
        with open(local_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
                    downloaded_size += len(chunk)
        
        # Validate download if content-length was provided
        if total_size > 0 and downloaded_size != total_size:
            os.remove(local_path)
            raise ValidationError(
                f"Download size mismatch: expected {total_size}, got {downloaded_size}"
            )
        
        logger.info(f"Successfully downloaded {downloaded_size} bytes to {local_path}")
        return True
        
    except requests.exceptions.RequestException as e:
        raise NetworkError(f"HTTP request failed for {url}: {e}")
    except (OSError, IOError) as e:
        raise StorageError(f"File operation failed for {local_path}: {e}")
    except Exception as e:
        raise FranceDataError(f"Unexpected error downloading {url}: {e}")


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    reraise=True
)
def upload_to_gcs(
    local_path: str,
    gcs_path: str,
    bucket_name: Optional[str] = None,
    content_type: Optional[str] = None,
    check_existing: bool = True
) -> bool:
    """Upload a file to Google Cloud Storage with idempotency.
    
    Args:
        local_path: Path to local file
        gcs_path: Destination path in GCS (without gs:// prefix)
        bucket_name: GCS bucket name (uses config if None)
        content_type: MIME type of file
        check_existing: Whether to check if file already exists with same content
        
    Returns:
        True if uploaded or already exists with same content
        
    Raises:
        StorageError: If upload fails
        ValidationError: If file validation fails
    """
    logger = logging.getLogger(__name__)
    
    try:
        if not os.path.exists(local_path):
            raise ValidationError(f"Local file does not exist: {local_path}")
        
        # Get bucket name from config if not provided
        if not bucket_name:
            config = get_config()
            bucket_name = config.get_required('gcs_config.bucket_name')
        
        # Initialize GCS client
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(gcs_path)
        
        # Check if file already exists and compare if requested
        if check_existing and blob.exists():
            matches, reason = _compare_files_gcs(blob, local_path)
            if matches:
                logger.info(f"File already exists and matches: gs://{bucket_name}/{gcs_path}")
                return True
            else:
                logger.info(f"File exists but differs ({reason}), uploading: {gcs_path}")
        
        # Set content type if provided
        if content_type:
            blob.content_type = content_type
        
        # Upload file
        blob.upload_from_filename(local_path)
        logger.info(f"Uploaded {local_path} to gs://{bucket_name}/{gcs_path}")
        return True
        
    except (NotFound, Forbidden, ServiceUnavailable) as e:
        raise StorageError(f"GCS operation failed: {e}")
    except Exception as e:
        raise StorageError(f"Failed to upload {local_path} to {gcs_path}: {e}")


def file_exists_in_gcs(
    gcs_path: str,
    bucket_name: Optional[str] = None
) -> bool:
    """Check if a file exists in Google Cloud Storage.
    
    Args:
        gcs_path: Path to check in GCS
        bucket_name: GCS bucket name (uses config if None)
        
    Returns:
        True if file exists, False otherwise
        
    Raises:
        StorageError: If GCS operation fails
    """
    try:
        if not bucket_name:
            config = get_config()
            bucket_name = config.get_required('gcs_config.bucket_name')
        
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(gcs_path)
        
        return blob.exists()
        
    except (NotFound, Forbidden, ServiceUnavailable) as e:
        raise StorageError(f"GCS operation failed: {e}")
    except Exception as e:
        raise StorageError(f"Failed to check file existence {gcs_path}: {e}")


def get_file_metadata(
    gcs_path: str,
    bucket_name: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    """Get metadata for a file in Google Cloud Storage.
    
    Args:
        gcs_path: Path to file in GCS
        bucket_name: GCS bucket name (uses config if None)
        
    Returns:
        Dictionary with file metadata or None if file doesn't exist
        
    Raises:
        StorageError: If GCS operation fails
    """
    try:
        if not bucket_name:
            config = get_config()
            bucket_name = config.get_required('gcs_config.bucket_name')
        
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(gcs_path)
        
        if blob.exists():
            blob.reload()
            return {
                'size': blob.size,
                'created': blob.time_created.isoformat() if blob.time_created else None,
                'updated': blob.updated.isoformat() if blob.updated else None,
                'etag': blob.etag,
                'md5_hash': blob.md5_hash,
                'content_type': blob.content_type,
                'generation': blob.generation,
                'metageneration': blob.metageneration
            }
        return None
        
    except (NotFound, Forbidden, ServiceUnavailable) as e:
        raise StorageError(f"GCS operation failed: {e}")
    except Exception as e:
        raise StorageError(f"Failed to get metadata for {gcs_path}: {e}")


def _compare_files_gcs(blob: storage.Blob, local_path: str) -> Tuple[bool, str]:
    """Compare a GCS blob with a local file.
    
    Args:
        blob: GCS blob object
        local_path: Path to local file
        
    Returns:
        Tuple of (files_match, reason)
    """
    try:
        # Check if local file exists
        if not os.path.exists(local_path):
            return False, "Local file does not exist"
        
        # Reload blob metadata
        blob.reload()
        
        # Compare sizes first
        local_size = os.path.getsize(local_path)
        if blob.size != local_size:
            return False, f"Size mismatch: local={local_size}, gcs={blob.size}"
        
        # Compare MD5 hashes
        with open(local_path, 'rb') as f:
            local_md5 = hashlib.md5(f.read()).hexdigest()
        
        # GCS stores MD5 in base64, convert to hex
        if blob.md5_hash:
            gcs_md5 = base64.b64decode(blob.md5_hash).hex()
            if local_md5 != gcs_md5:
                return False, f"MD5 mismatch: local={local_md5}, gcs={gcs_md5}"
        
        return True, "Files match"
        
    except Exception as e:
        return False, f"Comparison failed: {e}"


def validate_environment() -> Dict[str, Any]:
    """Validate that all required environment variables and dependencies are available.
    
    Returns:
        Dictionary with validation results
        
    Raises:
        ConfigurationError: If critical configuration is missing
    """
    results = {
        'valid': True,
        'errors': [],
        'warnings': [],
        'checks': {}
    }
    
    try:
        # Check configuration
        config = get_config()
        
        # Check required GCS configuration
        try:
            bucket_name = config.get_required('gcs_config.bucket_name')
            results['checks']['gcs_bucket_configured'] = True
        except Exception as e:
            results['valid'] = False
            results['errors'].append(f"GCS bucket not configured: {e}")
            results['checks']['gcs_bucket_configured'] = False
        
        # Check Google Cloud credentials
        try:
            client = storage.Client()
            # Try to access the client (this will fail if no credentials)
            list(client.list_buckets(max_results=1))
            results['checks']['gcs_credentials'] = True
        except Exception as e:
            results['valid'] = False
            results['errors'].append(f"GCS credentials issue: {e}")
            results['checks']['gcs_credentials'] = False
        
        # Check data source configurations
        data_sources = ['dvf', 'sirene', 'insee_contours', 'plu']
        for source in data_sources:
            try:
                source_config = config.get(f'data_sources.{source}')
                if source_config:
                    results['checks'][f'{source}_configured'] = True
                else:
                    results['warnings'].append(f"Data source {source} not configured")
                    results['checks'][f'{source}_configured'] = False
            except Exception:
                results['warnings'].append(f"Data source {source} configuration error")
                results['checks'][f'{source}_configured'] = False
        
        return results
        
    except Exception as e:
        raise ConfigurationError(f"Environment validation failed: {e}")