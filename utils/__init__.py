"""Utils package for France data collection project.

This package provides centralized utility functions for:
- HTTP downloads with retry logic
- GCS operations with idempotency  
- Logging setup
- Error handling with custom exceptions
"""

from .utils import (
    # Core utility functions
    setup_logging,
    download_file_with_retry,
    upload_to_gcs,
    file_exists_in_gcs,
    get_file_metadata,
    validate_environment,
    
    # Custom exceptions
    FranceDataError,
    NetworkError,
    StorageError,
    ConfigurationError,
    ValidationError
)

from .gcs_client import GCSClient, get_gcs_client

__all__ = [
    # Utility functions
    'setup_logging',
    'download_file_with_retry', 
    'upload_to_gcs',
    'file_exists_in_gcs',
    'get_file_metadata',
    'validate_environment',
    
    # GCS client
    'GCSClient',
    'get_gcs_client',
    
    # Exceptions
    'FranceDataError',
    'NetworkError', 
    'StorageError',
    'ConfigurationError',
    'ValidationError'
]