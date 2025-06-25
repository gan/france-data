"""Google Cloud Storage client utilities.

This module provides helper functions for interacting with Google Cloud Storage,
including uploading, downloading, listing files, and managing bucket directories.
"""

import os
import logging
from typing import List, Optional, Tuple, Generator
from pathlib import Path
from google.cloud import storage
from google.cloud.exceptions import NotFound, Conflict
from tenacity import retry, stop_after_attempt, wait_exponential

from config.config_loader import get_config


logger = logging.getLogger(__name__)


class GCSClient:
    """Client for Google Cloud Storage operations."""
    
    def __init__(self, bucket_name: Optional[str] = None):
        """Initialize GCS client.
        
        Args:
            bucket_name: Name of the GCS bucket. If None, uses config value.
        """
        self.config = get_config()
        self.bucket_name = bucket_name or self.config.get_required('gcs_config.bucket_name')
        self.client = storage.Client()
        self._bucket = None
    
    @property
    def bucket(self) -> storage.Bucket:
        """Get or create bucket instance."""
        if self._bucket is None:
            try:
                self._bucket = self.client.get_bucket(self.bucket_name)
            except NotFound:
                logger.warning(f"Bucket {self.bucket_name} not found. Creating...")
                self._bucket = self.create_bucket()
        return self._bucket
    
    def create_bucket(self) -> storage.Bucket:
        """Create the GCS bucket if it doesn't exist.
        
        Returns:
            Created bucket instance
        """
        try:
            bucket = self.client.create_bucket(
                self.bucket_name,
                location="EU"  # France data should be stored in EU
            )
            logger.info(f"Created bucket {self.bucket_name}")
            return bucket
        except Conflict:
            logger.info(f"Bucket {self.bucket_name} already exists")
            return self.client.get_bucket(self.bucket_name)
    
    def initialize_directory_structure(self) -> None:
        """Initialize the directory structure in GCS as defined in config."""
        raw_base = self.config.get('gcs_config.directory_structure.raw.base_path')
        raw_subdirs = self.config.get('gcs_config.directory_structure.raw.subdirs', [])
        
        processed_base = self.config.get('gcs_config.directory_structure.processed.base_path')
        processed_subdirs = self.config.get('gcs_config.directory_structure.processed.subdirs', [])
        
        logs_base = self.config.get('gcs_config.directory_structure.logs.base_path')
        metadata_base = self.config.get('gcs_config.directory_structure.metadata.base_path')
        
        # Create directory placeholders
        directories = []
        
        # Raw data directories
        for subdir in raw_subdirs:
            directories.append(f"{raw_base}/{subdir}/.gitkeep")
        
        # Processed data directories
        for subdir in processed_subdirs:
            directories.append(f"{processed_base}/{subdir}/.gitkeep")
        
        # Other directories
        directories.extend([
            f"{logs_base}/.gitkeep",
            f"{metadata_base}/.gitkeep"
        ])
        
        # Upload placeholder files
        for directory in directories:
            blob = self.bucket.blob(directory)
            if not blob.exists():
                blob.upload_from_string("")
                logger.info(f"Created directory: {directory}")
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def upload_file(self, local_path: str, gcs_path: str, 
                   content_type: Optional[str] = None) -> None:
        """Upload a file to GCS.
        
        Args:
            local_path: Path to local file
            gcs_path: Destination path in GCS
            content_type: MIME type of the file
        """
        blob = self.bucket.blob(gcs_path)
        
        if content_type:
            blob.content_type = content_type
        
        blob.upload_from_filename(local_path)
        logger.info(f"Uploaded {local_path} to gs://{self.bucket_name}/{gcs_path}")
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def download_file(self, gcs_path: str, local_path: str) -> None:
        """Download a file from GCS.
        
        Args:
            gcs_path: Source path in GCS
            local_path: Destination path on local filesystem
        """
        # Create parent directory if it doesn't exist
        Path(local_path).parent.mkdir(parents=True, exist_ok=True)
        
        blob = self.bucket.blob(gcs_path)
        blob.download_to_filename(local_path)
        logger.info(f"Downloaded gs://{self.bucket_name}/{gcs_path} to {local_path}")
    
    def file_exists(self, gcs_path: str) -> bool:
        """Check if a file exists in GCS.
        
        Args:
            gcs_path: Path to check in GCS
            
        Returns:
            True if file exists, False otherwise
        """
        blob = self.bucket.blob(gcs_path)
        return blob.exists()
    
    def get_file_metadata(self, gcs_path: str) -> Optional[dict]:
        """Get metadata for a file in GCS.
        
        Args:
            gcs_path: Path to file in GCS
            
        Returns:
            Dictionary with file metadata or None if file doesn't exist
        """
        blob = self.bucket.blob(gcs_path)
        if blob.exists():
            blob.reload()
            return {
                'size': blob.size,
                'created': blob.time_created,
                'updated': blob.updated,
                'etag': blob.etag,
                'md5_hash': blob.md5_hash,
                'content_type': blob.content_type
            }
        return None
    
    def list_files(self, prefix: str = "", delimiter: Optional[str] = None) -> List[str]:
        """List files in GCS bucket with given prefix.
        
        Args:
            prefix: Prefix to filter files
            delimiter: Delimiter for directory-like listing
            
        Returns:
            List of file paths
        """
        blobs = self.bucket.list_blobs(prefix=prefix, delimiter=delimiter)
        return [blob.name for blob in blobs if not blob.name.endswith('/')]
    
    def list_directories(self, prefix: str = "") -> List[str]:
        """List directories in GCS bucket with given prefix.
        
        Args:
            prefix: Prefix to filter directories
            
        Returns:
            List of directory paths
        """
        # Use delimiter to get directory-like listing
        iterator = self.bucket.list_blobs(prefix=prefix, delimiter='/')
        prefixes = set()
        
        # Consume iterator to populate prefixes
        for _ in iterator:
            pass
        
        if iterator.prefixes:
            prefixes.update(iterator.prefixes)
        
        return sorted(list(prefixes))
    
    def delete_file(self, gcs_path: str) -> bool:
        """Delete a file from GCS.
        
        Args:
            gcs_path: Path to file in GCS
            
        Returns:
            True if file was deleted, False if it didn't exist
        """
        blob = self.bucket.blob(gcs_path)
        if blob.exists():
            blob.delete()
            logger.info(f"Deleted gs://{self.bucket_name}/{gcs_path}")
            return True
        return False
    
    def copy_file(self, source_path: str, dest_path: str) -> None:
        """Copy a file within GCS.
        
        Args:
            source_path: Source path in GCS
            dest_path: Destination path in GCS
        """
        source_blob = self.bucket.blob(source_path)
        dest_blob = self.bucket.blob(dest_path)
        
        dest_blob.upload_from_string(
            source_blob.download_as_bytes(),
            content_type=source_blob.content_type
        )
        logger.info(f"Copied gs://{self.bucket_name}/{source_path} to "
                   f"gs://{self.bucket_name}/{dest_path}")
    
    def stream_download(self, gcs_path: str, chunk_size: int = 8192) -> Generator[bytes, None, None]:
        """Stream download a file from GCS.
        
        Args:
            gcs_path: Path to file in GCS
            chunk_size: Size of chunks to yield
            
        Yields:
            Chunks of file content
        """
        blob = self.bucket.blob(gcs_path)
        with blob.open("rb") as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                yield chunk
    
    def compare_files(self, gcs_path: str, local_path: str) -> Tuple[bool, str]:
        """Compare a local file with a GCS file.
        
        Args:
            gcs_path: Path to file in GCS
            local_path: Path to local file
            
        Returns:
            Tuple of (files_match, reason)
        """
        # Check if GCS file exists
        blob = self.bucket.blob(gcs_path)
        if not blob.exists():
            return False, "GCS file does not exist"
        
        # Check if local file exists
        if not os.path.exists(local_path):
            return False, "Local file does not exist"
        
        # Compare sizes first
        blob.reload()
        local_size = os.path.getsize(local_path)
        if blob.size != local_size:
            return False, f"Size mismatch: local={local_size}, gcs={blob.size}"
        
        # Compare MD5 hashes
        import hashlib
        with open(local_path, 'rb') as f:
            local_md5 = hashlib.md5(f.read()).hexdigest()
        
        # GCS stores MD5 in base64
        import base64
        gcs_md5 = base64.b64decode(blob.md5_hash).hex()
        
        if local_md5 != gcs_md5:
            return False, f"MD5 mismatch: local={local_md5}, gcs={gcs_md5}"
        
        return True, "Files match"


# Convenience functions
def get_gcs_client(bucket_name: Optional[str] = None) -> GCSClient:
    """Get a GCS client instance.
    
    Args:
        bucket_name: Optional bucket name override
        
    Returns:
        GCSClient instance
    """
    return GCSClient(bucket_name)