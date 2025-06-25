"""Base collector class for all data collectors.

This module provides the base functionality that all specific data collectors
(DVF, SIRENE, INSEE, PLU) will inherit from.
"""

import logging
import os
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, List, Optional, Any
import json

from config.config_loader import get_config
from utils.gcs_client import get_gcs_client


class BaseCollector(ABC):
    """Abstract base class for data collectors."""
    
    def __init__(self, collector_name: str):
        """Initialize the base collector.
        
        Args:
            collector_name: Name of the collector (e.g., 'dvf', 'sirene')
        """
        self.collector_name = collector_name
        self.config = get_config()
        self.gcs_client = get_gcs_client()
        
        # Setup logging
        self.logger = self._setup_logging()
        
        # Load collector-specific configuration
        self.collector_config = self.config.get(f'data_sources.{collector_name}', {})
        
        # Processing configuration
        self.batch_size = self.config.get('processing_config.batch_size', 1000)
        self.max_retries = self.config.get('processing_config.max_retries', 3)
        self.retry_delay = self.config.get('processing_config.retry_delay_seconds', 30)
        self.timeout = self.config.get('processing_config.timeout_seconds', 300)
        
        # Paths
        self.raw_path = f"raw/{collector_name}"
        self.processed_path = f"processed/{collector_name}"
        self.metadata_path = f"metadata/{collector_name}"
        
    def _setup_logging(self) -> logging.Logger:
        """Setup logging for the collector."""
        logger = logging.getLogger(f"collector.{self.collector_name}")
        
        # Configure based on config
        log_level = self.config.get('logging_config.level', 'INFO')
        logger.setLevel(getattr(logging, log_level))
        
        # Add handler if not already present
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    @abstractmethod
    def collect(self) -> Dict[str, Any]:
        """Main collection method to be implemented by subclasses.
        
        Returns:
            Dictionary with collection results and metadata
        """
        pass
    
    @abstractmethod
    def validate_data(self, data: Any) -> bool:
        """Validate collected data before storage.
        
        Args:
            data: Data to validate
            
        Returns:
            True if data is valid, False otherwise
        """
        pass
    
    def run(self) -> Dict[str, Any]:
        """Run the complete collection process."""
        start_time = datetime.utcnow()
        result = {
            'collector': self.collector_name,
            'start_time': start_time.isoformat(),
            'status': 'started',
            'files_collected': 0,
            'errors': []
        }
        
        try:
            self.logger.info(f"Starting {self.collector_name} collection")
            
            # Check if collection should run (idempotency)
            if self.should_collect():
                # Run the collection
                collection_result = self.collect()
                result.update(collection_result)
                
                # Save metadata
                self.save_metadata(result)
                
                result['status'] = 'completed'
            else:
                result['status'] = 'skipped'
                result['reason'] = 'No new data to collect'
                self.logger.info(f"Skipping {self.collector_name} - no new data")
            
        except Exception as e:
            self.logger.error(f"Error in {self.collector_name} collection: {str(e)}")
            result['status'] = 'failed'
            result['errors'].append({
                'error': str(e),
                'type': type(e).__name__
            })
        
        finally:
            end_time = datetime.utcnow()
            result['end_time'] = end_time.isoformat()
            result['duration_seconds'] = (end_time - start_time).total_seconds()
            
            self.logger.info(
                f"Completed {self.collector_name} collection - "
                f"Status: {result['status']}, "
                f"Files: {result['files_collected']}, "
                f"Duration: {result['duration_seconds']:.2f}s"
            )
        
        return result
    
    def should_collect(self) -> bool:
        """Check if collection should run based on idempotency rules.
        
        Returns:
            True if collection should proceed, False otherwise
        """
        if not self.config.get('features.enable_idempotency_check', True):
            return True
        
        # Check last run metadata
        last_run_path = f"{self.metadata_path}/last_run.json"
        
        try:
            # Download last run metadata if exists
            temp_path = "/tmp/last_run.json"
            self.gcs_client.download_file(last_run_path, temp_path)
            
            with open(temp_path, 'r') as f:
                last_run = json.load(f)
            
            # Check update schedule
            schedule = self.config.get(
                f'processing_config.update_schedule.{self.collector_name}',
                'daily'
            )
            
            last_run_time = datetime.fromisoformat(last_run['end_time'])
            time_since_last_run = datetime.utcnow() - last_run_time
            
            # Simple schedule check (can be enhanced)
            if schedule == 'daily' and time_since_last_run.days < 1:
                return False
            elif schedule == 'weekly' and time_since_last_run.days < 7:
                return False
            elif schedule == 'monthly' and time_since_last_run.days < 30:
                return False
            elif schedule == 'yearly' and time_since_last_run.days < 365:
                return False
            
        except Exception as e:
            # If no metadata or error, proceed with collection
            self.logger.debug(f"No previous run metadata found: {e}")
        
        return True
    
    def save_metadata(self, metadata: Dict[str, Any]) -> None:
        """Save collection metadata to GCS.
        
        Args:
            metadata: Metadata dictionary to save
        """
        # Save timestamped metadata
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        metadata_file = f"{self.metadata_path}/run_{timestamp}.json"
        
        temp_path = f"/tmp/metadata_{timestamp}.json"
        with open(temp_path, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        self.gcs_client.upload_file(
            temp_path,
            metadata_file,
            content_type='application/json'
        )
        
        # Also save as last_run.json
        self.gcs_client.copy_file(
            metadata_file,
            f"{self.metadata_path}/last_run.json"
        )
        
        # Cleanup temp file
        os.remove(temp_path)
    
    def download_file(self, url: str, local_path: str) -> bool:
        """Download a file with retry logic.
        
        Args:
            url: URL to download from
            local_path: Local path to save file
            
        Returns:
            True if successful, False otherwise
        """
        import requests
        from tenacity import retry, stop_after_attempt, wait_exponential
        
        @retry(
            stop=stop_after_attempt(self.max_retries),
            wait=wait_exponential(multiplier=1, min=4, max=self.retry_delay)
        )
        def _download():
            response = requests.get(url, stream=True, timeout=self.timeout)
            response.raise_for_status()
            
            # Create directory if needed
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            
            # Download in chunks
            chunk_size = self.config.get('processing_config.chunk_size_bytes', 8192)
            with open(local_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)
            
            return True
        
        try:
            return _download()
        except Exception as e:
            self.logger.error(f"Failed to download {url}: {e}")
            return False
    
    def upload_to_gcs(self, local_path: str, gcs_path: str) -> bool:
        """Upload a file to GCS with idempotency check.
        
        Args:
            local_path: Local file path
            gcs_path: GCS destination path
            
        Returns:
            True if uploaded or already exists with same content, False on error
        """
        try:
            # Check if file already exists and compare
            if self.config.get('features.enable_file_comparison', True):
                if self.gcs_client.file_exists(gcs_path):
                    matches, reason = self.gcs_client.compare_files(gcs_path, local_path)
                    if matches:
                        self.logger.info(f"File already exists and matches: {gcs_path}")
                        return True
                    else:
                        self.logger.info(f"File exists but differs ({reason}), uploading: {gcs_path}")
            
            # Upload the file
            self.gcs_client.upload_file(local_path, gcs_path)
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to upload {local_path} to {gcs_path}: {e}")
            return False
    
    def get_existing_files(self, prefix: str) -> List[str]:
        """Get list of existing files in GCS with given prefix.
        
        Args:
            prefix: GCS path prefix
            
        Returns:
            List of file paths
        """
        return self.gcs_client.list_files(prefix=prefix)