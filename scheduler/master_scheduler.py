"""Master Scheduler for France Data Collection Project.

This module orchestrates the execution of all data collectors (DVF, SIRENE, INSEE, PLU)
asynchronously with failure isolation and comprehensive status tracking.
"""

import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Dict, Any, Optional
import requests

# Optional imports for Cloud Tasks and Secret Manager
try:
    from google.cloud import tasks_v2
except ImportError:
    tasks_v2 = None

try:
    from google.cloud import secretmanager
except ImportError:
    secretmanager = None

from config.config_loader import get_config
from utils.gcs_client import get_gcs_client
from utils.utils import setup_logging


class MasterScheduler:
    """Master scheduler for orchestrating all data collectors."""
    
    def __init__(self):
        """Initialize the master scheduler."""
        self.config = get_config()
        self.gcs_client = get_gcs_client()
        
        # Setup logging
        self.logger = self._setup_logging()
        
        # Scheduler configuration
        self.scheduler_config = self.config.get('scheduler', {})
        self.use_cloud_tasks = self.scheduler_config.get('use_cloud_tasks', False)
        self.project_id = os.environ.get('GCP_PROJECT_ID', '')
        self.location = os.environ.get('GCP_LOCATION', 'europe-west9')
        self.queue_name = self.scheduler_config.get('queue_name', 'data-collectors')
        
        # Collector configurations
        self.collectors = self._get_enabled_collectors()
        
        # Execution tracking
        self.execution_results = {
            'start_time': None,
            'end_time': None,
            'collectors': {},
            'summary': {
                'total': 0,
                'succeeded': 0,
                'failed': 0,
                'skipped': 0
            }
        }
        
        # Initialize Cloud Tasks client if enabled
        if self.use_cloud_tasks:
            if tasks_v2 is None:
                self.logger.warning("Cloud Tasks enabled but google-cloud-tasks not installed")
                self.use_cloud_tasks = False
            else:
                self.tasks_client = tasks_v2.CloudTasksClient()
                self.parent = self.tasks_client.queue_path(
                    self.project_id, self.location, self.queue_name
                )
    
    def _setup_logging(self) -> logging.Logger:
        """Setup logging for the scheduler."""
        log_level = self.config.get('logging_config.level', 'INFO')
        enable_cloud = self.config.get('logging_config.enable_cloud_logging', True)
        log_format = self.config.get('logging_config.format', 'json')
        
        return setup_logging(
            "scheduler.master",
            level=log_level,
            enable_cloud_logging=enable_cloud,
            log_format=log_format
        )
    
    def _get_enabled_collectors(self) -> Dict[str, Dict]:
        """Get list of enabled collectors from configuration.
        
        Returns:
            Dictionary of enabled collectors with their configurations
        """
        all_collectors = {
            'dvf': {
                'name': 'DVF Collector',
                'function_name': 'dvf-collector',
                'url': self.scheduler_config.get('collector_urls', {}).get('dvf'),
                'timeout': 540,  # 9 minutes
                'enabled': self.scheduler_config.get('collectors', {}).get('dvf', {}).get('enabled', True)
            },
            'sirene': {
                'name': 'SIRENE Collector',
                'function_name': 'sirene-collector',
                'url': self.scheduler_config.get('collector_urls', {}).get('sirene'),
                'timeout': 540,
                'enabled': self.scheduler_config.get('collectors', {}).get('sirene', {}).get('enabled', True)
            },
            'insee_contours': {
                'name': 'INSEE Contours Collector',
                'function_name': 'insee-contours-collector',
                'url': self.scheduler_config.get('collector_urls', {}).get('insee_contours'),
                'timeout': 540,
                'enabled': self.scheduler_config.get('collectors', {}).get('insee_contours', {}).get('enabled', True)
            },
            'plu': {
                'name': 'PLU Collector',
                'function_name': 'plu-collector',
                'url': self.scheduler_config.get('collector_urls', {}).get('plu'),
                'timeout': 540,
                'enabled': self.scheduler_config.get('collectors', {}).get('plu', {}).get('enabled', True)
            }
        }
        
        # Filter only enabled collectors
        enabled_collectors = {
            key: config for key, config in all_collectors.items()
            if config['enabled']
        }
        
        self.logger.info(f"Enabled collectors: {list(enabled_collectors.keys())}")
        
        return enabled_collectors
    
    def schedule_collectors(self) -> Dict[str, Any]:
        """Main scheduling method that triggers all enabled collectors.
        
        Returns:
            Dictionary with overall execution results
        """
        self.execution_results['start_time'] = datetime.now(timezone.utc).isoformat()
        self.execution_results['summary']['total'] = len(self.collectors)
        
        self.logger.info(f"Starting master scheduler with {len(self.collectors)} collectors")
        
        # Execute collectors asynchronously
        if self.use_cloud_tasks:
            results = self._schedule_with_cloud_tasks()
        else:
            results = self._schedule_with_http_calls()
        
        # Process results
        for collector_name, result in results.items():
            self.execution_results['collectors'][collector_name] = result
            
            if result['status'] == 'success':
                self.execution_results['summary']['succeeded'] += 1
            elif result['status'] == 'failed':
                self.execution_results['summary']['failed'] += 1
            else:
                self.execution_results['summary']['skipped'] += 1
        
        self.execution_results['end_time'] = datetime.now(timezone.utc).isoformat()
        
        # Calculate duration
        start = datetime.fromisoformat(self.execution_results['start_time'])
        end = datetime.fromisoformat(self.execution_results['end_time'])
        self.execution_results['duration_seconds'] = (end - start).total_seconds()
        
        # Generate and save report
        self._save_execution_report()
        
        self.logger.info(
            f"Master scheduler completed - "
            f"Succeeded: {self.execution_results['summary']['succeeded']}, "
            f"Failed: {self.execution_results['summary']['failed']}, "
            f"Duration: {self.execution_results['duration_seconds']:.2f}s"
        )
        
        return self.execution_results
    
    def _schedule_with_cloud_tasks(self) -> Dict[str, Dict]:
        """Schedule collectors using Google Cloud Tasks.
        
        Returns:
            Dictionary of collector results
        """
        results = {}
        
        for collector_name, collector_config in self.collectors.items():
            try:
                self.logger.info(f"Creating Cloud Task for {collector_name}")
                
                # Create task
                task = {
                    'http_request': {
                        'http_method': tasks_v2.HttpMethod.POST,
                        'url': collector_config['url']
                    }
                }
                
                # Add authentication if service account email is provided
                if self.scheduler_config.get('service_account_email'):
                    task['http_request']['oidc_token'] = {
                        'service_account_email': self.scheduler_config['service_account_email']
                    }
                
                # Create the task
                response = self.tasks_client.create_task(
                    request={'parent': self.parent, 'task': task}
                )
                
                results[collector_name] = {
                    'status': 'scheduled',
                    'task_name': response.name,
                    'scheduled_time': datetime.now(timezone.utc).isoformat(),
                    'message': f"Task created: {response.name}"
                }
                
                self.logger.info(f"Created task for {collector_name}: {response.name}")
                
            except Exception as e:
                error_msg = f"Failed to create Cloud Task for {collector_name}: {str(e)}"
                self.logger.error(error_msg)
                
                results[collector_name] = {
                    'status': 'failed',
                    'error': str(e),
                    'error_type': type(e).__name__,
                    'scheduled_time': datetime.now(timezone.utc).isoformat()
                }
        
        return results
    
    def _schedule_with_http_calls(self) -> Dict[str, Dict]:
        """Schedule collectors using direct HTTP calls with thread pool.
        
        Returns:
            Dictionary of collector results
        """
        results = {}
        
        # Use ThreadPoolExecutor for concurrent HTTP calls
        max_workers = self.scheduler_config.get('max_concurrent_collectors', 4)
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all collector tasks
            future_to_collector = {
                executor.submit(
                    self._trigger_collector_http,
                    collector_name,
                    collector_config
                ): collector_name
                for collector_name, collector_config in self.collectors.items()
            }
            
            # Process completed tasks
            for future in as_completed(future_to_collector):
                collector_name = future_to_collector[future]
                
                try:
                    result = future.result()
                    results[collector_name] = result
                except Exception as e:
                    self.logger.error(f"Collector {collector_name} raised exception: {e}")
                    results[collector_name] = {
                        'status': 'failed',
                        'error': str(e),
                        'error_type': type(e).__name__,
                        'execution_time': datetime.now(timezone.utc).isoformat()
                    }
        
        return results
    
    def _trigger_collector_http(self, collector_name: str, collector_config: Dict) -> Dict:
        """Trigger a single collector via HTTP call.
        
        Args:
            collector_name: Name of the collector
            collector_config: Collector configuration
            
        Returns:
            Dictionary with execution result
        """
        start_time = datetime.now(timezone.utc)
        
        try:
            url = collector_config.get('url')
            if not url:
                # If URL not provided, construct from function name
                base_url = self.scheduler_config.get('base_function_url', '')
                url = f"{base_url}/{collector_config['function_name']}"
            
            self.logger.info(f"Triggering {collector_name} at {url}")
            
            # Make HTTP request
            headers = {'Content-Type': 'application/json'}
            
            # Add authentication token if available
            auth_token = self._get_auth_token()
            if auth_token:
                headers['Authorization'] = f'Bearer {auth_token}'
            
            response = requests.post(
                url,
                headers=headers,
                json={},
                timeout=collector_config.get('timeout', 300)
            )
            
            end_time = datetime.now(timezone.utc)
            duration = (end_time - start_time).total_seconds()
            
            if response.status_code == 200:
                result_data = response.json() if response.text else {}
                
                return {
                    'status': 'success',
                    'http_status': response.status_code,
                    'start_time': start_time.isoformat(),
                    'end_time': end_time.isoformat(),
                    'duration_seconds': duration,
                    'result': result_data
                }
            else:
                return {
                    'status': 'failed',
                    'http_status': response.status_code,
                    'error': f"HTTP {response.status_code}: {response.text}",
                    'start_time': start_time.isoformat(),
                    'end_time': end_time.isoformat(),
                    'duration_seconds': duration
                }
                
        except requests.exceptions.Timeout:
            end_time = datetime.now(timezone.utc)
            duration = (end_time - start_time).total_seconds()
            
            return {
                'status': 'failed',
                'error': f"Request timeout after {collector_config.get('timeout', 300)}s",
                'error_type': 'Timeout',
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat(),
                'duration_seconds': duration
            }
            
        except Exception as e:
            end_time = datetime.now(timezone.utc)
            duration = (end_time - start_time).total_seconds()
            
            return {
                'status': 'failed',
                'error': str(e),
                'error_type': type(e).__name__,
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat(),
                'duration_seconds': duration
            }
    
    def _get_auth_token(self) -> Optional[str]:
        """Get authentication token for Cloud Function calls.
        
        Returns:
            Authentication token or None if not available
        """
        # Try to get token from environment variable
        token = os.environ.get('FUNCTION_AUTH_TOKEN')
        if token:
            return token
        
        # Try to get from Secret Manager if configured
        if self.scheduler_config.get('use_secret_manager') and secretmanager:
            try:
                client = secretmanager.SecretManagerServiceClient()
                name = f"projects/{self.project_id}/secrets/function-auth-token/versions/latest"
                response = client.access_secret_version(request={"name": name})
                return response.payload.data.decode("UTF-8")
            except Exception as e:
                self.logger.warning(f"Failed to get auth token from Secret Manager: {e}")
        
        return None
    
    def _save_execution_report(self) -> None:
        """Save execution report to GCS."""
        try:
            # Generate report filename
            timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
            report_path = f"scheduler/reports/execution_{timestamp}.json"
            
            # Save to temporary file
            temp_path = f"/tmp/scheduler_report_{timestamp}.json"
            with open(temp_path, 'w') as f:
                json.dump(self.execution_results, f, indent=2)
            
            # Upload to GCS
            self.gcs_client.upload_file(
                temp_path,
                report_path,
                content_type='application/json'
            )
            
            # Also save as latest report
            self.gcs_client.copy_file(
                report_path,
                "scheduler/reports/latest_execution.json"
            )
            
            # Cleanup
            os.remove(temp_path)
            
            self.logger.info(f"Execution report saved to {report_path}")
            
        except Exception as e:
            self.logger.error(f"Failed to save execution report: {e}")
    
    def get_last_execution_status(self) -> Optional[Dict]:
        """Get the status of the last execution.
        
        Returns:
            Dictionary with last execution results or None if not found
        """
        try:
            temp_path = "/tmp/last_execution.json"
            self.gcs_client.download_file(
                "scheduler/reports/latest_execution.json",
                temp_path
            )
            
            with open(temp_path, 'r') as f:
                last_execution = json.load(f)
            
            os.remove(temp_path)
            
            return last_execution
            
        except Exception as e:
            self.logger.debug(f"No previous execution found: {e}")
            return None


def master_scheduler_main(event=None, context=None):
    """Cloud Function entry point for master scheduler.
    
    Args:
        event: Cloud Function event (unused)
        context: Cloud Function context (unused)
        
    Returns:
        Dictionary with scheduling results
    """
    # Suppress unused parameter warnings
    _ = event, context
    scheduler = MasterScheduler()
    results = scheduler.schedule_collectors()
    
    return {
        'statusCode': 200 if results['summary']['failed'] == 0 else 207,
        'body': json.dumps(results),
        'headers': {
            'Content-Type': 'application/json'
        }
    }


if __name__ == "__main__":
    # Direct execution for testing
    scheduler = MasterScheduler()
    results = scheduler.schedule_collectors()
    print(json.dumps(results, indent=2))