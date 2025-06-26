"""Unit tests for Master Scheduler."""

import json
import os
import tempfile
import unittest
from datetime import datetime, timezone
from unittest.mock import Mock, patch, MagicMock, call
from concurrent.futures import Future
import requests

from scheduler.master_scheduler import MasterScheduler, master_scheduler_main


class TestMasterScheduler(unittest.TestCase):
    """Test cases for Master Scheduler functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Mock configuration
        self.mock_config = {
            'scheduler': {
                'use_cloud_tasks': False,
                'queue_name': 'test-queue',
                'service_account_email': 'test@example.com',
                'use_secret_manager': False,
                'max_concurrent_collectors': 2,
                'base_function_url': 'https://test.cloudfunctions.net',
                'collectors': {
                    'dvf': {'enabled': True, 'schedule': '0 2 * * *'},
                    'sirene': {'enabled': True, 'schedule': '0 3 * * *'},
                    'insee_contours': {'enabled': False, 'schedule': '0 4 * * 0'},
                    'plu': {'enabled': True, 'schedule': '0 5 * * 0'}
                },
                'collector_urls': {
                    'dvf': 'https://test.com/dvf',
                    'sirene': 'https://test.com/sirene',
                    'plu': 'https://test.com/plu'
                }
            },
            'logging_config': {
                'level': 'INFO',
                'enable_cloud_logging': False,
                'format': 'json'
            }
        }
        
        # Set environment variables
        os.environ['GCP_PROJECT_ID'] = 'test-project'
        os.environ['GCP_LOCATION'] = 'europe-west9'
    
    def tearDown(self):
        """Clean up after tests."""
        # Clean up environment variables
        for key in ['GCP_PROJECT_ID', 'GCP_LOCATION', 'FUNCTION_AUTH_TOKEN']:
            if key in os.environ:
                del os.environ[key]
    
    @patch('scheduler.master_scheduler.get_config')
    @patch('scheduler.master_scheduler.get_gcs_client')
    @patch('scheduler.master_scheduler.setup_logging')
    def test_scheduler_initialization(self, mock_logging, mock_gcs, mock_config):
        """Test scheduler initialization."""
        mock_config.return_value = Mock()
        mock_config.return_value.get.side_effect = lambda key, default=None: {
            'scheduler': self.mock_config['scheduler'],
            'scheduler.use_cloud_tasks': False,
            'scheduler.queue_name': 'test-queue',
            'scheduler.collectors': self.mock_config['scheduler']['collectors'],
            'scheduler.collector_urls': self.mock_config['scheduler']['collector_urls'],
            'logging_config.level': 'INFO',
            'logging_config.enable_cloud_logging': False,
            'logging_config.format': 'json'
        }.get(key, default)
        
        mock_gcs.return_value = Mock()
        mock_logging.return_value = Mock()
        
        scheduler = MasterScheduler()
        
        # Verify initialization
        self.assertFalse(scheduler.use_cloud_tasks)
        self.assertEqual(scheduler.queue_name, 'test-queue')
        self.assertEqual(len(scheduler.collectors), 3)  # Only enabled collectors
        self.assertIn('dvf', scheduler.collectors)
        self.assertIn('sirene', scheduler.collectors)
        self.assertIn('plu', scheduler.collectors)
        self.assertNotIn('insee_contours', scheduler.collectors)  # Disabled
    
    @patch('scheduler.master_scheduler.get_config')
    @patch('scheduler.master_scheduler.get_gcs_client')
    @patch('scheduler.master_scheduler.setup_logging')
    def test_get_enabled_collectors(self, mock_logging, mock_gcs, mock_config):
        """Test getting enabled collectors."""
        mock_config.return_value = Mock()
        mock_config.return_value.get.side_effect = lambda key, default=None: {
            'scheduler': self.mock_config['scheduler'],
            'scheduler.use_cloud_tasks': False,
            'scheduler.collectors': self.mock_config['scheduler']['collectors'],
            'scheduler.collector_urls': self.mock_config['scheduler']['collector_urls'],
            'logging_config.level': 'INFO',
            'logging_config.enable_cloud_logging': False,
            'logging_config.format': 'json'
        }.get(key, default)
        
        mock_gcs.return_value = Mock()
        mock_logging.return_value = Mock()
        
        scheduler = MasterScheduler()
        enabled_collectors = scheduler._get_enabled_collectors()
        
        self.assertEqual(len(enabled_collectors), 3)
        self.assertTrue(enabled_collectors['dvf']['enabled'])
        self.assertTrue(enabled_collectors['sirene']['enabled'])
        self.assertTrue(enabled_collectors['plu']['enabled'])
        self.assertNotIn('insee_contours', enabled_collectors)
    
    @patch('scheduler.master_scheduler.get_config')
    @patch('scheduler.master_scheduler.get_gcs_client')
    @patch('scheduler.master_scheduler.setup_logging')
    @patch('requests.post')
    def test_schedule_collectors_http_success(self, mock_post, mock_logging, mock_gcs, mock_config):
        """Test successful scheduling with HTTP calls."""
        mock_config.return_value = Mock()
        mock_config.return_value.get.side_effect = lambda key, default=None: {
            'scheduler': self.mock_config['scheduler'],
            'scheduler.use_cloud_tasks': False,
            'scheduler.max_concurrent_collectors': 2,
            'scheduler.collectors': self.mock_config['scheduler']['collectors'],
            'scheduler.collector_urls': self.mock_config['scheduler']['collector_urls'],
            'logging_config.level': 'INFO',
            'logging_config.enable_cloud_logging': False,
            'logging_config.format': 'json'
        }.get(key, default)
        
        mock_gcs_client = Mock()
        mock_gcs.return_value = mock_gcs_client
        mock_logging.return_value = Mock()
        
        # Mock successful HTTP responses
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'status': 'completed', 'files_collected': 10}
        mock_response.text = '{"status": "completed", "files_collected": 10}'
        mock_post.return_value = mock_response
        
        scheduler = MasterScheduler()
        results = scheduler.schedule_collectors()
        
        # Verify results
        self.assertEqual(results['summary']['total'], 3)
        self.assertEqual(results['summary']['succeeded'], 3)
        self.assertEqual(results['summary']['failed'], 0)
        self.assertEqual(results['summary']['skipped'], 0)
        
        # Verify each collector was called
        self.assertEqual(mock_post.call_count, 3)
        
        # Verify report was saved
        mock_gcs_client.upload_file.assert_called()
        mock_gcs_client.copy_file.assert_called()
    
    @patch('scheduler.master_scheduler.get_config')
    @patch('scheduler.master_scheduler.get_gcs_client')
    @patch('scheduler.master_scheduler.setup_logging')
    @patch('requests.post')
    def test_schedule_collectors_with_failures(self, mock_post, mock_logging, mock_gcs, mock_config):
        """Test scheduling with some collector failures."""
        mock_config.return_value = Mock()
        mock_config.return_value.get.side_effect = lambda key, default=None: {
            'scheduler': self.mock_config['scheduler'],
            'scheduler.use_cloud_tasks': False,
            'scheduler.max_concurrent_collectors': 2,
            'scheduler.collectors': self.mock_config['scheduler']['collectors'],
            'scheduler.collector_urls': self.mock_config['scheduler']['collector_urls'],
            'logging_config.level': 'INFO',
            'logging_config.enable_cloud_logging': False,
            'logging_config.format': 'json'
        }.get(key, default)
        
        mock_gcs.return_value = Mock()
        mock_logging.return_value = Mock()
        
        # Mock mixed responses
        def side_effect(*args, **kwargs):
            url = args[0]
            if 'dvf' in url:
                response = Mock()
                response.status_code = 200
                response.json.return_value = {'status': 'completed'}
                response.text = '{"status": "completed"}'
                return response
            elif 'sirene' in url:
                response = Mock()
                response.status_code = 500
                response.text = 'Internal Server Error'
                return response
            else:  # plu
                raise requests.exceptions.Timeout()
        
        mock_post.side_effect = side_effect
        
        scheduler = MasterScheduler()
        results = scheduler.schedule_collectors()
        
        # Verify mixed results
        self.assertEqual(results['summary']['total'], 3)
        self.assertEqual(results['summary']['succeeded'], 1)  # Only DVF succeeded
        self.assertEqual(results['summary']['failed'], 2)  # SIRENE and PLU failed
        self.assertEqual(results['summary']['skipped'], 0)
        
        # Verify individual collector results
        self.assertEqual(results['collectors']['dvf']['status'], 'success')
        self.assertEqual(results['collectors']['sirene']['status'], 'failed')
        self.assertEqual(results['collectors']['plu']['status'], 'failed')
    
    @patch('scheduler.master_scheduler.get_config')
    @patch('scheduler.master_scheduler.get_gcs_client')
    @patch('scheduler.master_scheduler.setup_logging')
    def test_trigger_collector_http_success(self, mock_logging, mock_gcs, mock_config):
        """Test triggering a single collector successfully."""
        mock_config.return_value = Mock()
        mock_config.return_value.get.side_effect = lambda key, default=None: {
            'scheduler': self.mock_config['scheduler'],
            'logging_config.level': 'INFO',
            'logging_config.enable_cloud_logging': False,
            'logging_config.format': 'json'
        }.get(key, default)
        
        mock_gcs.return_value = Mock()
        mock_logging.return_value = Mock()
        
        scheduler = MasterScheduler()
        
        collector_config = {
            'name': 'Test Collector',
            'url': 'https://test.com/collector',
            'timeout': 300
        }
        
        with patch('requests.post') as mock_post:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = {'status': 'completed'}
            mock_response.text = '{"status": "completed"}'
            mock_post.return_value = mock_response
            
            result = scheduler._trigger_collector_http('test', collector_config)
            
            self.assertEqual(result['status'], 'success')
            self.assertEqual(result['http_status'], 200)
            self.assertIn('start_time', result)
            self.assertIn('end_time', result)
            self.assertIn('duration_seconds', result)
    
    @patch('scheduler.master_scheduler.get_config')
    @patch('scheduler.master_scheduler.get_gcs_client')
    @patch('scheduler.master_scheduler.setup_logging')
    def test_trigger_collector_http_timeout(self, mock_logging, mock_gcs, mock_config):
        """Test collector timeout handling."""
        mock_config.return_value = Mock()
        mock_config.return_value.get.side_effect = lambda key, default=None: {
            'scheduler': self.mock_config['scheduler'],
            'logging_config.level': 'INFO',
            'logging_config.enable_cloud_logging': False,
            'logging_config.format': 'json'
        }.get(key, default)
        
        mock_gcs.return_value = Mock()
        mock_logging.return_value = Mock()
        
        scheduler = MasterScheduler()
        
        collector_config = {
            'name': 'Test Collector',
            'url': 'https://test.com/collector',
            'timeout': 10
        }
        
        with patch('requests.post') as mock_post:
            mock_post.side_effect = requests.exceptions.Timeout()
            
            result = scheduler._trigger_collector_http('test', collector_config)
            
            self.assertEqual(result['status'], 'failed')
            self.assertEqual(result['error_type'], 'Timeout')
            self.assertIn('Request timeout after 10s', result['error'])
    
    @patch('scheduler.master_scheduler.get_config')
    @patch('scheduler.master_scheduler.get_gcs_client')
    @patch('scheduler.master_scheduler.setup_logging')
    @patch('scheduler.master_scheduler.tasks_v2')
    def test_schedule_with_cloud_tasks(self, mock_tasks_v2, mock_logging, mock_gcs, mock_config):
        """Test scheduling with Cloud Tasks."""
        # Update config to use Cloud Tasks
        config_with_tasks = self.mock_config.copy()
        config_with_tasks['scheduler']['use_cloud_tasks'] = True
        
        mock_config.return_value = Mock()
        mock_config.return_value.get.side_effect = lambda key, default=None: {
            'scheduler': config_with_tasks['scheduler'],
            'scheduler.use_cloud_tasks': True,
            'scheduler.queue_name': 'test-queue',
            'scheduler.service_account_email': 'test@example.com',
            'scheduler.collectors': config_with_tasks['scheduler']['collectors'],
            'scheduler.collector_urls': config_with_tasks['scheduler']['collector_urls'],
            'logging_config.level': 'INFO',
            'logging_config.enable_cloud_logging': False,
            'logging_config.format': 'json'
        }.get(key, default)
        
        mock_gcs.return_value = Mock()
        mock_logging.return_value = Mock()
        
        # Mock Cloud Tasks client
        mock_tasks_client = Mock()
        mock_task_response = Mock()
        mock_task_response.name = 'projects/test/locations/us-central1/queues/test-queue/tasks/12345'
        mock_tasks_client.create_task.return_value = mock_task_response
        mock_tasks_client.queue_path.return_value = 'projects/test/locations/us-central1/queues/test-queue'
        mock_tasks_v2.CloudTasksClient.return_value = mock_tasks_client
        
        scheduler = MasterScheduler()
        results = scheduler.schedule_collectors()
        
        # Verify Cloud Tasks were created
        self.assertEqual(mock_tasks_client.create_task.call_count, 3)  # 3 enabled collectors
        
        # Verify results
        self.assertEqual(results['summary']['total'], 3)
        for collector_name in ['dvf', 'sirene', 'plu']:
            self.assertEqual(results['collectors'][collector_name]['status'], 'scheduled')
            self.assertIn('task_name', results['collectors'][collector_name])
    
    @patch('scheduler.master_scheduler.get_config')
    @patch('scheduler.master_scheduler.get_gcs_client')
    @patch('scheduler.master_scheduler.setup_logging')
    def test_get_auth_token(self, mock_logging, mock_gcs, mock_config):
        """Test authentication token retrieval."""
        mock_config.return_value = Mock()
        mock_config.return_value.get.side_effect = lambda key, default=None: {
            'scheduler': self.mock_config['scheduler'],
            'logging_config.level': 'INFO',
            'logging_config.enable_cloud_logging': False,
            'logging_config.format': 'json'
        }.get(key, default)
        
        mock_gcs.return_value = Mock()
        mock_logging.return_value = Mock()
        
        scheduler = MasterScheduler()
        
        # Test with environment variable
        os.environ['FUNCTION_AUTH_TOKEN'] = 'test-token'
        token = scheduler._get_auth_token()
        self.assertEqual(token, 'test-token')
        
        # Clean up
        del os.environ['FUNCTION_AUTH_TOKEN']
        
        # Test without token
        token = scheduler._get_auth_token()
        self.assertIsNone(token)
    
    @patch('scheduler.master_scheduler.get_config')
    @patch('scheduler.master_scheduler.get_gcs_client')
    @patch('scheduler.master_scheduler.setup_logging')
    def test_save_execution_report(self, mock_logging, mock_gcs, mock_config):
        """Test saving execution report."""
        mock_config.return_value = Mock()
        mock_config.return_value.get.side_effect = lambda key, default=None: {
            'scheduler': self.mock_config['scheduler'],
            'logging_config.level': 'INFO',
            'logging_config.enable_cloud_logging': False,
            'logging_config.format': 'json'
        }.get(key, default)
        
        mock_gcs_client = Mock()
        mock_gcs.return_value = mock_gcs_client
        mock_logging.return_value = Mock()
        
        scheduler = MasterScheduler()
        scheduler.execution_results = {
            'start_time': datetime.now(timezone.utc).isoformat(),
            'end_time': datetime.now(timezone.utc).isoformat(),
            'collectors': {},
            'summary': {'total': 3, 'succeeded': 3, 'failed': 0, 'skipped': 0}
        }
        
        scheduler._save_execution_report()
        
        # Verify GCS operations
        mock_gcs_client.upload_file.assert_called_once()
        mock_gcs_client.copy_file.assert_called_once()
        
        # Verify file paths
        upload_call = mock_gcs_client.upload_file.call_args
        self.assertTrue(upload_call[0][1].startswith('scheduler/reports/execution_'))
        self.assertTrue(upload_call[0][1].endswith('.json'))
        
        copy_call = mock_gcs_client.copy_file.call_args
        self.assertEqual(copy_call[0][1], 'scheduler/reports/latest_execution.json')
    
    @patch('scheduler.master_scheduler.get_config')
    @patch('scheduler.master_scheduler.get_gcs_client')
    @patch('scheduler.master_scheduler.setup_logging')
    def test_get_last_execution_status(self, mock_logging, mock_gcs, mock_config):
        """Test retrieving last execution status."""
        mock_config.return_value = Mock()
        mock_config.return_value.get.side_effect = lambda key, default=None: {
            'scheduler': self.mock_config['scheduler'],
            'logging_config.level': 'INFO',
            'logging_config.enable_cloud_logging': False,
            'logging_config.format': 'json'
        }.get(key, default)
        
        mock_gcs_client = Mock()
        mock_gcs.return_value = mock_gcs_client
        mock_logging.return_value = Mock()
        
        # Mock successful download
        last_execution_data = {
            'start_time': '2024-01-01T10:00:00Z',
            'end_time': '2024-01-01T10:05:00Z',
            'summary': {'total': 3, 'succeeded': 3, 'failed': 0}
        }
        
        def download_side_effect(gcs_path, local_path):
            with open(local_path, 'w') as f:
                json.dump(last_execution_data, f)
        
        mock_gcs_client.download_file.side_effect = download_side_effect
        
        scheduler = MasterScheduler()
        result = scheduler.get_last_execution_status()
        
        self.assertIsNotNone(result)
        self.assertEqual(result['summary']['total'], 3)
        self.assertEqual(result['summary']['succeeded'], 3)
    
    @patch('scheduler.master_scheduler.MasterScheduler')
    def test_cloud_function_entry_point_success(self, mock_scheduler_class):
        """Test Cloud Function entry point with success."""
        mock_scheduler = Mock()
        mock_scheduler.schedule_collectors.return_value = {
            'summary': {'total': 3, 'succeeded': 3, 'failed': 0, 'skipped': 0},
            'collectors': {}
        }
        mock_scheduler_class.return_value = mock_scheduler
        
        result = master_scheduler_main()
        
        self.assertEqual(result['statusCode'], 200)
        self.assertIn('body', result)
        body = json.loads(result['body'])
        self.assertEqual(body['summary']['succeeded'], 3)
    
    @patch('scheduler.master_scheduler.MasterScheduler')
    def test_cloud_function_entry_point_partial_failure(self, mock_scheduler_class):
        """Test Cloud Function entry point with partial failures."""
        mock_scheduler = Mock()
        mock_scheduler.schedule_collectors.return_value = {
            'summary': {'total': 3, 'succeeded': 2, 'failed': 1, 'skipped': 0},
            'collectors': {}
        }
        mock_scheduler_class.return_value = mock_scheduler
        
        result = master_scheduler_main()
        
        self.assertEqual(result['statusCode'], 207)  # Multi-status for partial success
        self.assertIn('body', result)
        body = json.loads(result['body'])
        self.assertEqual(body['summary']['failed'], 1)


if __name__ == '__main__':
    unittest.main()