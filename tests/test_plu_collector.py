"""Unit tests for PLU (Plan Local d'Urbanisme) collector."""

import json
import os
import tempfile
import unittest
from unittest.mock import Mock, patch, MagicMock, mock_open
from pathlib import Path
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point, Polygon
import requests

from collectors.plu.plu_collector import PLUCollector, plu_collector_main
from utils.utils import setup_logging


class TestPLUCollector(unittest.TestCase):
    """Test cases for PLU collector core functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Mock config to avoid loading real config files
        self.mock_config = {
            'data_sources': {
                'plu': {
                    'wfs_endpoint': 'https://data.geopf.fr/wfs/ows',
                    'service_name': 'gpu',
                    'version': '2.0.0',
                    'output_format': 'application/json',
                    'max_features': 5000,
                    'layer_types': ['GPU.ZONE_URBA', 'GPU.PRESCRIPTION_SURF'],
                    'filter_options': {
                        'use_bbox': True,
                        'use_insee_codes': True,
                        'insee_codes': ['75101', '69001'],
                        'default_bbox': {
                            'min_x': 2.2, 'min_y': 48.8,
                            'max_x': 2.4, 'max_y': 48.9,
                            'srs': 'CRS:84'
                        }
                    },
                    'output_formats': ['geojson', 'geopackage'],
                    'input_srs': 'EPSG:4326',
                    'output_srs': 'EPSG:2154',
                    'enable_incremental': True,
                    'batch_by_department': True,
                    'validate_geometry': True,
                    'timeout_seconds': 120,
                    'retry_on_empty': True,
                    'max_empty_retries': 3
                }
            },
            'processing_config': {
                'batch_size': 1000,
                'max_retries': 3,
                'retry_delay_seconds': 30,
                'timeout_seconds': 300,
                'chunk_size_bytes': 8192,
                'update_schedule': {
                    'plu': 'weekly'
                }
            },
            'logging_config': {
                'level': 'INFO',
                'enable_cloud_logging': False,
                'format': 'json'
            },
            'features': {
                'enable_idempotency_check': True,
                'enable_file_comparison': True
            }
        }
        
        # Sample GeoJSON response for testing
        self.sample_geojson_response = {
            'type': 'FeatureCollection',
            'features': [
                {
                    'type': 'Feature',
                    'properties': {
                        'insee_com': '75101',
                        'libelle': 'Zone Urbaine',
                        'typezone': 'AU',
                        'gpu_timestamp': '2024-01-15T10:30:00Z'
                    },
                    'geometry': {
                        'type': 'Polygon',
                        'coordinates': [[[2.3, 48.85], [2.31, 48.85], [2.31, 48.86], [2.3, 48.86], [2.3, 48.85]]]
                    }
                },
                {
                    'type': 'Feature',
                    'properties': {
                        'insee_com': '75101',
                        'libelle': 'Zone Naturelle',
                        'typezone': 'N',
                        'gpu_timestamp': '2024-01-15T10:30:00Z'
                    },
                    'geometry': {
                        'type': 'Polygon',
                        'coordinates': [[[2.32, 48.85], [2.33, 48.85], [2.33, 48.86], [2.32, 48.86], [2.32, 48.85]]]
                    }
                }
            ]
        }
    
    @patch('collectors.plu.plu_collector.get_config')
    @patch('collectors.plu.plu_collector.get_gcs_client')
    @patch('utils.utils.setup_logging')
    def test_collector_initialization(self, mock_logging, mock_gcs, mock_config):
        """Test PLU collector initialization."""
        mock_config.return_value = Mock()
        mock_config.return_value.get.side_effect = lambda key, default=None: self.mock_config.get(key.replace('.', '_'), {}).get(key.split('.')[-1], default) if '.' in key else self.mock_config.get(key, default)
        mock_gcs.return_value = Mock()
        mock_logging.return_value = Mock()
        
        collector = PLUCollector()
        
        self.assertEqual(collector.collector_name, 'plu')
        self.assertEqual(collector.wfs_endpoint, 'https://data.geopf.fr/wfs/ows')
        self.assertEqual(collector.service_name, 'gpu')
        self.assertEqual(collector.wfs_version, '2.0.0')
        self.assertEqual(collector.max_features, 5000)
        self.assertEqual(len(collector.layer_types), 2)
        self.assertTrue(collector.use_bbox)
        self.assertTrue(collector.use_insee_codes)
    
    @patch('collectors.plu.plu_collector.get_config')
    @patch('collectors.plu.plu_collector.get_gcs_client')
    @patch('utils.utils.setup_logging')
    def test_collect_success(self, mock_logging, mock_gcs, mock_config):
        """Test successful PLU data collection."""
        mock_config.return_value = Mock()
        mock_config.return_value.get.side_effect = lambda key, default=None: self.mock_config.get(key.replace('.', '_'), {}).get(key.split('.')[-1], default) if '.' in key else self.mock_config.get(key, default)
        mock_gcs.return_value = Mock()
        mock_logging.return_value = Mock()
        
        collector = PLUCollector()
        
        # Mock layer collection
        with patch.object(collector, '_collect_layer_data') as mock_collect_layer:
            mock_collect_layer.return_value = {
                'features_count': 10,
                'files_created': 2,
                'requests_made': 1,
                'processing_time_seconds': 5.0
            }
            
            result = collector.collect()
            
            self.assertEqual(result['layers_processed'], 2)  # 2 layer types
            self.assertEqual(result['features_collected'], 20)  # 10 per layer
            self.assertEqual(result['files_collected'], 4)  # 2 per layer
            self.assertEqual(len(result['errors']), 0)
            self.assertIn('layers_summary', result)
    
    @patch('collectors.plu.plu_collector.get_config')
    @patch('collectors.plu.plu_collector.get_gcs_client')
    @patch('utils.utils.setup_logging')
    def test_collect_with_layer_error(self, mock_logging, mock_gcs, mock_config):
        """Test PLU collection with layer processing error."""
        mock_config.return_value = Mock()
        mock_config.return_value.get.side_effect = lambda key, default=None: self.mock_config.get(key.replace('.', '_'), {}).get(key.split('.')[-1], default) if '.' in key else self.mock_config.get(key, default)
        mock_gcs.return_value = Mock()
        mock_logging.return_value = Mock()
        
        collector = PLUCollector()
        
        # Mock layer collection with error
        with patch.object(collector, '_collect_layer_data') as mock_collect_layer:
            mock_collect_layer.side_effect = [
                {'features_count': 10, 'files_created': 2, 'requests_made': 1, 'processing_time_seconds': 5.0},
                Exception("WFS service unavailable")
            ]
            
            result = collector.collect()
            
            self.assertEqual(result['layers_processed'], 1)  # Only first layer succeeded
            self.assertEqual(result['features_collected'], 10)
            self.assertEqual(result['files_collected'], 2)
            self.assertEqual(len(result['errors']), 1)
            self.assertEqual(result['errors'][0]['layer'], 'GPU.PRESCRIPTION_SURF')
    
    @patch('collectors.plu.plu_collector.get_config')
    @patch('collectors.plu.plu_collector.get_gcs_client')
    @patch('utils.utils.setup_logging')
    def test_create_bbox_filter(self, mock_logging, mock_gcs, mock_config):
        """Test bounding box filter creation."""
        mock_config.return_value = Mock()
        mock_config.return_value.get.side_effect = lambda key, default=None: self.mock_config.get(key.replace('.', '_'), {}).get(key.split('.')[-1], default) if '.' in key else self.mock_config.get(key, default)
        mock_gcs.return_value = Mock()
        mock_logging.return_value = Mock()
        
        collector = PLUCollector()
        bbox_filter = collector._create_bbox_filter()
        
        self.assertEqual(bbox_filter['min_x'], 2.2)
        self.assertEqual(bbox_filter['min_y'], 48.8)
        self.assertEqual(bbox_filter['max_x'], 2.4)
        self.assertEqual(bbox_filter['max_y'], 48.9)
        self.assertEqual(bbox_filter['srs'], 'CRS:84')
    
    @patch('collectors.plu.plu_collector.get_config')
    @patch('collectors.plu.plu_collector.get_gcs_client')
    @patch('utils.utils.setup_logging')
    @patch('requests.get')
    def test_fetch_wfs_data_success(self, mock_get, mock_logging, mock_gcs, mock_config):
        """Test successful WFS data fetching."""
        mock_config.return_value = Mock()
        mock_config.return_value.get.side_effect = lambda key, default=None: self.mock_config.get(key.replace('.', '_'), {}).get(key.split('.')[-1], default) if '.' in key else self.mock_config.get(key, default)
        mock_gcs.return_value = Mock()
        mock_logging.return_value = Mock()
        
        # Mock successful HTTP response
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = self.sample_geojson_response
        mock_get.return_value = mock_response
        
        collector = PLUCollector()
        result = collector._fetch_wfs_data('GPU.ZONE_URBA')
        
        self.assertIsNotNone(result)
        self.assertEqual(result['type'], 'FeatureCollection')
        self.assertEqual(len(result['features']), 2)
        
        # Verify request was made with correct parameters
        mock_get.assert_called_once()
        call_args = mock_get.call_args
        self.assertIn('service=WFS', call_args[0][0])
        self.assertIn('typename=GPU.ZONE_URBA', call_args[0][0])
    
    @patch('collectors.plu.plu_collector.get_config')
    @patch('collectors.plu.plu_collector.get_gcs_client')
    @patch('utils.utils.setup_logging')
    @patch('requests.get')
    def test_fetch_wfs_data_with_bbox(self, mock_get, mock_logging, mock_gcs, mock_config):
        """Test WFS data fetching with bounding box filter."""
        mock_config.return_value = Mock()
        mock_config.return_value.get.side_effect = lambda key, default=None: self.mock_config.get(key.replace('.', '_'), {}).get(key.split('.')[-1], default) if '.' in key else self.mock_config.get(key, default)
        mock_gcs.return_value = Mock()
        mock_logging.return_value = Mock()
        
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = self.sample_geojson_response
        mock_get.return_value = mock_response
        
        collector = PLUCollector()
        bbox_filter = {
            'min_x': 2.2, 'min_y': 48.8,
            'max_x': 2.4, 'max_y': 48.9,
            'srs': 'CRS:84'
        }
        
        result = collector._fetch_wfs_data('GPU.ZONE_URBA', bbox_filter=bbox_filter)
        
        self.assertIsNotNone(result)
        
        # Verify bbox parameter was included
        call_args = mock_get.call_args
        self.assertIn('bbox=2.2%2C48.8%2C2.4%2C48.9%2CCRS%3A84', call_args[0][0])
    
    @patch('collectors.plu.plu_collector.get_config')
    @patch('collectors.plu.plu_collector.get_gcs_client')
    @patch('utils.utils.setup_logging')
    @patch('requests.get')
    def test_fetch_wfs_data_network_error(self, mock_get, mock_logging, mock_gcs, mock_config):
        """Test WFS data fetching with network error."""
        mock_config.return_value = Mock()
        mock_config.return_value.get.side_effect = lambda key, default=None: self.mock_config.get(key.replace('.', '_'), {}).get(key.split('.')[-1], default) if '.' in key else self.mock_config.get(key, default)
        mock_gcs.return_value = Mock()
        mock_logging.return_value = Mock()
        
        mock_get.side_effect = requests.exceptions.RequestException("Network error")
        
        collector = PLUCollector()
        result = collector._fetch_wfs_data('GPU.ZONE_URBA')
        
        self.assertIsNone(result)
    
    @patch('collectors.plu.plu_collector.get_config')
    @patch('collectors.plu.plu_collector.get_gcs_client')
    @patch('utils.utils.setup_logging')
    def test_fetch_data_by_insee_codes(self, mock_logging, mock_gcs, mock_config):
        """Test fetching data by INSEE codes."""
        mock_config.return_value = Mock()
        mock_config.return_value.get.side_effect = lambda key, default=None: self.mock_config.get(key.replace('.', '_'), {}).get(key.split('.')[-1], default) if '.' in key else self.mock_config.get(key, default)
        mock_gcs.return_value = Mock()
        mock_logging.return_value = Mock()
        
        collector = PLUCollector()
        
        with patch.object(collector, '_fetch_wfs_data') as mock_fetch:
            mock_fetch.return_value = self.sample_geojson_response
            
            result = collector._fetch_data_by_insee_codes('GPU.ZONE_URBA', ['75101', '69001'])
            
            self.assertIsNotNone(result)
            self.assertEqual(result['type'], 'FeatureCollection')
            self.assertEqual(len(result['features']), 2)
            
            # Should have been called once for the batch
            mock_fetch.assert_called_once()
            
            # Verify CQL filter was used
            call_args = mock_fetch.call_args
            self.assertIn('cql_filter', call_args[1])
            self.assertIn("INSEE_COM IN ('75101','69001')", call_args[1]['cql_filter'])
    
    @patch('collectors.plu.plu_collector.get_config')
    @patch('collectors.plu.plu_collector.get_gcs_client')
    @patch('utils.utils.setup_logging')
    def test_process_features_success(self, mock_logging, mock_gcs, mock_config):
        """Test successful feature processing."""
        mock_config.return_value = Mock()
        mock_config.return_value.get.side_effect = lambda key, default=None: self.mock_config.get(key.replace('.', '_'), {}).get(key.split('.')[-1], default) if '.' in key else self.mock_config.get(key, default)
        mock_gcs.return_value = Mock()
        mock_logging.return_value = Mock()
        
        collector = PLUCollector()
        
        # Mock CRS transformation
        with patch('geopandas.GeoDataFrame.from_features') as mock_from_features, \
             patch('geopandas.GeoDataFrame.to_crs') as mock_to_crs:
            
            # Create mock GeoDataFrame
            mock_gdf = Mock(spec=gpd.GeoDataFrame)
            mock_gdf.empty = False
            mock_gdf.geometry = Mock()
            mock_gdf.geometry.is_valid = pd.Series([True, True])
            mock_gdf.__len__ = Mock(return_value=2)
            mock_gdf.loc = Mock()
            
            mock_from_features.return_value = mock_gdf
            mock_to_crs.return_value = mock_gdf
            
            result = collector._process_features(self.sample_geojson_response, 'GPU.ZONE_URBA')
            
            self.assertIsNotNone(result)
            mock_from_features.assert_called_once()
            mock_to_crs.assert_called_once_with('EPSG:2154')
    
    @patch('collectors.plu.plu_collector.get_config')
    @patch('collectors.plu.plu_collector.get_gcs_client')
    @patch('utils.utils.setup_logging')
    def test_process_features_empty_data(self, mock_logging, mock_gcs, mock_config):
        """Test processing empty feature data."""
        mock_config.return_value = Mock()
        mock_config.return_value.get.side_effect = lambda key, default=None: self.mock_config.get(key.replace('.', '_'), {}).get(key.split('.')[-1], default) if '.' in key else self.mock_config.get(key, default)
        mock_gcs.return_value = Mock()
        mock_logging.return_value = Mock()
        
        collector = PLUCollector()
        
        empty_data = {'type': 'FeatureCollection', 'features': []}
        result = collector._process_features(empty_data, 'GPU.ZONE_URBA')
        
        self.assertIsNone(result)
    
    @patch('collectors.plu.plu_collector.get_config')
    @patch('collectors.plu.plu_collector.get_gcs_client')
    @patch('utils.utils.setup_logging')
    def test_save_as_geojson_success(self, mock_logging, mock_gcs, mock_config):
        """Test successful GeoJSON saving."""
        mock_config.return_value = Mock()
        mock_config.return_value.get.side_effect = lambda key, default=None: self.mock_config.get(key.replace('.', '_'), {}).get(key.split('.')[-1], default) if '.' in key else self.mock_config.get(key, default)
        mock_gcs.return_value = Mock()
        mock_logging.return_value = Mock()
        
        collector = PLUCollector()
        
        # Mock GeoDataFrame
        mock_gdf = Mock(spec=gpd.GeoDataFrame)
        mock_gdf.to_file = Mock()
        
        with patch.object(collector, 'upload_to_gcs', return_value=True) as mock_upload:
            result = collector._save_as_geojson(mock_gdf, 'test.geojson')
            
            self.assertTrue(result)
            mock_upload.assert_called_once()
            
            # Verify GCS path format
            call_args = mock_upload.call_args
            self.assertTrue(call_args[0][1].endswith('/test.geojson'))
    
    @patch('collectors.plu.plu_collector.get_config')
    @patch('collectors.plu.plu_collector.get_gcs_client')
    @patch('utils.utils.setup_logging')
    def test_save_as_geopackage_success(self, mock_logging, mock_gcs, mock_config):
        """Test successful GeoPackage saving."""
        mock_config.return_value = Mock()
        mock_config.return_value.get.side_effect = lambda key, default=None: self.mock_config.get(key.replace('.', '_'), {}).get(key.split('.')[-1], default) if '.' in key else self.mock_config.get(key, default)
        mock_gcs.return_value = Mock()
        mock_logging.return_value = Mock()
        
        collector = PLUCollector()
        
        # Mock GeoDataFrame
        mock_gdf = Mock(spec=gpd.GeoDataFrame)
        mock_gdf.to_file = Mock()
        
        with patch.object(collector, 'upload_to_gcs', return_value=True) as mock_upload:
            result = collector._save_as_geopackage(mock_gdf, 'test.gpkg')
            
            self.assertTrue(result)
            mock_upload.assert_called_once()
            
            # Verify GCS path format
            call_args = mock_upload.call_args
            self.assertTrue(call_args[0][1].endswith('/test.gpkg'))
    
    @patch('collectors.plu.plu_collector.get_config')
    @patch('collectors.plu.plu_collector.get_gcs_client')
    @patch('utils.utils.setup_logging')
    def test_validate_data_success(self, mock_logging, mock_gcs, mock_config):
        """Test successful data validation."""
        mock_config.return_value = Mock()
        mock_config.return_value.get.side_effect = lambda key, default=None: self.mock_config.get(key.replace('.', '_'), {}).get(key.split('.')[-1], default) if '.' in key else self.mock_config.get(key, default)
        mock_gcs.return_value = Mock()
        mock_logging.return_value = Mock()
        
        collector = PLUCollector()
        
        # Create mock GeoDataFrame
        mock_gdf = Mock(spec=gpd.GeoDataFrame)
        mock_gdf.empty = False
        mock_gdf.columns = ['geometry', 'insee_com', 'libelle']
        mock_gdf.geometry = Mock()
        mock_gdf.geometry.is_valid = pd.Series([True, True])
        mock_gdf.__len__ = Mock(return_value=2)
        
        result = collector.validate_data(mock_gdf)
        
        self.assertTrue(result)
    
    @patch('collectors.plu.plu_collector.get_config')
    @patch('collectors.plu.plu_collector.get_gcs_client')
    @patch('utils.utils.setup_logging')
    def test_validate_data_not_geodataframe(self, mock_logging, mock_gcs, mock_config):
        """Test data validation with non-GeoDataFrame input."""
        mock_config.return_value = Mock()
        mock_config.return_value.get.side_effect = lambda key, default=None: self.mock_config.get(key.replace('.', '_'), {}).get(key.split('.')[-1], default) if '.' in key else self.mock_config.get(key, default)
        mock_gcs.return_value = Mock()
        mock_logging.return_value = Mock()
        
        collector = PLUCollector()
        
        result = collector.validate_data("not a geodataframe")
        
        self.assertFalse(result)
    
    @patch('collectors.plu.plu_collector.get_config')
    @patch('collectors.plu.plu_collector.get_gcs_client')
    @patch('utils.utils.setup_logging')
    def test_validate_data_empty_geodataframe(self, mock_logging, mock_gcs, mock_config):
        """Test data validation with empty GeoDataFrame."""
        mock_config.return_value = Mock()
        mock_config.return_value.get.side_effect = lambda key, default=None: self.mock_config.get(key.replace('.', '_'), {}).get(key.split('.')[-1], default) if '.' in key else self.mock_config.get(key, default)
        mock_gcs.return_value = Mock()
        mock_logging.return_value = Mock()
        
        collector = PLUCollector()
        
        # Create empty mock GeoDataFrame
        mock_gdf = Mock(spec=gpd.GeoDataFrame)
        mock_gdf.empty = True
        
        result = collector.validate_data(mock_gdf)
        
        self.assertFalse(result)
    
    @patch('collectors.plu.plu_collector.get_config')
    @patch('collectors.plu.plu_collector.get_gcs_client')
    @patch('utils.utils.setup_logging')
    def test_validate_data_missing_geometry(self, mock_logging, mock_gcs, mock_config):
        """Test data validation with missing geometry column."""
        mock_config.return_value = Mock()
        mock_config.return_value.get.side_effect = lambda key, default=None: self.mock_config.get(key.replace('.', '_'), {}).get(key.split('.')[-1], default) if '.' in key else self.mock_config.get(key, default)
        mock_gcs.return_value = Mock()
        mock_logging.return_value = Mock()
        
        collector = PLUCollector()
        
        # Create mock GeoDataFrame without geometry column
        mock_gdf = Mock(spec=gpd.GeoDataFrame)
        mock_gdf.empty = False
        mock_gdf.columns = ['insee_com', 'libelle']  # Missing geometry
        
        result = collector.validate_data(mock_gdf)
        
        self.assertFalse(result)


class TestPLUCollectorIntegration(unittest.TestCase):
    """Integration tests for PLU collector."""
    
    def setUp(self):
        """Set up integration test fixtures."""
        self.mock_config = {
            'data_sources': {
                'plu': {
                    'wfs_endpoint': 'https://test.example.com/wfs',
                    'layer_types': ['GPU.ZONE_URBA'],
                    'filter_options': {'use_bbox': True, 'default_bbox': {'min_x': 2.2, 'min_y': 48.8, 'max_x': 2.4, 'max_y': 48.9, 'srs': 'CRS:84'}},
                    'output_formats': ['geojson']
                }
            },
            'processing_config': {'update_schedule': {'plu': 'weekly'}},
            'logging_config': {'level': 'INFO', 'enable_cloud_logging': False, 'format': 'json'},
            'features': {'enable_idempotency_check': False}
        }
    
    @patch('collectors.plu.plu_collector.get_config')
    @patch('collectors.plu.plu_collector.get_gcs_client')
    @patch('utils.utils.setup_logging')
    def test_collector_full_run_success(self, mock_logging, mock_gcs, mock_config):
        """Test complete collector run with success."""
        mock_config.return_value = Mock()
        mock_config.return_value.get.side_effect = lambda key, default=None: self.mock_config.get(key.replace('.', '_'), {}).get(key.split('.')[-1], default) if '.' in key else self.mock_config.get(key, default)
        mock_gcs.return_value = Mock()
        mock_logging.return_value = Mock()
        
        collector = PLUCollector()
        
        with patch.object(collector, 'collect') as mock_collect, \
             patch.object(collector, 'save_metadata') as mock_save_metadata:
            
            mock_collect.return_value = {
                'files_collected': 2,
                'layers_processed': 1,
                'features_collected': 10,
                'errors': []
            }
            
            result = collector.run()
            
            self.assertEqual(result['status'], 'completed')
            self.assertEqual(result['files_collected'], 2)
            mock_collect.assert_called_once()
            mock_save_metadata.assert_called_once()
    
    @patch('collectors.plu.plu_collector.get_config')
    @patch('collectors.plu.plu_collector.get_gcs_client')
    @patch('utils.utils.setup_logging')
    def test_cloud_function_entry_point_success(self, mock_logging, mock_gcs, mock_config):
        """Test Cloud Function entry point with success."""
        mock_config.return_value = Mock()
        mock_config.return_value.get.side_effect = lambda key, default=None: self.mock_config.get(key.replace('.', '_'), {}).get(key.split('.')[-1], default) if '.' in key else self.mock_config.get(key, default)
        mock_gcs.return_value = Mock()
        mock_logging.return_value = Mock()
        
        with patch('collectors.plu.plu_collector.PLUCollector') as mock_collector_class:
            mock_collector = Mock()
            mock_collector.run.return_value = {
                'status': 'completed',
                'files_collected': 2,
                'layers_processed': 1
            }
            mock_collector_class.return_value = mock_collector
            
            result = plu_collector_main()
            
            self.assertEqual(result['statusCode'], 200)
            self.assertIn('body', result)
            body = json.loads(result['body'])
            self.assertEqual(body['status'], 'completed')
    
    @patch('collectors.plu.plu_collector.get_config')
    @patch('collectors.plu.plu_collector.get_gcs_client')
    @patch('utils.utils.setup_logging')
    def test_cloud_function_entry_point_error(self, mock_logging, mock_gcs, mock_config):
        """Test Cloud Function entry point with error."""
        mock_config.return_value = Mock()
        mock_config.return_value.get.side_effect = lambda key, default=None: self.mock_config.get(key.replace('.', '_'), {}).get(key.split('.')[-1], default) if '.' in key else self.mock_config.get(key, default)
        mock_gcs.return_value = Mock()
        mock_logging.return_value = Mock()
        
        with patch('collectors.plu.plu_collector.PLUCollector') as mock_collector_class:
            mock_collector = Mock()
            mock_collector.run.return_value = {
                'status': 'failed',
                'files_collected': 0,
                'errors': [{'error': 'WFS service unavailable', 'type': 'RequestException'}]
            }
            mock_collector_class.return_value = mock_collector
            
            result = plu_collector_main()
            
            self.assertEqual(result['statusCode'], 500)
            self.assertIn('body', result)
            body = json.loads(result['body'])
            self.assertEqual(body['status'], 'failed')


if __name__ == '__main__':
    unittest.main()