"""
INSEE Contours Collector Tests

Tests for the INSEE geographic boundaries data collector functionality.

Author: Claude Code Assistant
Created: 2025-06-25
"""

import json
import os
import tempfile
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import pytest
import requests

from collectors.insee_contours.insee_contours_collector import INSEEContoursCollector, insee_contours_collector_main
from utils import FranceDataError, NetworkError, ValidationError


class TestINSEEContoursCollector:
    """INSEE地理边界收集器核心功能测试"""
    
    @pytest.fixture
    def sample_config(self):
        """测试配置"""
        return {
            'gcs_config': {
                'bucket_name': 'test-bucket'
            },
            'insee_contours': {
                'base_url': 'https://www.data.gouv.fr/fr/datasets/contours-iris-2023/',
                'api_endpoint': 'https://www.data.gouv.fr/api/1/datasets/',
                'ign_base_url': 'https://data.geopf.fr/telechargement/download/',
                'data_types': ['iris', 'communes'],
                'formats': ['shapefile', 'geojson'],
                'download_ign_data': True,
                'download_datagouv': True,
                'download_geozones': True,
                'target_year': 2024,
                'preferred_projection': 'lambert93'
            }
        }
    
    @pytest.fixture
    def mock_gcs_client(self):
        """模拟GCS客户端"""
        return Mock()
    
    @pytest.fixture
    def insee_collector(self, sample_config, mock_gcs_client):
        """创建INSEE收集器实例"""
        with patch('config.config_loader.get_config', return_value=sample_config), \
             patch('utils.gcs_client.get_gcs_client', return_value=mock_gcs_client):
            collector = INSEEContoursCollector(sample_config)
            collector.gcs_client = mock_gcs_client
            return collector
    
    @pytest.fixture
    def sample_datagouv_response(self):
        """模拟data.gouv.fr API响应"""
        return {
            'id': '5c34944e634f4164071119c5',
            'title': 'Contour des IRIS INSEE',
            'resources': [
                {
                    'id': 'resource1',
                    'title': 'IRIS_shapefile.zip',
                    'url': 'https://example.com/iris.zip',
                    'format': 'SHP',
                    'mime': 'application/zip',
                    'filesize': 1000000
                },
                {
                    'id': 'resource2',
                    'title': 'IRIS_geojson.json',
                    'url': 'https://example.com/iris.geojson',
                    'format': 'GeoJSON',
                    'mime': 'application/json',
                    'filesize': 2000000
                }
            ]
        }
    
    @pytest.fixture
    def sample_geojson_data(self):
        """示例GeoJSON数据"""
        return {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "properties": {
                        "code": "751010101",
                        "nom": "Quartier Saint-Germain-l'Auxerrois"
                    },
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [[[2.337, 48.860], [2.338, 48.860], [2.338, 48.861], [2.337, 48.861], [2.337, 48.860]]]
                    }
                }
            ]
        }
    
    def test_collect_success(self, insee_collector):
        """测试成功的数据收集"""
        with patch.object(insee_collector, '_collect_ign_data') as mock_ign, \
             patch.object(insee_collector, '_collect_datagouv_data') as mock_datagouv, \
             patch.object(insee_collector, '_collect_geozones_data') as mock_geozones:
            
            # 设置模拟返回值
            mock_ign.return_value = [
                {'status': 'success', 'source': 'ign', 'filename': 'iris_ign.zip'}
            ]
            mock_datagouv.return_value = [
                {'status': 'success', 'source': 'datagouv', 'filename': 'iris_datagouv.zip'}
            ]
            mock_geozones.return_value = [
                {'status': 'success', 'source': 'geozones', 'filename': 'iris_geozones.geojson'}
            ]
            
            # 执行收集
            result = insee_collector.collect()
            
            # 验证结果
            assert result['collector'] == 'insee_contours'
            assert result['status'] == 'success'
            assert result['files_processed'] == 3
            assert result['successful_downloads'] == 3
            assert result['data_sources']['ign_official'] == 1
            assert result['data_sources']['datagouv'] == 1
            assert result['data_sources']['geozones'] == 1
    
    def test_collect_partial_failure(self, insee_collector):
        """测试部分失败的数据收集"""
        with patch.object(insee_collector, '_collect_ign_data') as mock_ign, \
             patch.object(insee_collector, '_collect_datagouv_data') as mock_datagouv, \
             patch.object(insee_collector, '_collect_geozones_data') as mock_geozones:
            
            mock_ign.return_value = [
                {'status': 'success', 'source': 'ign', 'filename': 'iris_ign.zip'}
            ]
            mock_datagouv.return_value = [
                {'status': 'failed', 'source': 'datagouv', 'error': 'Network error'}
            ]
            mock_geozones.return_value = [
                {'status': 'success', 'source': 'geozones', 'filename': 'iris_geozones.geojson'}
            ]
            
            result = insee_collector.collect()
            
            assert result['status'] == 'partial'
            assert result['successful_downloads'] == 2
            assert result['failed_downloads'] == 1
    
    def test_collect_ign_data(self, insee_collector):
        """测试IGN数据收集"""
        with patch.object(insee_collector, '_download_geographic_file') as mock_download:
            mock_download.return_value = {
                'status': 'success',
                'filename': 'contours_iris_lambert93_2024.zip',
                'source': 'ign'
            }
            
            results = insee_collector._collect_ign_data()
            
            # 验证调用了下载方法
            assert mock_download.call_count >= 1
            assert all(r['source'] == 'ign' for r in results if 'source' in r)
    
    def test_get_dataset_resources_success(self, insee_collector, sample_datagouv_response):
        """测试成功获取数据集资源"""
        with patch('requests.get') as mock_get:
            mock_response = Mock()
            mock_response.json.return_value = sample_datagouv_response
            mock_response.raise_for_status.return_value = None
            mock_get.return_value = mock_response
            
            api_url = 'https://www.data.gouv.fr/api/1/datasets/test-id/'
            resources = insee_collector._get_dataset_resources(api_url)
            
            assert len(resources) == 2
            assert resources[0]['title'] == 'IRIS_shapefile.zip'
            assert resources[1]['format'] == 'GeoJSON'
    
    def test_get_dataset_resources_network_error(self, insee_collector):
        """测试网络错误时获取数据集资源"""
        with patch('requests.get') as mock_get:
            mock_get.side_effect = requests.RequestException("Connection error")
            
            with pytest.raises(NetworkError):
                insee_collector._get_dataset_resources('https://example.com/api')
    
    def test_should_download_resource(self, insee_collector):
        """测试资源下载决策"""
        # 应该下载的资源
        good_resource = {
            'url': 'https://example.com/iris.zip',
            'format': 'SHP',
            'mime': 'application/zip'
        }
        dataset_info = {'data_type': 'iris'}
        
        assert insee_collector._should_download_resource(good_resource, dataset_info) is True
        
        # 不应该下载的资源（格式不匹配）
        bad_resource = {
            'url': 'https://example.com/iris.txt',
            'format': 'TXT',
            'mime': 'text/plain'
        }
        
        assert insee_collector._should_download_resource(bad_resource, dataset_info) is False
    
    def test_detect_format(self, insee_collector):
        """测试文件格式检测"""
        test_cases = [
            ({'url': 'https://example.com/data.zip', 'format': 'SHP'}, 'shapefile'),
            ({'url': 'https://example.com/data.geojson', 'format': 'GeoJSON'}, 'geojson'),
            ({'url': 'https://example.com/data.gpkg', 'format': 'GPKG'}, 'geopackage'),
            ({'url': 'https://example.com/data.txt', 'format': 'TXT'}, 'unknown')
        ]
        
        for resource, expected_format in test_cases:
            detected_format = insee_collector._detect_format(resource)
            assert detected_format == expected_format
    
    def test_generate_filename(self, insee_collector):
        """测试文件名生成"""
        resource = {'title': 'Original IRIS Data', 'url': 'https://example.com/data.zip'}
        dataset_info = {'data_type': 'iris', 'year': 2024}
        
        # Mock格式检测
        with patch.object(insee_collector, '_detect_format', return_value='shapefile'):
            filename = insee_collector._generate_filename(resource, dataset_info)
            assert filename == 'iris_2024_shapefile.zip'
    
    def test_download_geographic_file_success(self, insee_collector):
        """测试成功的地理文件下载"""
        download_info = {
            'filename': 'test_iris.zip',
            'url': 'https://example.com/test.zip',
            'data_type': 'iris',
            'format': 'shapefile',
            'year': 2024
        }
        
        with patch('collectors.insee_contours.insee_contours_collector.file_exists_in_gcs') as mock_exists, \
             patch('collectors.insee_contours.insee_contours_collector.download_file_with_retry') as mock_download, \
             patch('collectors.insee_contours.insee_contours_collector.upload_to_gcs') as mock_upload, \
             patch.object(insee_collector, '_validate_geographic_file') as mock_validate, \
             patch.object(insee_collector, '_get_remote_file_metadata') as mock_metadata, \
             patch('pathlib.Path.unlink') as mock_unlink, \
             patch('pathlib.Path.exists') as mock_exists_path, \
             patch('pathlib.Path.stat') as mock_stat:
            
            mock_exists.return_value = False
            mock_exists_path.return_value = True
            mock_stat.return_value = Mock(st_size=1000)
            
            result = insee_collector._download_geographic_file(download_info, source='test')
            
            assert result['status'] == 'success'
            assert result['filename'] == 'test_iris.zip'
            assert result['source'] == 'test'
            mock_download.assert_called_once()
            mock_upload.assert_called_once()
            mock_validate.assert_called_once()
    
    def test_download_geographic_file_skip_existing(self, insee_collector):
        """测试跳过已存在的地理文件"""
        download_info = {
            'filename': 'test_iris.zip',
            'url': 'https://example.com/test.zip',
            'data_type': 'iris',
            'year': 2024
        }
        
        with patch('collectors.insee_contours.insee_contours_collector.file_exists_in_gcs') as mock_exists, \
             patch('collectors.insee_contours.insee_contours_collector.get_file_metadata') as mock_local_meta, \
             patch.object(insee_collector, '_get_remote_file_metadata') as mock_remote_meta:
            
            mock_exists.return_value = True
            mock_local_meta.return_value = {'size': 1000}
            mock_remote_meta.return_value = {'size': 1000}
            
            result = insee_collector._download_geographic_file(download_info, source='test')
            
            assert result['status'] == 'skipped'
            assert result['reason'] == 'file_exists_same_size'
    
    def test_download_geographic_file_failure(self, insee_collector):
        """测试地理文件下载失败"""
        download_info = {
            'filename': 'test_iris.zip',
            'url': 'https://example.com/test.zip',
            'data_type': 'iris',
            'year': 2024
        }
        
        with patch('collectors.insee_contours.insee_contours_collector.file_exists_in_gcs') as mock_exists, \
             patch('collectors.insee_contours.insee_contours_collector.download_file_with_retry') as mock_download:
            
            mock_exists.return_value = False
            mock_download.side_effect = Exception("Download failed")
            
            result = insee_collector._download_geographic_file(download_info, source='test')
            
            assert result['status'] == 'failed'
            assert 'Download failed' in result['error']
    
    def test_get_remote_file_metadata_success(self, insee_collector):
        """测试成功获取远程文件元数据"""
        with patch('requests.head') as mock_head:
            mock_response = Mock()
            mock_response.headers = {
                'content-length': '2000000',
                'last-modified': 'Mon, 01 Jan 2024 00:00:00 GMT',
                'content-type': 'application/zip'
            }
            mock_response.raise_for_status.return_value = None
            mock_head.return_value = mock_response
            
            metadata = insee_collector._get_remote_file_metadata('https://example.com/test.zip')
            
            assert metadata['size'] == 2000000
            assert metadata['last_modified'] == 'Mon, 01 Jan 2024 00:00:00 GMT'
            assert metadata['content_type'] == 'application/zip'
    
    def test_get_remote_file_metadata_failure(self, insee_collector):
        """测试获取远程文件元数据失败"""
        with patch('requests.head') as mock_head:
            mock_head.side_effect = requests.RequestException("Request failed")
            
            metadata = insee_collector._get_remote_file_metadata('https://example.com/test.zip')
            
            assert metadata is None
    
    def test_validate_shapefile_zip_success(self, insee_collector):
        """测试Shapefile ZIP验证成功"""
        with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as temp_file:
            # 创建一个有效的Shapefile ZIP
            with zipfile.ZipFile(temp_file.name, 'w') as zf:
                zf.writestr('test.shp', b'shapefile content')
                zf.writestr('test.shx', b'index content')
                zf.writestr('test.dbf', b'attributes content')
                zf.writestr('test.prj', b'projection content')
            
            temp_path = Path(temp_file.name)
            
            try:
                # 验证应该成功
                insee_collector._validate_shapefile_zip(temp_path)
            finally:
                temp_path.unlink()
    
    def test_validate_shapefile_zip_missing_components(self, insee_collector):
        """测试Shapefile ZIP缺少组件"""
        with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as temp_file:
            # 创建一个缺少必需组件的ZIP
            with zipfile.ZipFile(temp_file.name, 'w') as zf:
                zf.writestr('test.shp', b'shapefile content')
                # 缺少.shx和.dbf文件
            
            temp_path = Path(temp_file.name)
            
            try:
                with pytest.raises(ValidationError, match="缺少必需的Shapefile组件"):
                    insee_collector._validate_shapefile_zip(temp_path)
            finally:
                temp_path.unlink()
    
    def test_validate_geojson_file_success(self, insee_collector, sample_geojson_data):
        """测试GeoJSON文件验证成功"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.geojson', delete=False, encoding='utf-8') as temp_file:
            json.dump(sample_geojson_data, temp_file)
            temp_file.flush()
            
            temp_path = Path(temp_file.name)
            
            try:
                # 验证应该成功
                insee_collector._validate_geojson_file(temp_path)
            finally:
                temp_path.unlink()
    
    def test_validate_geojson_file_invalid_json(self, insee_collector):
        """测试无效JSON的GeoJSON文件"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.geojson', delete=False, encoding='utf-8') as temp_file:
            temp_file.write('{ invalid json content')
            temp_file.flush()
            
            temp_path = Path(temp_file.name)
            
            try:
                with pytest.raises(ValidationError, match="无效的JSON格式"):
                    insee_collector._validate_geojson_file(temp_path)
            finally:
                temp_path.unlink()
    
    def test_validate_geojson_file_missing_type(self, insee_collector):
        """测试缺少type字段的GeoJSON文件"""
        invalid_geojson = {"features": []}
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.geojson', delete=False, encoding='utf-8') as temp_file:
            json.dump(invalid_geojson, temp_file)
            temp_file.flush()
            
            temp_path = Path(temp_file.name)
            
            try:
                with pytest.raises(ValidationError, match="缺少type字段"):
                    insee_collector._validate_geojson_file(temp_path)
            finally:
                temp_path.unlink()
    
    def test_validate_geojson_file_no_features(self, insee_collector):
        """测试没有要素的GeoJSON文件"""
        empty_geojson = {"type": "FeatureCollection", "features": []}
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.geojson', delete=False, encoding='utf-8') as temp_file:
            json.dump(empty_geojson, temp_file)
            temp_file.flush()
            
            temp_path = Path(temp_file.name)
            
            try:
                with pytest.raises(ValidationError, match="不包含任何要素"):
                    insee_collector._validate_geojson_file(temp_path)
            finally:
                temp_path.unlink()
    
    def test_validate_geopackage_file_success(self, insee_collector):
        """测试GeoPackage文件验证成功"""
        with tempfile.NamedTemporaryFile(suffix='.gpkg', delete=False) as temp_file:
            # 写入SQLite文件头，确保文件大于1KB
            temp_file.write(b'SQLite format 3\x00' + b'\x00' * 2000)
            temp_file.flush()
            
            temp_path = Path(temp_file.name)
            
            try:
                # 验证应该成功
                insee_collector._validate_geopackage_file(temp_path)
            finally:
                temp_path.unlink()
    
    def test_validate_geopackage_file_invalid_header(self, insee_collector):
        """测试无效头部的GeoPackage文件"""
        with tempfile.NamedTemporaryFile(suffix='.gpkg', delete=False) as temp_file:
            # 写入无效头部，确保文件大于1KB
            temp_file.write(b'Invalid header content' + b'\x00' * 2000)
            temp_file.flush()
            
            temp_path = Path(temp_file.name)
            
            try:
                with pytest.raises(ValidationError, match="不是有效的SQLite/GeoPackage文件"):
                    insee_collector._validate_geopackage_file(temp_path)
            finally:
                temp_path.unlink()
    
    def test_validate_data_valid_shapefile(self, insee_collector):
        """测试有效Shapefile的数据验证"""
        with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as temp_file:
            with zipfile.ZipFile(temp_file.name, 'w') as zf:
                zf.writestr('test.shp', b'shapefile content')
                zf.writestr('test.shx', b'index content')
                zf.writestr('test.dbf', b'attributes content')
            
            try:
                result = insee_collector.validate_data(temp_file.name)
                assert result is True
            finally:
                Path(temp_file.name).unlink()
    
    def test_validate_data_valid_geojson(self, insee_collector, sample_geojson_data):
        """测试有效GeoJSON的数据验证"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.geojson', delete=False, encoding='utf-8') as temp_file:
            json.dump(sample_geojson_data, temp_file)
            temp_file.flush()
            
            try:
                result = insee_collector.validate_data(temp_file.name)
                assert result is True
            finally:
                Path(temp_file.name).unlink()
    
    def test_validate_data_invalid_file(self, insee_collector):
        """测试无效文件的数据验证"""
        result = insee_collector.validate_data('invalid.txt')
        assert result is False


class TestINSEEContoursCollectorIntegration:
    """INSEE地理边界收集器集成测试"""
    
    def test_collector_initialization(self):
        """测试收集器初始化"""
        config = {
            'gcs_config': {'bucket_name': 'test-bucket'},
            'insee_contours': {
                'base_url': 'https://www.data.gouv.fr/fr/datasets/contours-iris-2023/',
                'data_types': ['iris', 'communes'],
                'target_year': 2024
            }
        }
        
        with patch('config.config_loader.get_config', return_value=config), \
             patch('utils.gcs_client.get_gcs_client'):
            collector = INSEEContoursCollector(config)
            
            assert 'iris' in collector.data_types
            assert 'communes' in collector.data_types
            assert collector.target_year == 2024
            assert collector.download_ign_data is True
    
    def test_cloud_function_entry_point_success(self):
        """测试Cloud Function入口点成功"""
        mock_config = {
            'gcs_config': {'bucket_name': 'test-bucket'},
            'insee_contours': {'base_url': 'https://test.com/'}
        }
        
        with patch('builtins.open', mock_open_yaml(mock_config)), \
             patch('os.path.exists', return_value=True), \
             patch('os.environ.get', return_value='/test/config.yaml'), \
             patch('collectors.insee_contours.insee_contours_collector.setup_logging'), \
             patch.object(INSEEContoursCollector, 'collect') as mock_collect:
            
            mock_collect.return_value = {
                'collector': 'insee_contours',
                'status': 'success',
                'files_processed': 3
            }
            
            result = insee_contours_collector_main()
            result_data = json.loads(result)
            
            assert result_data['collector'] == 'insee_contours'
            assert result_data['status'] == 'success'
    
    def test_cloud_function_entry_point_error(self):
        """测试Cloud Function入口点错误处理"""
        with patch('os.path.exists', return_value=False):
            result = insee_contours_collector_main()
            result_data = json.loads(result)
            
            assert result_data['collector'] == 'insee_contours'
            assert result_data['status'] == 'error'
            assert 'error' in result_data


def mock_open_yaml(yaml_content):
    """模拟YAML文件读取"""
    import yaml
    from unittest.mock import mock_open
    
    yaml_str = yaml.dump(yaml_content)
    return mock_open(read_data=yaml_str)


if __name__ == "__main__":
    pytest.main([__file__])