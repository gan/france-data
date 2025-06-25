"""
SIRENE Collector Tests

Tests for the SIRENE data collector functionality.

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

from collectors.sirene.sirene_collector import SireneCollector, sirene_collector_main
from utils import FranceDataError, NetworkError, ValidationError


class TestSireneCollector:
    """SIRENE收集器核心功能测试"""
    
    @pytest.fixture
    def sample_config(self):
        """测试配置"""
        return {
            'gcs_config': {
                'bucket_name': 'test-bucket'
            },
            'sirene': {
                'base_url': 'https://files.data.gouv.fr/insee-sirene/',
                'stock_files': [
                    'StockEtablissement_utf8.zip',
                    'StockUniteLegale_utf8.zip'
                ],
                'optional_files': [
                    'StockEtablissementHistorique_utf8.zip',
                    'StockUniteLegaleHistorique_utf8.zip'
                ],
                'download_historical': False,
                'download_optional': False,
                'months_back': 3
            }
        }
    
    @pytest.fixture
    def mock_gcs_client(self):
        """模拟GCS客户端"""
        return Mock()
    
    @pytest.fixture
    def sirene_collector(self, sample_config, mock_gcs_client):
        """创建SIRENE收集器实例"""
        with patch('config.config_loader.get_config', return_value=sample_config), \
             patch('utils.gcs_client.get_gcs_client', return_value=mock_gcs_client):
            collector = SireneCollector(sample_config)
            collector.gcs_client = mock_gcs_client
            return collector
    
    @pytest.fixture
    def sample_html_response(self):
        """模拟HTML目录响应"""
        return """
        <html>
        <body>
        <a href="2024-06-01-StockEtablissement_utf8.zip">2024-06-01-StockEtablissement_utf8.zip</a>
        <a href="2024-06-01-StockUniteLegale_utf8.zip">2024-06-01-StockUniteLegale_utf8.zip</a>
        <a href="2024-05-01-StockEtablissement_utf8.zip">2024-05-01-StockEtablissement_utf8.zip</a>
        <a href="2024-05-01-StockEtablissementHistorique_utf8.zip">2024-05-01-StockEtablissementHistorique_utf8.zip</a>
        <a href="2024-05-01-StockDoublons_utf8.zip">2024-05-01-StockDoublons_utf8.zip</a>
        <a href="../">Parent Directory</a>
        <a href="invalid-file.txt">invalid-file.txt</a>
        </body>
        </html>
        """
    
    def test_collect_success(self, sirene_collector):
        """测试成功的数据收集"""
        with patch.object(sirene_collector, '_get_available_files') as mock_get_files, \
             patch.object(sirene_collector, '_filter_files_to_download') as mock_filter, \
             patch.object(sirene_collector, '_download_file') as mock_download:
            
            # 设置模拟返回值
            mock_get_files.return_value = [
                {'filename': 'test1.zip', 'date': datetime.now()},
                {'filename': 'test2.zip', 'date': datetime.now()}
            ]
            mock_filter.return_value = [
                {'filename': 'test1.zip', 'date': datetime.now()}
            ]
            mock_download.return_value = {
                'filename': 'test1.zip',
                'status': 'success',
                'gcs_path': 'raw/sirene/2024/test1.zip'
            }
            
            # 执行收集
            result = sirene_collector.collect()
            
            # 验证结果
            assert result['collector'] == 'sirene'
            assert result['status'] == 'success'
            assert result['files_processed'] == 1
            assert result['successful_downloads'] == 1
    
    def test_collect_partial_failure(self, sirene_collector):
        """测试部分失败的数据收集"""
        with patch.object(sirene_collector, '_get_available_files') as mock_get_files, \
             patch.object(sirene_collector, '_filter_files_to_download') as mock_filter, \
             patch.object(sirene_collector, '_download_file') as mock_download:
            
            mock_get_files.return_value = [
                {'filename': 'test1.zip'},
                {'filename': 'test2.zip'}
            ]
            mock_filter.return_value = [
                {'filename': 'test1.zip'},
                {'filename': 'test2.zip'}
            ]
            
            # 一个成功，一个失败
            mock_download.side_effect = [
                {'filename': 'test1.zip', 'status': 'success'},
                {'filename': 'test2.zip', 'status': 'failed', 'error': 'Network error'}
            ]
            
            result = sirene_collector.collect()
            
            assert result['status'] == 'partial'
            assert result['successful_downloads'] == 1
            assert result['failed_downloads'] == 1
    
    def test_get_available_files_success(self, sirene_collector, sample_html_response):
        """测试成功获取可用文件列表"""
        with patch('requests.get') as mock_get:
            mock_response = Mock()
            mock_response.content = sample_html_response.encode('utf-8')
            mock_response.raise_for_status.return_value = None
            mock_get.return_value = mock_response
            
            files = sirene_collector._get_available_files()
            
            # 应该找到4个有效的SIRENE文件
            assert len(files) == 5
            assert all(f['filename'].endswith('.zip') for f in files)
            
            # 检查排序（最新的在前）
            dates = [f['date'] for f in files]
            assert dates == sorted(dates, reverse=True)
    
    def test_get_available_files_network_error(self, sirene_collector):
        """测试网络错误时获取文件列表"""
        with patch('requests.get') as mock_get:
            mock_get.side_effect = requests.RequestException("Connection error")
            
            with pytest.raises(NetworkError):
                sirene_collector._get_available_files()
    
    def test_is_sirene_file(self, sirene_collector):
        """测试SIRENE文件名识别"""
        # 有效的SIRENE文件名
        valid_names = [
            '2024-06-01-StockEtablissement_utf8.zip',
            '2023-12-01-StockUniteLegale_utf8.zip',
            '2024-01-01-StockEtablissementHistorique_utf8.zip'
        ]
        for name in valid_names:
            assert sirene_collector._is_sirene_file(name) is True
        
        # 无效的文件名
        invalid_names = [
            'invalid-file.txt',
            '2024-06-StockEtablissement_utf8.zip',  # 缺少日期
            'StockEtablissement_utf8.zip',  # 缺少日期前缀
            '2024-06-01-Stock.txt'  # 错误扩展名
        ]
        for name in invalid_names:
            assert sirene_collector._is_sirene_file(name) is False
    
    def test_parse_file_info(self, sirene_collector):
        """测试文件信息解析"""
        filename = '2024-06-01-StockEtablissement_utf8.zip'
        info = sirene_collector._parse_file_info(filename)
        
        assert info is not None
        assert info['filename'] == filename
        assert info['year'] == 2024
        assert info['month'] == 6
        assert info['file_type'] == 'StockEtablissement'
        assert info['category'] == 'stock'
        assert info['is_required'] is True
    
    def test_parse_file_info_historical(self, sirene_collector):
        """测试历史文件信息解析"""
        filename = '2024-05-01-StockEtablissementHistorique_utf8.zip'
        info = sirene_collector._parse_file_info(filename)
        
        assert info['category'] == 'historical'
        assert info['is_required'] is False
    
    def test_categorize_file(self, sirene_collector):
        """测试文件分类"""
        test_cases = [
            ('2024-06-01-StockEtablissement_utf8.zip', 'stock'),
            ('2024-06-01-StockEtablissementHistorique_utf8.zip', 'historical'),
            ('2024-06-01-StockEtablissementLiensSuccession_utf8.zip', 'succession'),
            ('2024-06-01-StockDoublons_utf8.zip', 'duplicates'),
            ('2024-06-01-OtherFile_utf8.zip', 'other')
        ]
        
        for filename, expected_category in test_cases:
            category = sirene_collector._categorize_file(filename)
            assert category == expected_category
    
    def test_filter_files_to_download(self, sirene_collector):
        """测试文件下载过滤"""
        now = datetime.now(timezone.utc)
        old_date = now - timedelta(days=120)  # 4个月前
        recent_date = now - timedelta(days=30)  # 1个月前
        
        available_files = [
            {
                'filename': 'old_stock.zip',
                'date': old_date,
                'category': 'stock'
            },
            {
                'filename': 'recent_stock.zip',
                'date': recent_date,
                'category': 'stock'
            },
            {
                'filename': 'recent_historical.zip',
                'date': recent_date,
                'category': 'historical'
            }
        ]
        
        # 默认设置：只下载stock文件，不下载historical
        filtered = sirene_collector._filter_files_to_download(available_files)
        
        # 应该只有最近的stock文件
        assert len(filtered) == 1
        assert filtered[0]['filename'] == 'recent_stock.zip'
    
    def test_filter_files_with_historical(self, sirene_collector):
        """测试包含历史文件的过滤"""
        sirene_collector.download_historical = True
        
        now = datetime.now(timezone.utc)
        recent_date = now - timedelta(days=30)
        
        available_files = [
            {
                'filename': 'recent_stock.zip',
                'date': recent_date,
                'category': 'stock'
            },
            {
                'filename': 'recent_historical.zip',
                'date': recent_date,
                'category': 'historical'
            }
        ]
        
        filtered = sirene_collector._filter_files_to_download(available_files)
        
        # 应该包含两个文件
        assert len(filtered) == 2
    
    def test_download_file_success(self, sirene_collector):
        """测试成功的文件下载"""
        file_info = {
            'filename': 'test.zip',
            'url': 'https://example.com/test.zip',
            'year': 2024,
            'file_type': 'StockEtablissement',
            'category': 'stock',
            'date': datetime.now()
        }
        
        with patch('collectors.sirene.sirene_collector.file_exists_in_gcs') as mock_exists, \
             patch('collectors.sirene.sirene_collector.download_file_with_retry') as mock_download, \
             patch('collectors.sirene.sirene_collector.upload_to_gcs') as mock_upload, \
             patch.object(sirene_collector, '_validate_zip_file') as mock_validate, \
             patch.object(sirene_collector, '_get_remote_file_metadata') as mock_metadata, \
             patch('pathlib.Path.unlink') as mock_unlink, \
             patch('pathlib.Path.exists') as mock_exists_path, \
             patch('pathlib.Path.stat') as mock_stat:
            
            mock_exists.return_value = False
            mock_exists_path.return_value = True
            mock_stat.return_value = Mock(st_size=1000)
            
            result = sirene_collector._download_file(file_info)
            
            assert result['status'] == 'success'
            assert result['filename'] == 'test.zip'
            mock_download.assert_called_once()
            mock_upload.assert_called_once()
            mock_validate.assert_called_once()
    
    def test_download_file_skip_existing(self, sirene_collector):
        """测试跳过已存在的文件"""
        file_info = {
            'filename': 'test.zip',
            'url': 'https://example.com/test.zip',
            'year': 2024
        }
        
        with patch('collectors.sirene.sirene_collector.file_exists_in_gcs') as mock_exists, \
             patch('collectors.sirene.sirene_collector.get_file_metadata') as mock_local_meta, \
             patch.object(sirene_collector, '_get_remote_file_metadata') as mock_remote_meta:
            
            mock_exists.return_value = True
            mock_local_meta.return_value = {'size': 1000}
            mock_remote_meta.return_value = {'size': 1000}
            
            result = sirene_collector._download_file(file_info)
            
            assert result['status'] == 'skipped'
            assert result['reason'] == 'file_exists_same_size'
    
    def test_download_file_failure(self, sirene_collector):
        """测试文件下载失败"""
        file_info = {
            'filename': 'test.zip',
            'url': 'https://example.com/test.zip',
            'year': 2024
        }
        
        with patch('collectors.sirene.sirene_collector.file_exists_in_gcs') as mock_exists, \
             patch('collectors.sirene.sirene_collector.download_file_with_retry') as mock_download:
            
            mock_exists.return_value = False
            mock_download.side_effect = Exception("Download failed")
            
            result = sirene_collector._download_file(file_info)
            
            assert result['status'] == 'failed'
            assert 'Download failed' in result['error']
    
    def test_get_remote_file_metadata_success(self, sirene_collector):
        """测试成功获取远程文件元数据"""
        with patch('requests.head') as mock_head:
            mock_response = Mock()
            mock_response.headers = {
                'content-length': '1000',
                'last-modified': 'Mon, 01 Jan 2024 00:00:00 GMT',
                'content-type': 'application/zip'
            }
            mock_response.raise_for_status.return_value = None
            mock_head.return_value = mock_response
            
            metadata = sirene_collector._get_remote_file_metadata('https://example.com/test.zip')
            
            assert metadata['size'] == 1000
            assert metadata['last_modified'] == 'Mon, 01 Jan 2024 00:00:00 GMT'
            assert metadata['content_type'] == 'application/zip'
    
    def test_get_remote_file_metadata_failure(self, sirene_collector):
        """测试获取远程文件元数据失败"""
        with patch('requests.head') as mock_head:
            mock_head.side_effect = requests.RequestException("Request failed")
            
            metadata = sirene_collector._get_remote_file_metadata('https://example.com/test.zip')
            
            assert metadata is None
    
    def test_validate_zip_file_success(self, sirene_collector):
        """测试ZIP文件验证成功"""
        with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as temp_file:
            # 创建一个有效的ZIP文件
            with zipfile.ZipFile(temp_file.name, 'w') as zf:
                zf.writestr('test.csv', 'column1,column2\nvalue1,value2\n')
            
            temp_path = Path(temp_file.name)
            
            try:
                # 验证应该成功
                sirene_collector._validate_zip_file(temp_path)
            finally:
                temp_path.unlink()
    
    def test_validate_zip_file_no_csv(self, sirene_collector):
        """测试ZIP文件不包含CSV"""
        with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as temp_file:
            # 创建一个不包含CSV的ZIP文件
            with zipfile.ZipFile(temp_file.name, 'w') as zf:
                zf.writestr('test.txt', 'This is not a CSV file')
            
            temp_path = Path(temp_file.name)
            
            try:
                with pytest.raises(ValidationError, match="不包含CSV文件"):
                    sirene_collector._validate_zip_file(temp_path)
            finally:
                temp_path.unlink()
    
    def test_validate_zip_file_bad_zip(self, sirene_collector):
        """测试损坏的ZIP文件"""
        with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as temp_file:
            # 写入无效的ZIP内容
            temp_file.write(b'This is not a ZIP file')
            temp_file.flush()
            
            temp_path = Path(temp_file.name)
            
            try:
                with pytest.raises(ValidationError, match="无效的ZIP文件"):
                    sirene_collector._validate_zip_file(temp_path)
            finally:
                temp_path.unlink()
    
    def test_validate_data_valid_zip(self, sirene_collector):
        """测试有效ZIP文件的数据验证"""
        with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as temp_file:
            with zipfile.ZipFile(temp_file.name, 'w') as zf:
                zf.writestr('test.csv', 'column1,column2\nvalue1,value2\n')
            
            try:
                result = sirene_collector.validate_data(temp_file.name)
                assert result is True
            finally:
                Path(temp_file.name).unlink()
    
    def test_validate_data_invalid_file(self, sirene_collector):
        """测试无效文件的数据验证"""
        result = sirene_collector.validate_data('invalid.txt')
        assert result is False


class TestSireneCollectorIntegration:
    """SIRENE收集器集成测试"""
    
    def test_collector_initialization(self):
        """测试收集器初始化"""
        config = {
            'gcs_config': {'bucket_name': 'test-bucket'},
            'sirene': {
                'base_url': 'https://files.data.gouv.fr/insee-sirene/',
                'months_back': 6
            }
        }
        
        with patch('config.config_loader.get_config', return_value=config), \
             patch('utils.gcs_client.get_gcs_client'):
            collector = SireneCollector(config)
            
            assert collector.base_url == 'https://files.data.gouv.fr/insee-sirene/'
            assert collector.months_back == 6
            assert collector.download_historical is False
    
    def test_cloud_function_entry_point_success(self):
        """测试Cloud Function入口点成功"""
        mock_config = {
            'gcs_config': {'bucket_name': 'test-bucket'},
            'sirene': {'base_url': 'https://test.com/'}
        }
        
        with patch('builtins.open', mock_open_yaml(mock_config)), \
             patch('os.path.exists', return_value=True), \
             patch('os.environ.get', return_value='/test/config.yaml'), \
             patch('collectors.sirene.sirene_collector.setup_logging'), \
             patch.object(SireneCollector, 'collect') as mock_collect:
            
            mock_collect.return_value = {
                'collector': 'sirene',
                'status': 'success',
                'files_processed': 2
            }
            
            result = sirene_collector_main()
            result_data = json.loads(result)
            
            assert result_data['collector'] == 'sirene'
            assert result_data['status'] == 'success'
    
    def test_cloud_function_entry_point_error(self):
        """测试Cloud Function入口点错误处理"""
        with patch('os.path.exists', return_value=False):
            result = sirene_collector_main()
            result_data = json.loads(result)
            
            assert result_data['collector'] == 'sirene'
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