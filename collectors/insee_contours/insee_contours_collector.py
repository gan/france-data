"""
INSEE Geographic Contours Data Collector

Collects INSEE geographic boundary data (IRIS, communes, départements, régions)
from multiple official sources including IGN, data.gouv.fr, and INSEE APIs.

Data sources:
- IGN official IRIS boundaries (Shapefile/GeoPackage, high precision)
- Data.gouv.fr datasets (GeoJSON, standardized identifiers)  
- INSEE API for statistical metadata
- GeoZones for standardized geographic identifiers

Author: Claude Code Assistant
Created: 2025-06-25
"""

import json
import logging
import re
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Union
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from collectors.base_collector import BaseCollector
from utils import (
    FranceDataError,
    NetworkError,
    ValidationError,
    download_file_with_retry,
    setup_logging,
    upload_to_gcs,
    file_exists_in_gcs,
    get_file_metadata
)


class INSEEContoursCollector(BaseCollector):
    """INSEE地理边界数据收集器，处理IRIS、市镇、省、大区边界数据"""
    
    def __init__(self, config: Dict = None, **kwargs):
        """
        初始化INSEE地理边界收集器
        
        Args:
            config: 配置字典，包含INSEE相关设置
            **kwargs: 其他初始化参数
        """
        super().__init__('insee_contours')
        
        # 如果提供了自定义配置，使用它；否则使用base类的配置
        if config:
            self.insee_config = config.get('insee_contours', {})
        else:
            self.insee_config = self.collector_config
            
        # 数据源配置
        self.base_url = self.insee_config.get('base_url', 'https://www.data.gouv.fr/fr/datasets/contours-iris-2023/')
        self.api_endpoint = self.insee_config.get('api_endpoint', 'https://www.data.gouv.fr/api/1/datasets/')
        
        # IGN数据源（高精度官方数据）
        self.ign_base_url = self.insee_config.get('ign_base_url', 'https://data.geopf.fr/telechargement/download/')
        
        # 数据类型和格式配置
        self.data_types = self.insee_config.get('data_types', [
            'iris',         # IRIS统计区域
            'communes',     # 市镇
            'departements', # 省
            'regions'       # 大区
        ])
        
        self.formats = self.insee_config.get('formats', [
            'shapefile',    # .shp文件（GIS标准）
            'geojson',      # .geojson文件（Web友好）
            'geopackage'    # .gpkg文件（现代GIS格式）
        ])
        
        # 下载选项
        self.download_ign_data = self.insee_config.get('download_ign_data', True)    # IGN官方高精度数据
        self.download_datagouv = self.insee_config.get('download_datagouv', True)    # data.gouv.fr数据
        self.download_geozones = self.insee_config.get('download_geozones', True)    # 标准化标识符
        self.preferred_projection = self.insee_config.get('preferred_projection', 'lambert93')  # lambert93 或 wgs84
        
        # 年份配置
        self.target_year = self.insee_config.get('target_year', datetime.now().year)
        self.fallback_years = self.insee_config.get('fallback_years', [2024, 2023, 2022])
        
        # 获取GCS配置
        if config:
            gcs_config = config.get('gcs_config', {})
            self.bucket_name = gcs_config.get('bucket_name', 'france-data-bucket')
        else:
            self.bucket_name = self.config.get('gcs_config.bucket_name', 'france-data-bucket')
        
        self.logger = logging.getLogger(f'{__name__}.INSEEContoursCollector')
    
    def collect(self) -> Dict:
        """
        执行INSEE地理边界数据收集
        
        Returns:
            Dict: 收集结果报告
        """
        try:
            self.logger.info("开始INSEE地理边界数据收集...")
            
            download_results = []
            
            # 1. 收集IGN官方数据（如果启用）
            if self.download_ign_data:
                self.logger.info("开始收集IGN官方地理边界数据...")
                ign_results = self._collect_ign_data()
                download_results.extend(ign_results)
            
            # 2. 收集data.gouv.fr数据（如果启用）
            if self.download_datagouv:
                self.logger.info("开始收集data.gouv.fr地理数据...")
                datagouv_results = self._collect_datagouv_data()
                download_results.extend(datagouv_results)
            
            # 3. 收集GeoZones标准化数据（如果启用）
            if self.download_geozones:
                self.logger.info("开始收集GeoZones标准化标识符数据...")
                geozones_results = self._collect_geozones_data()
                download_results.extend(geozones_results)
            
            # 统计结果
            successful_downloads = [r for r in download_results if r['status'] == 'success']
            failed_downloads = [r for r in download_results if r['status'] == 'failed']
            skipped_downloads = [r for r in download_results if r['status'] == 'skipped']
            
            result = {
                'collector': 'insee_contours',
                'status': 'success' if len(failed_downloads) == 0 else 'partial',
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'files_processed': len(download_results),
                'successful_downloads': len(successful_downloads),
                'failed_downloads': len(failed_downloads),
                'skipped_downloads': len(skipped_downloads),
                'data_sources': {
                    'ign_official': len([r for r in successful_downloads if r.get('source') == 'ign']),
                    'datagouv': len([r for r in successful_downloads if r.get('source') == 'datagouv']),
                    'geozones': len([r for r in successful_downloads if r.get('source') == 'geozones'])
                },
                'details': download_results
            }
            
            self.logger.info(f"INSEE地理边界数据收集完成: {result['status']}")
            return result
            
        except Exception as e:
            self.logger.error(f"INSEE地理边界数据收集失败: {e}")
            raise FranceDataError(f"INSEE contours collection failed: {e}") from e
    
    def _collect_ign_data(self) -> List[Dict]:
        """
        收集IGN官方地理边界数据
        
        Returns:
            List[Dict]: 下载结果列表
        """
        results = []
        
        # IGN IRIS数据下载链接（基于研究结果）
        ign_datasets = {
            'iris_lambert93_shp': {
                'url': f'https://data.geopf.fr/telechargement/download/CONTOURS-IRIS/CONTOURS-IRIS_3-0__SHP_LAMB93_FXX_{self.target_year}-01-01/',
                'filename': f'contours_iris_lambert93_{self.target_year}.zip',
                'data_type': 'iris',
                'format': 'shapefile',
                'projection': 'lambert93'
            },
            'iris_lambert93_gpkg': {
                'url': f'https://data.geopf.fr/telechargement/download/CONTOURS-IRIS/CONTOURS-IRIS_3-0__GPKG_LAMB93_FXX_{self.target_year}-01-01/',
                'filename': f'contours_iris_lambert93_{self.target_year}.gpkg',
                'data_type': 'iris',
                'format': 'geopackage',
                'projection': 'lambert93'
            }
        }
        
        for dataset_id, dataset_info in ign_datasets.items():
            if dataset_info['data_type'] in self.data_types and dataset_info['format'] in self.formats:
                try:
                    result = self._download_geographic_file(dataset_info, source='ign')
                    results.append(result)
                except Exception as e:
                    self.logger.error(f"下载IGN数据集 {dataset_id} 失败: {e}")
                    results.append({
                        'dataset_id': dataset_id,
                        'status': 'failed',
                        'error': str(e),
                        'source': 'ign'
                    })
        
        return results
    
    def _collect_datagouv_data(self) -> List[Dict]:
        """
        收集data.gouv.fr地理数据
        
        Returns:
            List[Dict]: 下载结果列表
        """
        results = []
        
        # 已知的data.gouv.fr数据集ID
        datagouv_datasets = {
            'iris_2016': {
                'dataset_id': '5c34944e634f4164071119c5',
                'name': 'Contour des IRIS INSEE tout en un',
                'data_type': 'iris',
                'format': 'shapefile',
                'year': 2016,
                'note': 'Older but comprehensive IRIS dataset'
            },
            'geozones': {
                'dataset_id': 'zones-geo',
                'name': 'GeoZones - Zones géographiques française',
                'data_type': 'all',
                'format': 'geojson',
                'year': 2024,
                'note': 'Standardized geographic identifiers'
            }
        }
        
        for dataset_key, dataset_info in datagouv_datasets.items():
            try:
                # 获取数据集信息
                api_url = f"https://www.data.gouv.fr/api/1/datasets/{dataset_info['dataset_id']}/"
                resources = self._get_dataset_resources(api_url)
                
                # 下载匹配的资源
                for resource in resources:
                    if self._should_download_resource(resource, dataset_info):
                        download_info = {
                            'url': resource['url'],
                            'filename': self._generate_filename(resource, dataset_info),
                            'data_type': dataset_info['data_type'],
                            'format': self._detect_format(resource),
                            'dataset_name': dataset_info['name'],
                            'year': dataset_info['year']
                        }
                        
                        result = self._download_geographic_file(download_info, source='datagouv')
                        results.append(result)
                
            except Exception as e:
                self.logger.error(f"处理data.gouv.fr数据集 {dataset_key} 失败: {e}")
                results.append({
                    'dataset_key': dataset_key,
                    'status': 'failed',
                    'error': str(e),
                    'source': 'datagouv'
                })
        
        return results
    
    def _collect_geozones_data(self) -> List[Dict]:
        """
        收集GeoZones标准化标识符数据
        
        Returns:
            List[Dict]: 下载结果列表
        """
        results = []
        
        # GeoZones API端点
        geozones_endpoints = {
            'iris': 'https://geo.api.gouv.fr/iris',
            'communes': 'https://geo.api.gouv.fr/communes',
            'departements': 'https://geo.api.gouv.fr/departements',
            'regions': 'https://geo.api.gouv.fr/regions'
        }
        
        for data_type, api_url in geozones_endpoints.items():
            if data_type in self.data_types:
                try:
                    # 获取完整的地理数据（包含边界）
                    api_url_with_geometry = f"{api_url}?fields=code,nom,contour&format=geojson"
                    
                    download_info = {
                        'url': api_url_with_geometry,
                        'filename': f'geozones_{data_type}_{datetime.now().year}.geojson',
                        'data_type': data_type,
                        'format': 'geojson',
                        'source': 'geozones_api'
                    }
                    
                    result = self._download_geographic_file(download_info, source='geozones')
                    results.append(result)
                    
                except Exception as e:
                    self.logger.error(f"下载GeoZones {data_type} 数据失败: {e}")
                    results.append({
                        'data_type': data_type,
                        'status': 'failed',
                        'error': str(e),
                        'source': 'geozones'
                    })
        
        return results
    
    def _get_dataset_resources(self, api_url: str) -> List[Dict]:
        """
        获取data.gouv.fr数据集的资源列表
        
        Args:
            api_url: 数据集API URL
            
        Returns:
            List[Dict]: 资源列表
        """
        try:
            response = requests.get(api_url, timeout=30)
            response.raise_for_status()
            dataset_data = response.json()
            
            return dataset_data.get('resources', [])
            
        except requests.RequestException as e:
            raise NetworkError(f"Failed to fetch dataset resources from {api_url}: {e}") from e
        except (ValueError, KeyError) as e:
            raise FranceDataError(f"Invalid dataset response from {api_url}: {e}") from e
    
    def _should_download_resource(self, resource: Dict, dataset_info: Dict) -> bool:
        """
        判断是否应该下载某个资源
        
        Args:
            resource: 资源信息
            dataset_info: 数据集信息
            
        Returns:
            bool: 是否下载
        """
        resource_format = self._detect_format(resource)
        
        # 检查格式是否匹配
        if resource_format not in self.formats:
            return False
        
        # 检查数据类型是否匹配
        if dataset_info['data_type'] != 'all' and dataset_info['data_type'] not in self.data_types:
            return False
        
        # 检查文件是否可用
        if not resource.get('url'):
            return False
        
        return True
    
    def _detect_format(self, resource: Dict) -> str:
        """
        检测资源文件格式
        
        Args:
            resource: 资源信息
            
        Returns:
            str: 文件格式
        """
        url = resource.get('url', '').lower()
        format_field = resource.get('format', '').lower()
        mime_type = resource.get('mime', '').lower()
        
        # 基于URL扩展名
        if url.endswith('.zip') and ('shp' in url or 'shape' in url):
            return 'shapefile'
        elif url.endswith('.geojson') or url.endswith('.json'):
            return 'geojson'
        elif url.endswith('.gpkg'):
            return 'geopackage'
        
        # 基于format字段
        if 'shp' in format_field or 'shape' in format_field:
            return 'shapefile'
        elif 'geojson' in format_field or 'json' in format_field:
            return 'geojson'
        elif 'gpkg' in format_field or 'geopackage' in format_field:
            return 'geopackage'
        
        # 基于MIME类型
        if 'application/zip' in mime_type:
            return 'shapefile'  # 假设ZIP文件是Shapefile
        elif 'application/json' in mime_type:
            return 'geojson'
        
        return 'unknown'
    
    def _generate_filename(self, resource: Dict, dataset_info: Dict) -> str:
        """
        生成标准化的文件名
        
        Args:
            resource: 资源信息
            dataset_info: 数据集信息
            
        Returns:
            str: 标准化文件名
        """
        data_type = dataset_info['data_type']
        year = dataset_info['year']
        format_type = self._detect_format(resource)
        
        # 原始文件名
        original_name = resource.get('title', resource.get('url', '').split('/')[-1])
        
        # 生成标准化文件名
        if format_type == 'shapefile':
            return f"{data_type}_{year}_shapefile.zip"
        elif format_type == 'geojson':
            return f"{data_type}_{year}.geojson"
        elif format_type == 'geopackage':
            return f"{data_type}_{year}.gpkg"
        else:
            # 保留原始扩展名
            ext = Path(original_name).suffix
            return f"{data_type}_{year}{ext}"
    
    def _download_geographic_file(self, download_info: Dict, source: str) -> Dict:
        """
        下载单个地理数据文件
        
        Args:
            download_info: 下载信息字典
            source: 数据源标识
            
        Returns:
            Dict: 下载结果
        """
        filename = download_info['filename']
        url = download_info['url']
        data_type = download_info['data_type']
        
        try:
            # 构建GCS路径
            year = download_info.get('year', self.target_year)
            gcs_path = f"raw/insee-contours/{year}/{source}/{filename}"
            
            # 检查文件是否已存在且相同
            if file_exists_in_gcs(self.gcs_client, self.bucket_name, gcs_path):
                # 获取远程文件元数据进行比较
                remote_metadata = self._get_remote_file_metadata(url)
                local_metadata = get_file_metadata(self.gcs_client, self.bucket_name, gcs_path)
                
                if (local_metadata and remote_metadata and 
                    local_metadata.get('size') == remote_metadata.get('size')):
                    self.logger.info(f"文件 {filename} 已存在且相同，跳过下载")
                    return {
                        'filename': filename,
                        'status': 'skipped',
                        'reason': 'file_exists_same_size',
                        'gcs_path': gcs_path,
                        'source': source,
                        'data_type': data_type
                    }
            
            # 下载文件到临时位置
            temp_file = Path(f"/tmp/{filename}")
            self.logger.info(f"开始下载 {filename} 从 {source}...")
            
            download_file_with_retry(url, str(temp_file))
            
            # 验证地理数据文件
            self._validate_geographic_file(temp_file, download_info)
            
            # 上传到GCS
            upload_to_gcs(
                gcs_client=self.gcs_client,
                bucket_name=self.bucket_name,
                source_file=str(temp_file),
                destination_blob=gcs_path,
                metadata={
                    'source_url': url,
                    'data_type': data_type,
                    'format': download_info['format'],
                    'source': source,
                    'collection_date': datetime.now(timezone.utc).isoformat(),
                    'target_year': str(year),
                    'projection': download_info.get('projection', 'unknown')
                }
            )
            
            # 清理临时文件
            temp_file.unlink()
            
            self.logger.info(f"成功下载并上传 {filename}")
            return {
                'filename': filename,
                'status': 'success',
                'gcs_path': gcs_path,
                'source': source,
                'data_type': data_type,
                'file_size': temp_file.stat().st_size if temp_file.exists() else None
            }
            
        except Exception as e:
            self.logger.error(f"下载文件 {filename} 失败: {e}")
            return {
                'filename': filename,
                'status': 'failed',
                'error': str(e),
                'source': source,
                'data_type': data_type
            }
    
    def _get_remote_file_metadata(self, url: str) -> Optional[Dict]:
        """
        获取远程文件元数据
        
        Args:
            url: 文件URL
            
        Returns:
            Optional[Dict]: 文件元数据
        """
        try:
            response = requests.head(url, timeout=10)
            response.raise_for_status()
            
            return {
                'size': int(response.headers.get('content-length', 0)),
                'last_modified': response.headers.get('last-modified'),
                'content_type': response.headers.get('content-type')
            }
        except Exception as e:
            self.logger.warning(f"无法获取远程文件元数据 {url}: {e}")
            return None
    
    def _validate_geographic_file(self, file_path: Path, download_info: Dict) -> None:
        """
        验证地理数据文件的完整性
        
        Args:
            file_path: 文件路径
            download_info: 下载信息
            
        Raises:
            ValidationError: 如果文件验证失败
        """
        try:
            file_format = download_info['format']
            
            if file_format == 'shapefile':
                # 验证Shapefile ZIP
                self._validate_shapefile_zip(file_path)
            elif file_format == 'geojson':
                # 验证GeoJSON
                self._validate_geojson_file(file_path)
            elif file_format == 'geopackage':
                # 验证GeoPackage（基本检查）
                self._validate_geopackage_file(file_path)
            else:
                # 基本文件存在检查
                if not file_path.exists() or file_path.stat().st_size == 0:
                    raise ValidationError(f"文件 {file_path.name} 为空或不存在")
                
        except Exception as e:
            raise ValidationError(f"地理数据文件验证失败 {file_path.name}: {e}") from e
    
    def _validate_shapefile_zip(self, file_path: Path) -> None:
        """验证Shapefile ZIP文件"""
        with zipfile.ZipFile(file_path, 'r') as zip_file:
            zip_file.testzip()  # 测试ZIP完整性
            
            file_list = zip_file.namelist()
            
            # 检查必需的Shapefile组件
            has_shp = any(f.endswith('.shp') for f in file_list)
            has_shx = any(f.endswith('.shx') for f in file_list)
            has_dbf = any(f.endswith('.dbf') for f in file_list)
            
            if not (has_shp and has_shx and has_dbf):
                raise ValidationError(f"ZIP文件缺少必需的Shapefile组件 (.shp, .shx, .dbf)")
    
    def _validate_geojson_file(self, file_path: Path) -> None:
        """验证GeoJSON文件"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                geojson_data = json.load(f)
            
            # 基本GeoJSON结构检查
            if 'type' not in geojson_data:
                raise ValidationError("GeoJSON文件缺少type字段")
            
            if geojson_data['type'] not in ['FeatureCollection', 'Feature']:
                raise ValidationError(f"无效的GeoJSON类型: {geojson_data['type']}")
            
            # 检查是否有要素
            if geojson_data['type'] == 'FeatureCollection':
                features = geojson_data.get('features', [])
                if len(features) == 0:
                    raise ValidationError("GeoJSON文件不包含任何要素")
                
        except json.JSONDecodeError as e:
            raise ValidationError(f"无效的JSON格式: {e}")
    
    def _validate_geopackage_file(self, file_path: Path) -> None:
        """验证GeoPackage文件"""
        # 基本文件检查（GeoPackage是SQLite数据库）
        if file_path.stat().st_size < 1024:  # 小于1KB可能不是有效的GeoPackage
            raise ValidationError("GeoPackage文件过小，可能损坏")
        
        # 检查文件头（SQLite数据库的魔数）
        with open(file_path, 'rb') as f:
            header = f.read(16)
            if not header.startswith(b'SQLite format 3\x00'):
                raise ValidationError("不是有效的SQLite/GeoPackage文件")
    
    def validate_data(self, file_path: str) -> bool:
        """
        验证INSEE地理数据文件
        
        Args:
            file_path: 文件路径
            
        Returns:
            bool: 验证是否通过
        """
        try:
            file_path_obj = Path(file_path)
            
            # 根据文件扩展名确定格式
            if file_path.endswith('.zip'):
                download_info = {'format': 'shapefile'}
            elif file_path.endswith('.geojson') or file_path.endswith('.json'):
                download_info = {'format': 'geojson'}
            elif file_path.endswith('.gpkg'):
                download_info = {'format': 'geopackage'}
            else:
                return False
            
            # 验证文件
            self._validate_geographic_file(file_path_obj, download_info)
            return True
            
        except Exception as e:
            self.logger.error(f"数据验证失败 {file_path}: {e}")
            return False


def insee_contours_collector_main(request=None):  # pylint: disable=unused-argument
    """
    Cloud Function入口点
    
    Args:
        request: HTTP请求对象（Cloud Functions）
        
    Returns:
        str: JSON格式的响应
    """
    import os
    
    try:
        # 设置日志
        setup_logging()
        
        # 加载配置
        config_path = os.environ.get('CONFIG_PATH', '/workspace/config/config.yaml')
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"配置文件未找到: {config_path}")
        
        import yaml
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        # 创建收集器并执行
        collector = INSEEContoursCollector(config)
        result = collector.collect()
        
        return json.dumps(result, ensure_ascii=False, indent=2)
        
    except Exception as e:
        error_result = {
            'collector': 'insee_contours',
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        return json.dumps(error_result, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    # 本地测试
    import yaml
    
    config_path = "../../config/config.yaml"
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    collector = INSEEContoursCollector(config)
    result = collector.collect()
    print(json.dumps(result, ensure_ascii=False, indent=2))