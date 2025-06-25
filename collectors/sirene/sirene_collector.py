"""
SIRENE Data Collector

Collects SIRENE (企业名录) data from INSEE official data source.
Handles monthly stock files containing enterprise and legal unit data.

Data sources:
- Stock Etablissement (企业机构存量数据)
- Stock Unite Legale (法人单位存量数据)
- Historical data files
- Succession links and duplicates

Author: Claude Code Assistant
Created: 2025-06-25
"""

import json
import logging
import re
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urljoin

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


class SireneCollector(BaseCollector):
    """SIRENE数据收集器，处理INSEE企业名录数据"""
    
    def __init__(self, config: Dict = None, **kwargs):
        """
        初始化SIRENE收集器
        
        Args:
            config: 配置字典，包含SIRENE相关设置
            **kwargs: 其他初始化参数
        """
        super().__init__('sirene')
        
        # 如果提供了自定义配置，使用它；否则使用base类的配置
        if config:
            self.sirene_config = config.get('sirene', {})
        else:
            self.sirene_config = self.collector_config
            
        self.base_url = self.sirene_config.get('base_url', 'https://files.data.gouv.fr/insee-sirene/')
        
        # 存量文件配置
        self.stock_files = self.sirene_config.get('stock_files', [
            'StockEtablissement_utf8.zip',
            'StockUniteLegale_utf8.zip'
        ])
        
        # 可选的额外文件类型
        self.optional_files = self.sirene_config.get('optional_files', [
            'StockEtablissementHistorique_utf8.zip',
            'StockUniteLegaleHistorique_utf8.zip',
            'StockEtablissementLiensSuccession_utf8.zip',
            'StockDoublons_utf8.zip'
        ])
        
        # 下载选项
        self.download_historical = self.sirene_config.get('download_historical', False)
        self.download_optional = self.sirene_config.get('download_optional', False)
        self.months_back = self.sirene_config.get('months_back', 3)  # 默认获取最近3个月
        
        # 获取GCS配置
        if config:
            gcs_config = config.get('gcs_config', {})
            self.bucket_name = gcs_config.get('bucket_name', 'france-data-bucket')
        else:
            self.bucket_name = self.config.get('gcs_config.bucket_name', 'france-data-bucket')
        
        self.logger = logging.getLogger(f'{__name__}.SireneCollector')
    
    def collect(self) -> Dict:
        """
        执行SIRENE数据收集
        
        Returns:
            Dict: 收集结果报告
        """
        try:
            self.logger.info("开始SIRENE数据收集...")
            
            # 获取可用的文件列表
            available_files = self._get_available_files()
            self.logger.info(f"发现 {len(available_files)} 个可用文件")
            
            # 过滤需要下载的文件
            files_to_download = self._filter_files_to_download(available_files)
            self.logger.info(f"需要下载 {len(files_to_download)} 个文件")
            
            # 下载文件
            download_results = []
            for file_info in files_to_download:
                try:
                    result = self._download_file(file_info)
                    download_results.append(result)
                except Exception as e:
                    self.logger.error(f"下载文件 {file_info['filename']} 失败: {e}")
                    download_results.append({
                        'filename': file_info['filename'],
                        'status': 'failed',
                        'error': str(e)
                    })
            
            # 统计结果
            successful_downloads = [r for r in download_results if r['status'] == 'success']
            failed_downloads = [r for r in download_results if r['status'] == 'failed']
            skipped_downloads = [r for r in download_results if r['status'] == 'skipped']
            
            result = {
                'collector': 'sirene',
                'status': 'success' if len(failed_downloads) == 0 else 'partial',
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'files_processed': len(download_results),
                'successful_downloads': len(successful_downloads),
                'failed_downloads': len(failed_downloads),
                'skipped_downloads': len(skipped_downloads),
                'details': download_results
            }
            
            self.logger.info(f"SIRENE数据收集完成: {result['status']}")
            return result
            
        except Exception as e:
            self.logger.error(f"SIRENE数据收集失败: {e}")
            raise FranceDataError(f"SIRENE collection failed: {e}") from e
    
    def _get_available_files(self) -> List[Dict]:
        """
        获取SIRENE数据源的可用文件列表
        
        Returns:
            List[Dict]: 包含文件信息的列表
        """
        try:
            self.logger.info(f"扫描SIRENE数据源: {self.base_url}")
            
            response = requests.get(self.base_url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            files = []
            
            # 解析目录列表
            for link in soup.find_all('a'):
                href = link.get('href')
                if not href or href.startswith('/') or href.startswith('?'):
                    continue
                
                # 匹配SIRENE文件命名模式
                if self._is_sirene_file(href):
                    file_info = self._parse_file_info(href)
                    if file_info:
                        files.append(file_info)
            
            # 按日期排序，最新的在前
            files.sort(key=lambda x: x['date'], reverse=True)
            
            self.logger.info(f"找到 {len(files)} 个SIRENE文件")
            return files
            
        except requests.RequestException as e:
            raise NetworkError(f"Failed to fetch SIRENE file list: {e}") from e
        except Exception as e:
            raise FranceDataError(f"Error parsing SIRENE file list: {e}") from e
    
    def _is_sirene_file(self, filename: str) -> bool:
        """
        检查文件名是否为SIRENE数据文件
        
        Args:
            filename: 文件名
            
        Returns:
            bool: 是否为SIRENE文件
        """
        # SIRENE文件命名模式: YYYY-MM-01-Stock*.zip
        pattern = r'^\d{4}-\d{2}-01-Stock.*\.zip$'
        return re.match(pattern, filename) is not None
    
    def _parse_file_info(self, filename: str) -> Optional[Dict]:
        """
        解析SIRENE文件信息
        
        Args:
            filename: 文件名
            
        Returns:
            Optional[Dict]: 文件信息字典
        """
        try:
            # 解析日期: YYYY-MM-01-StockType_utf8.zip
            parts = filename.split('-')
            if len(parts) < 4:
                return None
            
            year = int(parts[0])
            month = int(parts[1])
            date = datetime(year, month, 1)
            
            # 解析文件类型
            file_type = parts[3].split('_')[0]  # Stock后面的类型
            
            # 判断文件类别
            category = self._categorize_file(filename)
            
            return {
                'filename': filename,
                'date': date,
                'year': year,
                'month': month,
                'file_type': file_type,
                'category': category,
                'url': urljoin(self.base_url, filename),
                'is_required': category == 'stock'
            }
            
        except (ValueError, IndexError) as e:
            self.logger.warning(f"无法解析文件名 {filename}: {e}")
            return None
    
    def _categorize_file(self, filename: str) -> str:
        """
        分类SIRENE文件
        
        Args:
            filename: 文件名
            
        Returns:
            str: 文件类别
        """
        filename_lower = filename.lower()
        
        if 'historique' in filename_lower:
            return 'historical'
        elif 'succession' in filename_lower:
            return 'succession'
        elif 'doublons' in filename_lower:
            return 'duplicates'
        elif any(stock_file.lower().replace('.zip', '') in filename_lower 
                for stock_file in self.stock_files):
            return 'stock'
        else:
            return 'other'
    
    def _filter_files_to_download(self, available_files: List[Dict]) -> List[Dict]:
        """
        过滤需要下载的文件
        
        Args:
            available_files: 可用文件列表
            
        Returns:
            List[Dict]: 需要下载的文件列表
        """
        files_to_download = []
        
        # 计算时间范围
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=self.months_back * 30)
        
        for file_info in available_files:
            should_download = False
            
            # 检查文件日期 - 确保时区兼容
            file_date = file_info['date']
            if file_date.tzinfo is None:
                file_date = file_date.replace(tzinfo=timezone.utc)
            if file_date < cutoff_date:
                continue
            
            # 检查文件类别
            if file_info['category'] == 'stock':
                should_download = True
            elif file_info['category'] == 'historical' and self.download_historical:
                should_download = True
            elif file_info['category'] in ['succession', 'duplicates'] and self.download_optional:
                should_download = True
            
            if should_download:
                files_to_download.append(file_info)
        
        return files_to_download
    
    def _download_file(self, file_info: Dict) -> Dict:
        """
        下载单个SIRENE文件
        
        Args:
            file_info: 文件信息字典
            
        Returns:
            Dict: 下载结果
        """
        filename = file_info['filename']
        url = file_info['url']
        
        try:
            # 构建GCS路径
            gcs_path = f"raw/sirene/{file_info['year']}/{filename}"
            
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
                        'gcs_path': gcs_path
                    }
            
            # 下载文件到临时位置
            temp_file = Path(f"/tmp/{filename}")
            self.logger.info(f"开始下载 {filename}...")
            
            download_file_with_retry(url, str(temp_file))
            
            # 验证ZIP文件
            self._validate_zip_file(temp_file)
            
            # 上传到GCS
            upload_to_gcs(
                gcs_client=self.gcs_client,
                bucket_name=self.bucket_name,
                source_file=str(temp_file),
                destination_blob=gcs_path,
                metadata={
                    'source_url': url,
                    'file_type': file_info['file_type'],
                    'category': file_info['category'],
                    'collection_date': datetime.now(timezone.utc).isoformat(),
                    'source_date': file_info['date'].isoformat()
                }
            )
            
            # 清理临时文件
            temp_file.unlink()
            
            self.logger.info(f"成功下载并上传 {filename}")
            return {
                'filename': filename,
                'status': 'success',
                'gcs_path': gcs_path,
                'file_size': temp_file.stat().st_size if temp_file.exists() else None
            }
            
        except Exception as e:
            self.logger.error(f"下载文件 {filename} 失败: {e}")
            return {
                'filename': filename,
                'status': 'failed',
                'error': str(e)
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
    
    def _validate_zip_file(self, file_path: Path) -> None:
        """
        验证ZIP文件的完整性
        
        Args:
            file_path: ZIP文件路径
            
        Raises:
            ValidationError: 如果文件验证失败
        """
        try:
            with zipfile.ZipFile(file_path, 'r') as zip_file:
                # 测试ZIP文件完整性
                zip_file.testzip()
                
                # 检查是否包含CSV文件
                csv_files = [name for name in zip_file.namelist() if name.endswith('.csv')]
                if not csv_files:
                    raise ValidationError(f"ZIP文件 {file_path.name} 不包含CSV文件")
                
                self.logger.debug(f"ZIP文件验证通过: {file_path.name}, 包含 {len(csv_files)} 个CSV文件")
                
        except zipfile.BadZipFile as e:
            raise ValidationError(f"无效的ZIP文件 {file_path.name}: {e}") from e
        except Exception as e:
            raise ValidationError(f"ZIP文件验证失败 {file_path.name}: {e}") from e
    
    def validate_data(self, file_path: str) -> bool:
        """
        验证SIRENE数据文件
        
        Args:
            file_path: 文件路径
            
        Returns:
            bool: 验证是否通过
        """
        try:
            if not file_path.endswith('.zip'):
                return False
            
            # 验证ZIP文件
            self._validate_zip_file(Path(file_path))
            
            # 可以添加更多的数据质量验证
            # 例如：检查CSV结构、必需字段等
            
            return True
            
        except Exception as e:
            self.logger.error(f"数据验证失败 {file_path}: {e}")
            return False


def sirene_collector_main(request=None):  # pylint: disable=unused-argument
    """
    Cloud Function入口点
    
    Args:
        request: HTTP请求对象（Cloud Functions）
        
    Returns:
        str: JSON格式的响应
    """
    import json
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
        collector = SireneCollector(config)
        result = collector.collect()
        
        return json.dumps(result, ensure_ascii=False, indent=2)
        
    except Exception as e:
        error_result = {
            'collector': 'sirene',
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
    
    collector = SireneCollector(config)
    result = collector.collect()
    print(json.dumps(result, ensure_ascii=False, indent=2))