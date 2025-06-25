# 法国PLU（Plan Local d'Urbanisme）数据和GPU WFS API深度研究报告

## 概述

本报告详细分析了法国Géoportail de l'Urbanisme (GPU) WFS服务的技术规格、PLU数据结构以及最佳实践方案。

## 1. GPU WFS服务分析

### 1.1 官方服务端点

- **主要WFS端点**: `https://data.geopf.fr/wfs/ows`
- **服务能力文档**: `https://data.geopf.fr/wfs/ows?SERVICE=WFS&VERSION=2.0.0&REQUEST=GetCapabilities`
- **GPU特定元数据**: `https://data.geopf.fr/annexes/ressources/wfs/gpu.xml`

### 1.2 服务特性

- **WFS版本**: 2.0.0
- **坐标系统**: 主要支持EPSG:4326 (WGS84) 和 EPSG:2154 (Lambert-93)
- **输出格式**: GML, JSON, KML, CSV, SHAPE-ZIP
- **分页限制**: 每次请求最多5,000个对象
- **查询限制**: 建议使用COUNT参数限制结果数量

### 1.3 核心操作

- `GetCapabilities`: 获取服务能力
- `DescribeFeatureType`: 获取要素类型定义
- `GetFeature`: 获取要素数据
- `Transaction`: 事务操作（限制访问）
- `ListStoredQueries`: 存储查询列表

## 2. PLU数据结构分析

### 2.1 主要要素类型

根据WFS服务提供的图层，主要PLU相关要素类型包括：

1. **wfs_du:zone_urba** - 城市规划分区
2. **wfs_du:prescription_surf** - 面状规定
3. **wfs_du:prescription_lin** - 线状规定
4. **wfs_du:prescription_pct** - 点状规定
5. **wfs_du:doc_urba** - 城市规划文档
6. **wfs_du:municipality** - 市政边界

### 2.2 zone_urba（分区）字段结构

```xml
核心字段：
- gid: 唯一标识符 (int)
- gpu_doc_id: GPU文档ID (string)
- gpu_status: GPU状态 (string)
- gpu_timestamp: GPU时间戳 (dateTime)
- partition: 分区标识 (string, 格式: DU_INSEE)
- libelle: 标签 (string, 如"1AUh")
- libelong: 完整描述 (string)
- typezone: 分区类型 (string, 如"AUc")
- destdomi: 主要用途 (string)
- insee: INSEE代码 (string)
- idurba: 城市规划文档ID (string)
- idzone: 分区ID (string)
- the_geom: 几何图形 (GeometryPropertyType)
- symbole: 符号 (string)

扩展字段：
- formdomi: 主要形式 (string)
- destoui: 允许用途 (string)
- destcdt: 条件用途 (string)
- destnon: 禁止用途 (string)
```

### 2.3 prescription_surf（面状规定）字段结构

```xml
核心字段：
- gid: 唯一标识符 (int)
- gpu_doc_id: GPU文档ID (string)
- libelle: 标签 (string)
- txt: 文本描述 (string)
- typepsc: 规定类型 (string)
- stypepsc: 规定子类型 (string)
- idpsc: 规定ID (string)
- lib_idpsc: 规定ID标签 (string)
- nature: 性质 (string)
- the_geom: 几何图形 (GeometryPropertyType)
```

### 2.4 PLU分区类型

根据法国城市规划法，PLU分区主要分为四大类：

1. **Zone U** (已城市化区域) - 允许新建设
2. **Zone AU** (待城市化区域) - 未来开发区
3. **Zone A** (农业区域) - 仅允许农业相关建设
4. **Zone N** (自然区域) - 受保护区域，禁止新建设

## 3. 查询策略研究

### 3.1 基本WFS查询参数

```
必需参数：
- SERVICE=WFS
- VERSION=2.0.0
- REQUEST=GetFeature
- TYPENAME=wfs_du:zone_urba

可选参数：
- OUTPUTFORMAT=application/json
- SRSNAME=EPSG:4326
- COUNT=限制数量
- STARTINDEX=分页起始
- BBOX=边界框查询
- CQL_FILTER=CQL过滤条件
```

### 3.2 空间查询示例

#### BBOX查询（边界框）
```
https://data.geopf.fr/wfs/ows?
SERVICE=WFS&
VERSION=2.0.0&
REQUEST=GetFeature&
TYPENAME=wfs_du:zone_urba&
OUTPUTFORMAT=application/json&
SRSNAME=EPSG:4326&
BBOX=2.0,46.0,3.0,47.0,EPSG:4326&
COUNT=100
```

#### CQL过滤查询
```
# 按INSEE代码查询
CQL_FILTER=insee='75001'

# 按分区类型查询
CQL_FILTER=typezone='U'

# 空间相交查询
CQL_FILTER=INTERSECTS(the_geom, POINT(2.3522 48.8566))

# 组合查询
CQL_FILTER=insee='75001' AND typezone='U'
```

### 3.3 分页处理策略

由于GPU WFS限制每次返回5,000个对象，需要实现分页：

```python
def fetch_all_features(wfs_url, typename, filters=None):
    features = []
    start_index = 0
    count = 5000
    
    while True:
        params = {
            'SERVICE': 'WFS',
            'VERSION': '2.0.0',
            'REQUEST': 'GetFeature',
            'TYPENAME': typename,
            'OUTPUTFORMAT': 'application/json',
            'SRSNAME': 'EPSG:4326',
            'COUNT': count,
            'STARTINDEX': start_index,
            'sortBy': 'gid'  # 必需用于分页
        }
        
        if filters:
            params['CQL_FILTER'] = filters
            
        # 执行请求并处理响应
        # ...
        
        if len(batch_features) < count:
            break
            
        start_index += count
        
    return features
```

## 4. 技术实现要点

### 4.1 推荐Python库

```python
# WFS访问和几何处理
from owslib.wfs import WebFeatureService
import geopandas as gpd
import requests
from shapely.geometry import Point, Polygon

# 坐标转换
import pyproj
from pyproj import Transformer

# 数据处理
import pandas as pd
import json
```

### 4.2 坐标系转换

```python
# Lambert-93 (EPSG:2154) 到 WGS84 (EPSG:4326)
transformer = Transformer.from_crs("EPSG:2154", "EPSG:4326", always_xy=True)

# 单点转换
lon, lat = transformer.transform(x_lambert, y_lambert)

# GeoPandas批量转换
gdf_wgs84 = gdf_lambert.to_crs('EPSG:4326')
```

### 4.3 WFS客户端实现示例

```python
class GPUWFSClient:
    def __init__(self):
        self.base_url = "https://data.geopf.fr/wfs/ows"
        self.version = "2.0.0"
    
    def get_capabilities(self):
        """获取WFS服务能力"""
        wfs = WebFeatureService(self.base_url, version=self.version)
        return wfs
    
    def describe_feature_type(self, typename):
        """获取要素类型定义"""
        params = {
            'SERVICE': 'WFS',
            'VERSION': self.version,
            'REQUEST': 'DescribeFeatureType',
            'TYPENAME': typename
        }
        response = requests.get(self.base_url, params=params)
        return response.text
    
    def get_features(self, typename, bbox=None, cql_filter=None, count=1000):
        """获取要素数据"""
        params = {
            'SERVICE': 'WFS',
            'VERSION': self.version,
            'REQUEST': 'GetFeature',
            'TYPENAME': typename,
            'OUTPUTFORMAT': 'application/json',
            'SRSNAME': 'EPSG:4326',
            'COUNT': count,
            'sortBy': 'gid'
        }
        
        if bbox:
            params['BBOX'] = f"{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]},EPSG:4326"
        
        if cql_filter:
            params['CQL_FILTER'] = cql_filter
        
        response = requests.get(self.base_url, params=params)
        return response.json()
    
    def features_to_geodataframe(self, geojson_data):
        """转换GeoJSON为GeoPandas GeoDataFrame"""
        return gpd.GeoDataFrame.from_features(
            geojson_data['features'], 
            crs='EPSG:4326'
        )
```

### 4.4 常见错误处理

```python
def handle_wfs_errors(response):
    """处理WFS常见错误"""
    if response.status_code != 200:
        raise Exception(f"HTTP错误: {response.status_code}")
    
    # 检查是否返回XML错误
    if 'ExceptionReport' in response.text:
        raise Exception(f"WFS错误: {response.text}")
    
    # 检查空结果
    data = response.json()
    if data.get('numberReturned', 0) == 0:
        print("警告: 查询未返回任何结果")
    
    return data
```

## 5. 最佳实践建议

### 5.1 性能优化

1. **使用适当的BBOX**: 限制查询范围以减少数据传输
2. **分页查询**: 避免一次获取过多数据
3. **字段选择**: 使用PROPERTYNAME参数只获取需要的字段
4. **缓存策略**: 对静态数据实施本地缓存

### 5.2 查询优化

```python
# 优化示例：只获取必要字段
params['PROPERTYNAME'] = 'gid,libelle,typezone,the_geom'

# 使用索引字段进行过滤
params['CQL_FILTER'] = 'insee IN (\'75001\',\'75002\',\'75003\')'
```

### 5.3 数据处理工作流

```python
def process_plu_data(insee_codes, output_path):
    """PLU数据处理工作流"""
    client = GPUWFSClient()
    
    # 1. 获取分区数据
    zones = []
    for insee in insee_codes:
        zone_data = client.get_features(
            'wfs_du:zone_urba',
            cql_filter=f"partition='DU_{insee}'"
        )
        zones.extend(zone_data['features'])
    
    # 2. 转换为GeoDataFrame
    gdf_zones = gpd.GeoDataFrame.from_features(zones, crs='EPSG:4326')
    
    # 3. 坐标转换（如需要）
    if target_crs != 'EPSG:4326':
        gdf_zones = gdf_zones.to_crs(target_crs)
    
    # 4. 数据清理和验证
    gdf_zones = gdf_zones.dropna(subset=['the_geom'])
    gdf_zones = gdf_zones[gdf_zones.geometry.is_valid]
    
    # 5. 保存结果
    gdf_zones.to_file(output_path, driver='GeoJSON')
    
    return gdf_zones
```

## 6. 资源链接

### 6.1 官方文档
- [GPU服务页面](https://www.geoportail-urbanisme.gouv.fr/services/)
- [IGN Géoservices GPU](https://geoservices.ign.fr/gpu)
- [开发者文档GitHub](https://github.com/IGNF/geoportail-urbanisme-dev)

### 6.2 标准规范
- [CNIG PLU标准v2024](https://cnig.gouv.fr/IMG/pdf/231220_standard_cnig_plu_v2024-01.pdf)
- [WFS 2.0.0规范](http://www.opengeospatial.org/standards/wfs)

### 6.3 相关API
- [API Carto GPU模块](https://api.gouv.fr/les-api/api_carto_gpu)

## 7. 结论

GPU WFS服务提供了访问法国PLU数据的标准化接口，但需要注意分页限制和性能优化。推荐使用OWSLib和GeoPandas的组合来处理数据获取和地理处理任务。坐标转换在Lambert-93和WGS84之间的转换是常见需求，应当在数据处理流程中妥善处理。

通过合理的查询策略和错误处理机制，可以构建稳定可靠的PLU数据获取和分析系统。