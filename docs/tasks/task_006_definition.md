# Task 6: PLU (Plan Local d'Urbanisme) 数据收集器开发

## 任务基本信息
- **任务ID**: 6
- **标题**: Develop PLU (Plan Local d'Urbanisme) Data Collector  
- **优先级**: 高
- **依赖任务**: Task 2 (核心工具), Task 5 (地理边界)
- **预估工时**: 45-60分钟
- **负责人**: Claude Code Assistant

## 任务目标
实现PLU（地方城市规划）数据收集器，通过WFS (Web Feature Service) API从GPU服务收集法国城市规划数据，支持按地理区域过滤和多种输出格式。

## 详细需求

### 1. 核心功能要求
- **WFS API集成**: 实现与GPU (Géoportail de l'Urbanisme) WFS服务的完整集成
- **地理数据查询**: 支持按边界框(BBOX)、市镇代码等进行空间查询
- **增量更新**: 实现基于时间戳的增量数据收集
- **多格式输出**: 支持GeoJSON、Shapefile、GeoPackage格式导出
- **数据验证**: 验证PLU数据的完整性和有效性

### 2. 技术规格
- **继承BaseCollector**: 遵循项目统一的收集器架构
- **配置驱动**: 支持通过config.yaml灵活配置数据源和过滤条件
- **容错处理**: 处理WFS服务的各种异常情况
- **云函数兼容**: 支持部署为Google Cloud Function

### 3. 数据源规格
- **主要API**: GPU WFS服务 (https://wxs.ign.fr/gpu/geoportail/wfs)
- **数据类型**: 
  - PLU边界 (zonage)
  - 规划文档元数据
  - 法规信息
- **坐标系**: 支持Lambert-93和WGS84投影
- **更新频率**: 每日增量，每周全量备份

## 子任务分解

### 6.1 研究PLU数据源和WFS API规格
- [x] 分析GPU WFS服务API文档
- [x] 理解PLU数据结构和字段含义
- [x] 确定最佳的查询策略和过滤条件

### 6.2 实现PLU收集器核心类
- [ ] 创建PLUCollector类，继承BaseCollector
- [ ] 实现WFS查询构建和执行逻辑
- [ ] 添加地理边界过滤功能
- [ ] 实现多格式数据导出

### 6.3 添加空间查询和数据处理
- [ ] 实现BBOX (边界框) 查询功能
- [ ] 添加按市镇/省份的数据过滤
- [ ] 处理大型数据集的分页查询
- [ ] 实现坐标系转换

### 6.4 数据验证和质量控制
- [ ] 验证WFS响应的XML/GeoJSON格式
- [ ] 检查PLU数据的几何有效性
- [ ] 实现数据完整性检查
- [ ] 添加数据质量报告

### 6.5 编写完整的单元测试
- [ ] 测试WFS API集成
- [ ] 测试地理查询和过滤功能
- [ ] 测试多格式导出
- [ ] 测试错误处理和容错机制
- [ ] 集成测试和端到端测试

### 6.6 更新配置和依赖
- [ ] 完善config.yaml中的PLU配置
- [ ] 更新requirements.txt（如需新依赖）
- [ ] 更新README.md文档

### 6.7 功能验证和性能测试
- [ ] 运行完整测试套件
- [ ] 验证数据收集流程
- [ ] 测试大数据量处理性能
- [ ] 验证云函数部署兼容性

## 验收标准
- [ ] PLU数据收集器核心功能完成
- [ ] 支持WFS API的完整集成
- [ ] 实现地理空间查询和过滤
- [ ] 支持多种输出格式 (GeoJSON, Shapefile, GeoPackage)
- [ ] 提供完整的单元测试覆盖 (目标: >25个测试用例)
- [ ] 集成到BaseCollector架构
- [ ] 支持Cloud Function部署
- [ ] 配置文件完整更新
- [ ] 性能满足要求 (处理1000个PLU边界 < 5分钟)

## 技术挑战和风险
1. **WFS协议复杂性**: OGC WFS标准比简单HTTP API复杂
2. **大数据量处理**: PLU数据可能非常大，需要分页和流式处理
3. **坐标系转换**: 处理Lambert-93与WGS84之间的精确转换
4. **服务稳定性**: GPU服务可能有访问限制或不稳定

## 期望成果
- `collectors/plu/plu_collector.py` - PLU收集器核心实现
- `tests/test_plu_collector.py` - 完整的单元测试
- 更新的配置文件和文档
- 性能良好、测试覆盖完整的PLU数据收集解决方案

---
*任务定义创建时间: 2025-06-25*  
*创建者: Claude Code Assistant*