# 欧洲部署指南 - Europe Deployment Guide

## 概述

本项目默认部署在欧洲地区（`europe-west9` - 巴黎），这是基于数据合规性、性能和法律要求的最佳实践。

## 为什么选择欧洲部署？

### 1. 🇪🇺 GDPR合规性
- **数据主权**: 法国数据存储在欧盟境内
- **法律合规**: 满足GDPR数据处理要求
- **审计友好**: 简化合规性审计流程

### 2. 🚀 性能优化
- **网络延迟**: 距离法国数据源更近，网络延迟更低
- **下载速度**: 从法国政府网站下载数据更快
- **用户体验**: 为欧洲用户提供更好的响应速度

### 3. 📍 地理位置优势
- **时区对齐**: 使用巴黎时区进行调度
- **工作时间**: 在法国工作时间内进行维护
- **监管环境**: 符合法国和欧盟的监管要求

## 可用的欧洲地区

| 地区代码 | 地理位置 | 特点 | 推荐用途 |
|----------|----------|------|----------|
| `europe-west9` | 巴黎，法国 | 🟢 **推荐** | 法国数据项目 |
| `europe-west1` | 圣吉斯兰，比利时 | 成本效益 | 通用欧盟项目 |
| `europe-west3` | 法兰克福，德国 | 高可用性 | 企业级应用 |
| `europe-west4` | 埃姆斯哈文，荷兰 | 可持续能源 | 环保优先项目 |

## 配置说明

### 环境变量配置
```bash
# .env 文件
GCP_LOCATION=europe-west9  # 巴黎地区

# 其他可选地区
# GCP_LOCATION=europe-west1  # 比利时
# GCP_LOCATION=europe-west3  # 德国  
# GCP_LOCATION=europe-west4  # 荷兰
```

### 调度时区配置
```bash
# 所有调度任务使用巴黎时区
TIME_ZONE="Europe/Paris"

# 调度时间（巴黎时间）:
# - DVF: 每天凌晨2点
# - SIRENE: 每天凌晨3点
# - INSEE: 每周日凌晨4点
# - PLU: 每周日凌晨5点
```

## 部署步骤

### 1. 快速部署
```bash
# 使用提供的部署脚本
./scripts/deploy_europe.sh
```

### 2. 手动部署
```bash
# 设置环境变量
export GCP_LOCATION=europe-west9
export GCP_PROJECT_ID=your-project-id

# 部署函数到欧洲
gcloud functions deploy dvf-collector \
    --region $GCP_LOCATION \
    --runtime python311 \
    --trigger-http \
    --entry-point dvf_collector_main \
    --source collectors/dvf

# 创建欧洲地区的调度任务
gcloud scheduler jobs create http dvf-daily-job \
    --location $GCP_LOCATION \
    --schedule "0 2 * * *" \
    --time-zone "Europe/Paris" \
    --uri "https://$GCP_LOCATION-$GCP_PROJECT_ID.cloudfunctions.net/dvf-collector"
```

## 成本分析

### 欧洲 vs 美国成本对比

| 服务 | 美国（us-central1） | 欧洲（europe-west9） | 差异 |
|------|---------------------|----------------------|------|
| Cloud Functions | $0.0000004/调用 | $0.0000004/调用 | 无差异 |
| Cloud Storage | $0.020/GB | $0.023/GB | +15% |
| 网络出口 | $0.12/GB | $0.12/GB | 无差异 |
| Cloud Scheduler | $0.10/作业 | $0.10/作业 | 无差异 |

**月度额外成本**: 约€5-10（基于200GB存储）

### 隐性成本节省
- ✅ **合规成本**: 避免GDPR违规罚款（可达年收入4%）
- ✅ **性能提升**: 网络延迟降低50-70%
- ✅ **维护效率**: 时区对齐减少运维成本

## 网络和安全

### 数据传输路径
```
法国数据源 → GCP欧洲 → 处理 → GCS欧洲存储
     ↓
最短网络路径，最低延迟
```

### 安全增强
- **数据驻留**: 数据不离开欧盟
- **加密传输**: 所有数据传输都加密
- **访问控制**: IAM控制访问权限
- **审计日志**: 完整的访问审计记录

## 监控和告警

### 欧洲特定监控
```yaml
监控配置:
  地区: europe-west9
  时区: Europe/Paris
  告警时间: 工作时间 (9:00-18:00 CET)
  
健康检查:
  - DVF数据源可用性
  - SIRENE API响应时间
  - INSEE服务状态
  - PLU WFS服务健康度
```

### 性能基准
```yaml
预期性能（欧洲部署）:
  数据下载: 50-100 MB/s
  API响应: <500ms
  函数冷启动: <2s
  调度精度: ±30s
```

## 故障转移和备份

### 多地区备份策略
```yaml
主地区: europe-west9 (巴黎)
备份地区: europe-west3 (法兰克福)

备份配置:
  - 每日备份到备份地区
  - 异地复制关键数据
  - 自动故障检测
  - 15分钟RTO（恢复时间目标）
```

## 迁移指南

### 从美国地区迁移
如果你的项目目前部署在美国地区，可以按以下步骤迁移：

```bash
# 1. 导出数据
gsutil -m cp -r gs://your-us-bucket/* gs://your-eu-bucket/

# 2. 更新环境变量
export GCP_LOCATION=europe-west9

# 3. 重新部署所有服务
./scripts/deploy_europe.sh

# 4. 更新DNS和负载均衡器
# 5. 测试所有功能
# 6. 删除美国地区资源
```

## 常见问题

### Q: 为什么选择巴黎而不是其他欧洲地区？
A: 巴黎地区（europe-west9）是法国数据项目的最佳选择：
- 地理位置最近
- 符合法国数据主权要求
- 网络延迟最低
- 时区对齐

### Q: 欧洲部署会增加多少成本？
A: 存储成本增加约15%，但通过性能提升和合规性收益，总体TCO更优。

### Q: 如何处理跨地区数据访问？
A: 所有数据处理都在欧洲完成，避免跨地区数据传输。

### Q: 灾难恢复计划是什么？
A: 多地区备份 + 自动故障转移，RTO < 15分钟。

## 最佳实践

### 1. 配置管理
```bash
# 使用环境变量而非硬编码
GCP_LOCATION=${GCP_LOCATION:-europe-west9}
```

### 2. 资源命名
```bash
# 包含地区信息的资源名称
france-data-eu-west9-bucket
dvf-collector-eu-function
```

### 3. 监控配置
```yaml
# 设置合适的告警阈值
alerts:
  latency: <500ms
  error_rate: <1%
  availability: >99.9%
```

### 4. 备份策略
```bash
# 定期备份到多个地区
gsutil rsync -r gs://primary-bucket gs://backup-bucket
```

---

**部署到欧洲是法国数据项目的明智选择，兼顾了合规性、性能和成本效益。**