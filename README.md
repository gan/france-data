# France Data Collector

自动化收集法国房地产相关公开数据的Google Cloud Functions项目。

## 项目概述

本项目使用Google Cloud Functions自动化收集以下法国公开数据：
- **DVF (Demandes de Valeurs Foncières)**: 房产交易数据
- **SIRENE**: 企业名录数据
- **INSEE Geographic Contours**: 地理边界数据（IRIS轮廓）
- **PLU (Plan Local d'Urbanisme)**: 城市规划数据

## 目录结构

```
france-data/
├── collectors/          # 数据收集器模块
│   ├── dvf/            # DVF数据收集器
│   ├── sirene/         # SIRENE数据收集器
│   ├── insee_contours/ # INSEE地理轮廓收集器
│   └── plu/            # PLU数据收集器
├── config/             # 配置文件
│   ├── config.yaml     # 主配置文件
│   └── config_loader.py # 配置加载器
├── utils/              # 工具模块
│   └── gcs_client.py   # Google Cloud Storage客户端
├── tests/              # 测试文件
├── scripts/            # 脚本文件
│   └── validate_setup.py # 安装验证脚本
├── credentials/        # 凭证文件目录（不提交到Git）
├── requirements.txt    # Python依赖
└── .env               # 环境变量（不提交到Git）
```

## 安装步骤

### 1. 克隆项目

```bash
git clone <repository-url>
cd france-data
```

### 2. 创建Python虚拟环境

```bash
python3.11 -m venv venv
source venv/bin/activate  # Linux/Mac
# 或
venv\Scripts\activate  # Windows
```

### 3. 安装依赖

```bash
pip install -r requirements.txt
```

### 4. 配置Google Cloud

1. 创建GCP项目
2. 启用必要的API：
   - Cloud Storage API
   - Cloud Functions API
   - Cloud Scheduler API
   - Cloud Logging API

3. 创建服务账号并下载凭证：
   ```bash
   gcloud iam service-accounts create france-data-collector \
     --display-name="France Data Collector Service Account"
   
   gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
     --member="serviceAccount:france-data-collector@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
     --role="roles/storage.admin"
   
   gcloud iam service-accounts keys create credentials/service-account-key.json \
     --iam-account=france-data-collector@YOUR_PROJECT_ID.iam.gserviceaccount.com
   ```

### 5. 配置环境变量

复制示例文件并填写实际值：
```bash
cp .env.example .env
```

编辑`.env`文件：
```
GCP_PROJECT_ID=your-actual-project-id
GCS_BUCKET_NAME=france-real-estate-data-prod
GOOGLE_APPLICATION_CREDENTIALS=./credentials/service-account-key.json
```

### 6. 验证安装

运行验证脚本：
```bash
python scripts/validate_setup.py
```

所有检查应该显示✓。

## 使用方法

### 本地测试

```bash
# 测试DVF收集器
python -m collectors.dvf.main

# 测试SIRENE收集器
python -m collectors.sirene.main
```

### 部署到Cloud Functions

```bash
# 部署单个函数
gcloud functions deploy collect-dvf \
  --runtime python311 \
  --trigger-http \
  --entry-point main \
  --source collectors/dvf \
  --set-env-vars GCS_BUCKET_NAME=$GCS_BUCKET_NAME

# 部署所有函数
./scripts/deploy_all.sh
```

### 配置定时任务

使用Cloud Scheduler设置定时触发：
```bash
# DVF - 每月执行
gcloud scheduler jobs create http dvf-monthly \
  --schedule="0 2 1 * *" \
  --uri="https://REGION-PROJECT_ID.cloudfunctions.net/collect-dvf" \
  --http-method=GET

# SIRENE - 每日执行
gcloud scheduler jobs create http sirene-daily \
  --schedule="0 3 * * *" \
  --uri="https://REGION-PROJECT_ID.cloudfunctions.net/collect-sirene" \
  --http-method=GET
```

## 数据存储结构

在Google Cloud Storage中：
```
gs://your-bucket/
├── raw/                    # 原始数据
│   ├── dvf/               # DVF原始文件
│   ├── sirene/            # SIRENE原始文件
│   ├── insee-contours/    # INSEE轮廓文件
│   └── plu/               # PLU原始数据
├── processed/             # 处理后的数据
│   └── ...               # 与raw相同的结构
├── logs/                  # 日志文件
└── metadata/              # 元数据文件
```

## 监控和日志

- 日志通过Google Cloud Logging查看
- 可配置邮件或Slack告警
- 使用Cloud Monitoring监控函数执行

## 开发指南

### 添加新的数据源

1. 在`collectors/`下创建新目录
2. 实现收集器类，继承基础收集器
3. 在`config.yaml`中添加配置
4. 编写测试
5. 部署并配置定时任务

### 运行测试

```bash
pytest tests/
```

### 代码风格

```bash
# 格式化代码
black .

# 检查代码风格
flake8 .

# 类型检查
mypy .
```

## 故障排除

### 常见问题

1. **认证错误**
   - 确认`GOOGLE_APPLICATION_CREDENTIALS`环境变量设置正确
   - 确认服务账号有足够权限

2. **依赖错误**
   - 确保使用Python 3.11+
   - 运行`pip install -r requirements.txt`

3. **配置错误**
   - 检查`.env`文件是否正确配置
   - 运行`python scripts/validate_setup.py`验证

## 许可证

[添加许可证信息]

## 贡献

[添加贡献指南]