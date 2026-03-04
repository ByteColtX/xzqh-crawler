# 行政区划代码爬虫

一个简单的Python爬虫，用于从民政部网站获取行政区划代码数据。

## 功能特性

- ✅ **完整数据获取**：支持获取1-4级行政区划数据（省、地、县、乡）
- ✅ **树形结构处理**：API返回树形结构，自动扁平化处理
- ✅ **SQLite数据库**：数据存储到SQLite，支持批量操作和查询
- ✅ **配置管理**：TOML格式配置文件，支持命令行参数覆盖
- ✅ **多线程并发**：支持多线程获取乡级数据，提高效率
- ✅ **错误处理**：完善的错误处理和重试机制
- ✅ **进度显示**：详细的进度信息和统计摘要
- ✅ **版本管理**：自动记录数据版本信息

## 安装

```bash
# 克隆项目
git clone https://github.com/ByteColt/xzqh-crawler.git
cd xzqh-crawler

# 安装依赖
uv sync

# 安装到虚拟环境
uv pip install -e .
```

## 快速使用

### 安装依赖
```bash
uv sync
```

### 基本使用
```bash
# 创建默认配置文件
python -m xzqh_crawler --create-config

# 使用默认配置运行爬虫（获取1-3级数据）
python -m xzqh_crawler

# 使用自定义配置运行
python -m xzqh_crawler --config ./config.toml --db-path ./data/xzqh.db --max-workers 10

# 查看帮助
python -m xzqh_crawler --help
```

### Python代码中使用
```python
from xzqh_crawler import XzqhCrawler

# 创建爬虫实例（获取完整数据）
crawler = XzqhCrawler(
    db_path="./data/xzqh.db",
    fetch_townships=True,  # 获取乡级数据
    max_workers=10,
    batch_size=100,
    township_batch_delay=2.0,  # 批次间延迟
    township_max_retries=3,  # 最大重试次数
)

# 获取数据
success = crawler.fetch_all()
if success:
    print("数据获取成功")
    
# 只获取1-3级数据（不获取乡级）
crawler_simple = XzqhCrawler(
    db_path="./data/xzqh_simple.db",
    fetch_townships=False,  # 不获取乡级数据
)
```

### 数据库查询
```python
from xzqh_crawler import Database

db = Database(db_path="./data/xzqh.db")

# 查询统计信息
stats = db.get_statistics()
print(f"总记录数: {stats.get('total', 0)}")

# 查询省级数据
provinces = db.get_divisions_by_level(1)
for province in provinces[:5]:
    print(f"{province.name} ({province.code})")
```

## 配置

创建 `config.toml` 文件：

```toml
[database]
path = "./data/xzqh.db"

[api]
base_url = "https://dmfw.mca.gov.cn"
timeout = 30
max_retries = 3
retry_delay = 1.0

[crawler]
max_workers = 10
batch_size = 100
fetch_townships = true  # 是否获取乡级数据
township_batch_delay = 2.0  # 乡级数据批次间延迟（秒）
township_max_retries = 3  # 乡级数据最大重试次数

[logging]
level = "INFO"
```

### 配置说明

- **fetch_townships**: 是否获取乡级数据，默认为true
- **township_batch_delay**: 乡级数据批次间延迟，避免API限制
- **township_max_retries**: 乡级数据获取失败时的最大重试次数
- **max_workers**: 并发工作线程数，用于获取乡级数据
- **batch_size**: 批量处理大小，影响内存使用和性能

## 项目结构

```
xzqh-crawler/
├── src/xzqh_crawler/
│   ├── models.py      # 数据模型
│   ├── client.py      # HTTP客户端
│   ├── database.py    # 数据库操作
│   ├── crawler.py     # 爬虫逻辑
│   ├── config.py      # 配置管理
│   ├── cli.py         # 命令行接口
│   └── utils.py       # 工具函数
├── docs/              # 文档
├── pyproject.toml     # 项目配置
└── README.md          # 项目说明
```

## 开发

```bash
# 安装开发依赖
uv sync --dev

# 运行测试
uv run pytest

# 代码检查
uv run ruff check src/

# 代码格式化
uv run black src/
```

## 许可证

MIT