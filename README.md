# xzqh-crawler（行政区划数据爬虫）

从民政部[「全国行政区划信息查询平台」](https://dmfw.mca.gov.cn/XzqhVersionPublish.html)接口抓取 1–4 级行政区划数据，并落库到 SQLite。

- 数据层级：
  - L1 省/直辖市/自治区
  - L2 地级市/地区/自治州
  - L3 区县
  - L4 乡镇/街道
  - 暂无 L5（村/社区）数据
- 数据源接口：`https://dmfw.mca.gov.cn/xzqh/getList?code=...&trimCode=true&maxLevel=...`
- 输出：SQLite3 数据库，表名 `xzqh`
- 本项目数据已更新至: [2025-12-31](./data/xzqh_20251231.db)

---

## 你需要准备什么

- Python 3.10+
- 推荐使用 [uv](https://github.com/astral-sh/uv) 管理依赖

---

## 快速开始

### 使用 uv（推荐）

```bash
git clone https://github.com/ByteColtX/xzqh-crawler.git
cd xzqh-crawler
uv sync
uv run python -m xzqh_crawler --db-path ./data/xzqh.db
```

如不使用 uv，也可自行创建 venv 后执行 `pip install -e .`。

查看全部参数：

```bash
uv run python -m xzqh_crawler --help
```

---

## 重要说明（L4 抓取策略）

`maxLevel=4` 时，接口允许用 **L2 或 L3** 作为 `code`：

- 对很多省份：用 L2（例如 `4602`）请求 `maxLevel=4` 会直接返回该市下面的 L3 + L4。
- 对直辖市等结构：需要对每个 L3（例如 `110101`）请求 `maxLevel=4`。

---

## 失败重试与断点续跑

抓取 L4 时，程序会把“待抓取任务/已完成任务/失败任务”记录在 SQLite 里（表：`xzqh_jobs`），从而支持断点续跑。

字段示例：
- `status`: `pending | ok | failed`
- `try_count`: 已尝试次数
- `last_error`: 最近一次错误

用法：
- 运行过程中即使中断也没关系。
- 再次运行同一个 DB 时，程序会继续处理 `xzqh_jobs` 中的 `pending` / `failed` 任务，无需依赖额外失败文件。

---

## 配置文件（可选）

支持 TOML 配置（例如 `config.toml`），并允许命令行参数覆盖配置文件中的默认值。

```bash
python -m xzqh_crawler --config ./config.toml --db-path ./data/xzqh.db
```

具体可用参数请以 `--help` 和源码中的配置模型为准。

---

## 数据说明

统计用区划代码由1～12位代码构成，其各代码表示为：  
第1～2位，为省级代码；  
第3～4 位，为地级代码；  
第5～6位，为县级代码；  
第7～9位，为乡级代码；  
第10～12位，为村级代码；  

示例：
- 省级数据(L1): 广东（44）
- 地市级数据(L2): 广州市（4401）
- 区县级数据(L3): 越秀区（440104）
- 乡镇级数据(L4): 白云街道（440104020）
- 村/社区(L5): **本项目暂无**

## 数据库结构（概览）

- `xzqh`
  - 最小字段：`code, name, level, type, parent_code, name_path, created_at, updated_at`
  - `type` 允许为空（例如根节点）

- `xzqh_jobs`
  - 用于 L4 抓取任务的断点续跑/补跑

## 许可证：MIT License