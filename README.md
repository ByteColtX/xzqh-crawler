# xzqh-crawler（行政区划数据爬虫）

从民政部「全国行政区划信息查询平台」接口抓取 1–4 级行政区划数据，并落库到 SQLite。

- 数据层级：
  - L1 省/直辖市/自治区
  - L2 地级市/地区/自治州
  - L3 区县
  - L4 乡镇/街道
- 数据源接口：`https://dmfw.mca.gov.cn/xzqh/getList?code=...&trimCode=true&maxLevel=...`
- 输出：SQLite（默认写入 `data/`）

> 本项目已适配 `trimCode=true` 的“短码”语义：L1–L3 返回的是去尾 0 的短码，代码中不再对其做“去尾 0 归一化”。

---

## 你需要准备什么

- Python 3.10+
- 推荐使用 [uv](https://github.com/astral-sh/uv) 管理依赖（也可使用你自己的 venv/pip）

---

## 安装

### 方式 A：使用 uv（推荐）

```bash
uv sync
```

### 方式 B：使用 pip（可选）

```bash
python -m venv .venv
. .venv/bin/activate
pip install -e .
```

---

## 一键运行

抓取并写入一个新的 SQLite（包含 L1–L4）：

```bash
python -m xzqh_crawler --db-path ./data/xzqh.db
```

只抓取 L1–L3（不抓 L4）：

```bash
python -m xzqh_crawler --db-path ./data/xzqh_l1_l3.db --max-level 3
```

查看全部参数：

```bash
python -m xzqh_crawler --help
```

---

## 重要说明（L4 抓取策略）

`maxLevel=4` 时，接口允许用 **L2 或 L3** 作为 `code`：

- 对很多省份：用 L2（例如 `4602`）请求 `maxLevel=4` 会直接返回该市下面的 L3 + L4。
- 对直辖市等结构：需要对每个 L3（例如 `110101`）请求 `maxLevel=4`。

本项目内部会根据库中的结构派发合适的 L4 抓取任务。

---

## 失败重试与断点续跑

抓取 L4 时，程序会把“待抓取任务/已完成任务/失败任务”记录在 SQLite 里（表：`xzqh_jobs`），从而支持断点续跑。

字段示例：
- `status`: `pending | ok | failed`
- `try_count`: 已尝试次数
- `last_error`: 最近一次错误

用法：
- 运行过程中即使中断也没关系，之后**再次运行同一个 DB**，程序会继续处理未完成的任务，直到都变成 `ok`。

---

## 配置文件（可选）

支持 TOML 配置（例如 `config.toml`），并允许命令行覆盖。

```bash
python -m xzqh_crawler --config ./config.toml --db-path ./data/xzqh.db
```

---

## 数据库结构（概览）

- `xzqh`
  - 最小字段：`code, name, level, type, parent_code, name_path, created_at, updated_at`
  - `type` 允许为空（例如根节点）

- `xzqh_jobs`
  - 用于 L4 抓取任务的断点续跑/补跑

---

## 项目结构

```
xzqh-crawler/
  src/xzqh_crawler/
  tests/
  data/
  docs/
  pyproject.toml
  README.md
```

---

## 相关项目

- `address-picker`：前端地址选择器 Demo（消费本项目生成的 SQLite 快照）
