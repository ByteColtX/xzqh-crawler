# xzqh-crawler

从民政部[「全国行政区划信息查询平台」](https://dmfw.mca.gov.cn/XzqhVersionPublish.html)接口抓取中国 1-4 级行政区划代码和地址基础数据，并写入 SQLite。

可直接用作行政区划库、地址库基础表，适合省市区街道级联地址选择、地址补全、地址标准化前置字典和地理数据清洗。

当前实现是纯异步批处理工具：

- HTTP：`aiohttp`
- 数据库：`aiosqlite`
- 并发模型：`asyncio.Queue + 固定 worker + 单 writer`
- 终端输出：普通日志 + 最终摘要

## 运行要求

- Python 3.12+
- 推荐使用 [uv](https://github.com/astral-sh/uv)

## 安装

```bash
git clone https://github.com/ByteColtX/xzqh-crawler.git
cd xzqh-crawler
uv sync
```

## 快速开始

直接抓取并写入默认数据库 `./data/xzqh.db`：

```bash
uv run xzqh
```

指定数据库路径：

```bash
uv run xzqh --db ./data/custom.db
```

只补抓失败或未完成任务：

```bash
uv run xzqh --resume
```

调大并发和超时：

```bash
uv run xzqh --db ./data/xzqh.db -c 40 -t 20
```

查看帮助：

```bash
uv run xzqh --help
```

## 命令行参数

常用参数只有这几个：

- `--db PATH`：SQLite 文件路径，默认 `./data/xzqh.db`
- `-c, --concurrency`：最大并发抓取数
- `-t, --timeout`：单请求总超时时间（秒）
- `-r, --resume`：只处理 `crawl_jobs` 中 `pending/failed` 的任务
- `-d, --debug`：输出调试日志
- `-l, --log FILE`：写入日志文件

## 数据表

- `divisions`
  - `code, name, level, type, parent_code, parent_name, name_path, fetched_at`
- `crawl_jobs`
  - `parent_code, state, retry_count, last_error, updated_at`

`crawl_jobs` 用于承载 L4 抓取任务，因此同一个数据库支持断点续跑和失败补抓。

## 数据说明

统计用区划代码通常由 1～12 位数字构成，各位含义如下：

- 第 1～2 位：省级代码
- 第 3～4 位：地级代码
- 第 5～6 位：县级代码
- 第 7～9 位：乡级代码
- 第 10～12 位：村级代码

示例：

- 省级数据（L1）：广东（`44`）
- 地市级数据（L2）：广州市（`4401`）
- 区县级数据（L3）：越秀区（`440104`）
- 乡镇级数据（L4）：白云街道（`440104020`）
- 村 / 社区（L5）：本项目暂无

当前工具抓取并落库的范围是 L1-L4，不包含 L5。

## 数据约束

- 仅保存 `code` 为纯数字的行政区划记录
- 非数字 `code` 会在落库前被过滤，并输出 warning 日志

这是为了规避上游偶发脏数据，例如非标准区划代码混入返回结果。

## 测试

运行全部测试：

```bash
uv run pytest
```

真实网络集成测试默认关闭，只有显式设置环境变量后才执行：

```bash
XZQH_INTEGRATION=1 uv run pytest -m integration
```
