## 技术选择
- 后端：python3
- 数据库：SQLite

## 数据库结构

### 重要更新（基于API测试结果）
1. **API返回树形结构**，不包含year、parentCode、parentName、namePath字段
2. 需要从树形结构推导这些字段
3. 乡级数据获取方式需要重新研究

### 主要表：xzqh（行政区划主表）
- code: 12位行政区划代码（从API获取）
- name: 行政区划名称（从API获取）
- level: 层级（1-3，对应省、地、县，从API获取）
- type: 行政区划类型（如：直辖市、地级市、县级市、市辖区等，从API获取）
- year: 数据年份（默认为0，API不返回）
- parent_code: 上级代码（从树形结构推导）
- parent_name: 上级名称（从树形结构推导）
- name_path: 完整名称路径（从树形结构推导）

### 辅助表
- data_versions（数据版本）
- change_history（变更历史）
