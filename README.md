# Hive SQL 血缘分析器（hive_lineage）

Rust 实现的 Hive SQL 血缘分析工具，支持 **HTTP 服务模式**（默认）和 **CLI 命令行模式**：
- 列级血缘：产出 `db.table.column <- sources` 形式的映射
- 表级血缘：产出 `db.target <- db.source1, db.source2`
- 支持 CTAS、INSERT、CREATE VIEW、CTE、窗口函数、过滤与别名等常见语法
- 支持将血缘数据持久化到 PostgreSQL 数据库

## 运行模式

### HTTP 服务模式（默认）

默认情况下，程序以 HTTP 服务模式运行，提供 RESTful API 接口。

**启动服务：**
```bash
cargo run
# 或使用 release 版本
cargo build --release
./target/release/hive_lineage
```

**配置文件：**

服务启动时会读取 `config.toml` 配置文件（可通过环境变量 `CONFIG_PATH` 指定路径）：

```toml
[server]
host = "0.0.0.0"
port = 8080
worker_threads = 4

[database]
host = "localhost"
port = 5432
dbname = "lineage"
user = "postgres"
password = "postgres"
pool_size = 16

[logging]
log_dir = "./logs"        # 日志文件目录
log_level = "info"        # 日志级别: trace, debug, info, warn, error
max_log_files = 7         # 保留的日志文件数量（按天）
```

**日志配置说明：**

- **日志轮转**：每天自动创建新的日志文件，文件名格式为 `hive_lineage.log.YYYY-MM-DD`
- **自动压缩**：服务启动时自动将非当天的日志文件压缩为 `.log.gz` 格式，节省磁盘空间
- **自动清理**：服务启动时自动删除超过 `max_log_files` 天数的旧日志文件（包括压缩文件）
- **日志输出**：同时输出到控制台（stdout）和日志文件
- **日志级别**：支持 `trace`、`debug`、`info`、`warn`、`error` 五个级别
- **环境变量**：可通过 `RUST_LOG` 环境变量覆盖配置文件中的日志级别

示例：
```bash
# 使用 debug 级别启动服务
RUST_LOG=debug ./target/release/hive_lineage

# 查看当天日志文件（未压缩）
tail -f ./logs/hive_lineage.log.*

# 查看历史压缩日志
zcat ./logs/hive_lineage.log.2025-11-24.gz | less
```

**数据库设置：**

服务需要 PostgreSQL 数据库支持。首次使用前需要运行数据库迁移：

```bash
# 安装 diesel CLI（如果尚未安装）
cargo install diesel_cli --no-default-features --features postgres

# 运行迁移
diesel migration run
```

**API 端点：**

服务提供以下 HTTP API 端点：

#### 1. 健康检查

```bash
GET /health
```

**示例：**
```bash
curl http://localhost:8080/health
```

**响应：**
```
OK
```

#### 2. 表级血缘分析

```bash
POST /table-lineage
Content-Type: application/json
```

**请求体：**
```json
{
  "sql": "USE db1; CREATE TABLE t AS SELECT * FROM s;",
  "dag_id": "my_dag",
  "task_id": "my_task"
}
```

**示例：**
```bash
curl -X POST http://localhost:8080/table-lineage \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "USE db1; CREATE TABLE t AS SELECT * FROM s;",
    "dag_id": "my_dag",
    "task_id": "my_task"
  }'
```

**成功响应（200 OK）：**
```json
{
  "db1.t": "db1.s"
}
```

**错误响应（400 Bad Request）：**
```json
{
  "error": "SQL analysis failed: ..."
}
```

#### 3. 列级血缘分析

```bash
POST /field-lineage
Content-Type: application/json
```

**请求体：**
```json
{
  "sql": "USE db1; CREATE TABLE t AS SELECT id, upper(name) AS uname FROM s;",
  "dag_id": "my_dag",
  "task_id": "my_task"
}
```

**示例：**
```bash
curl -X POST http://localhost:8080/field-lineage \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "USE db1; CREATE TABLE t AS SELECT id, upper(name) AS uname FROM s;",
    "dag_id": "my_dag",
    "task_id": "my_task"
  }'
```

**成功响应（200 OK）：**
```json
{
  "db1.t": {
    "id": ["db1.s.id"],
    "uname": ["db1.s.name"]
  }
}
```

**错误响应（400 Bad Request）：**
```json
{
  "error": "SQL analysis failed: ..."
}
```

**说明：**
- `dag_id` 和 `task_id` 用于标识血缘数据来源，会被存储到数据库中
- 所有血缘数据会自动持久化到 PostgreSQL 数据库
- 支持复杂 SQL，包括 CTE、子查询、JOIN、窗口函数等


### CLI 命令行模式

使用 `--cli` 参数可以切换到传统的命令行模式，无需数据库和配置文件。

**快速开始：**
```bash
# 构建
cargo build --release

# 分析文件
./target/release/hive_lineage --cli path/to/query.sql

# 从标准输入读取
cat query.sql | ./target/release/hive_lineage --cli -

# 或使用 cargo run
cargo run -- --cli path/to/query.sql
```

**CLI 参数：**
- `--cli` - 启用 CLI 模式（必需）
- `-t, --tables` - 输出表级血缘（默认为列级血缘）
- `-j, --json` - 以 JSON 格式输出
- `-p, --pretty` - JSON 美化输出
- `-` - 从标准输入读取 SQL

**示例：**

输入 SQL（`test.sql`）：
```sql
USE db1;
CREATE TABLE t AS SELECT id, upper(name) AS uname FROM s;
```

列级输出（默认）：
```bash
$ ./target/release/hive_lineage --cli test.sql
db1.t.id <- db1.s.id
db1.t.uname <- db1.s.name
```

表级输出：
```bash
$ ./target/release/hive_lineage --cli -t test.sql
db1.t <- db1.s
```

JSON 输出：
```bash
$ ./target/release/hive_lineage --cli --json --pretty test.sql
[
  {
    "stmt_type": "CTAS",
    "target": "db1.t",
    "column": "id",
    "sources": ["db1.s.id"],
    ...
  },
  ...
]
```

## 作为库使用

```rust
use hive_lineage::analyze_sql_lineage;

fn main() -> anyhow::Result<()> {
    let sql = "USE db1; CREATE TABLE t AS SELECT id FROM s;";
    let lines = analyze_sql_lineage(sql)?;
    for l in lines { 
        println!("{}", l); 
    }
    Ok(())
}
```

**可用的公共 API：**
- `analyze_sql_lineage(sql: &str)` - 返回列级血缘字符串列表
- `analyze_sql_tables(sql: &str)` - 返回表级血缘字符串列表
- `analyze_sql_lineage_detailed(sql: &str)` - 返回详细的列级血缘结构体
- `analyze_sql_tables_detailed(sql: &str)` - 返回详细的表级血缘结构体

## 开发与测试

```bash
# 运行测试
cargo test

# 代码格式化
cargo fmt

# 静态检查
cargo clippy

# 构建 release 版本
cargo build --release
```

## 构建静态跨平台二进制包

`pq-sys` 依赖启用了 `bundled` 特性，会在构建时自动编译嵌入的 `libpq`，配合 `musl` 目标即可生成无需额外依赖的完全静态 Linux 可执行文件。

```bash
# 安装需要的交叉编译目标（在 Linux 上示例）
rustup target add x86_64-unknown-linux-musl

# 生成默认的静态 Linux 产物（会放在 dist/ 目录）
scripts/build-dist.sh

# 指定多个目标（在对应平台执行，或提前配置好交叉编译工具链）
TARGETS="x86_64-unknown-linux-musl x86_64-pc-windows-gnu" scripts/build-dist.sh
```

脚本会在 `dist/` 目录下生成形如 `hive_lineage-v0.1.0-x86_64-unknown-linux-musl.tar.gz` 的压缩包，每个压缩包包含：
- 对应平台的可执行文件（Linux 为静态 `musl`，Windows 为 `.exe`，macOS 建议在 macOS 上运行脚本生成）
- `README.md` 与 `config.example.toml`

**重要提示**
- 不需要额外编写 `.cargo/config` 去强制 `musl-gcc` 或 `-static` 链接。Rust 自带的 `x86_64-unknown-linux-musl` 目标默认输出“static-pie”可执行文件（包含 musl loader），在 CentOS 7 / Alpine 等旧系统上运行不会触发 `_start_c` 处的段错误。
- 如果之前为了解决依赖问题添加过 `-C link-arg=-static` 等全局覆盖，请移除；否则可执行文件可能在早期启动阶段崩溃。

可以通过 `ldd dist/.../hive_lineage` 验证 Linux 产物为 “not a dynamic executable”，以确认静态链接生效。若需要构建其他体系架构，请确保安装了相应的 `rustup target` 以及系统级交叉编译工具链（例如 `musl-gcc`、`x86_64-w64-mingw32-gcc` 等）。

**依赖说明：**
- `sqlparser = =0.59.0` - SQL 解析器（固定版本，升级请谨慎评估兼容性）
- `actix-web = "4"` - HTTP 服务框架
- `diesel = "2"` - ORM 和数据库迁移工具
- 其他依赖请参考 `Cargo.toml`

## 默认数据库（schema）

- 若 SQL 中未通过 `USE db;` 指定当前数据库，则未带库名的表引用默认使用 `default` 库。
- 例如：
  - 输入：`INSERT OVERWRITE TABLE sdl.tae SELECT id, n AS name FROM sals;`
  - 解析：源表 `sals` 被解析为 `default.sals`

## 表达式回退策略

- **目标**：当投影表达式中没有任何明确的列依赖时，尽可能给出合理、可追踪的归属。

- **单表归属**：
  - 若当前作用域内仅有一个真实来源表（或最终可解析为唯一来源表），则将该表达式归属到该表，使用"原始表达式文本"作为伪列名。
  - 示例：`SELECT 1 AS n1 FROM s` 在 `INSERT/CTAS/VIEW` 中会产出 `db.s.1`，因此血缘行形如 `db.t.n1 <- db.s.1`。

- **多表或不唯一**：
  - 若作用域内存在多个来源且无法唯一确定归属，则保留原始表达式字符串作为右侧（表示无法定位到具体列）。
  - 对于"匿名投影"（未命名表达式）在不唯一场景下，还会回退为基础表的 `*`（例如 `a.*`, `b.*`），用以提示列级无法精确定位。

- **作用范围**：
  - 该策略同时适用于派生子查询（Derived Table）、CTE 以及顶层 SELECT。
  - 当上层引用派生/CTE 列且该列来源为常量/纯表达式时，若其子查询/CTE 的来源唯一，则会将其转化为对该唯一表的"伪列表达"（例如 `db.s.upper('abc')`）。

- **简单字面量定义**（触发"单表归属"）：
  - 所有"不包含字段引用"的表达式均视为简单字面量。
  - 包括但不限于：数值/字符串/布尔/NULL 字面量、带一元正负号（如 `-1`）、`CAST(1 AS INT)`、函数调用（如 `upper('abc')`）、CASE 表达式、`IN` 列表等，以及它们的括号嵌套形式。

## JSON 详细输出示例（含回退）

### 单表常量回退（sources 带伪列名，expr 为空）

输入：
```sql
USE db1; 
CREATE TABLE t AS SELECT 1 AS c FROM s;
```

输出（`--cli --json --pretty`）：
```json
{
  "stmt_type": "CTAS",
  "target": "db1.t",
  "column": "c",
  "sources": ["db1.s.1"],
  "expr": null,
  ...
}
```

### 多表且表达式不唯一（sources 缺失，expr 保留原始表达式）

输入：
```sql
USE db1; 
CREATE TABLE t2 AS SELECT 1 AS c FROM a JOIN b ON a.id=b.id;
```

输出（`--cli --json --pretty`）：
```json
{
  "stmt_type": "CTAS",
  "target": "db1.t2",
  "column": "c",
  "sources": null,
  "expr": "1",
  ...
}
```

说明：此时无法将常量表达式唯一归属到某一来源表，因此以 `expr` 字段承载。

### 函数/CASE/IN 等不依赖字段的复杂表达式（单表归属）

输入：
```sql
USE d; 
CREATE TABLE s(id INT); 
CREATE TABLE t AS SELECT 
  upper('abc') AS u, 
  CASE WHEN 1=1 THEN 2 ELSE 3 END AS c, 
  2 IN (1,2,3) AS f 
FROM s;
```

输出（节选，每列一条）：
```json
{ "stmt_type": "CTAS", "target": "d.t", "column": "u", "sources": ["d.s.upper('abc')"], "expr": null, ... }
{ "stmt_type": "CTAS", "target": "d.t", "column": "c", "sources": ["d.s.CASE WHEN 1 = 1 THEN 2 ELSE 3 END"], "expr": null, ... }
{ "stmt_type": "CTAS", "target": "d.t", "column": "f", "sources": ["d.s.2 IN (1, 2, 3)"], "expr": null, ... }
```

### 更多简单字面量（单表归属）示例（负号、CAST）

输入：
```sql
USE d; 
CREATE TABLE s(id INT); 
CREATE TABLE t AS SELECT -1 AS m, CAST(1 AS INT) AS c FROM s;
```

输出（节选，每列一条）：
```json
{ "stmt_type": "CTAS", "target": "d.t", "column": "m", "sources": ["d.s.-1"], "expr": null, ... }
{ "stmt_type": "CTAS", "target": "d.t", "column": "c", "sources": ["d.s.CAST(1 AS INT)"], "expr": null, ... }
```

## 贡献

请查看 `AGENTS.md` 获取项目结构、编码规范、测试与提交流程等贡献指南。

## 许可证

本项目采用 MIT 许可证。
