# MiniDB 总体设计方案

## 1. 项目概述

MiniDB 是一个用 Go 语言实现的轻量级关系型数据库引擎。项目目标是提供一个功能完整、架构清晰、可嵌入的数据库系统，支持 SQL 查询、事务处理和持久化存储。

### 1.1 核心特性

| 特性 | 说明 |
|------|------|
| SQL 支持 | DDL（CREATE/DROP/ALTER）、DML（SELECT/INSERT/UPDATE/DELETE）、事务控制（BEGIN/COMMIT/ROLLBACK） |
| 存储引擎 | 基于 B+ 树的磁盘持久化存储，支持范围扫描和批量操作 |
| 事务机制 | MVCC 多版本并发控制 + 行级悲观写锁，快照隔离级别 |
| WAL 预写日志 | 异步追加写入，保证事务持久性，支持崩溃恢复 |
| 二级索引 | 支持唯一索引和普通索引的创建与维护 |
| 外键约束 | 支持外键定义（ALTER TABLE ADD FOREIGN KEY） |
| MySQL 协议兼容 | 通过 go-mysql 库实现 MySQL 客户端协议，可使用 MySQL 客户端和驱动直接连接 |
| Prometheus 监控 | 内置全面的性能指标采集，支持 Prometheus 拉取 |

### 1.2 技术栈

- **语言**: Go 1.25
- **SQL 解析**: 借用 PingCAP/TiDB 的 SQL Parser
- **网络协议**: go-mysql-org/go-mysql（MySQL 协议兼容）
- **压缩**: Snappy（可选，用于 B+ 树页缓存）
- **监控**: Prometheus client_golang

### 1.3 运行参数

```
minidb --port 3307 --data ./test/testdb --metrics-port 2112
```

| 参数 | 默认值 | 说明 |
|------|--------|------|
| --port | 3307 | MySQL 协议监听端口 |
| --data | ./test/testdb | 数据存储目录 |
| --metrics-port | 2112 | Prometheus 监控指标端口 |

---

## 2. 系统架构

### 2.1 整体架构图

```
┌─────────────────────────────────────────────────────────────────┐
│                      MySQL Client                               │
│                   (mysql/cli/jdbc等)                             │
└──────────────────────────┬──────────────────────────────────────┘
                           │ MySQL Protocol
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Protocol Layer (protocol/)                    │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  SvrHandler: 连接管理、SQL分发、事务自动管理、结果转换   │    │
│  └─────────────────────────────────────────────────────────┘    │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                    SQL Layer (sql/)                              │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌───────────────┐   │
│  │  Parser   │→│ Executor │→│ Optimizer│  │  Expression   │   │
│  │ (TiDB)    │  │          │  │          │  │  Evaluator    │   │
│  └──────────┘  └──────────┘  └──────────┘  └───────────────┘   │
└──────────────────────────┬──────────────────────────────────────┘
                           │
              ┌────────────┼────────────┐
              ▼            ▼            ▼
┌──────────────────┐ ┌──────────┐ ┌──────────────────┐
│  Catalog Layer   │ │  Txn     │ │  WAL Layer        │
│  (catalog/)      │ │ Manager  │ │  (wal/)           │
│                  │ │ (txn/)   │ │                    │
│ • 数据库元数据   │ │ • 事务   │ │ • 预写日志         │
│ • 表定义         │ │   生命周期│ │ • 异步写入         │
│ • 索引定义       │ │ • 工作区 │ │ • 崩溃恢复         │
│ • 自增序列       │ │ • 行锁   │ │                    │
└──────────────────┘ └────┬─────┘ └────────┬───────────┘
                          │                │
                          ▼                ▼
              ┌───────────────────────────────────────┐
              │        Storage Engine (storage/)       │
              │                                        │
              │  ┌─────────┐  ┌──────────┐  ┌──────┐  │
              │  │ MVCC    │  │ RowCodec  │  │ GC   │  │
              │  │ Version │  │ 编解码    │  │ 回收 │  │
              │  │ Chain   │  │          │  │      │  │
              │  └─────────┘  └──────────┘  └──────┘  │
              └───────────────────┬───────────────────┘
                                  │
                                  ▼
              ┌───────────────────────────────────────┐
              │         B+ Tree Layer (bptree/)        │
              │                                        │
              │  ┌─────────┐ ┌──────┐ ┌────────────┐  │
              │  │Persist  │ │ LRU  │ │   Pager    │  │
              │  │BPTree   │ │Cache │ │  (磁盘IO)  │  │
              │  └─────────┘ └──────┘ └────────────┘  │
              │  ┌─────────┐ ┌──────┐ ┌────────────┐  │
              │  │ Bloom   │ │Latch │ │ Compression│  │
              │  │ Filter  │ │ Lock │ │  (Snappy)  │  │
              │  └─────────┘ └──────┘ └────────────┘  │
              └───────────────────────────────────────┘
                                  │
                                  ▼
              ┌───────────────────────────────────────┐
              │            Disk (文件系统)              │
              │                                        │
              │  *.db (B+树文件)  wal.log  __timestamp.bin  │
              └───────────────────────────────────────┘
```

### 2.2 模块依赖关系

```
main.go
  ├── protocol/    (MySQL 协议层)
  │     ├── sql/         (SQL 执行层)
  │     ├── catalog/     (元数据管理)
  │     ├── txn/         (事务管理)
  │     │     ├── storage/   (存储引擎)
  │     │     │     └── bptree/  (B+ 树)
  │     │     └── wal/       (预写日志)
  │     ├── storage/
  │     └── wal/
  ├── storage/
  ├── wal/
  ├── catalog/
  ├── txn/
  └── metrics/     (监控指标，被所有模块引用)
```

### 2.3 包结构说明

| 包 | 路径 | 职责 |
|---|------|------|
| bptree | bptree/ | B+ 树实现，包括持久化、LRU 缓存、Bloom 过滤器、压缩、并发控制 |
| catalog | catalog/ | 数据库/表/索引的元数据管理，持久化到独立的 B+ 树文件 |
| protocol | protocol/ | MySQL 协议处理，SQL 请求分发，连接和会话管理 |
| sql | sql/ | SQL 解析（TiDB Parser）和执行，包括 DDL/DML/事务语句处理 |
| storage | storage/ | 存储引擎，MVCC 版本管理，行编解码，垃圾回收 |
| txn | txn/ | 事务管理器，时间戳 Oracle，工作空间，行级写锁 |
| wal | wal/ | 预写日志，异步写入，崩溃恢复 |
| metrics | metrics/ | Prometheus 监控指标定义 |

---

## 3. 启动与关闭流程

### 3.1 启动流程

```
main()
  │
  ├─ 1. 解析命令行参数（端口、数据目录、监控端口）
  ├─ 2. 创建数据目录（os.MkdirAll）
  ├─ 3. 打开 WAL（wal.Open）
  │     └─ 扫描现有 wal.log 恢复最高时间戳
  │     └─ 启动异步写入协程 writeLoop
  ├─ 4. 打开存储引擎（storage.OpenEngine）
  │     └─ 扫描数据目录，打开所有 .db 文件对应的 B+ 树
  ├─ 5. WAL 恢复（engine.RecoverFromWAL）
  │     └─ 读取所有 WAL 记录
  │     └─ 识别已提交事务（有 RecCommit 标记）
  │     └─ 重放已提交事务的操作到 B+ 树
  ├─ 6. 启动后 GC（engine.RunFullGC）
  │     └─ 清理历史遗留的 MVCC 旧版本
  ├─ 7. 打开 Catalog（catalog.Open）
  │     └─ 打开 __catalog_dbs.db、__catalog_tables.db、__catalog_autoinc.db
  │     └─ 加载表定义到内存缓存
  ├─ 8. 创建时间戳 Oracle（txn.OpenTimestampOracle）
  │     └─ 从 __timestamp.bin 恢复时间戳计数器
  ├─ 9. 创建事务管理器（txn.NewManager）
  ├─ 10. 启动 Prometheus 指标服务
  └─ 11. 启动 MySQL 协议服务器（protocol.NewServer）
```

### 3.2 关闭流程

```
signal (SIGINT/SIGTERM)
  │
  ├─ 1. svr.Close()        关闭网络服务器，断开所有连接
  ├─ 2. cat.Close()         刷新 Catalog 的 B+ 树到磁盘
  ├─ 3. engine.Close()      刷新所有数据 B+ 树到磁盘
  │     └─ 逐个 tree: cache.Flush() → pager.WriteHeader() → pager.Close()
  └─ 4. w.Truncate()        清空 WAL 文件（数据已持久化，不再需要恢复）
```

---

## 4. 数据目录布局

```
data_dir/
├── wal.log                          # WAL 预写日志文件
├── __timestamp.bin                  # 时间戳 Oracle 持久化
├── __catalog_dbs.db                 # 数据库目录（B+ 树）
├── __catalog_tables.db              # 表定义目录（B+ 树）
├── __catalog_autoinc.db             # 自增序列（B+ 树）
├── mydb__users.db                   # 用户表数据（B+ 树，MVCC 版本链）
├── mydb__users__idx__email.db       # email 索引（B+ 树，原始 KV）
├── mydb__orders.db                  # 订单表数据
└── mydb__orders__idx__user_id.db    # user_id 索引
```

**命名规则：**
- 表数据文件：`{db}__{table}.db`
- 索引文件：`{db}__{table}__idx__{index}.db`
- 系统文件：`__catalog_*.db`、`__timestamp.bin`、`wal.log`

---

## 5. 关键数据流

### 5.1 查询执行流（SELECT）

```
SQL 文本 → TiDB Parser → AST
  → convertStmt() → SelectStmt
  → execSelect()
    → execSelectSimple() 或 execSelectJoin()
      → 优化路径选择：
        │
        ├─ 路径1: PK IN 优化（tryINOnPK）
        │   WHERE id IN (1,2,3) → 多次 txn.Get() 点查
        │
        ├─ 路径2: 索引扫描（tryIndexScan）
        │   WHERE indexed_col = ? → 索引树查 PK → 数据树查行
        │
        ├─ 路径3: 主键范围扫描（extractPKRange）
        │   WHERE pk BETWEEN a AND b → engine.ScanRange()
        │
        └─ 路径4: 全表扫描
            → engine.ScanRange(start, end)
```

### 5.2 写入执行流（INSERT）

```
SQL 文本 → TiDB Parser → AST → InsertStmt
  → execDML() → 自动 BEGIN（如果不在事务中）
  → execInsert()
    ├─ 处理列映射（显式/隐式列）
    ├─ 生成自增值（catalog.NextAutoInc）
    ├─ 类型强转（CoerceValue）
    ├─ 编码主键（EncodePrimaryKey）
    ├─ 编码行数据（EncodeRow）
    ├─ txn.Insert(treeKey, pk, rowData)
    │   └─ 写入 Workspace（缓冲区）
    ├─ 维护二级索引（InsertRaw → 索引 B+ 树）
    └─ execDML 自动 COMMIT
        └─ txn.Commit()
            ├─ 获取行级写锁（sorted，防死锁）
            ├─ 分配 commitTS
            ├─ 写入 WAL
            ├─ 准备 B+ 树批次（PrepareInsertRow）
            ├─ 应用批次（ApplyBatch）
            ├─ 写入 Commit 记录到 WAL
            ├─ 释放行锁
            └─ 触发 GC（每100次提交）
```

### 5.3 事务提交流

```
txn.Commit()
  │
  ├─ 1. 检查 finalized 标志
  ├─ 2. 从 activeTxns 中移除
  ├─ 3. 快速路径：只读事务直接返回
  │
  ├─ 4. 快照写入集（writeSet, writeKeys, inserted）
  ├─ 5. 获取行级写锁（按 key 排序，防止死锁）
  ├─ 6. 分配 commitTS（ts.Next()）
  │
  ├─ 7. 遍历 writeSet：
  │     ├─ 构建 WAL 记录 → wal.Append()
  │     └─ 构建 B+ 树批次 → Prepare*Row()
  │
  ├─ 8. 顺序应用所有 B+ 树批次 → ApplyBatch()
  │     ├─ tree.BatchInsert() → 写入 B+ 树
  │     ├─ 更新版本缓存（verCache）
  │     └─ 标记脏 PK（markDirty，供 GC 使用）
  │
  ├─ 9. 写入 Commit WAL 记录
  ├─ 10. 释放行锁
  └─ 11. 触发 GC（maybeRunGC）
```

---

## 6. 并发模型

### 6.1 锁层次

MiniDB 采用多层锁机制来保证并发安全：

```
层级 1: 连接级
  └─ 每个连接独立的 SQL Executor 和事务状态（protocol.SvrHandler）
  └─ 无共享可变状态

层级 2: 事务级
  └─ 行级写锁（rowLockMgr）→ 同一行的并发写入串行化
  └─ 事务活跃表（activeMu）→ 保护活跃事务映射

层级 3: 存储引擎级
  └─ 树映射锁（sync.RWMutex）→ 保护 trees map 的读写
  └─ 版本缓存（sync.Map）→ 无锁并发的 MVCC 缓存
  └─ 脏 PK 锁（dirtyMu）→ 保护 GC 候选集合

层级 4: B+ 树级
  └─ 全局写锁（writeMu）→ 串行化所有写操作
  └─ 节点级读写锁（pnode.mu）→ 螃蟹式协议实现并发读
  └─ 根 ID 原子操作（sync/atomic）→ 乐观读根节点

层级 5: WAL 级
  └─ WAL 互斥锁（sync.Mutex）→ 保护 WAL 写入
  └─ 缓冲通道（chan bufEntry）→ 异步批量写入
```

### 6.2 读写并发

**读操作（SELECT）：**
- 不获取 writeMu，不阻塞写入
- 使用节点级 RLock 螃蟹式协议遍历 B+ 树
- 通过 MVCC 快照读取一致版本，无需加锁

**写操作（INSERT/UPDATE/DELETE）：**
- 获取 B+ 树 writeMu 串行化写入
- 乐观螃蟹锁：仅锁住可能需要分裂/合并的祖先节点
- 提交时按排序顺序获取行级写锁，避免死锁

### 6.3 事务隔离

```
事务 A (startTS=100)            事务 B (startTS=101)
  │                                │
  ├─ READ(x) → 快照读取           │
  │   └─ Workspace 检查            │
  │   └─ verCache 检查             │
  │   └─ B+ 树 Scan(≤100)         │
  │                                │
  ├─ WRITE(y) → 写入 Workspace     ├─ READ(x) → 快照读取
  │                                │   └─ 看到 startTS≤100 的版本
  │                                │
  ├─ COMMIT()                      ├─ WRITE(x) → 写入 Workspace
  │   ├─ 获取行锁(y)               │
  │   ├─ commitTS = 102            │
  │   ├─ WAL + B+树写入            │
  │   └─ 释放行锁(y)               │
  │                                │
  │                                ├─ COMMIT()
  │                                │   ├─ 获取行锁(x)（不冲突）
  │                                │   ├─ commitTS = 103
  │                                │   └─ 完成
```

---

## 7. 监控体系

MiniDB 通过 Prometheus 暴露了全面的性能指标，分为三类：

### 7.1 直方图指标（耗时分布）

| 指标名 | 说明 |
|--------|------|
| minidb_query_duration_seconds | 查询总耗时 |
| minidb_parse_duration_seconds | SQL 解析耗时 |
| minidb_execute_duration_seconds | 语句执行耗时（按类型标签） |
| minidb_txn_duration_seconds | 事务各阶段耗时 |
| minidb_wal_append_duration_seconds | WAL 追加写入耗时 |
| minidb_bptree_op_duration_seconds | B+ 树操作耗时 |
| minidb_cache_get_or_load_duration_seconds | 缓存加载耗时 |
| minidb_pager_io_duration_seconds | 磁盘 I/O 耗时 |
| minidb_mvcc_get_duration_seconds | MVCC 点查耗时 |
| minidb_mvcc_scan_duration_seconds | MVCC 扫描耗时 |
| minidb_gc_duration_seconds | GC 耗时 |

### 7.2 计数器指标（累计量）

| 指标名 | 说明 |
|--------|------|
| minidb_queries_total | 查询总数（按类型标签） |
| minidb_txn_commits_total | 事务提交总数 |
| minidb_txn_rollbacks_total | 事务回滚总数 |
| minidb_rows_read_total | 行读取总数 |
| minidb_rows_written_total | 行写入总数 |
| minidb_cache_hits_total | 缓存命中数 |
| minidb_cache_misses_total | 缓存未命中数 |
| minidb_pager_reads_total | 磁盘读取次数 |
| minidb_pager_writes_total | 磁盘写入次数 |
| minidb_gc_versions_removed_total | GC 清理版本数 |

### 7.3 仪表盘指标（当前值）

| 指标名 | 说明 |
|--------|------|
| minidb_active_connections | 当前活跃连接数 |
| minidb_cache_size | LRU 缓存当前大小 |
| minidb_active_transactions | 当前活跃事务数 |
