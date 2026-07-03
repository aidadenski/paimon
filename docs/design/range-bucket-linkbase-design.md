# Paimon Sort-Range-Bucket（类 HBase Region）改造技术设计方案 v2
## —— 万亿级 Linkbase 全量表场景

> 基于 Apache Paimon 1.5-SNAPSHOT（本仓库 master）代码级调研产出。
> v2 修订：按评审意见修复 8 项问题（P0：主键 hash 位宽；P1：sequence tie-breaker、$ro 成本、relabel 引用安全矩阵、兼容性声明、前缀边界语义、rowkey grammar、规模估算补项），并将实施拆为两阶段。

---

## 1. 背景与目标

### 1.1 业务场景
爬虫 linkbase 表记录 URL 全生命周期（约 35 个属性列），现存 HBase，rowkey = `salt + 字符级反转(host) + "_" + 128bit-hash(path) 的 hex`（样例：`0000nc.moc.anis.swen.4tnemmoc_00000152a65d713aa1f6710feacba01a`，grammar 待固化，见 §11.0）。4 万亿行 → 规划 10 万亿行，列存压缩后约 1~3 PB。链路：**Flink → Paimon 小时分区 Append 表 → Spark 小时批 → 全量无分区表（Spark 批写批读）**。

### 1.2 痛点与需求
| # | 需求 | 现状问题 |
|---|------|---------|
| R1 | 下游 T+1 全量 scan 高效 | HBase 全表 scan IO 放大严重（KV 行存） |
| R2 | 按 host 取数/更新只扫小范围 | 依赖 rowkey 前缀，湖上哈希桶做不到 |
| R3 | 不能建分区 | 每天可能接近全量更新，分区无意义 |
| R4 | 类 HBase 的横向扩展 | Paimon fixed bucket 只能全表 rescale，4 万亿行下不可行 |
| R5 | 手动运维 split/merge 单个 bucket | Paimon 无此原语（社区亦无 PIP，已核实） |

### 1.3 用户已确认的关键决策
- 交付物：**详细设计文档**（推进为 fork 级严肃改造 PoC 的蓝本）。
- 记录合并由 **Paimon merge-engine 承担**，不在 Spark 里做业务合并。
- split/merge 可在**停写窗口**执行，无需在线 split。
- 规模：压缩后 1~3 PB；每日更新占比 10%~50%。

### 1.4 非目标
- 点查性能；Flink 直写该表（阶段一明确禁止并快速失败）；在线（与写并发的）split/merge；旧版本 Paimon 兼容（见 §5.5）。

---

## 2. 方案结论（TL;DR）

新增 **`BucketMode.KEY_RANGE`**（`bucket = -3`，仿 postpone `-2` 前例）：bucket 由随快照原子演进的 **Range Map（桶区间映射表）** 决定——按主键排序切成 N 个连续、不重叠、覆盖全键空间的区间，等价于 HBase Region。

**实施分两阶段（评审要求，采纳）**：

- **阶段一：证明 range bucket 本体成立**。只做 KEY_RANGE 模式、Range Map、Spark 写路由、host 裁剪读、stale-map 提交校验、manifest 桶排序；**不做 split/merge**，只允许离线 bootstrap 固定 Range Map。验收：host 查询确实只读少数 bucket；全量 scan planning 不炸；10 万 bucket Spark 写入跑通；sequence/aggregation 结果确定可复现。
- **阶段二：split/merge**。以 `externalPath relabel` 的**引用安全矩阵**（§8.4）全部验收通过为合入前提——这是全方案风险最高的点，宁慢勿错。

---

## 3. 现状差距分析（为什么必须改造）

对 Paimon 现有五种 bucket 模式逐一排除（详见 §14），核心差距三点：

1. **路由**：所有模式最终都是 `Math.abs(hash % numBuckets)`（`DefaultBucketFunction`）或每键索引（dynamic/cross-partition；官方文档明确 dynamic bucket 单写者限制 + 每 1 亿 entry 约 1 GB 内存，10^13 键不可行）。无按键区间路由。
2. **读裁剪**：`BucketSelector` 只支持 bucket-key 全字段等值/IN 谓词的哈希反推（`paimon-core/.../operation/BucketSelector.java`），无区间→桶映射；manifest 级裁剪（`ManifestsReader.filterManifestFileMeta:166`）只支持单点 `specifiedBucket`。
3. **扩缩容**：`RescaleProcedure` = 整表/整分区 INSERT OVERWRITE 重写；`ConflictDetection.checkBucketKeepSame` 强制同分区 `totalBuckets` 一致（但 **`totalBuckets <= 0` 跳过校验**——合规通道）。无单桶 split/merge 原语。

两堵已验证的规模墙（100k+ 桶、万亿行下）：
- **Executor 墙**：每桶首次写入触发一次全量 base manifest 扫描（`AbstractFileStoreWrite.createWriterContainer` → `FileSystemWriteRestore.restoreFiles`），且 delta manifest 条目**不按 bucket 聚簇**，`_MIN_BUCKET/_MAX_BUCKET` 区间松弛导致裁剪失效 → 准平方开销。
- **Driver 墙**：`PaimonSparkWriter` collect 全部 CommitMessage；`FileStoreCommitImpl.tryCommitOnce` 单线程写 manifest、每次 commit 重读全量 base manifest list；无分区表冲突检测读全表条目。

---

## 4. 总体架构：`KEY_RANGE` bucket 模式

### 4.1 核心概念与不变量

- **区间键 = trimmed primary key**（`BinaryRow`），用表的 codegen `RecordComparator` 比较——与 `DataFileMeta.minKey/maxKey`、`ConflictDetection.checkKeyRange`、MOR 归并同一比较器，语义闭环。
- **I1（覆盖性）**：Range Map 区间按 startKey 排序，首 startKey = −∞，`entry[i+1].startKey == entry[i].endKey`，末 endKey = +∞；不重叠、无空洞。
- **I2（桶号永不复用）**：map 携带 `nextBucketId`，split/merge 只铸新号弃旧号。任何引用已退休桶号的提交可被确定性拒绝——防陈旧路由的核心机制。
- **I3（epoch）**：map 携带单调递增 `epoch`，每次 bootstrap/split/merge +1。
- **I4（DELETE 先于 ADD）**：同一提交内，同一物理文件的 DELETE 条目必须先于 ADD 条目写入 delta manifest（快照过期的删除取消机制依赖此序，§8.4；桶号单调铸造 + 稳定排序自动保证）。

### 4.2 选项入口（仿 postpone 前例，最小侵入）

- `paimon-common/.../table/BucketMode.java`：新增 `KEY_RANGE` 与 `RANGE_BUCKET = -3`。
- `KeyValueFileStore.bucketMode()`（`:91-101`）：`case -3 → KEY_RANGE`。
- `SchemaValidation:681`：新增 `isRangeBucketTable` 豁免（同 `isPostponeBucketTable:714`）；校验：必须主键表、不得设 `bucket-key`。
- manifest 条目写 **`_TOTAL_BUCKETS = -3`**：`checkBucketKeepSame` 因 `totalBuckets <= 0` 跳过；`AbstractFileStoreWrite.scanExistingFileMetas` 因 restored(-3)==configured(-3) 通过——无需改冲突检测（已核实）。
- 稀疏桶号安全（已核实）：`SnapshotReaderImpl.generateSplits` 按实际桶号分组；`withShard` 的 `bucket % n` 对稀疏号仍正确；`FileStorePathFactory.bucketPath` 接受任意非负 int。

### 4.3 Linkbase 表 DDL（v2 修订）

**主键（P0 修复）**：10¹³ 行下 64-bit hash 期望碰撞 `n²/(2·2⁶⁴) ≈ 271 万次`——不可接受。改用 **128-bit**：`n²/(2·2¹²⁸) ≈ 1.5×10⁻¹³`，可忽略。且样例 rowkey 的 hash 段为 32 hex 字符（128-bit），**现有 HBase 即 128-bit**，沿用即可。

```sql
CREATE TABLE linkbase (
  host_rev       STRING,      -- 规范化反转 host（grammar 见 §11.0）
  path_hash_128  BINARY(16),  -- 128-bit path hash（与现有 HBase hash 对齐）
  url STRING, host STRING, domain STRING, http_code INT,
  first_found_time BIGINT, last_found_time BIGINT, last_download_time BIGINT,
  -- ... 其余 ~30 个属性列
  event_time  BIGINT,          -- 源侧事件时间
  ingest_time BIGINT,          -- 入湖时间（小时批注入）
  source_seq  BIGINT,          -- 源侧单调序号（Kafka offset / binlog offset / HBase ts）
  PRIMARY KEY (host_rev, path_hash_128) NOT ENFORCED
) TBLPROPERTIES (
  'bucket' = '-3',
  'merge-engine' = 'aggregation',
  'fields.default-aggregate-function' = 'last_non_null_value',
  'fields.first_found_time.aggregate-function' = 'min',
  'fields.last_found_time.aggregate-function'  = 'max',
  'fields.last_download_time.aggregate-function' = 'max',
  'sequence.field' = 'event_time,ingest_time,source_seq',   -- 复合 sequence，见下
  'file.format' = 'parquet', 'file.compression' = 'zstd',
  'target-file-size' = '256 MB',
  'metadata.stats-mode' = 'none',
  'manifest.target-file-size' = '64 MB',
  'manifest.full-compaction-threshold-size' = '10 GB',
  'manifest.merge-min-count' = '1000000',
  'write-only' = 'true',
  'num-sorted-run.compaction-trigger' = '10',
  'snapshot.time-retained' = '3d', 'snapshot.num-retained.min' = '10'
)
```

- **碰撞终极兜底（可选）**：若业务要求绝对零碰撞风险，PK 追加第三列 `normalized_url`：`PRIMARY KEY (host_rev, path_hash_128, normalized_url)`。代价：trimmed PK 进入每个文件的 `minKey/maxKey`（manifest 条目增大 ~2×URL 均长），并增大排序/比较开销。host 裁剪只依赖第一列，不受影响。**默认推荐两列 PK**，128-bit 已足够；此项作为开关记录在案。
- **`sequence.field` 复合（P1 修复）**：Paimon 语义为 sequence 最大者胜，**相同则回退输入顺序**——批写 shuffle 下不确定。爬虫场景同 URL 同毫秒多条更新并不罕见，单一 `event_time` 不够。复合 `event_time,ingest_time,source_seq` 中 `source_seq` 必须是能稳定打破平局的源侧单调量（Kafka offset / binlog offset / HBase timestamp）；若上游暂无此字段，由小时批注入 `monotonically_increasing_id` 级别的批内序号作过渡，并在验收中加入"同 key 多更新乱序重放，结果逐字节一致"的确定性测试。
- `metadata.stats-mode=none` 砍每列 valueStats，`minKey/maxKey` 独立字段保留——区间机制所需元数据仍在。
- 若引入 DELETE 行（MERGE INTO 删除），aggregation 引擎需配 `fields.*.ignore-retract=true`。

---

## 5. Range Map 元数据设计

### 5.1 持久化选型：index manifest 新 indexType（推荐）

| 候选 | 结论 |
|------|------|
| Snapshot JSON 新字段 | 否：10 万桶时 map 6~20 MB，快照被高频读/缓存，膨胀不可接受 |
| Schema option | 否：split/merge 刷 schema 版本，且与数据变更非同快照原子 |
| **index manifest 新 indexType `RANGE_BUCKET_MAP`** | **是**：与数据同快照原子提交（`snapshot.indexManifest`）；每快照引用自己的 map 版本 → time-travel 天然正确；替换语义现成——`IndexManifestFileHandler.getIndexManifestFileCombine:96` 将非 HASH/DV 类型路由到 `GlobalIndexCombiner`（按文件名合并、先 DELETE 后 ADD），零改动获得原子替换；快照过期/孤儿清理对 index 文件已有引用管理 |

提交形态：一条 `IndexManifestEntry(ADD, partition=EMPTY_ROW, bucket=0, IndexFileMeta(indexType="RANGE_BUCKET_MAP", ...))` 指向 index 目录下的边界文件。

### 5.2 边界文件格式（Avro + zstd，单文件）

```
Header: version INT | epoch BIGINT | nextBucketId INT | keySchemaId BIGINT
Entry(按 startKey 排序):
  bucketId INT
  startKey BYTES   -- BinaryRowSerializer 序列化的 trimmed-PK，inclusive；空 = -∞
  endKey   BYTES   -- exclusive；空 = +∞（冗余，用于校验）
  rowCount BIGINT | sizeBytes BIGINT | createdEpoch BIGINT
```

10 万桶约 6~20 MB，加载 <1s，可直接广播。不做 memcmp 编码：比较一律走 codegen `RecordComparator`。

### 5.3 运行时对象与加载

新包 `paimon-core/.../rangebucket/`：`RangeBucketMap`（不可变；`bucketOf` 二分、`bucketsInRange`、`validate()`）、`RangeBucketMapFile/Loader/Serializer`。从 `snapshot.indexManifest` 按 indexType 扫出**恰好一条**（0 条 = 未 bootstrap，快速失败）；按不可变文件名做缓存键。写端 job 启动加载一次；读端在 plan 时按被扫描快照加载。

### 5.4 Bootstrap：`CALL sys.init_range_buckets(...)`

- **迁移首选：导入 HBase region 边界**（前提：§11.0 rowkey grammar 固化并通过测试）。
- 备选：采样法——复用 Flink `RangeShuffle.allocateRangeBaseSize` / Spark `repartitionByRange` 的加权分位数思路。
- 产出 epoch=1 首版 map；无 map 时写端快速失败。

### 5.5 兼容性策略（P1 修复：明确为新 table feature，不承诺旧版本兼容）

评审意见采纳。"老版本读端跳过未知 indexType" 只是 index manifest 这一个持久化点的**工程实现细节**，**不构成兼容性结论**：旧版本不认识 `bucket=-3`/`KEY_RANGE`/新路由与裁剪逻辑。正式策略写死为：

```
KEY_RANGE 表是新 table feature，不保证旧版本 Paimon 读写兼容。
所有 Spark / Flink / Java API reader & writer 必须升级到支持 range bucket 的版本。
旧版本访问必须快速失败，而非降级读取。
```

落地机制：借用/对齐社区 **table feature/版本门禁** 惯例——在表 options 写入特性标记，并确保旧版本打开该表时在 `KeyValueFileStore.bucketMode()`（旧代码对 `-3` 落入 `HASH_FIXED` 分支前）就被拦截。具体做法：`SchemaValidation` 对 `bucket < -2` 在旧版本本就抛 "The number of buckets needs to be greater than 0"（`:681-682`，已核实旧代码行为）——**旧版本天然快速失败**，这是选 `-3` sentinel 的额外好处；仍需在设计文档与发布说明中显式声明，并对 fork 内所有引擎入口（含 Flink sink/source builder）加显式 `UnsupportedOperationException`。

---

## 6. 写入路径（Spark 批）

### 6.1 路由接线

- **core**：`RangeBucketRowKeyExtractor extends RowKeyExtractor`，`bucket() = map.bucketOf(trimmedPrimaryKey())`；接入 `AbstractFileStoreTable.createRowKeyExtractor():258-272`。
- **Spark**：`PaimonSparkWriter.write` 的 `bucketMode match`（`PaimonSparkWriter.scala:~205`）新增 `case KEY_RANGE`：driver 加载 map → 广播边界 → 新 `RangeBucketProcessor`（`BucketProcessor.scala`）每 task 反序列化一次、逐行二分写 `_bucket_` 列 → 复用 `repartitionByPartitionsAndBucket` → `writeWithBucket`，下游零改动。
- **MERGE INTO / UPDATE / DELETE 免费获得**：三者在 PK 表上均为带 RowKind 的 upsert 写，全部汇入 `PaimonSparkWriter.write`。

### 6.2 陈旧 map 防护协议

停写窗口是运维承诺，安全性双保险：
1. **运维锁（可用性层）**：平台串行化 {小时批、compaction、split/merge}；procedure 内部沿用 `RescaleProcedure` 模式设 `COMMIT_STRICT_MODE_LAST_SAFE_SNAPSHOT`，窗口内混入提交则 `StrictModeChecker` 失败。
2. **提交校验（安全层）**：`FileStoreCommitImpl` 对 KEY_RANGE 表 APPEND 提交前校验所有 ADD 条目桶号存在于最新 map（I2 ⇒ 旧 map 路由必然引用退休桶号），抛可重试 `StaleRangeMapException`。提交属性记录路由 epoch。

### 6.3 Writer restore 优化（拆 executor 墙）

新 `BatchedWriteRestore implements WriteRestore`（接口现成，`FileStoreWrite.withWriteRestore`）：task 首次 restore 按本 task 桶号集合做**一次** `withBucketFilter` 扫描并 memoize。叠加 §7.2 后每 task 只读自己桶号区段的 manifest 切片。小时批 `write-only=true`。

### 6.4 Commit 路径（拆 driver 墙）

- 写批与 compaction 串行调度 → 常态无冲突重读。
- 内联 manifest 合并全关，夜间 `CALL sys.compact_manifest`。
- `spark.driver.maxResultSize=4g`、driver 64 GB。CommitMessage 体量兜底：按桶号区段切 K 次顺序提交（K 可配，默认 1；中间快照"一致但部分更新"，需下游确认，见 §13-e）。

---

## 7. 读取路径

### 7.1 桶裁剪：`RangeBucketSelectConverter`

为 KEY_RANGE 新增 snapshot-aware 转换器（在 `FileStoreScan.plan()` 内、快照锁定后执行）：
- 从下推谓词提取 **PK 前缀区间**：首列 `host_rev` 的 `Equal`/`In`/大小比较/`Between`/`StartsWith`；And/Or 做区间交/并（并集过大放弃裁剪退化全扫，不影响正确性）。部分前缀键用**前缀比较器**（只比前 k 字段）。
- 区间 → 桶号集合（`bucketsInRange`）→ `FileStoreScan.withBucketFilter` + §7.2 manifest 区间裁剪。
- 纯加速：文件级 `minKey/maxKey` 裁剪与 Parquet 谓词下推继续兜底正确性。

**前缀边界语义（P1 修复，写成可测试规范）**：

```
输入规范化（写读两侧同一实现，单一工具类 HostKeyCodec，禁止内联复制）:
  host_norm = punycode(lowercase(trim_trailing_dot(host)))     -- IDN 统一转 punycode ASCII
  host_rev  = reverse_utf8_bytes(host_norm)                    -- 与 HBase 现行一致（字符级反转，§11.0 确认）
前缀上界（prefix successor，对 UTF-8 bytes）:
  lower = p
  upper = successor(p) := 将 p 末尾所有 0xFF 字节剥离后，最后一个非 0xFF 字节 +1；
          若 p 全为 0xFF → upper = +∞
  （即"大于所有以 p 为前缀的字节串的最小字节串"；禁止 naive 的 p+0x01 拼接）
必测边角（property-based, HostKeyCodecTest）:
  精确 host vs 子域前缀（"nc.moc.anis" / "nc.moc.anis." / "nc.moc.anisx" 三者区分）
  域名边界必须带点：整域取数一律用 "reverse(domain) + '.'" 作前缀，避免 anisx 误入
  IDN/punycode、大小写、尾部点、非法 host（快速失败）、空串、全 0xFF
```

### 7.2 Manifest 桶排序 + 区间裁剪（三件套，`paimon-core`）

1. **写时排序**：`FileStoreCommitImpl` 在 `manifestFile.write(deltaFiles)` 前按 `(bucket, level)` 稳定排序（桶号单调铸造 + 稳定排序自动保持 I4）。
2. **合并时保序**：`ManifestFileMerger` 对 KEY_RANGE 表按 `(bucket, minKey)` 归并输出 → 夜间 compact 后 `_MIN_BUCKET/_MAX_BUCKET` 紧致。
3. **区间过滤**：`ManifestsReader` 新增 `withBucketSetFilter(NavigableSet<Integer>)`，`filterManifestFileMeta:166` 做区间相交测试。

效果：host 取数从读全量 2~6 GB manifest 降为 1~2 个 64 MB manifest。

### 7.3 查询/消费形态（v2 修订：$ro 降级为可选优化）

- **Host 取数**：`WHERE host_rev = ...` → 1 桶 → 1~2 manifest → 文件级再裁。整域名：`host_rev >= p AND host_rev < successor(p)`。
- **Host 更新**：Spark `MERGE INTO`，扫描侧同享裁剪。
- **T+1 全量 scan（P1 修复）**：**默认按 MOR 路径设计并压测**（每桶一个 task；官方文档明确单 LSM 单线程读，16~32 GB 桶尺寸的单 task 耗时必须实测）。`$ro`（`ReadOptimizedTable`）**只作为"下游可接受读到最近一次 full compaction 状态"时的优化路径**：若 T+1 必须读到昨日全部小时批的最新状态，`$ro` 需要对**所有被触达的桶**做 full compaction——10%~50% 日更若均匀散布，几乎所有桶每天都被触达，full compaction 写放大可能接近每日全表重写（L0 key range 宽 → 与高层大范围重叠）。**结论写死：未经实测证明 full compaction 写放大可接受前，不得把 $ro 作为 T+1 默认方案**。压测矩阵见 §10.2。

---

## 8. SPLIT / MERGE procedure（阶段二）

### 8.1 `CALL sys.split_bucket(table, bucket => 123, split_key => NULL, max_rewrite_bytes => '2g')`

1. 锁定最新快照 S、加载 map（epoch E）、校验桶存在。
2. `FileStoreScan.withBucket(123)` 取该桶全部条目。
3. **选分裂键（纯元数据）**：最高层 L_max 文件沿累计 fileSize 走到 50%，取该文件 `minKey`——L_max（字节大头）零跨界。
4. 铸 left/right = nextBucketId, +1；epoch+1。
5. 划分：`maxKey < splitKey` → left；`minKey >= splitKey` → right；跨界文件（L0 与少量中间层）逐文件单遍重写（同层、保留每行 sequence、纯复制）。
6. **非跨界文件零拷贝 relabel**：克隆 `DataFileMeta`（前例 `PojoDataFileMeta.upgrade`），设 `_EXTERNAL_PATH` 指向原物理位置，以新桶号发条目。选 externalPath 而非 rename：与提交同原子、对象存储友好、后续 compaction 自然重写进真目录、逐步排干间接引用。
7. **一次原子提交**（COMPACT kind + strict-mode）：DELETE 旧条目 + ADD relabel/重写条目 + map 替换（DELETE(map@E)/ADD(map@E+1)）。`checkKeyRange` 构造性通过。
8. **L0 约束**：跨界 L0 超阈值则中止，提示先 `CALL sys.compact(..., buckets => '123')`。

成本：32 GB 桶典型 = ~31.5 GB 纯元数据 relabel + ≤1~2 GB 跨界重写。幂等/恢复：提交前只产生未引用文件（孤儿兜底）；提交要么落地（epoch 前进，重跑 no-op）要么无事发生。Sequence：键集不相交 ⇒ 永不跨谱系比较。

### 8.2 `CALL sys.merge_buckets(table, left => 45, right => 67)`

1. 校验**键相邻**（adjacent-only，Hudi RFC-42 同款不变量）。
2. 铸 merged 新号，epoch+1。
3. **纯元数据** relabel 两桶全部条目到 merged：相邻区间键不相交 ⇒ level≥1 拼接仍是合法 sorted run（`checkKeyRange`/`SortedRun.validate` 通过）；L0 本就允许重叠；writer 序号自动从 max(两侧) 续种。
4. 一次原子提交。可选 `rewrite => true` 链一次桶 compaction。

### 8.3 已核实的引用安全事实

- ✅ 快照过期：`FileDeletionBase.getDataFileToDelete:221` 按**物理 Path**（`toPath` 优先 externalPath）聚合，同快照 ADD 同路径把 DELETE 移出删除集（代码注释明言为 level upgrade 而设）；依赖 I4。
- ✅ Rollback：`RollbackHelper` 只删快照指针，数据文件交孤儿清理。
- ✅ 孤儿清理：`OrphanFilesClean` 按**文件名**（全局唯一）匹配引用集。
- ⚠️ Tag/邻接快照 skipper：`FileDeletionBase.containsDataFile:415` 按 `(partition, bucket, fileName)` 匹配——pre/post-split 快照对同一物理文件互不相认，删 pre-split 的 tag 会误删活文件。**修复**：skipper 跳过集合改按物理 Path 或文件名构建。

### 8.4 引用安全矩阵（P1 修复：阶段二合入的强制验收门禁）

评审意见采纳：**不能只修 tag 一处就认为安全**。建立强制 checklist，每一项都必须给出"引用判断按物理 path 或全局唯一文件名"的代码级证明 + 专项 IT（构造 relabel 后快照，执行该操作，断言物理文件存活性与读取正确性）：

| # | 路径 | 待证明点 |
|---|------|---------|
| 1 | `expire_snapshots`（SnapshotDeletion） | DELETE/ADD 同路径取消机制在跨桶 relabel 下成立（I4 序） |
| 2 | `remove_orphan_files`（含分布式版） | 引用集按文件名；externalPath 目录被列举覆盖 |
| 3 | tag deletion（TagDeletion + dataFileSkipper） | §8.3 修复后按物理 path/文件名 |
| 4 | branch deletion / fast-forward / merge_branch | 分支清理与引用计算对 externalPath 条目正确 |
| 5 | rollback（RollbackHelper / CommitRollback） | 指针删除语义不误删；败者快照回滚不删被 relabel 引用的文件 |
| 6 | compact / full compact（MergeTree 读写） | 读端按条目 externalPath 打开文件；compact 输出落新桶目录并排干间接引用 |
| 7 | `$ro` ReadOptimizedTable | level 过滤 + externalPath 读取正确 |
| 8 | `rewrite_file_index` | 索引重写按条目路径寻址 |
| 9 | `compact_manifest`（ManifestFileMerger） | 合并保留 externalPath 字段无损 |
| 10 | 增量消费 / incremental scan（DELTA 读） | split 快照的 DELETE+ADD 不被误读为数据变更两倍量（语义标注或消费侧说明） |
| 11 | conflict detection（ConflictDetection 全套） | Identifier 含 externalPath 时 merge/dedup 语义正确 |
| 12 | metrics / 系统表（`$files`、`$buckets`、`$range_buckets` 等） | 路径展示与统计对 externalPath 正确 |
| 13 | `expire_partitions` / `purge_files` / `remove_unexisting_files` | 同上寻址正确性 |
| 14 | DataSplit 序列化（v8+）/ Spark reader | externalPath 随 split 传输，executor 端寻址正确 |

验收标准：矩阵全绿（代码证明 + IT）之前，**split/merge procedure 不得合入主干**；阶段一期间以固定 Range Map 运行，不产生任何 relabel 条目，天然不触发上述风险。

---

## 9. Compaction 与日常运维链路

- 写侧 `write-only`，**独立 Spark compaction 作业**。`CompactProcedure` 本就按 `(partition, bucket)` 并行，新增 `buckets => '0-4096,8192'` 参数 → `withBucketFilter`，支持分桶组滚动 compaction（错峰、避开小时批提交）。
- 节奏：夜间按组 minor compaction（trigger=10）把 MOR 读维持在 ~5 run 以内；full compaction 只按需（`$ro` 路径被实测批准后才纳入日常）。
- 夜间 `CALL sys.compact_manifest` 执行 manifest 合并 + 桶排序重建。
- 无 changelog producer。
- **运维闭环**：新系统表 `linkbase$range_buckets`（`RangeBucketsTable`，仿 `BucketsTable`/`FileKeyRangesTable`）：`bucket_id, start_key, end_key, epoch, created_epoch, row_count, size_bytes, file_count, last_modified`。巡检 `size > 2×target → split`、`size < 0.25×target 且相邻 → merge`（阶段二），停写窗口执行：锁 → split/merge → compact 受影响桶 → 解锁。
- 监控：提交 delta manifest 体量、planning 耗时、桶尺寸 p99/max、L0 run 数与 backlog、StaleRangeMapException、epoch 审计、compaction lag、**每日 L0 文件净增与 compaction 读写字节（§10.2 两项硬指标）**。

---

## 10. 规模估算与压测验收（v2 补强）

### 10.1 静态估算（假设显式声明）

假设：压缩后 250~300 B/行；`target-file-size=256MB`；stats-mode=none 下每条 ManifestEntry ≈300 B；LSM 5 层。**桶目标 16 GB（1 PB 期）→ 32 GB（3 PB 期）**，split 阈值 2×、merge 阈值 0.25×：

| 指标 | 1 PB @16 GB | 3 PB @32 GB |
|---|---|---|
| 桶数 | ~65,536 | ~98,304 |
| 每桶文件数（高层 + ≤24 日 L0） | ~110 | ~190 |
| manifest 总条目 / 字节 | ~7.2 M / ~2.2 GB | ~18.7 M / ~5.6 GB |
| 全量 scan planning（32 并行） | ~1 min | ~2–3 min |
| planning driver 堆 | ~8 GB | ~20 GB → 64 GB driver |
| 小时批 delta manifest | ≤65K 条 ≈20 MB | ≤98K 条 ≈30 MB |
| CommitMessage 回传 | ~65 MB | ~100 MB |
| host 取数 planning | 1~2 × 64 MB manifest | 同左 |

- 不选 1 GB 桶：1~3 M 桶 ⇒ 10^8 manifest 条目、10^6 写分区/scan task，三面撞墙。不选 128 GB：运维粒度粗、单 task MOR 长尾。
- DV 在 v1 不用：lookup 式 compaction 在 10~50% 日更下读代价逼近全量重写。

### 10.2 硬指标补项与压测矩阵（P1 修复）

评审意见采纳，两项**必须进压测**的硬指标：

```
每日新增 L0 文件数 = touched_buckets × 24（小时批）× files/bucket/batch
  ≈ 100,000 × 24 × 1 = 240 万 L0 文件/天（几乎全桶触达的悲观界）
每日 compaction 读写字节 = minor + full + manifest compact 三项分别核算
```

压测必须覆盖（1/100 缩放 + 全量彩排两级）：
1. 240 万 L0/天 节奏下：HDFS NameNode（或对象存储 LIST）压力、manifest 增长与 compact_manifest 耗时、compaction backlog 是否收敛（净增 = 写入 − 合并 必须 ≤ 0 于稳态）。
2. **MOR T+1 全量 scan 实测**：16 GB 与 32 GB 桶各测单 task 耗时/内存，确定桶尺寸终值。
3. **full compaction 写放大实测**（决定 `$ro` 去留）：10% 与 50% 日更、更新按 host 局部 vs 均匀散布两种分布，测"被触达桶 full compaction"的日重写字节。
4. 10 万分区 Spark shuffle 的 AQE 行为与小时批端到端时延。
5. sequence 确定性：同 key 多更新乱序重放 N 次，聚合结果逐字节一致。

---

## 11. 迁移方案（HBase → Paimon）

### 11.0 前置：rowkey grammar 固化（P1 修复，迁移的第一个交付物）

样例 `0000nc.moc.anis.swen.4tnemmoc_00000152a65d713aa1f6710feacba01a` 表明 rowkey **不只是** `reversed_host + "_" + hash(path)`：存在 `0000` 前缀（salt/shard？）、host 为**字符级**反转、hash 段 32 hex（128-bit）且自带前导零填充。导入 region 边界前必须固化 grammar 并落成可测试规范：

```
salt_len        = ?   (样例前 4 字符 "0000" 的语义：固定 salt？分片号？)
host_rev_start  = salt_len
separator       = '_'
hash_encoding   = hex (lowercase?)
hash_bits       = 128 (32 hex chars)
反转粒度        = 字符级 reverse（非按 label 反转）——与 §7.1 HostKeyCodec 对齐
```

产出 `RowkeyGrammar` 解析器 + 针对生产 rowkey 抽样 10⁶ 条的往返测试（parse → rebuild == 原串）。**若 salt 是分片号**：同一 host 会散在多个 salt 空间，region 边界与 host 顺序不一致——此时初始边界不能直接照搬 region split key，需按去 salt 后的 `(host_rev, path_hash_128)` 重新采样（§5.4 备选路径升为首选）。此问题必须在 bootstrap 前澄清。

### 11.1 步骤

1. **边界导入**：grammar 验证通过后，`init_range_buckets` 解析 region split keys（或采样法）→ epoch=1 map（region ~10 GB 聚合至 16~32 GB）。
2. **存量导入**：Spark 读 HBase snapshot/HFile → 按广播 map repartition → 分区内按 PK 排序 → INSERT 写入 + 分桶组 full compaction 落高层。
3. **增量链路**：小时批读 Append 表当期分区 → 注入 `ingest_time/source_seq` →（可选批内预去重）→ upsert 写入。
4. **双跑与切换**：HBase 继续在线服务，T+1 下游切 Paimon 验证 N 周期后下线 HBase 离线读。

---

## 12. 实施路线图（两阶段，本仓库类级改动）

### 阶段一：range bucket 本体（M1–M4，不含 split/merge）

| 里程碑 | 内容 | 关键文件 | 规模 |
|---|---|---|---|
| **M1 核心模式+元数据** | `KEY_RANGE`/`-3`；schema 校验豁免；`rangebucket/` 包；`RangeBucketRowKeyExtractor`；提交期陈旧桶号校验；Flink 入口显式拒绝 | `BucketMode.java`、`KeyValueFileStore.java`、`SchemaValidation.java`、`AbstractFileStoreTable.java`、`FileStoreCommitImpl.java`、Flink sink builder | ~2.5 kLOC |
| **M2 Spark 读写** | `PaimonSparkWriter` KEY_RANGE 分支；`RangeBucketProcessor`；`RangeBucketSelectConverter`；`HostKeyCodec`（§7.1 规范）；`BatchedWriteRestore` | `PaimonSparkWriter.scala`、`BucketProcessor.scala`、`BucketSelectConverter.java`、`KeyValueFileStoreScan.java` | ~2.2 kLOC |
| **M3 Bootstrap+运维面** | `init_range_buckets`；`RowkeyGrammar` 解析器；`RangeBucketsTable` 系统表；`CompactProcedure` `buckets` 参数 | `paimon-spark/.../procedure/`、`table/system/`、`CompactProcedure.java` | ~1.5 kLOC |
| **M4 Manifest 优化** | 提交排序；`ManifestFileMerger` 按桶归并；`ManifestsReader.withBucketSetFilter` | `FileStoreCommitImpl.java`、`ManifestFileMerger.java`、`ManifestsReader.java` | ~1 kLOC |

**阶段一验收（全部实测）**：host 查询只读命中桶（manifest/文件读取量断言）；10 万桶写入/commit/planning 达 §10 指标；§10.2 压测矩阵 1/2/4/5 通过；sequence 确定性通过。

### 阶段二：split/merge（M5–M6，合入门禁 = §8.4 矩阵全绿）

| 里程碑 | 内容 | 关键文件 | 规模 |
|---|---|---|---|
| **M5 引用安全矩阵** | `containsDataFile` skipper 修复；矩阵 14 项逐项代码证明 + 专项 IT | `FileDeletionBase.java` 等 | ~1 kLOC（含测试为主） |
| **M6 Procedures** | `split_bucket` / `merge_buckets`（仿 `RescaleProcedure`+StrictMode）；跨界文件重写器；崩溃恢复 IT；split 前后 host 查询等价性对拍 | `paimon-spark/.../procedure/`、`rangebucket/RangeSplitFileRewriter` | ~2 kLOC |

**测试策略**：`RangeBucketMapTest`（不变量/二分/序列化往返）；`HostKeyCodecTest`（§7.1 边角，property-based）；`PrimaryKeyFileStoreTableTest` 矩阵加 `bucket=-3`；Spark 端 `PaimonSparkTestBase` 派生套件（写/读/裁剪命中率/MERGE INTO/UPDATE/DELETE/time-travel）；§8.4 矩阵 IT；`paimon-benchmark` 10 万桶基准。

---

## 13. 风险与开放问题

- (a) 前缀区间比较边角（null/多字段/IDN）→ `HostKeyCodec` property-based 测试封死。
- (b) externalPath × 清理/消费路径组合 → §8.4 矩阵门禁（阶段二）。
- (c) 10^5 分区 Spark shuffle AQE 行为 → 压测矩阵项 4。
- (d) Flink 写端 v1 显式抛 `UnsupportedOperationException`。
- (e) K>1 分段提交的下游一致性确认。
- (f) `0000` salt 语义待业务澄清（§11.0）——若为分片号则初始边界改采样法。
- (g) full compaction 写放大实测决定 `$ro` 去留（§10.2 项 3）。
- (h) 若 PK 追加 `normalized_url` 兜底列，manifest 体积与比较开销需重估（§4.3）。

---

## 14. 备选方案对比（为何不采用）

| 方案 | 否决理由 |
|---|---|
| HASH_FIXED 超大桶数 | 无键局部性；rescale 全表重写；无单桶运维粒度 |
| HASH_DYNAMIC / KEY_DYNAMIC | 10^13 键索引不可行（官方文档：~1 GB/亿键 + 单写者） |
| POSTPONE_MODE | 只解决桶数后定，仍是 hash，无键区间、无 split/merge |
| Append 表 + clustering + row tracking | 失去 PK merge-engine（与用户决策相悖）；高日更下 DV/重写式 UPDATE 劣于 LSM upsert |
| 哈希前缀伪分区 | 分区是 overwrite/compaction 单元，近全量日更使其失义 |
| HBase 服务 + Paimon scan 副本 | HBase 成本痛点原封不动，另添双写一致性问题 |

社区先例：Paimon 无 range bucket PIP/issue（已核实）；最强参考 Hudi RFC-42（一致性哈希桶 split/merge）——借其"映射元数据版本化、split 中点、merge 仅限相邻"不变量，但把区间从 hash 空间搬到**语义排序键**空间。

---
