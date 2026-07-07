# Range Bucket SPLIT 设计方案（split_buckets procedure）
## —— KEY_RANGE 桶的高效分裂：对标 HBase Region Split 的万亿级承载

> 隶属于《Paimon Sort-Range-Bucket 改造技术设计方案》（`range-bucket-linkbase-design.md`，下称"主设计"）阶段二。
> 本文档单独深化 SPLIT 操作：独立 Spark procedure 运维入口 + compact 集成入口，重点论证大规模数据下的性能与正确性。
>
> **评审历史**：本设计经 5 轮独立评审并逐轮修订，全部结论均对照 Paimon 1.5-SNAPSHOT 源码逐条核实——
> R1 LSM/提交协议正确性（externalPath 全读删路径核实、I4 序义务化、tag-skipper 归因修正）；
> R2 规模与性能（发现并修复 P0：COMPACT 提交冲突检测全表读 → §7.1 桶限域；manifest 裁剪最好/最坏两档化）；
> R3 并发/故障恢复（发现并修复 2 个 P0：strict mode 对 APPEND 盲区 → I8；map combiner 静默 no-op → §7.3 防腐 + 禁分区表）；
> R4 API/运维/HBase 对标（发现并修复 P1：relabel 路径继承 → I9；参数面结构化、血缘/失败可见性）；
> R5 红队终审（8 个攻击角度全部存活，无新 P0；~20 处代码引用复核准确）。

---

## 1. 定位与目标

SPLIT 是 KEY_RANGE 表的核心运维原语：当某个桶（键区间）数据量超过目标尺寸（主设计 §10：16~32 GB，split 阈值 2×）时，把它切分为 N（默认 2）个子桶，且：

| 目标 | 量化标准（验收线） |
|---|---|
| G1 单桶分裂高效 | metadata-only 路径 **秒~分钟级**，单桶数据 IO ≤ `max_rewrite_bytes_per_bucket`（默认 2 GB） |
| G2 批量分裂高效 | 一次 procedure 调用处理 **≥500 桶**，总耗时 ≤ 30 分钟（停写窗口内完成） |
| G3 正确性零妥协 | 分裂前后任意查询结果逐字节等价；LSM 不变量、快照原子性、time-travel 全部保持 |
| G4 万亿级元数据规模可用 | 10 万桶表上，分裂 planning 不做全表 manifest 扫描；单次提交条目数有界 |
| G5 对标 HBase | 分裂延迟与 HBase region split（reference file + 异步 major compaction）同量级；运维语义（policy/auto-split/dry-run）不弱于 HBase |

**非目标**：在线 split（与写并发；主设计已确认停写窗口执行）；跨桶 rebalance（由 split+merge 组合完成）。

**明确围栏**：点查/lookup 面（Flink lookup join、`LocalTableQuery`）按哈希 bucket function 路由键，KEY_RANGE 下会误路由——在 range 感知改造完成前**一律不支持**（入口显式抛异常），防止分裂后被误接入。批式读路径不受影响（读端从不按键反推桶号，只按条目分组归并——已核实 `KeyValueFileStoreScan.groupByBucket`）。

---

## 2. 入口设计：两个入口，一套内核

### 2.1 独立 procedure（运维主入口）

```sql
CALL sys.split_buckets(          -- 复数命名：本 procedure 天然批量（与 merge_buckets 对齐）
  table               => 'db.linkbase',
  -- 目标圈选（二选一）：显式桶号，或按结构化条件自动圈选（省略 buckets 即自动模式）
  buckets             => '123,456,1000-1024',   -- 桶号列表/区段（与 compact 的 buckets 参数同一 grammar）
  min_size            => '64g',                 -- 自动圈选条件（AND 组合；MemorySize.parse 语义）
  min_file_count      => NULL,                  --   可选：文件数下限
  min_l0_runs         => NULL,                  --   可选：L0 sorted-run 数下限
  -- 选点
  split_keys          => NULL,                  -- EXPLICIT 显式分裂键列表（编码见 §4.2；数量=fanout-1，严格递增；仅单桶时允许）
  fanout              => 2,                     -- N 路分裂
  key_policy          => 'HOST_BOUNDARY',       -- MIDPOINT_SIZE | HOST_BOUNDARY | EXPLICIT
  min_child_size      => '16g',                 -- 防抖护栏：预估子桶 < 该值则 SKIPPED（默认 target/2；EXPLICIT 豁免）
  -- 预算与限流（运维按字节和分钟做窗口预算，不是按桶数）
  max_rewrite_bytes_per_bucket => '2g',         -- 单桶跨界重写上限（超出转 Mode B 或 SKIPPED）
  max_rewrite_bytes_per_run    => '1t',         -- 整次运行重写字节预算（达到即停止圈入新桶）
  max_duration        => '25m',                 -- 运行时长预算（在段边界干净中止；已提交段有效）
  max_buckets_per_run => 500,
  rewrite_parallelism => NULL,                  -- 缺省沿用 Spark 作业并行度
  -- 执行
  mode                => 'auto',                -- auto | metadata (Mode A) | rewrite (Mode B)
  commit_batch        => 100,                   -- 分段提交打包粒度
  drain               => 'none',                -- none | full：full = 立即 full compact 子桶排干 externalPath（Mode B 量级成本；
                                                --   minor 触不到 L_max 的 externalPath 大文件故不提供；默认靠例行 full compaction 自然排干）
  dry_run             => false
)
```

返回结果集（每个被处理的桶一行；**列与 status 枚举为冻结的 API 契约**，扩展只允许加列）：
`bucket_id | children | split_keys | mode | relabeled_files | rewritten_bytes | rewritten_files | duration_ms | commit_snapshot_id | epoch | status | error_message`
- `status ∈ {DONE, SKIPPED, PLANNED, FAILED}`（封闭枚举）。
- **部分成功必须可见**（评审 P1）：分段提交下段 3 失败不得抛异常吞掉段 1-2 的结果——已提交段返回 `DONE + commit_snapshot_id + epoch`，失败段返回 `FAILED + error_message`（错误信息需可机读区分：I8 完备性失败=可自动重试重 planning；strict-mode 冲突=运维锁疑似失效需告警），仅当零段提交时才抛异常。平台据 `commit_snapshot_id` 与 `$range_buckets VERSION AS OF` 对账。

### 2.2 compact 集成入口（进阶，默认关闭）

```sql
CALL sys.compact(table => 'db.linkbase', buckets => '0-8192',
  options => 'range-bucket.split.enabled=true, range-bucket.split.threshold=64g, range-bucket.split.target=32g, range-bucket.split.fanout-max=8')
```

语义：compaction 作业对某桶执行 **full compaction** 时，若该桶 size > threshold，则将合并输出**按选点直接分流写入 N 个子桶目录**（N = ceil(size/target)，上限 fanout-max），并在同一个 COMPACT 提交中携带 Range Map 替换。价值：**分裂零额外 IO**——full compaction 本来就要读全桶写全桶，分流只是改变输出文件的归属；这是"顺路分裂"。约束：仅 full compaction 触发（minor 不动 map）；与 2.1 共用同一套选点/校验/提交内核（`RangeSplitPlanner` / `RangeSplitCommitter`）。

**提交形态约束（评审 P1，已核实）**：`CompactProcedure.compactAwareBucketTable` 的既有骨架是**全部桶收拢后单次提交**（`:374-381`），没有分段——因此 §2.2 入口的单次调用必须**限定桶组规模**（沿用 `buckets` 参数圈小组，如每次 ≤ 数十桶），使单提交条目数有界、停写窗口可预估；G4 的分段提交能力仅属于 §2.1 独立 procedure（其 `RangeSplitCommitter` 自带分段）。若未来要给 §2.2 分段，需要新的协调器代码，v1 不做。

**为什么两个入口都要**：procedure 是确定性的运维指令（平台巡检 → 明确圈选 → 停写窗口执行 → 审计）；compact 集成消灭"先 compact 再 split 读两遍"的浪费，适合增长期例行化。运维平台可只用其一。

---

## 3. 不变量（在主设计 I1~I4 之上新增）

- **I5（键归属封闭性）**：任意时刻，桶 B 内所有数据文件的 `[minKey, maxKey] ⊆ B 的区间 [startKey, endKey)`。
  这是读端**桶级裁剪**的正确性前提：查询按 Range Map 定位桶后跳过其余桶，若某键物理躺在"错误"的桶里，会被裁剪漏读。推论：**跨分裂键的文件（straddler）必须物理切分，禁止"整体归一侧"**。
- **I6（分裂封闭性）**：一次 split 只消费父桶的文件、只产出子桶的文件与 map 变更；不触碰任何其他桶。多个 split 打包进一次提交时两两不相交，可任意并行。
- **I7'（选点有效性，按策略分档）**：split key 严格落在父桶区间开区间内部且两两递增；对**自动选点**（MIDPOINT_SIZE / HOST_BOUNDARY / min_size 圈选）额外要求每个子桶预估 size ≥ `min_child_size`（防"今晚分裂、明晚合并"的抖动——与 2× split / 0.25× merge 阈值共同构成 8× 迟滞带）；对 **EXPLICIT** 显式选点豁免尺寸下限（允许空/近空子桶——合法的预分裂工作流，见 §4.2）。
- **I8（父桶完全消费）**：分裂提交在（每次重试的）最新快照上归并 base+delta 后，被退休父桶必须不存在任何存活 ADD 条目（定义与动机见 §7.2——它闭合 strict mode 对外来 APPEND 的盲区）。
- **I9（relabel 路径保持）**：任何 relabel 的新 externalPath = `entry.externalPath() 若已存在则原样继承，否则 bucketPath(源桶) + "/" + fileName`（与 `PojoDataFileMeta.rename():282-283` 的既有语义一致——`newExternalPath` 是裸 setter 不会自动继承）。**违反后果（评审 P1-A，已核实）**：`drain=none` 为默认值，桶常态携带未排干的 external 条目进入下个窗口；naive 公式 `bucketPath(P)+"/"+f` 在 split→split 或 split→merge 组合时会把指针指向**物理为空的中间桶目录** → 读取打开不存在的路径。I9 成立时**不存在指针链**：任意代际的后代都直接指向唯一的原始物理目录，孤儿清理与空目录清扫永远只有一跳。

---

## 4. Split 选点算法（纯元数据，不读数据文件）

输入：父桶全部 `DataFileMeta`（`minKey/maxKey/fileSize/level/rowCount`）。输出：N-1 个 split key。

### 4.1 候选边界集

取最高层 L_max 的文件（互不重叠、按 minKey 有序），**候选 split key = 各文件的 minKey**。理由：
- L_max 占桶字节的绝对大头（full compaction 后 >90%），在文件边界切分 ⇒ **L_max 零 straddler**；
- 文件粒度 256 MB，对 16~32 GB 桶的切分精度是 ±1/64~1/128，足够。

若 L_max 文件数 < fanout（桶未 compact 或极小），回退：用全部层级文件端点做候选，接受更多 straddler，或直接建议 Mode B。

### 4.2 策略族（对标 HBase split policies）

| 策略 | 语义 | HBase 对应物 |
|---|---|---|
| `MIDPOINT_SIZE`（基础） | 沿 L_max 累计 fileSize，在 k/N（k=1..N-1）处取最近候选边界 | `ConstantSizeRegionSplitPolicy` 的选点 |
| `HOST_BOUNDARY`（默认） | 先按 MIDPOINT_SIZE 取初始点，再**就近吸附**到 host 前缀变化边界：在候选点左右各扫 ≤`snap_window`（默认 ±4 个文件）的候选边界，判据为 **`file[i].minKey` 的 host 前缀 ≠ `file[i-1].maxKey` 的 host 前缀**（两者都是现成元数据，已核实 `metadata.stats-mode=none` 下 minKey/maxKey 独立保留）。找不到、或吸附后的键违反 I7'（如吸附到区间端点/子桶尺寸低于下限）则回退未吸附的 MIDPOINT_SIZE 点（允许 host 内切分，host 查询退化为命中 2 桶，仍正确） | `KeyPrefixRegionSplitPolicy` / `DelimitedKeyPrefixRegionSplitPolicy` |
| `EXPLICIT` | 运维显式指定 `split_keys`（文本编码见下；校验 I7'；**允许产生空/近空子桶**——这是合法的预分裂工作流，HBase 运维会对即将变热的冷区间提前切分，空桶分裂恰是最便宜的分裂：零文件零 straddler 纯 map diff） | `split 'region', 'splitkey'` |

**`split_keys` 文本编码（运维必须能用键盘敲出一个键）**：沿用主设计 §11.0 rowkey grammar 与 `$range_buckets` 的渲染格式——`'<host_rev 字符串>'` 或 `'<host_rev>_<32位hex path_hash>'`（hash 段可省略 = 该 host 的下界）；多键用逗号分隔、必须严格递增、数量 = fanout-1。`HostKeyCodec` 负责文本 ↔ BinaryRow 的双向转换（与 `$range_buckets` 的 start_key/end_key 渲染同一实现，保证"从系统表复制出来就能贴进 procedure"）。

⚠ 预期管理（评审指出的统计弱点）：±4 文件窗口 ≈ ±1 GB。**恰恰是最需要分裂的桶最可能因单一巨型 host 而吸附失败**（linkbase 负载天然存在少数超大站点）——此时回退 host 内切分是设计内行为（正确性无损，该 host 查询命中 2 个相邻桶）。§13-1 的性质测试必须在真实 host 尺寸分布下量化回退率，运维平台对"host 内切分"的桶打标观测。**明确不提供读热度驱动的分裂**（批式湖仓无服务端每桶读计数；如需按热度分裂，由平台从查询侧遥测圈选后走显式桶号列表）——这是与 HBase 的真实差距，如实披露。

host 前缀提取：`HostKeyCodec.hostPrefixOf(BinaryRow key)`（主设计 §7.1 的同一工具类），从 trimmed PK 首字段取 host_rev；吸附判据只依赖文件 minKey/maxKey 元数据，无需读数据。

### 4.3 N 路分裂（fanout > 2）

HBase 只支持二分，热点 region 需要多轮 split-compact 循环。我们批量运维场景下直接支持 N 路：128 GB 热桶一次切 4 份，避免 3 轮"分裂→等 compaction→再分裂"。选点即 4.2 在 k/N 处取 N-1 个点，逐点校验 I7 与两两有序。

---

## 5. 执行模式

### 5.1 Mode A：metadata-only split（默认）

```
plan    : FileStoreScan.withBucket(P) @ 快照 S  →  条目按 level 分类
classify: 每个文件与 N-1 个 split key 求交 →  完全落入某子区间 → relabel 集
                                            跨界（straddler）      → rewrite 集
rewrite : straddler 逐文件单遍读、按 split key 分流写 ≤N 个输出文件
          （同 level、纯复制不归并；Spark 并行，文件级任务）
          ⚠ 正确性硬约束：必须逐行保留原始 raw sequence（走 compaction 式写出路径，
          禁止经 MergeTreeWriter.newSequenceNumber() 重新赋号），否则子桶 MOR 归并次序被破坏；
          §13-2 对拍测试必须显式断言 sequence 保持。
relabel : 非跨界文件用 DataFileMeta.newExternalPath(String) 克隆（现成 API，
          PojoDataFileMeta.java:433-455，裸 setter 仅改 externalPath 字段），
          新值按不变量 I9 计算：entry 已有 externalPath 则原样继承（undrained 桶再分裂/合并
          的正确性关键——见 §3-I9），否则 = bucketPath(P) + "/" + fileName
          （必须是含文件名的完整路径：DataFilePathFactory.toPath 原样取用，只给目录会静默坏读），
          以子桶号发 ADD 条目
commit  : 单快照原子提交（见 §7）
```

已核实：externalPath 在全部读/删路径生效——MOR 读（`KeyValueFileReaderFactory:143`）、raw/`$ro` 读（`RawFileSplitRead:286`）、过期删除（`FileDeletionBase:230`）；子桶后续 compaction 的输出天然落入真实子桶目录（`KeyValueFileStoreWrite:190-204` 的 path factory 以 bucket-C 为 parent），间接引用随例行 compaction 排干。

成本模型（32 GB 桶、fanout=2、full compaction 后），**分最好/最坏两档陈述**：
- planning IO **最好档**（M4 的 `withBucketSetFilter` + manifest 桶排序已建立、夜间 `compact_manifest` 之后）：≈ 该桶所在的 1~2 个 64 MB manifest。
- planning IO **最坏档**（bootstrap 后首次 compact_manifest 之前，或白天累积了未排序 delta）：接近全量 manifest 扫描（3 PB 档 ~5.6 GB，32 并行 ~1-3 分钟）。两个已核实的原因：(a) 现有 `withBucketFilter` 只做**条目级**过滤、不裁剪要打开的 manifest 文件（`AbstractFileStoreScan:159-162, 516-554`），文件级裁剪需 M4 新增 `withBucketSetFilter`；(b) 小时批 delta（~30 MB < 64 MB 目标）落成**单个** manifest 文件，无论内部怎么排序其 `[minBucket,maxBucket]` 都横跨全桶域、不可裁剪——每天最多累积 ~24×30 MB ≈ 0.7 GB 不可裁剪 delta。**运维守则：`split_buckets` 前置执行 `compact_manifest`（procedure 文档化约定并在 dry_run 输出提示）**；可选优化（M4 范围）：delta manifest 写出时按桶边界切文件，使单批 delta 也获得紧致区间。
- 数据 IO：straddler = 各中间层跨界文件 + 跨界 L0 ≈ 数百 MB ~ 2 GB（受 `max_rewrite_bytes_per_bucket` 硬约束）；L_max（≈30+ GB）**零 IO**（纯 relabel）。
- 单桶端到端：**秒~分钟级**（G1，在 §7.1 冲突读限域落地后成立）。

### 5.2 Mode B：compact-split 融合（重写式）

对需要 full compaction 的桶（L0 厚、run 多、或 straddler 超限）：单遍 MOR 读全桶 → 按 split key 分流 → 直接写 N 个子桶的 L_max 文件 → 一次 COMPACT 提交（DELETE 父桶全部文件 + ADD 子桶文件 + map swap）。
- IO = 1×读 + 1×写 = 常规 full compaction 的成本，**分裂免费**；
- 输出无任何 externalPath 间接引用（干净状态）；
- 这正是 §2.2 compact 集成入口的内核。

实现现实（已核实，防止低估工作量）：现有 `MergeTreeCompactRewriter.rewriteCompaction` 只持有**单个**绑定到本桶的 `RollingFileWriter`（`:80-91`）——N 路分流需要**净新的自定义 rewriter**（持 N 个 `KeyValueFileWriterFactory`，按区间路由每个 KeyValue；好消息：compaction 写出路径天然透传原始 sequence，满足 §5.1 的硬约束）。成本形态：一个桶 = 一个 `write.compact` 任务（`CompactProcedure.compactAwareBucketTable` 的分发粒度是桶不是文件），64 GB 桶 = 单任务数十分钟长尾——Mode B 适合"本来就要 full compaction 时顺路分裂"，**不适合作为 Mode A 的常规替代**（500 桶 Mode B = 15-30 TB 重写）。

### 5.3 模式自动选择（mode='auto'）

```
straddlerBytes(含跨界 L0) ≤ max_rewrite_bytes_per_bucket   → Mode A
否则若该桶本就满足 full-compaction 触发条件 或 mode 允许   → Mode B
否则                                                     → SKIPPED（结果集提示先 compact）
```

### 5.4 与 HBase reference file 的对比（为何不做"半文件引用"）

HBase split 秒级返回靠 Reference/HalfStoreFile：子 region 持有父文件的 top/bottom 半引用，读时按边界过滤，后台 major compaction 再物化。我们评估过等价方案（ManifestEntry 携带 key 过滤区间，读端按区间过滤文件内容）：**否决**。理由：
1. 破坏 I5 的文件级语义，所有读路径（MOR 归并、$ro、file index、谓词裁剪）都要新增"条目级 key 过滤"维度，侵入面巨大；
2. 我们的 straddler 天然极少（L_max 边界选点 ⇒ 只剩中间层/L0 跨界，≤2 GB），同步重写的代价远小于引入半引用的复杂度；
3. externalPath relabel 已经承担了"秒级返回"的角色——它是 HBase reference 思想在"整文件粒度"上的安全子集。

---

## 6. 批量 split（G2/G4）

1. **圈选**：自动圈选模式（省略 `buckets`，按 `min_size` 等条件）时 driver 端读 `$range_buckets`（`FileStoreScan.readBucketEntries()` 聚合）。注意（已核实）：该聚合本身是一次全量 manifest 扫描（~5.6 GB / 1-3 分钟量级）——夜间跑一次没问题，**不要高频轮询**；按 policy 过滤 + `max_buckets_per_run` 限流（默认 500，防单窗口过载）。
2. **planning 并行**：每桶的条目获取共享**一次** bucket-set 过滤扫描；driver 只物化目标桶条目（500 桶 × ~190 文件 ≈ 9.5 万条 ≈ 数十 MB 堆，无压力）。manifest **文件级**读取量取决于 M4 裁剪与排序是否就位——最好/最坏两档见 §5.1，G4 的准确表述是"**条目物化恒定桶限域；manifest 字节读取在 M4 + compact_manifest 前置下做到桶限域**"。
3. **rewrite 并行**：全部 straddler 文件铺成 Spark 任务（文件级粒度），集群并行。**分发与提交管道是净新代码**（已核实无现成文件级原语）：模板取 `CompactProcedure.compactAwareBucketTable` 的 `parallelize → 任务内产出 → CommitMessageSerializer → driver 单点 commit` 骨架，但分发单元从桶改为文件描述符；`RescaleProcedure` 的 Dataset 重写路径**不可复用**（经 `PaimonSparkWriter` 会重新赋 sequence，违反 §5.1 硬约束）。
4. **分段提交**：每 `commit_batch`（默认 100）个桶的变更打包为一次原子提交（I6 保证任意打包组合合法）；每次提交 = 一次 map swap（epoch+1）。500 桶 = 5 次提交。分段的意义：单提交条目数有界（100 桶 × ~200 条目 × 2 ≈ 4 万条 ≈ 12 MB delta manifest）、失败重试范围有界。
5. **进度与恢复**：某段提交后失败，重跑 procedure：已分裂的父桶号已退休 → 自动跳过（幂等），未处理的继续。

规模账（G2 验收）：500 桶 × straddler ≤2 GB = 最坏 1 TB 重写，数千 core 集群 <20 分钟；relabel/commit 均为元数据操作，分钟内（前提：§7.1 冲突读桶限域已落地，否则 commit 被全表冲突读主导）。

---

## 7. 提交协议与并发防护

- 每段提交 = 单快照（`CommitKind.COMPACT`），**提交形态已按现有 API 核实可表达**：每个分裂构造 N+1 个 `CommitMessageImpl`（每个绑定一个 (partition, bucket)）——父桶 P 一条（`compactBefore` = 父桶全部原条目，`compactAfter` = ∅ → 产出 `DELETE(P,...)`），子桶各一条（`compactBefore` = ∅，`compactAfter` = relabel + 重写产物 → 产出 `ADD(C,..., totalBuckets=-3)`）；Range Map 替换（`indexFiles[DELETE(map@E), ADD(map@E+1)]`）搭载于 compactIncrement 的 index 变更（`ManifestEntryChanges.collect:96-129` 的既有映射）。
- **父桶 DELETE 条目必须携带各文件的原始 level**（`compactBefore` 直接使用扫描到的原条目即可）：`FileEntry.Identifier` 含 level，级别不符将无法在 base+delta 归并中抵消原 ADD。
- **I4 是 committer 的显式义务，不是自动性质**：当前 commit 路径按 CommitMessage 顺序写 delta manifest（`FileStoreCommitImpl:998` 无任何排序）。保障手段（两者取一，建议双重）：(a) `RangeSplitCommitter` 固定先发父桶（DELETE）消息、后发子桶（ADD）消息；(b) 主设计 M4 的 `(bucket, level)` 稳定排序落地于 `:998`（对 append 与 compact delta 同一处生效）后，子桶号 > 父桶号自动保证序。滚动切分（rolling）按写入序切文件、manifest list 保序，故该序在多 manifest 文件间依然成立——此依赖须在实现注释中显式声明。
- **strict mode 的真实覆盖面（评审修正，务必精确理解）**：`COMMIT_STRICT_MODE_LAST_SAFE_SNAPSHOT = S`（planning 快照）。已核实 `StrictModeChecker.check`（`StrictModeChecker.java:61-108`）：对外来 **COMPACT/OVERWRITE** 提交，非分区表下必然分区重叠（EMPTY_ROW）→ 分裂提交确定性失败，覆盖"迟到的 compaction 作业"；但**外来 APPEND 仅在本提交是 OVERWRITE 时才被检查（`:85-86`）**——split 以 COMPACT 提交，**外来 APPEND（小时写批）不会被 strict mode 拦截**（`RescaleProcedure` 之所以拦得住是因为它走 OVERWRITE；本设计不能改走 OVERWRITE——OVERWRITE 的删除域是整个分区而非选定桶）。该缺口由下方 §7.2 完备性校验闭合。分段提交时，段 k+1 的 planning 基于段 k 的产出快照（S 前滚，`strictModeChecker.update` 已核实随同一 committer 自动前滚）。
- **commitUser 契约（承重，必须显式规定）**：每次 procedure 调用铸**一个** commitUser（`createCommitUser` 自带 UUID）并在其**全部分段间复用**——否则段 k+1 会把段 k 的 COMPACT 快照当作"他人提交"而自我冲突；不同 procedure 运行之间天然不同 commitUser——这是并发双 split 被 strict mode 确定性拦截的前提。加测试：5 段单次运行不自冲突；双并发运行第二个确定性失败。
- **map 一致性**：procedure 启动时加载 map@E；每段提交内的 map diff 由本段分裂集合确定性推导（移除 N 父区间、插入子区间、nextBucketId 前进）；提交时 `FileStoreCommitImpl` 的 KEY_RANGE 校验（主设计 §6.2-2）复核所有 ADD 桶号 ∈ 新 map。

### 7.1 冲突检测的桶级限域（评审发现的 P0，必改）

现状（已核实）：COMPACT 提交无条件 `detectConflicts=true`（`FileStoreCommitImpl:345-362`），首次尝试即调 `CommitScanner.readAllEntriesFromChangedPartitions`（`:877-899`）；`changedPartitions` 只按 partition 聚合（`ManifestEntryChanges.changedPartitions:166-178`），**无分区表 = {EMPTY_ROW} = 全表**，且该读取**无任何桶过滤**（`CommitScanner:92-102`）。3 PB 规模下 = 每段提交读全量 ~5.6 GB manifest 并在 driver 物化 **18.7M 条 SimpleFileEntry（~5-10 GB 堆）**——这会同时压垮 G2（500 桶 5 段提交 × 全表读）与 G4，也**同样拖累日常 compaction 作业的每次提交**。

**必改方案（新增核心改动，随 M4 落地，split 与日常 compaction 共同受益）**：为 KEY_RANGE 表的 COMPACT 提交把冲突检测的 base 读取限域到**受影响桶集合**——split 的变更集合恰为 {父桶} ∪ {子桶}（I6），而 `checkKeyRange` 按 `(partition, bucket, level)` 分组（`ConflictDetection:363-372`）、`mergeEntries` 只需被 DELETE 桶的 base 条目 ⇒ **桶过滤后的 base 读取充分且正确**。实现：`readAllEntriesFromChangedPartitions` 增加桶集合参数，下推到 manifest 文件级（依赖 M4 `withBucketSetFilter` + 桶紧致区间）与条目级过滤；`RangeSplitCommitter`/compaction 提交把各自的受影响桶集合传入。备选（更激进，可作开关）：停写窗口 + strict mode 已排除并发写者时，跳过 base 归并冲突检查、仅做 delta 内构造性 `checkKeyRange`——默认不启用，作为超大表逃生阀。

修正 §6 成本模型：在 7.1 落地前，每段提交的主导成本是全表冲突读（分钟级 + 数 GB driver 堆），"relabel/commit 均为元数据操作分钟内"仅在 7.1 之后成立；另注：同一次运行内，段 1..k 新写出的 delta manifest 桶号跨度大（低位父桶 + 高位新子桶），对段 k+1 不可做文件级裁剪——但每段仅 ~12 MB、5 段合计 ≤60 MB 条目级过滤开销，可忽略；另外 base manifest 合并在所选参数下是 passthrough（`tryFullCompaction` 因 5.6 GB < 10 GB 阈值直接跳过、`tryMinorCompaction` 因 merge-min-count=100 万不触发），单次提交的 manifest-list 处理是 KB 级——commit 的贵不在合并而在冲突读。

---

### 7.2 分裂提交的完备性校验（评审 P0 修复：闭合外来 APPEND 缺口）

新增 KEY_RANGE 分裂提交不变量 **I8（父桶完全消费）**：提交时（每次重试均重新校验，基于最新快照的 base+delta 归并结果）断言**每个被退休的父桶在归并后不存在任何存活 ADD 条目**——等价于"split 的 compactBefore(P) 恰好等于最新快照中 P 的完整文件集合"。若窗口内有写批把 L0 落进了父桶（strict mode 拦不住的 APPEND），split 的 DELETE 集不含这些新文件 → 归并后父桶有存活条目 → **提交确定性失败**（可重试：重新 planning 把新文件纳入分类）。代价近乎为零：§7.1 的桶限域冲突读已把受影响桶的 base 条目拿在手上。此校验同时是防御任何未知交错的兜底。
反向交错（split 先提交、写批后提交）由主设计 §6.2-2 的陈旧桶号校验覆盖（写批 ADD 引用已退休桶号 → `StaleRangeMapException` 重跑）。两向闭合后，**任何 ops 锁失效的交错都不会丢数据**。

### 7.3 Range Map 替换的防腐（评审 P0 修复：combiner 静默 no-op）

已核实 `GlobalIndexCombiner.combine` 对 DELETE 目标不存在时**静默跳过**（`IndexManifestFileHandler.java:221-223`，对比 `GlobalCombiner:150-154` 有 `checkState`），且其区间校验仅作用于 `globalIndexMeta != null` 的条目——若陈旧的 `DELETE(map@E)+ADD(map@E+1')` 应用在当前条目已是 map@E+1 的快照上，会产生**两条 RANGE_BUCKET_MAP 条目的腐坏快照**。非分区表 + 独立 commitUser 下该路径被 strict mode 挡住（不可达），但防线不能只靠"碰巧"：
1. **专用 combiner 或补 `checkState`**：`RANGE_BUCKET_MAP` 的 DELETE 目标不存在 → 提交失败（快速失败优于静默腐坏）；
2. **写后断言**：index manifest 合并产物中 `RANGE_BUCKET_MAP` 条目**恰好一条**，否则中止提交；
3. **禁止分区化 KEY_RANGE 表**（`SchemaValidation` 硬校验分区键必须为空）：已核实 `changedPartitions` 对非 DV 的 index 条目**不计入**变更分区集（`ManifestEntryChanges.java:173`）——分区表上两个不同分区的 split 不会分区重叠，strict mode 失明，双 map 路径可达。map 是全表全局对象，KEY_RANGE v1 与分区语义天然互斥，直接封死。
- **与 merge 互斥**：split/merge/compact/写批 由运维锁串行（主设计 §6.2-1）；strict mode 兜底锁失效。

## 8. 正确性论证（逐不变量）

| 不变量 | 论证 |
|---|---|
| I1 覆盖性 | map diff 是区间的等价细分：父区间 [s,e) → [s,k₁),[k₁,k₂),…,[k_{N-1},e)，连续性/覆盖性/不重叠保持；`RangeBucketMap.validate()` 提交前复核 |
| I2 桶号 | 只铸新号（nextBucketId 前进 N），父号退休永不复用 |
| I3 epoch | 每段提交 +1，审计连续 |
| I4 删加序 | §7 构造保证；快照过期的物理删除取消机制（`FileDeletionBase.getDataFileToDelete` 按物理 Path 的 DELETE/ADD 抵消）成立 |
| I5 键封闭 | relabel 集：文件区间 ⊆ 某子区间（分类判据）；rewrite 集：按 split key 物理切分后各半 ⊆ 对应子区间；**L0 跨界文件同样强制切分**（禁止归一侧，否则漏读） |
| I6 分裂封闭 | plan/rewrite/commit 均以父桶号为作用域；批量时父桶两两不同 ⇒ 变更集不相交 |
| I7' 选点有效性 | `RangeSplitPlanner` 对每个（吸附后的）split key 逐点校验：严格内点、递增、子桶尺寸下限（自动策略档）；违反即回退未吸附 MIDPOINT_SIZE 或 SKIPPED（§4.2/§4.3） |
| I8 父桶完全消费 | §7.2：提交期断言退休父桶归并后零存活 ADD——外来 APPEND（strict mode 对 COMPACT 提交不查 APPEND，`StrictModeChecker:85-86`）落入父桶的文件会使断言失败 → 提交拒绝、重新 planning，杜绝"数据落入无人路由的退休桶"型丢失 |
| I9 relabel 路径保持 | §3-I9 公式（继承已有 externalPath）；§13-5b 血缘回归 IT 封死 split→split / split→merge 组合；后代永远直指原始物理目录，无指针链 |
| 失败残留无害性 | 提交失败留下的子桶目录孤儿文件对读端**惰性**（liveness 只由 manifest 条目决定，无任何按目录列举当数据的路径）；后续若同号桶被重新铸造（nextBucketId 未推进的失败场景不存在——号只随已提交 map 铸造；即便目录名复用），文件名也不会冲突：`DataFilePathFactory` 每实例随机 UUID 前缀（`:63,:140`），孤儿清理按全局唯一文件名精确回收 |
| 注定失败写入的序号复位无害 | ops 锁失效时写批可能在 split 后的最新快照上 restore 到已退休父桶（空 → seq 从 0 重置，`FileSystemWriteRestore` 读最新快照而非写批 planning 快照）——但该写入必然被陈旧桶号提交校验拒绝（主设计 §6.2-2），任何状态都不会落库，复位无后果 |
| checkKeyRange | 子桶每 level≥1：父桶该 level 本不重叠，单调 split key 切分后仍不重叠；重写产物区间被切开 ⇒ 构造性通过（提交期 `ConflictDetection.checkKeyRange` 复核兜底） |
| sequence 语义 | 子桶继承父桶行级 sequence；子桶键集两两不相交 ⇒ MOR 永不发生跨谱系 seq 比较；后续写入 seed = max(子桶文件) 单调延续；复合 `sequence.field`（主设计 §4.3）下 raw seq 仅作最后 tie-breaker |
| time-travel | 旧快照引用父桶条目 + map@E（无 externalPath，路径 bucket-P/F 物理仍在——relabel 从不搬文件），新快照引用子桶条目 + map@E+1，各自封闭一致。保护机制需精确区分两段：**(a) split 快照自身**的 DELETE(P,F)/ADD(C,F-ext) 同物理路径抵消由 I4 保证（`FileDeletionBase.getDataFileToDelete` 按物理 Path last-write-wins）；**(b) 文件被子桶 compaction 排干之后**，drain 快照只有 DELETE(C,F-ext) 无抵消 ADD——此时保护 pre-split 快照/tag 引用的**唯一机制**是主设计 §8.4-3 的 skipper 物理路径/文件名匹配修复（现行 `containsDataFile` 按 (partition,bucket,fileName) 匹配会因桶号不同互不相认 → 误删）。**该修复是本 procedure 的硬前置**，不可与 I4 混同 |
| 查询等价 | 分裂前后：任意谓词 → map 路由的桶集合的并 = 父桶数据全集（I1+I5）；行内容不变（relabel 零改动、rewrite 纯复制）⇒ 逐字节等价（测试对拍验收） |

**失败原子性**：提交前产生的一切输出（重写文件、新 map 文件）均无快照引用 → 天然孤儿，`remove_orphan_files` 兜底清理；提交是唯一可见性开关。重跑幂等：父桶号是否仍在 map 是确定性的"已完成"判据。

---

## 9. 性能分析与 HBase 对标

| 维度 | HBase region split | 本方案 |
|---|---|---|
| 分裂延迟 | 秒级（reference file，无数据 IO） | Mode A 秒~分钟（relabel 零 IO + ≤2 GB straddler） |
| 分裂后开销 | 子 region 首次 major compaction 重写全量 | 可选 `drain`/例行 compaction 渐进排干 externalPath；Mode B 则零残留 |
| 批量能力 | 逐 region 触发，master 调度 | 单 procedure 500 桶/窗口，分段原子提交 |
| N 路分裂 | 不支持（只二分） | fanout=N 一次到位 |
| 选点 | midpoint / KeyPrefix policy | MIDPOINT_SIZE / HOST_BOUNDARY 吸附 / EXPLICIT |
| 自动化 | RegionServer 内置 policy 自动触发 | 运维平台巡检 `$range_buckets` + 自动圈选模式；或 compact 集成顺路分裂 |
| 万亿承载 | 数万 region/集群 | 10 万桶：planning 靠 manifest 桶排序+区间裁剪去全表化；提交条目分段有界 |

容量规划视角：桶数增长 = 数据翻倍驱动（16→32 GB 阈值），10 万桶表全表翻倍也只需 ~10 万次分裂，按 500/晚 = 200 个窗口 ≈ 7 个月自然摊销；实际增长（每日 10~50% 更新中新增占比有限）远缓于此，split 不会成为运维瓶颈。

---

## 10. 故障、幂等与 dry-run

- **dry_run**：完整执行圈选+选点+分类，输出计划行（split_keys/mode/rewritten_bytes 预估），**无任何持久状态变更**（注意：自动圈选模式仍要付一次全量 manifest 统计扫描 ~1-3 分钟的读负载，运维平台需知悉，勿高频轮询）。
- **结果集 = 稳定 API 契约**：列名/类型/顺序冻结（§2.1），`status ∈ {DONE, SKIPPED, PLANNED, FAILED}` 为封闭枚举（与 §2.1 一致）——运维平台按此解析，扩展只允许加列。
- **崩溃点矩阵**（IT 必测）：重写中途 kill（孤儿）；重写完成提交前 kill（孤儿）；提交重试撞 commitIdentifier 幂等；段间 kill（已完成段生效，重跑跳过）；strict mode 冲突失败（全段回退无副作用）；**显式桶号列表重跑**——请求的桶号不在当前 map 中 = 已完成的父桶 → `status=SKIPPED`（no-op），**不是校验错误**（否则崩溃后按原参数重跑必然失败，破坏幂等）；仅对"既不在 map 也不是已退休号"的输入才报错。
- **快照过期防护（评审 P1）**：planning 快照 S 在整个 procedure 期间必须存活（Mode A 的 relabel ADD 指向 S 引用的物理文件；Mode B 要读它们数十分钟）。硬要求（双管齐下）：(a) `expire_snapshots` 纳入运维锁串行集合 {写批, compaction, split/merge, expire}；(b) procedure 启动时校验 `snapshot.time-retained`/`num-retained.min` ≥ 预估运行时长，不满足则拒绝启动。不允许只靠默认 3d 保留期"碰巧"覆盖。
- **部分 SKIPPED**：单桶超 `max_rewrite_bytes_per_bucket` 且不允许 Mode B → 结果集标 SKIPPED 并给出建议（先 compact），不阻塞其余桶。

---

### 10.1 已退休父桶目录的清扫（评审发现的泄漏点）

`cleanEmptyDirectories` 只探测 `deletionBuckets` 中记录的桶目录；relabel 文件被排干时其 DELETE 条目桶号是子桶 C，故 `bucket-P/` 变空后**永远不会被探测**——数月运维后累积大量空目录，加剧 NameNode/对象存储 LIST 压力。修复（随 procedure 落地）：`FileDeletionBase` 对带 externalPath 的被删条目额外探测其 `externalPathDir()` 父目录；或在 `remove_orphan_files` 中加一次退休父桶空目录清扫。列入主设计 §8.4 矩阵第 2 项的验收内容。
**清扫判空必须是跨桶引用计数语义**（评审 P2-D）：I9 下多个子孙桶的存活条目可能同时指向 `bucket-P/`——目录可删的条件是"**所有**桶的存活条目中不存在指向该目录的 externalPath"，而非"该目录所属桶已空"；实现上沿用非递归 delete（目录非空自然失败）+ 仅在探测点扩展，天然满足该语义。

## 11. 可观测性与运维心智模型（评审 P1-G/H 增补）

桶号不透明且每次 split/merge 即退休——必须给 HBase 运维等价的"region 名 + split 历史"体验：

1. **血缘/历史**：新系统表 **`$range_bucket_history`**，split/merge 提交时顺带落一条事件（写入 map 文件的事件区或独立小文件）：`{epoch, op(SPLIT/MERGE/INIT), parent_bucket_ids[], child_bucket_ids[], split_keys[], snapshot_id, timestamp, commit_user}`。这是运维排障的第一工件："桶 123 后来变成了谁"一查即得，不必对两个 epoch 的 map 做区间对撞。
2. **键→桶查询函数**：`SELECT range_bucket_of('cn.com.sina.news.comment4')`（复用 `RangeBucketMap.bucketOf` 二分）——运维从 host 直达桶号，不必肉眼比对二进制区间键。
3. **日志规约**：写入/compaction/split/慢任务日志一律输出 `(bucket_id, epoch)` 二元组（= HBase region name 的等价物；孤立的 bucket_id 在下次分裂后即不可考）。
4. **`$range_buckets` 列增补**（在主设计 §9 基础上）：`external_file_count`（仍指向退休目录的文件数——drain=none 默认下运维必须能区分"干净桶"与"90% 间接引用的桶"）、`l0_file_count`、`sorted_run_count`（split 圈选与 MOR 读代价评估都需要）；`start_key/end_key` 用 `HostKeyCodec` 渲染为人类可读字符串（同 `FileKeyRangesTable` 借 `CastExecutors` 渲染 min/max key 的先例），与 `split_keys` 参数编码互逆——**从系统表复制即可贴回 procedure**。
5. **时间旅行**：`$range_buckets VERSION AS OF s` 天然可用（map 随快照版本化）——"快照 T 时 host X 在哪个桶"可直接回答。
6. 指标：split 耗时分布、straddler 字节、relabel/重写文件数、SKIPPED/FAILED 计数、strict-mode 冲突次数、host 内切分（吸附回退）计数。

**Normalizer 等价物声明**（评审 P2-B）：本设计不做单独的 `normalize` procedure，"夜间巡检 → size>2×target 分裂 / size<0.25×target 且相邻合并"的平台闭环即 HBase Region Normalizer 的等价物；其防振荡保证 = 2×/0.25× 阈值的 **8× 迟滞带** + I7' 的 `min_child_size` 子桶下限。与 HBase 的 SLA 差异如实披露：HBase 是 RegionServer 内持续评估的 split policy（分钟级响应），本方案是外部夜间循环（响应延迟至多一个窗口周期）——这是批式湖仓停写窗口模型的固有取舍。

---

## 12. 实现落点（类级）

| 组件 | 位置 | 说明 |
|---|---|---|
| `SplitBucketsProcedure` | `paimon-spark/paimon-spark-common/.../procedure/` | 仿 `RescaleProcedure` 骨架（strict mode、modifySparkTable） |
| `RangeSplitPlanner` | `paimon-core/.../rangebucket/` | 圈选、选点（策略族）、分类（relabel/rewrite/skip）、I7 校验；纯元数据 |
| `RangeSplitFileRewriter` | `paimon-core/.../rangebucket/` | **净新**：straddler 文件级分布式重写（持 ≤N 个 `KeyValueFileWriterFactory` 按区间分流；走 compaction 式写出路径透传原始 sequence）；分发/回收模板取 `CompactProcedure` 的 parallelize+CommitMessageSerializer 骨架 |
| `RangeSplitCommitter` | `paimon-core/.../rangebucket/` | 条目构造（父桶 CommitMessage 在前保 I4 序、compactBefore 用原条目保 level）、map diff、分段打包、提交 |
| **冲突读桶限域** | `CommitScanner` / `FileStoreCommitImpl` / `ConflictDetection` | §7.1 P0 修复：COMPACT 提交的 base 冲突读按受影响桶集合限域（split 与日常 compaction 共同受益）；依赖 M4 `withBucketSetFilter` |
| **I8 完备性校验** | `ConflictDetection` KEY_RANGE 分支 | §7.2 P0 修复：退休父桶归并后零存活 ADD 断言（复用 §7.1 已读入的受影响桶 base 条目） |
| **map combiner 防腐** | `IndexManifestFileHandler`（专用 combiner 或补 `checkState`） | §7.3 P0 修复：RANGE_BUCKET_MAP 的 DELETE 目标缺失 → 快速失败；写后恰好一条断言 |
| `$range_bucket_history` | `paimon-core/.../table/system/` + map 事件区 | §11-1 血缘系统表：split/merge/init 事件 {epoch, op, parents, children, split_keys, snapshot_id, ts, commit_user} |
| `range_bucket_of()` | Spark function（`PaimonFunctions`） | §11-2 键→桶查询（复用 `RangeBucketMap.bucketOf`） |
| compact 集成（Mode B） | `CompactProcedure` + **净新 N 路 rewriter**（非 `MergeTreeCompactRewriter` 扩展可达） | full compaction 输出按 split key 分流至子桶目录 + 提交带 map swap |
| 复用 | `RangeBucketMap(.validate)`、`HostKeyCodec`、`PojoDataFileMeta` 克隆、`ConflictDetection.checkKeyRange`、`StrictModeChecker` | 主设计阶段一产物 |

依赖门禁：**主设计 §8.4 引用安全矩阵全绿**（externalPath × 14 条清理/消费路径）是本 procedure 合入前提；Mode B 不产生 externalPath，可先行合入作为保守版本。

---

## 13. 测试计划

1. **选点性质测试**（property-based）：任意文件 size/边界分布下，MIDPOINT_SIZE 两侧 size 比 ∈ [1/N ± 文件粒度误差]；HOST_BOUNDARY 吸附不越过 snap_window；I7 恒成立。
2. **等价性对拍**：随机数据 + 随机分裂序列，split 前后全表/host 点查/前缀区间查询结果逐字节 diff（MOR 与 $ro 两路径）。
3. **崩溃恢复 IT**：§10 崩溃点矩阵。
4. **并发 IT**：(a) 窗口内注入外来 COMPACT/OVERWRITE → strict mode 使 split 段提交失败且无副作用；(b) **窗口内注入外来 APPEND（写批落 L0 进父桶）→ I8 完备性校验使提交失败**，重跑后新 L0 被纳入分类且数据零丢失（P0-1 回归）；(c) 双并发 split procedure → 第二个确定性失败（commitUser 契约 + strict mode），且 map 恰好一条（P0-2 回归）；(d) 单次运行 5 段提交不自我冲突（commitUser 复用）。
5. **清理组合 IT**：split → expire_snapshots / remove_orphan_files / tag 建删 → 文件存活性与读取正确性（衔接主设计 §8.4 矩阵）。
5b. **I9 血缘回归 IT**（评审 P1-A）：`split → (不 drain) → 再 split 同一谱系` 与 `split → (不 drain) → merge 相邻子桶`，断言每个 external 条目解析到的物理路径存在且等于最初的原始路径（无中间桶目录、无指针链）；再叠加 expire/orphan 组合复验。
6. **规模基准**（paimon-benchmark）：10 万桶表上 500 桶批量 split 的 planning/rewrite/commit 分项耗时；G1/G2 验收线。

---

## 14. 被否决的替代设计

| 方案 | 否决理由 |
|---|---|
| HBase 式半文件引用（条目携带 key 过滤区间） | 侵入所有读路径与 compaction，破坏 I5 文件级语义；straddler 本就极少（§5.4） |
| split = 整桶 INSERT OVERWRITE 重写 | 32 GB 全重写 vs ≤2 GB straddler，浪费 15×+；批量 500 桶 = 16 TB 重写不可接受 |
| 跨界 L0 归一侧不切分 | 违反 I5，按 map 路由的读会漏键——正确性红线 |
| 在线 split（与写并发） | 需要双写/重定向机制（Hudi RFC-42 dual-write），复杂度陡增；停写窗口已被业务接受 |
| rename 物理搬移代替 externalPath | rename 与快照提交非原子，且旧快照 time-travel 会找不到文件；externalPath 与提交同原子 |
