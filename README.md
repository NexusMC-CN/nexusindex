# NexusIndex

NexusIndex 是一个基于 PostgreSQL 的搜索与索引服务，提供全文检索、结构化筛选、facet 聚合、标签索引、签名 cursor 分页，以及可重建的影子索引代际切换。

服务可以作为独立进程部署，索引库与业务主库分离。业务主库仍然是唯一权威数据源，NexusIndex 只保存可丢弃、可重建的搜索快照。

> 当前版本内置的 source adapter 面向 NexusMC 论坛数据库 schema。HTTP 搜索和索引运行时可以复用到其他系统，但接入其他数据模型前需要实现对应的 source adapter，不能把任意 PostgreSQL 数据库直接作为 MAIN_DATABASE_READONLY_URL。

## 能力概览

- PostgreSQL tsvector 召回与 SQL 内相关性、热度、新鲜度排序。
- must、should、must_not 布尔查询，以及 match、phrase、prefix、受限 fuzzy 操作符。
- 实体、分类、状态、标签、平台、数值、日期和 Minecraft 版本范围筛选。
- 按当前查询条件返回 entity、category、tag facets。
- relevance、latest、popular 三种明确排序契约。
- HMAC 签名的 keyset cursor 分页；搜索链路不使用 SQL OFFSET。
- 全量 shadow rebuild：构建、校验、追赶 outbox 事件后原子切换 active generation。
- 至少一次投递的 outbox worker、事件合并、source_version 乱序写保护和失败重试。
- 进程内 L1 缓存，可选通过 HTTP 接入 EdgeCache 作为 L2。
- runtime scoring、同义词和缓存配置热更新。

## 架构

~~~text
业务主库（只读业务表 + search_index_events）
        |
        +-- outbox worker --> active generation --> search_index / tag_index
        |
HTTP client ----------------> 搜索、标签、状态和管理 API
                               |
                               +-- 可选 EdgeCache L2
~~~

NexusIndex 使用两条数据库连接：

- 主库连接用于读取源数据，并领取、标记 search_index_events。
- 索引库连接用于读取和写入 generation、search_index、tag_index 与同步日志。
- migration owner 连接只用于执行 schema migration，常驻服务不会执行 DDL。

## 环境要求

- Go 1.25 或更高版本。
- PostgreSQL 15 或更高版本。
- 索引库允许启用 pg_trgm 扩展；migration owner 需要创建扩展和表、索引、约束。
- 主库需要提供内置 source adapter 使用的业务表，以及 NexusIndex outbox v2 的 search_index_events.event_order（非空 bigint）。
- 可选：可通过 HTTP 访问的 EdgeCache 服务。

## 快速开始

以下命令在独立仓库根目录执行。请先创建索引库运行账号和 migration owner 账号，并按部署环境替换连接串。

~~~bash
go mod download

export MAIN_DATABASE_READONLY_URL='postgresql://readonly_user:password@127.0.0.1:5432/forum'
export INDEX_DATABASE_URL='postgresql://index_user:password@127.0.0.1:5432/nexusindex'
export INDEX_DATABASE_MIGRATION_URL='postgresql://index_owner:password@127.0.0.1:5432/nexusindex'

# API token 控制管理和搜索路由；cursor secret 用于签名分页游标。
export NEXUSINDEX_API_TOKEN='replace-with-a-long-random-token'
export NEXUSINDEX_CURSOR_SECRET='replace-with-a-different-long-random-secret'

go run ./cmd/nexusindex migrate status
go run ./cmd/nexusindex migrate up
go run ./cmd/nexusindex
~~~

PowerShell 使用 $env:NAME = 'value' 设置环境变量。服务启动时会校验索引库 schema 版本和主库 outbox schema；任一校验失败都会退出，不会自动修改数据库结构。

健康检查不需要 token：

~~~bash
curl http://127.0.0.1:4412/health
~~~

搜索和管理接口需要在设置 NEXUSINDEX_API_TOKEN 时携带 token：

~~~bash
curl http://127.0.0.1:4412/api/search \
  -H 'Authorization: Bearer replace-with-a-long-random-token' \
  -H 'content-type: application/json' \
  -d '{"q":"fabric 1.20.1","limit":20}'
~~~

## 配置

### 数据库与鉴权

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| MAIN_DATABASE_READONLY_URL | 无 | 主库读取连接。worker 还需要对 outbox 消费状态列执行有限 UPDATE。 |
| INDEX_DATABASE_URL | 无 | 常驻服务使用的索引库连接。运行账号不应拥有 DDL 权限。 |
| INDEX_DATABASE_MIGRATION_URL | 无 | 仅 migrate status/up 使用的 owner 连接。 |
| NEXUSINDEX_API_TOKEN | 空 | 设置后保护 /search、/api/* 和 /index/*；支持 Bearer 和 X-NexusIndex-Token。 |
| NEXUSINDEX_CURSOR_SECRET | 空 | HMAC cursor 密钥。生产环境必须显式设置，并在所有 NexusIndex 副本之间保持一致。 |

服务要求 NEXUSINDEX_API_TOKEN 与 NEXUSINDEX_CURSOR_SECRET 至少设置一个。生产环境建议两个都设置：API token 负责路由鉴权，cursor secret 只负责签名游标，不应暴露给论坛前端或任何客户端。

### HTTP 与 worker

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| NEXUSINDEX_PORT | 4412 | HTTP 监听端口。 |
| NEXUSINDEX_REQUEST_TIMEOUT_MS | 5000 | handler 和数据库操作的单请求超时，范围 100..120000 ms。 |
| NEXUSINDEX_WORKER_ENABLED | true | 是否启动 outbox worker。 |
| NEXUSINDEX_WORKER_BATCH_SIZE | 200 | 每批领取事件数，最大 500。 |
| NEXUSINDEX_WORKER_POLL_INTERVAL_MS | 2000 | 空闲轮询间隔，单位 ms。 |
| NEXUSINDEX_WORKER_LOCK_TTL_MS | 120000 | worker 崩溃后锁可重新领取的时间。 |
| NEXUSINDEX_WORKER_RETRY_DELAY_MS | 30000 | 失败事件的重试间隔。 |
| NEXUSINDEX_WORKER_MAX_ATTEMPTS | 10 | 单事件最大领取次数。超过后保留失败现场，等待 replay。 |

### 缓存

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| NEXUSINDEX_EDGECACHE_ENABLED | false | 是否启用 EdgeCache L2。关闭时仍可使用进程内 L1。 |
| NEXUSINDEX_EDGECACHE_URL | http://127.0.0.1:4410 | EdgeCache HTTP 地址。 |
| NEXUSINDEX_EDGECACHE_TIMEOUT_MS | 300 | EdgeCache 请求超时。 |
| NEXUSINDEX_EDGECACHE_TTL_SECONDS | 60 | tags 和默认缓存 TTL。 |
| NEXUSINDEX_CACHE_QUERY_TTL_SECONDS | 60 | 有文本查询的 TTL。 |
| NEXUSINDEX_CACHE_FILTER_TTL_SECONDS | 120 | 无文本筛选的 TTL。 |
| NEXUSINDEX_CACHE_HIGHLIGHT_TTL_SECONDS | 300 | 高亮片段 TTL。 |
| NEXUSINDEX_CACHE_NAMESPACE | nexusindex:v2 | 缓存 key 前缀。 |
| NEXUSINDEX_CACHE_L1_MAX_ENTRIES | 2048 | L1 最大条目数。 |

业务代码不直接访问 Redis。需要 Redis 时，应由 EdgeCache 负责其 L2 实现。

### runtime 与评分

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| NEXUSINDEX_RUNTIME_CONFIG_FILE | 空 | runtime config JSON 文件；可用 /api/config/reload 重载。 |
| NEXUSINDEX_CANDIDATE_LIMIT | 1000 | 旧 runtime config 的兼容字段，范围 100..5000；当前最终排序和分页在 SQL 中完成，不依赖固定候选窗口。 |
| NEXUSINDEX_SCORING_TITLE_WEIGHT | 8 | 标题文本权重。 |
| NEXUSINDEX_SCORING_TAGS_WEIGHT | 5 | 标签文本权重。 |
| NEXUSINDEX_SCORING_KEYWORDS_WEIGHT | 3 | 关键词文本权重。 |
| NEXUSINDEX_SCORING_PAYLOAD_WEIGHT | 1 | payload 文本权重。 |
| NEXUSINDEX_SCORING_VIEW_COUNT_WEIGHT | 0.30 | 浏览量热度权重。 |
| NEXUSINDEX_SCORING_DOWNLOAD_COUNT_WEIGHT | 0.50 | 下载量热度权重。 |
| NEXUSINDEX_SCORING_LIKE_COUNT_WEIGHT | 0.70 | 点赞量热度权重。 |
| NEXUSINDEX_SCORING_TIME_DECAY_FACTOR | 30 | 新鲜度衰减天数。 |

runtime JSON 还支持 text_score_weight、popularity_score_weight、freshness_score_weight 和 query.synonyms。POST /api/config 的更新只存在于当前进程；需要重启后保留时，请写入配置文件或环境变量。

## Schema migration

迁移文件以不可变 SQL 嵌入二进制，当前目标版本为 v4：

1. v1 创建 search_index、tag_index 和 index_sync_log。
2. v2 加入 index_generations，并把索引行绑定到 generation。
3. v3 加入 generation 约束。
4. v4 启用 pg_trgm 和受限 fuzzy 查询所需的标题 trigram 索引。

发布或升级时由 migration owner 执行：

~~~bash
./nexusindex migrate status
./nexusindex migrate up
~~~

migrate up 只允许向前升级；不支持 down。数据库版本高于当前二进制、同一版本 SQL hash 不一致或 migration owner 未配置时，命令会失败。常驻服务使用 INDEX_DATABASE_URL 启动，并只验证目标版本，不执行 DDL。

如果启动日志出现 `schema migration required: database=<n> binary=4`（当前生产常见的是 `database=3`），说明 migration 尚未完成。请先确认 migration 命令和常驻服务指向同一个索引库，然后使用 owner 连接执行：

~~~bash
export INDEX_DATABASE_MIGRATION_URL='postgresql://index_owner:password@127.0.0.1:5432/nexusindex'
./nexusindex migrate status
./nexusindex migrate up
./nexusindex migrate status
~~~

第二次 `status` 应显示 `currentVersion: 4`，再启动服务。不要给 `INDEX_DATABASE_URL` 对应的常驻运行账号添加 DDL 权限来绕过迁移流程。

如果 `migrate up` 在 v3 停止，请保留命令的完整错误输出。v4 会创建 `pg_trgm` 扩展和标题 GIN trigram 索引；常见原因是 migration 连接没有数据库 `CREATE` 权限、不是 `search_index` 的 owner，或连接到了另一套数据库。可以由 PostgreSQL 管理员在目标索引库执行 `CREATE EXTENSION IF NOT EXISTS pg_trgm;`，再使用拥有目标表和索引权限的 migration owner 重试，不要手工插入 v4 的 migration 记录。

## HTTP API

除 GET /health 外，以下路由在设置 API token 后都需要鉴权：

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| GET | /health | 同时检查主库和索引库连通性。 |
| GET、POST | /search、/api/search | 搜索，两个路径使用同一契约。 |
| GET | /api/index/status | 当前 generation 的文档数量、实体分布和同步失败统计。 |
| GET、POST | /api/tags | 标签建议、热门标签和标签页分页。 |
| GET、POST、PUT | /api/config | 查看或更新进程内 runtime config。 |
| POST | /api/config/reload | 从 NEXUSINDEX_RUNTIME_CONFIG_FILE 重载配置。 |
| GET | /api/cache/stats | 查看 L1/L2 缓存统计。 |
| POST | /api/cache/prewarm | 预热最多 50 个搜索请求。 |
| PUT、POST、DELETE | /index/{docId}、/api/index/{docId} | 维护单条索引文档；建议只用于管理或迁移。 |
| POST | /api/index/rebuild | 异步启动全量 shadow rebuild，返回 202 和 jobId。 |
| POST | /api/index/rebuild/{entityType}/{entityId} | 异步重建单条实体。 |

### 搜索请求

POST /api/search 请求体示例：

~~~json
{
  "q": "fabric 1.20.1",
  "sort": "relevance",
  "must": [
    { "field": "title", "operator": "phrase", "value": "fabric api" }
  ],
  "should": [
    { "field": "tags", "operator": "prefix", "value": "fabr" },
    { "field": "keywords", "operator": "fuzzy", "value": "fabric" }
  ],
  "must_not": [
    { "field": "_all", "operator": "match", "value": "旧版" }
  ],
  "filters": {
    "entityTypes": ["resource"],
    "platforms": ["java"],
    "minecraftVersion": { "gte": "1.20", "lte": "1.21.1" },
    "downloadCount": { "gte": 10 },
    "updatedAt": { "gte": "2026-01-01T00:00:00Z" }
  },
  "limit": 20,
  "cursor": ""
}
~~~

兼容字段 entityType、categoryId、status 和 tags 会被合并到对应 filters；新接入代码建议直接使用 filters。

查询 clause 支持以下字段和操作符：

| 字段 | 说明 |
| --- | --- |
| _all | title、tags、keywords 和 payload 的合并文本。 |
| title | 只匹配标题。 |
| tags | 只匹配标签。 |
| keywords | 只匹配关键词。 |
| match | 使用 PostgreSQL plainto_tsquery。 |
| phrase | 使用 phraseto_tsquery，要求词序连续。 |
| prefix | 使用 to_tsquery 前缀匹配，值至少 2 个字符。 |
| fuzzy | 使用 pg_trgm similarity，值至少 3 个字符，每次请求最多 2 个 fuzzy clause。 |

must 内的条件使用 AND，should 内的条件使用 OR，must_not 内的条件逐项排除。默认会排除 hidden 和 deleted；显式提供 filters.statuses 后按指定状态匹配。

### 筛选与复杂度上限

服务在进入 SQL 前校验边界，超限返回结构化 400 错误：

| 项目 | 上限 |
| --- | --- |
| 查询字符数 | 256 个 Unicode 字符 |
| 查询词数 | 24 |
| clause 总数 | 32 |
| must_not clause | 8 |
| 单个 filter 数组 | 50 |
| 所有 filter 值合计 | 128 |
| 搜索 limit | 1 到 100 |
| 标签 limit | 1 到 200 |
| 标签 offset | 0 到 10000 |
| cache prewarm 查询数 | 50 |
| HTTP 请求超时 | 默认 5000 ms，可配置 |

数值范围支持 viewCount、likeCount、downloadCount 的 gte、gt、lte、lt；日期范围支持 createdAt、updatedAt 的同名比较符；Minecraft 版本范围接受 major.minor.patch 形式，例如 1.20、1.21.1。

### 响应与 facets

搜索响应包含 items、total、limit、next_cursor、sort、generation_id、facets、candidate_window、exhausted_candidate_set 和 timing。facets 形状如下：

~~~json
{
  "entity": [{ "value": "resource", "count": 42 }],
  "category": [{ "value": "mod", "count": 31 }],
  "tag": [{ "value": "fabric", "count": 18 }]
}
~~~

facet 在当前查询和筛选条件上聚合，不只统计当前页。candidate_window 和 exhausted_candidate_set 是兼容字段；当前搜索使用 SQL keyset 分页，exhausted_candidate_set 固定为 false。设置 explain=true 时，计时和评分信息放在 debug 中。

同一搜索响应的 generation、total、items 和所有 facets 使用一个 PostgreSQL 只读 repeatable-read 事务；worker 在查询中途提交的变更从下次搜索开始可见。此保证限于单次搜索，不跨分页请求或缓存有效期。

`pg_ms` 逐段累加索引事务的获取与开启、generation/count 查询、结果查询与完整取行、三个 facets 查询与取行、提交等数据库调用耗时；SQL 评分也计入其中。两次数据库调用之间的查询哈希、分词、filters/clauses 与 SQL 构造等 Go 准备工作不计入 `pg_ms`，目前不单独暴露其计时。`scoring_ms` 只包含 Go 中的 payload 解码、matched fields、评分说明和分页元数据组装；`highlight_ms` 只包含高亮生成。这些字段并不相加构成整个请求的耗时。

只有 `explain=true` 会额外查询主库 outbox 延迟。查询成功时，`debug.index_lag_ms`、`debug.timing.index_lag_ms` 及 `X-NexusIndex-Index-Lag-Ms` 返回毫秒数；没有积压明确返回 `0`。查询失败（包括该观测查询超时）不会丢弃已完成的搜索，而是省略这些字段和响应头，表示数据不可用。普通搜索不查询或返回 lag。延迟观测在索引事务提交后执行，不属于索引快照或 `pg_ms`；缓存命中时保留生成该响应时的观测值。

### 排序契约

- relevance：要求有文本查询。按 PostgreSQL 文本相关性、runtime 文本权重、热度和新鲜度综合评分。
- latest：按 updated_at，缺失时回退到 created_at 或 indexed_at 降序；无文本查询时默认使用此排序。
- popular：按浏览、下载、点赞和 weight 的热度分，加上新鲜度分；有文本查询时仍先应用文本召回条件。

相同排序值按时间、entity_type、entity_id 继续排序，保证结果顺序稳定。latest 的 item score 为 0，这是因为该模式只表达时间顺序。

### Cursor 分页

首次请求不传 cursor；后续请求将响应的 next_cursor 原样放回请求体或 query string。cursor 由服务使用 HMAC-SHA256 签名，并绑定排序、查询条件、generation 和 runtime config 版本。generation 切换或 runtime config 更新后，旧 cursor 可能返回 invalid_cursor，客户端应从第一页重新请求。

cursor secret 只需要在 NexusIndex 服务实例之间共享。论坛后端或 TypeScript client 不需要、也不应该配置这个 secret；它们只保存 API token，并透传 cursor。

## 索引同步与重建

### Outbox worker

正常写入路径不要求业务服务直接调用索引写接口：

1. 业务事务在主库写入 search_index_events。
2. worker 按 event_order 领取事件，并按 entity_type + entity_id 合并同批变更。
3. worker 读取实体最新快照，执行版本门禁的 upsert/delete。
4. 成功后标记 processed_at；失败记录 attempts、failed_at 和 last_error，按配置退避重试。
5. 索引变化后重建 tag_index 并使搜索缓存版本递增。

该链路提供 at-least-once，不提供 exactly-once。重复 upsert/delete 必须可安全重放，source_version 用于阻止旧事件覆盖新快照。

### Shadow rebuild

POST /api/index/rebuild 是异步操作，流程为：

1. 记录当前 outbox watermark，创建 building generation。
2. 从 source adapter 批量构建所有实体和标签索引。
3. 校验文档数量以及实体主键完整性。
4. 追赶 watermark 之后产生的事件，并在切换前再次追赶。
5. 在 PostgreSQL advisory lock 下把旧 generation 标记为 retired，把新 generation 原子标记为 active。

搜索、标签和状态接口只读取 active generation。构建失败时新 generation 标记为 failed，旧 active generation 保持服务，不会出现半成品索引对外可见。全量重建和 worker 共用 generation switch lock；同一时间只允许一个全量重建。

单实体 rebuild 会直接更新当前 active generation，适合修复单条数据，不等价于全量 shadow rebuild。

## Source adapter 边界

内置 adapter 将以下实体映射到 NexusMC 论坛表：

| entityType | 默认来源 |
| --- | --- |
| resource | Resource |
| post | Post |
| server | PlayerServer |
| video | Video |
| document | CustomPage |
| user | User |
| organization | Organization |
| tag | Tag |
| official_tag | OfficialTag 与 OfficialTagGroup |

适配器会把标题、标签、关键词、统计字段、可公开 payload 和时间字段写入统一 SearchDocument。用户索引只允许公开资料快照；邮箱、手机号、密码、token 等敏感字段不得进入 payload。

接入其他系统时，应替换 source 查询和文档映射，并保留以下契约：

- 每个实体都有稳定的 entity_type 与 entity_id。
- source 事件提供单调递增的 event_order。
- delete 事件可以幂等重放。
- 文档中的时间、状态和标签字段含义稳定。

## TypeScript client（可选）

仓库可同时发布 client-packages/nexusindex-client，由服务端应用调用 HTTP API。client 使用以下环境变量：

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| NEXUSINDEX_URL | http://127.0.0.1:4412 | NexusIndex 基础地址。 |
| NEXUSINDEX_TOKEN | 空 | 发送为 Authorization: Bearer；它是 API token，不是 cursor secret。 |
| NEXUSINDEX_TIMEOUT_MS | 由 transport 决定 | HTTP 请求超时。 |

主要方法包括 search、getTags、getIndexStatus、rebuildIndex、rebuildEntityIndex、runtime config 和 cache 管理方法。浏览器端不应直接使用管理 token，也不应接触 cursor secret。

当前 TypeScript package 在本工程中仍标记为 private。拆分为独立仓库时，需要单独决定 npm 包名、版本策略、private/publishConfig 和 API 兼容策略。

## 开发与验证

~~~bash
go test -count=1 ./...
go vet ./...
go build ./cmd/nexusindex
~~~

迁移集成测试在未设置 NEXUSINDEX_TEST_DATABASE_URL 时会跳过；配置可用 PostgreSQL 后再运行：

~~~bash
NEXUSINDEX_TEST_DATABASE_URL='postgresql://tester:password@127.0.0.1:5432/nexusindex_test' \
  go test -count=1 ./internal/migrations
~~~

提交 migration 后必须保持旧版本 SQL 不变，因为服务会校验每个已安装 migration 的 definition hash。修改已有 schema 时，请新增递增版本的 SQL 文件和对应测试。

## 生产运行建议

- 用反向代理提供 TLS、访问日志和限流，不要把索引库直接暴露到公网。
- API token 和 cursor secret 使用独立的随机长字符串，并通过 secret manager 注入。
- INDEX_DATABASE_URL 使用无 DDL 权限的运行账号，INDEX_DATABASE_MIGRATION_URL 只在迁移命令环境中出现。
- 多副本部署时，所有副本使用相同 cursor secret；切换 generation 或更新评分配置后，客户端应允许 cursor 失效并重新搜索。
- 监控 /health、/api/index/status、failedJobCount、runningJobCount、recentSyncTime 和 timing.index_lag_ms。
- 生产发布顺序固定为：构建二进制、执行 migration、确认 status、再重启或 reload 服务。
- 业务列表和搜索应保留降级路径；NexusIndex 不可用时，主库仍应能够提供基本业务功能。

## 独立开源仓库发布前清单

当前代码仍位于 AVMCBBS monorepo。迁移到独立仓库时，至少需要完成以下工作：

- 修改 go.mod module path，并批量更新 Go import；移除当前 monorepo 路径依赖。
- 将 packages/edgecache-go-client 一并迁移，或发布为独立 Go module，移除本地 replace。
- 决定 source adapter 是继续保留 NexusMC 实现，还是改为接口加可选 adapter；不要在 README 中把论坛 schema 描述成通用协议。
- 如果发布 TypeScript client，移除 package 的 private: true，确定 npm scope、版本和构建发布流程。
- 增加 CI：Go test、vet、build、migration hash 检查，以及带 PostgreSQL 的集成测试。
- 增加 LICENSE、CONTRIBUTING.md、SECURITY.md、变更日志和兼容性策略。当前工程没有声明可直接复用的开源许可证，发布前必须由仓库所有者补充。
- 为公开 API 固化版本策略；修改查询字段、facet 形状、cursor 或 migration 契约时，应提供迁移说明。

## 贡献

提交改动前请运行开发与验证命令，并在 PR 中说明是否涉及 API、索引 schema、source adapter 或权限边界。涉及 migration 的改动必须包含升级路径和回滚影响说明；不接受用低版本覆盖高版本或重写已发布 migration SQL。

## License

当前工程没有包含 LICENSE 文件。独立仓库公开前请补充实际许可证，并将本节改为对应的 SPDX 名称和链接。
