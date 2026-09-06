# nexusindex

`nexusindex` 是 NexusMC 的独立索引与数据聚合微服务。它通过最小权限连接读取论坛主库、更新索引事件状态，并写入独立 PostgreSQL 索引库，用于搜索、标签、分类聚合和内容快照等低风险加速能力。

论坛主库仍然是唯一权威数据源。索引库是辅助库，可以删除后从主库全量重建。

## 环境变量

- `MAIN_DATABASE_READONLY_URL`：论坛主库同步连接串。变量名为兼容历史配置保留；账号只读取业务表，并对 `search_index_events` 拥有 `SELECT` 和消费状态列的 `UPDATE` 权限。
- `INDEX_DATABASE_URL`：独立索引库 PostgreSQL 连接串。
- `NEXUSINDEX_PORT`：HTTP 服务端口，默认 `4412`。
- `NEXUSINDEX_API_TOKEN`：可选 API token；设置后 `/api/*` 路由需要鉴权。
- `NEXUSINDEX_REQUEST_TIMEOUT_MS`：HTTP handler 超时时间，默认 `5000`。
- `NEXUSINDEX_EDGECACHE_ENABLED`：是否启用 EdgeCache read-through 缓存，默认 `false`。
- `NEXUSINDEX_EDGECACHE_URL`：EdgeCache HTTP 地址，默认 `http://127.0.0.1:4410`。
- `NEXUSINDEX_EDGECACHE_TTL_SECONDS`：默认缓存 TTL，默认 `60`。
- `NEXUSINDEX_CACHE_QUERY_TTL_SECONDS`：query cache TTL，默认 `60`。
- `NEXUSINDEX_CACHE_FILTER_TTL_SECONDS`：filter cache TTL，默认 `120`。
- `NEXUSINDEX_CACHE_HIGHLIGHT_TTL_SECONDS`：highlight cache TTL，默认 `300`。
- `NEXUSINDEX_EDGECACHE_TIMEOUT_MS`：EdgeCache 请求超时，默认 `300`。
- `NEXUSINDEX_CACHE_NAMESPACE`：缓存 key 命名前缀，默认 `nexusindex:v2`。
- `NEXUSINDEX_CACHE_L1_MAX_ENTRIES`：L1 memory cache 最大条目数，默认 `2048`。
- `NEXUSINDEX_WORKER_ENABLED`：是否消费索引事件，默认 `true`。
- `NEXUSINDEX_WORKER_BATCH_SIZE`：每批领取事件数，默认 `200`，最大 `500`。
- `NEXUSINDEX_WORKER_POLL_INTERVAL_MS`：空闲轮询间隔，默认 `2000`。
- `NEXUSINDEX_WORKER_LOCK_TTL_MS`：崩溃后事件锁可被重新领取的时间，默认 `120000`。
- `NEXUSINDEX_WORKER_RETRY_DELAY_MS`：失败事件的重试间隔，默认 `30000`。
- `NEXUSINDEX_WORKER_MAX_ATTEMPTS`：单个事件最大领取次数，默认 `10`。
- `NEXUSINDEX_RUNTIME_CONFIG_FILE`：可选 runtime config JSON 文件路径。
- `NEXUSINDEX_CANDIDATE_LIMIT`：PostgreSQL 候选集上限，默认 `1000`。
- `NEXUSINDEX_SCORING_TITLE_WEIGHT`
- `NEXUSINDEX_SCORING_TAGS_WEIGHT`
- `NEXUSINDEX_SCORING_KEYWORDS_WEIGHT`
- `NEXUSINDEX_SCORING_PAYLOAD_WEIGHT`
- `NEXUSINDEX_SCORING_VIEW_COUNT_WEIGHT`
- `NEXUSINDEX_SCORING_DOWNLOAD_COUNT_WEIGHT`
- `NEXUSINDEX_SCORING_LIKE_COUNT_WEIGHT`
- `NEXUSINDEX_SCORING_TIME_DECAY_FACTOR`

`MAIN_DATABASE_READONLY_URL` 应使用最小权限账号：论坛内容表只授予 `SELECT`，`search_index_events` 授予 `SELECT`，并仅允许更新 `locked_at/attempts/processed_at/failed_at/last_error/updated_at`。事件表及索引必须由论坛 Prisma schema/部署迁移提前创建；NexusIndex 运行账号不需要主库 DDL 权限。

## 运行

```bash
go run ./cmd/nexusindex
```

## HTTP API

- `GET /health`
- `GET /search?q=keyword&entityType=resource&limit=20&cursor=...`
- `POST /search`
- `GET /api/index/status`
- `GET /api/search?q=keyword&entityType=resource&limit=20&cursor=...`
- `POST /api/search`
- `GET /api/config`
- `POST /api/config`
- `POST /api/config/reload`
- `GET /api/cache/stats`
- `POST /api/cache/prewarm`
- `PUT /index/{docId}`
- `POST /index/{docId}`
- `DELETE /index/{docId}`
- `GET /api/tags?q=fabric&limit=50`
- `POST /api/tags`
- `POST /api/index/rebuild`
- `POST /api/index/rebuild/{entityType}/{entityId}`

`POST /api/search` 请求体：

```json
{
  "q": "fabric",
  "entityType": "resource",
  "categoryId": "",
  "status": "",
  "tags": ["mod"],
  "limit": 20,
  "cursor": ""
}
```

搜索响应包含 `items`、`total`、`limit`、`next_cursor`、`candidate_window`、`exhausted_candidate_set`、`timing`。V2 使用 cursor pagination，不在核心搜索链路使用 SQL `OFFSET`。cursor 在当前 candidate window 内生效；如果 `exhausted_candidate_set=true`，表示还有总命中但当前候选窗口已耗尽。`items` 中包含 `id`、`score`、`matched_fields`、`highlight`、原始 `payload`，请求传 `explain: true` 时额外返回 `explanation`。

## V2 Runtime

V2 将 NexusIndex 升级为轻量级 Search Engine Runtime：

- Query Pipeline：normalize、Minecraft 领域同义词扩展、轻量中文分词、版本/加载器 boost 识别、tsquery 文本构建。
- Scoring Config：title/tags/keywords/payload、view/download/like、time decay 都支持 runtime 配置。
- Cache：L1 memory cache 优先，L2 通过 EdgeCacheAdapter 访问 EdgeCache；业务代码不直接访问 Redis。
- PostgreSQL：固定候选集上限，`search_vector` GIN 召回，active partial index，Go 层重排。

runtime config 示例：

```json
{
  "candidate_limit": 1000,
  "scoring": {
    "title_weight": 8,
    "tags_weight": 5,
    "keywords_weight": 3,
    "payload_weight": 1,
    "view_count_weight": 0.3,
    "download_count_weight": 0.5,
    "like_count_weight": 0.7,
    "time_decay_factor": 30
  },
  "query": {
    "synonyms": {
      "fabric": ["fabric loader", "fabric api"],
      "模组": ["mod", "mods"]
    }
  }
}
```

热更新：

```bash
curl -X POST http://127.0.0.1:4412/api/config \
  -H 'content-type: application/json' \
  -d '{"scoring":{"title_weight":10}}'
```

热点预热：

```json
{
  "queries": [
    { "q": "fabric 1.20.1", "entityType": "resource", "limit": 20 },
    { "q": "forge 光影", "entityType": "resource", "limit": 20 }
  ]
}
```

`PUT /index/{docId}` 请求体：

```json
{
  "entityType": "resource",
  "title": "Fabric 模组加载器",
  "status": "active",
  "categoryId": "mod",
  "tags": ["fabric", "mod"],
  "keywords": ["minecraft", "loader"],
  "viewCount": 1200,
  "downloadCount": 300,
  "likeCount": 80,
  "payload": {
    "description": "轻量级 Minecraft 模组加载器"
  }
}
```

`POST /api/tags` 请求体：

```json
{
  "q": "fabric",
  "sort": "count",
  "limit": 50,
  "offset": 0
}
```

`sort` 支持 `count`、`name`、`latest`。标签响应包含 `items`、`total`、`limit`、`offset`、`sort`，可用于热门标签、标签建议、全部标签页和后续标签聚合视图。

如果设置了 `NEXUSINDEX_API_TOKEN`，`/api/*` 路由必须携带以下任一请求头：

- `Authorization: Bearer <token>`
- `X-NexusIndex-Token: <token>`

错误响应格式：

```json
{
  "error": {
    "code": "invalid_request",
    "message": "invalid json"
  }
}
```

当前支持的统一搜索索引实体类型：

- `resource`
- `post`
- `server`
- `video`
- `document`
- `user`
- `organization`
- `tag`
- `official_tag`

这些实体统一写入 `search_index`。其中用户索引只包含公开资料快照，不包含邮箱、手机号、密码、Token 等敏感字段。

## 索引表

服务启动时会在 `INDEX_DATABASE_URL` 指向的数据库中创建：

- `search_index`
- `tag_index`
- `index_sync_log`

`search_index` 会把高频筛选字段单独建列，把作者快照、分类快照、用户公开资料、组织公开资料、平台版本、SEO 信息和统计快照等复杂信息放入 JSONB `payload`。

`tag_index` 聚合用户自行输入的标签、官方标签以及内容索引中的标签关系，用于热度、关联内容数量、分类聚合和标签页分页。`nexusindex` 不是“搜索专用服务”，而是可重建的索引与聚合内核；搜索、标签页、标签建议、分类聚合和内容快照都可以通过它加速。

## EdgeCache 接入

第一阶段不让论坛后端参与索引写入或缓存编排。`nexusindex` 使用本地包 `packages/edgecache-go-client` 通过 HTTP 调用 EdgeCache，不链接 EdgeCache 运行时代码。

推荐读取路径：

1. 论坛后端通过 `@nexusmc/nexusindex-client` 请求 `nexusindex`。
2. `nexusindex` 使用稳定 key 查询 EdgeCache，例如 `nexusindex:v2:query:{hash}`、`nexusindex:v2:filter:{hash}` 或 `nexusindex:v2:tags:{hash}`。
3. EdgeCache 未命中时，`nexusindex` 查询 PostgreSQL 索引库，并用短 TTL 写回 EdgeCache。
4. 如果 `nexusindex` 不可用，论坛后端仍可保留现有搜索/列表逻辑作为兜底。

## Index Sync

稳定化阶段后，论坛写入路径不直接调用 NexusIndex 更新接口。论坛后端统一写入 `search_index_events`：

1. PostgreSQL 使用行级数据库触发器，在 forum create/update/delete 的同一事务内写入 `search_index_events`；SQLite 开发环境保留 Prisma middleware 兼容路径。
2. NexusIndex worker 批量拉取未处理事件，按 `entity_type + entity_id` 合并为最后一次变更。
3. 每条事件携带数据库生成的单调 `event_order`；worker 从论坛主库读取最新实体快照，并把该序号作为索引文档的 `source_version`。
4. `search_index` 的 upsert/delete 只有在事件版本不小于当前 `source_version` 时才能生效，因此旧 worker 晚完成也不能覆盖新快照。
5. 事件处理成功后才写入 `processed_at`；失败时记录 `attempts/failed_at/last_error`，按配置退避重试，达到最大尝试次数后保留现场等待人工 replay。
6. 每批索引变更后重建 `tag_index` 并触发搜索缓存 version bump，避免 stale search result。

该链路提供 **at-least-once**，不提供 exactly-once。worker 可能在索引写成功、`processed_at` 尚未提交时崩溃，同一事件会被重新领取。重复 upsert/delete 必须是幂等的，`source_version` 同时负责拒绝乱序旧写入。Outbox 保证事件至少投递一次，NexusIndex 通过幂等操作和版本门禁保证最终一致性。

事件支持 replay。普通已处理事件将目标事件的 `processed_at` 置空即可；达到最大尝试次数的事件还需要把 `attempts` 重置为 `0`，并清空 `failed_at/locked_at`。生产环境建议给 NexusIndex 主库同步账号最小权限：内容表 `SELECT`，`search_index_events` 的 `SELECT` 和消费状态列 `UPDATE`。

不要让论坛后端直接写索引库，也不要让常规索引查询通过论坛后端代理。

PostgreSQL Outbox 触发器覆盖 `Resource/Post/PlayerServer/Video/CustomPage/User/Organization/Tag/OfficialTag`。触发器通过显式数据库 migration 安装，论坛后端启动时只校验 migration 版本、定义 hash、函数版本标记、`event_order` 列和触发器集合，不再刷新定义。

部署前在 `apps/server` 执行 `npm run db:search-outbox:install`。migration 只允许升级，拒绝用低版本覆盖高版本，也拒绝同一版本号对应不同定义。生产环境应使用独立 migration owner 执行安装，论坛运行账号只保留业务 DML、`search_index_events` 的 `INSERT`、序列 `USAGE` 以及 migration 元数据 `SELECT` 权限，从权限层阻止旧版本实例回写触发器。
