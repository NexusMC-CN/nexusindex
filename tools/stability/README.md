# NexusIndex Stability Tools

本目录提供本地稳定性验证 CLI，只用于压测和验收，不修改 NexusIndex 核心代码。

## Worker 稳定性

先启动 NexusIndex 服务，并确保：

- `NEXUSINDEX_WORKER_ENABLED=true`
- `MAIN_DATABASE_READONLY_URL` 指向包含 `search_index_events` 的论坛 PostgreSQL
- worker 账号有 `search_index_events` 的 `SELECT/INSERT/UPDATE` 权限

运行：

```powershell
go run ./tools/stability worker --events 10000 --database-url "postgresql://user:pass@127.0.0.1:5432/nexusmc"
go run ./tools/stability worker --events 50000 --watch-seconds 120
go run ./tools/stability worker --events 100000 --require-drained
```

输出 JSON summary，包含：

- inserted / distinct_docs
- processed / failed / backlog / locked
- action/entity 分布
- backlog_growing
- possible_lost_events

## 查询分布测试

运行：

```powershell
go run ./tools/stability query --base-url "http://127.0.0.1:4412" --per-class 200 --concurrency 8
go run ./tools/stability query --per-class 1000 --token "$env:NEXUSINDEX_TOKEN"
```

内置 query 分类：

- exact_title：`fabric 1.20.1` / `optifine 光影`
- tag：`mod` / `plugin` / `resourcepack`
- chinese：`模组` / `插件` / `资源包`
- version：`1.20.1` / `1.21`
- loader_domain：`fabric mod` / `paper plugin`
- garbage：`aaaa` / `???` / `hello`

输出 JSON summary，包含：

- latency p50/p95/p99
- cache hit ratio
- pg_ms / scoring_ms / highlight_ms
- candidate_size
- top10 稳定性检查
- cursor 翻页稳定性检查

