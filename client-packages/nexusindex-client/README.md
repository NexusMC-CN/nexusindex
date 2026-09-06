# @nexusmc/nexusindex-client

HTTP JSON client for calling the `nexusindex` service from the forum backend.

The client keeps backend business code from manually building NexusIndex URLs.

## Environment

- `NEXUSINDEX_URL`: NexusIndex base URL, defaults to `http://127.0.0.1:4412`.
- `NEXUSINDEX_TOKEN`: optional API token sent as `Authorization: Bearer <token>`.
- `NEXUSINDEX_TIMEOUT_MS`: request timeout in milliseconds, defaults to the transport default.

## Usage

```ts
import { createNexusIndexClientFromEnv } from '@nexusmc/nexusindex-client';

const nexusIndex = createNexusIndexClientFromEnv();

const results = await nexusIndex.search({
  q: 'fabric',
  entityType: 'resource',
  limit: 20,
});

const tags = await nexusIndex.getTags({
  sort: 'count',
  limit: 50,
  offset: 0,
});
```

Available methods:

- `search()`
- `getTags()` for hot tags, suggestions, tag pages, and tag aggregation
- `getIndexStatus()`
- `rebuildIndex()`
- `rebuildEntityIndex()`
