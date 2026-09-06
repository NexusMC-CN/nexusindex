export * from './httpTransport.js';
export * from './transport.js';

import { NexusIndexHttpTransport } from './httpTransport.js';
import type { NexusIndexTransport } from './transport.js';

export type NexusIndexEntityType =
  | 'resource'
  | 'post'
  | 'server'
  | 'video'
  | 'document'
  | 'user'
  | 'organization'
  | 'tag'
  | 'official_tag';

export interface NexusIndexClientOptions {
  baseUrl?: string;
  token?: string;
  timeoutMs?: number;
  fetchImpl?: typeof fetch;
  transport?: NexusIndexTransport;
}

export interface NexusIndexEnv {
  NEXUSINDEX_URL?: string;
  NEXUSINDEX_TOKEN?: string;
  NEXUSINDEX_TIMEOUT_MS?: string;
}

export interface SearchParams {
  q?: string;
  entityType?: NexusIndexEntityType;
  categoryId?: string;
  status?: string;
  tags?: string[];
  limit?: number;
  cursor?: string;
  explain?: boolean;
}

export interface SearchItem {
  id: string;
  entityType: NexusIndexEntityType | string;
  entityId: string;
  title: string;
  score: number;
  matched_fields: string[];
  highlight: Record<string, unknown>;
  explanation?: Record<string, unknown>;
  status: string;
  categoryId?: string;
  authorId?: string;
  tags: string[];
  keywords: string[];
  weight: number;
  viewCount: number;
  likeCount: number;
  downloadCount: number;
  payload: Record<string, unknown>;
  createdAt: string;
  updatedAt: string;
  indexedAt: string;
}

export interface SearchResult {
  items: SearchItem[];
  total?: number;
  limit: number;
  offset?: number;
  next_cursor?: string;
  candidate_window?: number;
  exhausted_candidate_set?: boolean;
  timing?: {
    pg_ms: number;
    scoring_ms: number;
    highlight_ms: number;
  };
}

export interface TagParams {
  q?: string;
  sort?: 'count' | 'name' | 'latest';
  limit?: number;
  offset?: number;
}

export interface TagItem {
  tag: string;
  entityTypes: string[];
  totalCount: number;
  resourceCount: number;
  postCount: number;
  serverCount: number;
  videoCount: number;
  documentCount: number;
  userCount: number;
  organizationCount: number;
  tagCount: number;
  officialTagCount: number;
  categoryCounts: Record<string, number>;
  payload: Record<string, unknown>;
  updatedAt: string;
}

export interface TagResult {
  items: TagItem[];
  total?: number;
  limit: number;
  offset?: number;
  sort?: 'count' | 'name' | 'latest';
}

export interface IndexStatus {
  total: number;
  byEntityType: Record<string, number>;
  recentSyncTime?: string;
  failedJobCount: number;
  runningJobCount: number;
  lastFailedMessage?: string;
}

export interface RebuildResult {
  ok: boolean;
  jobId: number;
}

export interface RuntimeConfig {
  scoring: Record<string, number>;
  query: {
    synonyms?: Record<string, string[]>;
  };
  candidate_limit: number;
}

export interface CacheStats {
  l1Hits: number;
  l1Misses: number;
  l2Hits: number;
  l2Misses: number;
  sets: number;
  entries: number;
}

export class NexusIndexClient {
  private readonly transport: NexusIndexTransport;

  constructor(options: NexusIndexClientOptions = {}) {
    this.transport = options.transport || new NexusIndexHttpTransport({
      baseUrl: options.baseUrl || 'http://127.0.0.1:4412',
      token: options.token,
      timeoutMs: options.timeoutMs,
      fetchImpl: options.fetchImpl,
    });
  }

  search(params: SearchParams = {}): Promise<SearchResult> {
    return this.postJson<SearchResult>('/api/search', normalizeSearchParams(params));
  }

  getTags(params: TagParams = {}): Promise<TagResult> {
    return this.postJson<TagResult>('/api/tags', normalizeTagParams(params));
  }

  getIndexStatus(): Promise<IndexStatus> {
    return this.transport.requestJson<IndexStatus>('/api/index/status');
  }

  rebuildIndex(): Promise<RebuildResult> {
    return this.postJson<RebuildResult>('/api/index/rebuild', {});
  }

  rebuildEntityIndex(entityType: NexusIndexEntityType, entityId: string): Promise<RebuildResult> {
    return this.postJson<RebuildResult>(
      `/api/index/rebuild/${encodeURIComponent(entityType)}/${encodeURIComponent(entityId)}`,
      {},
    );
  }

  getRuntimeConfig(): Promise<RuntimeConfig> {
    return this.transport.requestJson<RuntimeConfig>('/api/config');
  }

  updateRuntimeConfig(config: Partial<RuntimeConfig>): Promise<RuntimeConfig> {
    return this.postJson<RuntimeConfig>('/api/config', config);
  }

  reloadRuntimeConfig(): Promise<RuntimeConfig> {
    return this.postJson<RuntimeConfig>('/api/config/reload', {});
  }

  getCacheStats(): Promise<CacheStats> {
    return this.transport.requestJson<CacheStats>('/api/cache/stats');
  }

  prewarmCache(queries: SearchParams[]): Promise<{ ok: boolean; warmed: number }> {
    return this.postJson<{ ok: boolean; warmed: number }>('/api/cache/prewarm', { queries });
  }

  private postJson<T>(path: string, payload: unknown): Promise<T> {
    return this.transport.requestJson<T>(path, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(payload),
    });
  }
}

export function createNexusIndexClientFromEnv(env: NexusIndexEnv = process.env): NexusIndexClient {
  return new NexusIndexClient({
    baseUrl: env.NEXUSINDEX_URL || 'http://127.0.0.1:4412',
    token: env.NEXUSINDEX_TOKEN,
    timeoutMs: parseTimeout(env.NEXUSINDEX_TIMEOUT_MS),
  });
}

function normalizeSearchParams(params: SearchParams): SearchParams {
  return {
    q: params.q?.trim() || '',
    entityType: params.entityType,
    categoryId: params.categoryId?.trim() || '',
    status: params.status?.trim() || '',
    tags: params.tags?.map((tag) => tag.trim()).filter(Boolean),
    limit: clampNumber(params.limit, 20, 1, 100),
    cursor: params.cursor?.trim() || '',
    explain: params.explain === true,
  };
}

function normalizeTagParams(params: TagParams): TagParams {
  return {
    q: params.q?.trim() || '',
    sort: normalizeTagSort(params.sort),
    limit: clampNumber(params.limit, 50, 1, 200),
    offset: clampNumber(params.offset, 0, 0, 10000),
  };
}

function normalizeTagSort(value: TagParams['sort'] | undefined): TagParams['sort'] {
  if (value === 'name' || value === 'latest') return value;
  return 'count';
}

function parseTimeout(value: string | undefined): number | undefined {
  if (!value) return undefined;
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) return undefined;
  return parsed;
}

function clampNumber(value: number | undefined, fallback: number, min: number, max: number): number {
  if (!Number.isFinite(value)) return fallback;
  const parsed = Math.trunc(Number(value));
  if (parsed < min) return min;
  if (parsed > max) return max;
  return parsed;
}
