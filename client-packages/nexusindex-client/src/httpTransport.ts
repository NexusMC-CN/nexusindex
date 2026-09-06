import type { NexusIndexTransport } from './transport.js';

export interface NexusIndexHttpTransportOptions {
  baseUrl: string;
  token?: string;
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
}

export class NexusIndexError extends Error {
  readonly status: number;
  readonly code: string;

  constructor(status: number, code: string, message: string) {
    super(message);
    this.name = 'NexusIndexError';
    this.status = status;
    this.code = code;
  }
}

export class NexusIndexHttpTransport implements NexusIndexTransport {
  private readonly baseUrl: string;
  private readonly token: string;
  private readonly fetchImpl: typeof fetch;
  private readonly timeoutMs: number;

  constructor(options: NexusIndexHttpTransportOptions) {
    this.baseUrl = options.baseUrl.replace(/\/+$/, '');
    this.token = String(options.token || '').trim();
    this.fetchImpl = options.fetchImpl || fetch;
    this.timeoutMs = options.timeoutMs ?? 5000;
  }

  async requestJson<T>(path: string, init: RequestInit = {}): Promise<T> {
    const headers = new Headers(init.headers || {});
    headers.set('accept', 'application/json');
    if (init.body !== undefined && !headers.has('content-type')) {
      headers.set('content-type', 'application/json');
    }
    if (this.token) {
      headers.set('authorization', `Bearer ${this.token}`);
    }

    const response = await this.fetchImpl(`${this.baseUrl}${path}`, this.withTimeout({
      ...init,
      headers,
    }));

    if (!response.ok) {
      throw await this.toError(response);
    }
    return (await response.json()) as T;
  }

  private withTimeout(init: RequestInit): RequestInit {
    if (this.timeoutMs <= 0 || init.signal) return init;
    return {
      ...init,
      signal: AbortSignal.timeout(this.timeoutMs),
    };
  }

  private async toError(response: Response): Promise<NexusIndexError> {
    try {
      const data = await response.json() as { error?: { code?: string; message?: string } };
      return new NexusIndexError(
        response.status,
        data.error?.code || 'nexusindex_request_failed',
        data.error?.message || `nexusindex request failed with status ${response.status}`,
      );
    } catch {
      return new NexusIndexError(
        response.status,
        'nexusindex_request_failed',
        `nexusindex request failed with status ${response.status}`,
      );
    }
  }
}
