export interface NexusIndexTransport {
  requestJson<T>(path: string, init?: RequestInit): Promise<T>;
}
