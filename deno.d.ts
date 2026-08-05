/* eslint-disable @typescript-eslint/no-explicit-any */
declare namespace Deno {
  export interface KvU64 {
    value: bigint;
  }

  export interface KvEntry<T = unknown> {
    key: unknown[];
    value: T;
    versionstamp: string;
  }

  export interface KvCommitResult {
    ok: boolean;
    versionstamp: string;
  }

  export interface AtomicOperation {
    set(key: unknown[], value: unknown, options?: { expireIn?: number }): this;
    sum(key: unknown[], n: bigint): this;
    min(key: unknown[], n: bigint): this;
    max(key: unknown[], n: bigint): this;
    delete(key: unknown[]): this;
    check(...checks: any[]): this;
    commit(): Promise<KvCommitResult>;
  }

  export interface KvListOptions {
    limit?: number;
    cursor?: string;
    reverse?: boolean;
    consistency?: "strong" | "eventual";
  }

  export interface Kv {
    get<T = unknown>(key: unknown[]): Promise<KvEntry<T>>;
    getMany<T extends readonly unknown[]>(keys: { [K in keyof T]: unknown[] }): Promise<{ [K in keyof T]: KvEntry<T[K]> }>;
    set(key: unknown[], value: unknown, options?: { expireIn?: number }): Promise<KvCommitResult>;
    delete(key: unknown[]): Promise<void>;
    list<T = unknown>(selector: { prefix: unknown[] }, options?: KvListOptions): AsyncIterableIterator<KvEntry<T>>;
    atomic(): AtomicOperation;
    watch<T extends readonly unknown[]>(keys: { [K in keyof T]: unknown[] }): AsyncIterableIterator<{ [K in keyof T]: KvEntry<T[K]> }>;
    close(): void;
  }

  export function openKv(path?: string): Promise<Kv>;

  export function serve(handler: (req: Request, info?: any) => Response | Promise<Response>): any;
  export function serve(options: { port?: number; hostname?: string }, handler: (req: Request, info?: any) => Response | Promise<Response>): any;

  export function cron(name: string, spec: string, handler: () => void | Promise<void>): void;

  export function upgradeWebSocket(req: Request, options?: any): {
    socket: WebSocket;
    response: Response;
  };

  export const env: {
    get(key: string): string | undefined;
    set(key: string, value: string): void;
    delete(key: string): void;
    toObject(): Record<string, string>;
  };

  export const version: {
    deno: string;
    v8: string;
    typescript: string;
  };
}
