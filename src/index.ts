/**
 * SessionAnalytics service: offline aggregation of cross-session usage
 * statistics into a derived SQLite read model, plus read-only query access.
 *
 * The service owns database opening (lazy unless openAt: 'startup'), the
 * offline ingest orchestration, and the query surface consumed by the Web
 * panel's exact Fetch route. Zero message bodies are ever stored or served.
 *
 * @module session-analytics
 */

import { join } from 'node:path'
import type { DatabaseSync } from 'node:sqlite'
import { Context, Service } from '@deepseek-ai/cordis'
import z from '@deepseek-ai/schemastery'
import { resolveDshHome } from '@deepseek-ai/dsh-home-paths'
// Type-only: activates the `sessionPersistence` Context declaration merged by
// dsh-session-persistence into the cordis context surface.
import type {} from '@deepseek-ai/dsh-session-persistence'
import { upsertSessionRows, recordIngestState } from './aggregator.ts'
import { ingestAllSessions, type IngestResult } from './ingest.ts'
import { estimateCost, resolvePrice, type CostSummary, type PriceTable } from './pricing.ts'
import { registerAnalyticsRoutes } from './route.ts'
import { openAnalyticsDatabase } from './schema.ts'

/** Derived database default location under the DSH home. */
export const SESSION_ANALYTICS_DB_RELATIVE = ['session-analytics', 'session-analytics.sqlite3'] as const

/** Plugin configuration; all fields optional, validated by schemastery. */
export interface Config {
  /** Derived database path; defaults to $DSH_HOME/session-analytics/session-analytics.sqlite3. */
  readonly root?: string
  /** Capture mode: offline batch (default) or live firehose (M4). */
  readonly capture?: 'offline' | 'live'
  /** Offline cold-read concurrency bound. @default 4 */
  readonly concurrency?: number
  /** Open the database at activation or on first query/ingest. @default 'first-query' */
  readonly openAt?: 'startup' | 'first-query'
  /** USD price table per 1M tokens, keyed `provider/model` or `provider/*`; absent disables cost estimates. */
  readonly priceTable?: PriceTable
}

/** Schemastery validator for {@link Config}. */
export const Config: z<Config> = z.object({
  root: z.string(),
  capture: z.union([z.const('offline'), z.const('live')]),
  concurrency: z.number().min(1).max(16),
  openAt: z.union([z.const('startup'), z.const('first-query')]),
  // z.any() mirrors the otel backend passthrough: pricing normalizes entries
  // at read time and treats invalid rows as absent.
  priceTable: z.any(),
})

declare module '@deepseek-ai/cordis' {
  interface Context {
    sessionAnalytics: SessionAnalytics
  }
}

/**
 * Cross-session usage analytics service: derived read model + offline ingest.
 * @module
 */
export class SessionAnalytics extends Service {
  static inject = ['sessionPersistence']

  private dbPromise: Promise<DatabaseSync> | undefined
  private readonly resolvedRoot: string
  private readonly concurrency: number
  private readonly priceTable: PriceTable | undefined

  constructor(ctx: Context, config: Config = {}) {
    super(ctx, 'sessionAnalytics')
    this.resolvedRoot = config.root ?? join(resolveDshHome(), ...SESSION_ANALYTICS_DB_RELATIVE)
    // config.capture: 'live' capture is the M4 enhancement; offline is the only
    // implemented mode until then.
    this.concurrency = config.concurrency ?? 4
    this.priceTable = config.priceTable
    if (config.openAt === 'startup') void this.db()
    // Close the derived database when the fiber disposes (teardown quiescence).
    ctx.effect(() => () => {
      // v8 ignore next 1 -- best-effort teardown: a close rejection must not fail disposal
      void this.dbPromise?.then(db => db.close()).catch(() => {})
    }, 'session-analytics db close')
    // Register the host Fetch route; a host connection is optional, so this
    // degrades to a no-op in non-host compositions.
    ctx.effect(() => registerAnalyticsRoutes(ctx, this), 'session-analytics fetch route')
  }

  private db(): Promise<DatabaseSync> {
    this.dbPromise ??= openAnalyticsDatabase(this.resolvedRoot, 'wal')
    return this.dbPromise
  }

  /**
   * Run one offline scan: enumerate stored sessions, fold each, and persist
   * every folded session under its own transaction.
   * @param options - optional cancellation.
   * @returns the scan result (including contained failures).
   */
  async ingest(options: { signal?: AbortSignal } = {}): Promise<IngestResult> {
    const db = await this.db()
    const persistence = this.ctx.sessionPersistence
    // Live-session appends are buffered by the persistence backend; flush
    // advances their durability so list()/cold reads observe them. The flush
    // persists the same committed events — no canonical content is changed.
    await persistence.flush()
    const result = await ingestAllSessions(persistence, options.signal === undefined
      ? { concurrency: this.concurrency }
      : { concurrency: this.concurrency, signal: options.signal })
    for (const ingested of result.ingested) {
      upsertSessionRows(db, ingested.sessionId, ingested.rows)
    }
    recordIngestState(db, JSON.stringify({
      at: Date.now(),
      ingested: result.ingested.map(entry => entry.sessionId),
      failed: result.failed.map(entry => entry.sessionId),
    }))
    return result
  }

  /**
   * Cost estimates per model route; disabled without a configured price table.
   */
  async costSummary(): Promise<CostSummary> {
    const db = await this.db()
    if (this.priceTable === undefined) return { enabled: false, rows: [], totalUsd: null }
    const rows = db.prepare(
      'SELECT provider, model,'
      + ' sum(input_tokens) AS input_tokens, sum(output_tokens) AS output_tokens,'
      + ' sum(cache_read_tokens) AS cache_read_tokens, sum(cache_write_tokens) AS cache_write_tokens'
      + ' FROM steps GROUP BY provider, model ORDER BY provider, model',
    ).all() as Array<Record<string, number | null>>
    const costed = rows.map((row) => {
      const provider = String(row.provider ?? '')
      const model = String(row.model ?? '')
      const usage = {
        inputTokens: row.input_tokens ?? 0,
        outputTokens: row.output_tokens ?? 0,
        cacheReadTokens: row.cache_read_tokens ?? 0,
        cacheWriteTokens: row.cache_write_tokens ?? 0,
      }
      return {
        provider,
        model,
        ...usage,
        costUsd: estimateCost(resolvePrice(this.priceTable, provider, model), usage),
      }
    })
    const priced = costed.filter(entry => entry.costUsd !== null)
    // A single unpriced model makes the aggregate unknowable: report null
    // rather than a total that silently omits a route.
    const totalUsd = priced.length === costed.length && priced.length > 0
      ? Math.round(priced.reduce((sum, entry) => sum + (entry.costUsd ?? 0), 0) * 1_000_000) / 1_000_000
      : null
    return { enabled: true, rows: costed, totalUsd }
  }

  /** All sessions, newest first. */
  async bySessions(): Promise<Array<Record<string, unknown>>> {
    const db = await this.db()
    return db.prepare('SELECT * FROM sessions ORDER BY created_at DESC').all() as Array<Record<string, unknown>>
  }

  /** Per-day aggregates. */
  async byDay(): Promise<Array<Record<string, unknown>>> {
    const db = await this.db()
    return db.prepare('SELECT * FROM v_daily ORDER BY day').all() as Array<Record<string, unknown>>
  }

  /** Per-provider/model request aggregates. */
  async byModel(): Promise<Array<Record<string, unknown>>> {
    const db = await this.db()
    return db.prepare('SELECT * FROM v_by_model ORDER BY requests DESC').all() as Array<Record<string, unknown>>
  }

  /** Per-tool invocation aggregates. */
  async byTool(): Promise<Array<Record<string, unknown>>> {
    const db = await this.db()
    return db.prepare('SELECT * FROM v_by_tool ORDER BY calls DESC').all() as Array<Record<string, unknown>>
  }

  /** Error distribution by error name/code. */
  async byErrors(): Promise<Array<Record<string, unknown>>> {
    const db = await this.db()
    return db.prepare('SELECT * FROM v_errors ORDER BY n DESC').all() as Array<Record<string, unknown>>
  }

  /** Session lineage (parent, depth). */
  async lineage(): Promise<Array<Record<string, unknown>>> {
    const db = await this.db()
    return db.prepare('SELECT * FROM v_lineage').all() as Array<Record<string, unknown>>
  }
}

export default SessionAnalytics
