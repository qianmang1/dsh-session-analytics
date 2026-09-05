/**
 * Offline session ingest: enumerate stored sessions through the persistence
 * seam, read each cold log, and fold it into analytics rows.
 *
 * The ingest step is pure (no database): it returns rows per session for the
 * aggregator to persist. Read failures are contained per session and reported
 * in the result so one broken log cannot abort the scan.
 *
 * @module session-analytics/ingest
 */

import type { SessionPersistence } from '@deepseek-ai/dsh-session-persistence'
import { readColdSessionLog } from '@deepseek-ai/dsh-session-query'
import { foldSessionAnalytics, type SessionAnalyticsRows } from './fold.ts'

/** One session's folded rows ready for persistence. */
export interface IngestedSession {
  readonly sessionId: string
  readonly rows: SessionAnalyticsRows
}

/** One failed cold read, contained and reported. */
export interface IngestFailure {
  readonly sessionId: string
  readonly error: unknown
}

/** Result of one offline scan. */
export interface IngestResult {
  readonly scanned: number
  readonly ingested: IngestedSession[]
  readonly failed: IngestFailure[]
}

/** Default bound on concurrent cold reads per scan. */
export const SESSION_ANALYTICS_DEFAULT_CONCURRENCY = 4

/**
 * Scan every stored session and fold each cold log once.
 * @param persistence - the mounted persistence service (read-only use).
 * @param options - concurrency bound and optional cancellation.
 * @returns per-session rows plus contained failures.
 */
export async function ingestAllSessions(
  persistence: SessionPersistence,
  options: { concurrency?: number; signal?: AbortSignal } = {},
): Promise<IngestResult> {
  const concurrency = options.concurrency ?? SESSION_ANALYTICS_DEFAULT_CONCURRENCY
  const snapshots = await persistence.list(options.signal === undefined ? undefined : { signal: options.signal })
  const ingested: IngestedSession[] = []
  const failed: IngestFailure[] = []
  await runPool(snapshots, concurrency, async (snapshot) => {
    const sessionId = snapshot.header.id
    try {
      const cold = await readColdSessionLog(persistence, sessionId, options.signal)
      ingested.push({
        sessionId,
        rows: foldSessionAnalytics(cold.header, cold.events, Number(cold.inheritedEventCount)),
      })
    } catch (error) {
      failed.push({ sessionId, error })
    }
  })
  return { scanned: snapshots.length, ingested, failed }
}

/** Run a worker over every item with at most `size` concurrent executions. */
export async function runPool<T>(
  items: readonly T[],
  size: number,
  worker: (item: T) => Promise<void>,
): Promise<void> {
  const queue = [...items]
  const limit = Math.max(1, size)
  await Promise.all(Array.from({ length: Math.min(limit, queue.length) }, async () => {
    while (queue.length > 0) {
      const item = queue.shift()!
      await worker(item)
    }
  }))
}
