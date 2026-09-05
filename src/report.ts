/**
 * Analytics report assembly: the pure data face for the Web panel.
 *
 * buildReport runs the service's read-only query surface once and shapes the
 * rows into a JSON document the panel renders. The same function serves the
 * exact Fetch route and, because it is pure over the reader interface, is
 * unit-testable with a fake reader. Zero message bodies are ever present.
 *
 * @module session-analytics/report
 */

/** A row typed as a plain record (SQLite row -> JSON value). */
export type AnalyticsRow = Record<string, unknown>

/** The read-only query surface the report consumes. */
export interface AnalyticsReader {
  readonly bySessions: () => Promise<AnalyticsRow[]>
  readonly byDay: () => Promise<AnalyticsRow[]>
  readonly byModel: () => Promise<AnalyticsRow[]>
  readonly byTool: () => Promise<AnalyticsRow[]>
  readonly byErrors: () => Promise<AnalyticsRow[]>
  readonly lineage: () => Promise<AnalyticsRow[]>
  /** Optional USD cost estimates; absent means disabled. */
  readonly costSummary?: () => Promise<CostSummary>
}

/** The complete panel document. */
import type { CostSummary } from './pricing.ts'

export interface AnalyticsReport {
  readonly updatedAt: number
  readonly sessions: AnalyticsRow[]
  readonly byDay: AnalyticsRow[]
  readonly byModel: AnalyticsRow[]
  readonly byTool: AnalyticsRow[]
  readonly byErrors: AnalyticsRow[]
  readonly lineage: AnalyticsRow[]
  readonly cost: CostSummary
}

/**
 * Assemble the panel document from the reader's query surface.
 * @param reader - the analytics service (or any conformant fake).
 * @param updatedAt - report generation time, wall clock in ms.
 * @returns the JSON-serializable panel document.
 */
export async function buildReport(reader: AnalyticsReader, updatedAt = Date.now()): Promise<AnalyticsReport> {
  const costPromise = reader.costSummary === undefined
    ? Promise.resolve(undefined)
    : reader.costSummary()
  const [sessions, byDay, byModel, byTool, byErrors, lineage, costRow] = await Promise.all([
    reader.bySessions(),
    reader.byDay(),
    reader.byModel(),
    reader.byTool(),
    reader.byErrors(),
    reader.lineage(),
    costPromise,
  ])
  const cost = costRow ?? { enabled: false, rows: [], totalUsd: null }
  return { updatedAt, sessions, byDay, byModel, byTool, byErrors, lineage, cost }
}
