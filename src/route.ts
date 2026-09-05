/**
 * Read-only analytics HTTP surface for the Web panel.
 *
 * Mirrors dsh-session-log-export's exact Fetch route registration: the
 * feature plugin registers one GET route on the host connection service and
 * answers with the assembled report JSON. The connection service is a
 * host-only presence, so registration degrades to a no-op elsewhere (the
 * registration itself remains an effect on the mounting fiber).
 *
 * @module session-analytics/route
 */

import type { Context } from '@deepseek-ai/cordis'
import { buildReport, type AnalyticsReader } from './report.ts'

/** The exact GET path the panel reads. */
export const ANALYTICS_SUMMARY_PATH = '/api/analytics/summary'

/** Host connection subset this feature needs (mirrors session-log-export). */
export interface AnalyticsFetchConnection {
  readonly fetch: {
    register(route: {
      readonly path: string
      readonly methods: readonly ('GET' | 'HEAD')[]
      readonly requestBody: 'buffered'
      readonly fetch: (request: Request) => Promise<Response>
    }): () => Promise<void>
  }
}

/**
 * Register the summary route when a host connection is present.
 * @param ctx - host context; `connection` is read via Reflect like session-log-export.
 * @param reader - analytics query surface backing the route.
 * @returns the route disposer (no-op outside a host).
 */
export function registerAnalyticsRoutes(ctx: Context, reader: AnalyticsReader): () => Promise<void> {
  // Optional host service: ctx.get never throws for an absent service (the
  // cordis property proxy would reject an undeclared access, so no Reflect.get).
  const connection = ctx.get('connection' as never) as AnalyticsFetchConnection | undefined
  if (connection?.fetch === undefined) return () => Promise.resolve()
  return connection.fetch.register({
    path: ANALYTICS_SUMMARY_PATH,
    methods: ['GET', 'HEAD'],
    requestBody: 'buffered',
    fetch: async (request) => {
      try {
        const report = await buildReport(reader)
        request.signal.throwIfAborted()
        const response = new Response(JSON.stringify(report), {
          headers: { 'content-type': 'application/json' },
        })
        if (request.method !== 'HEAD') return response
        await response.body?.cancel()
        return new Response(null, { status: 200, headers: response.headers })
      } catch (error) {
        request.signal.throwIfAborted()
        // Fail closed without echoing internals (the error may carry host paths).
        return new Response('analytics unavailable', { status: 500 })
      }
    },
  })
}
