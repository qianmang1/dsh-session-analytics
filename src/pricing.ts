/**
 * Cost estimation: usage buckets times a configured per-1M-token price table.
 *
 * token-meter prices tokens, never currency; the USD table is this feature's
 * own configuration. A model without a matching price row (or no table at
 * all) reports no cost — the panel renders the estimate as disabled rather
 * than inventing a rate.
 *
 * @module session-analytics/pricing
 */

/** Price of one model route, USD per 1,000,000 tokens per bucket. */
export interface ModelPrice {
  readonly input?: number
  readonly output?: number
  readonly cacheRead?: number
  readonly cacheWrite?: number
}

/** provider/model -> price; keys are `${provider}/${model}` or `${provider}/*`. */
export type PriceTable = Record<string, ModelPrice>

/** One costed model aggregate. */
export interface CostedModel {
  readonly provider: string
  readonly model: string
  readonly inputTokens: number
  readonly outputTokens: number
  readonly cacheReadTokens: number
  readonly cacheWriteTokens: number
  readonly costUsd: number | null
}

/** Cost summary for the panel; `enabled: false` means no table configured. */
export interface CostSummary {
  readonly enabled: boolean
  readonly rows: CostedModel[]
  readonly totalUsd: number | null
}

const PER_MILLION = 1_000_000

function cents(millionRate: number | undefined, tokens: number): number {
  return millionRate === undefined ? 0 : (millionRate * tokens) / PER_MILLION
}

/**
 * Resolve the price row for one route, with `${provider}/*` fallback.
 * @param table - configured price table, or undefined.
 * @param provider - registered provider route key.
 * @param model - provider model id.
 * @returns the matching price, or undefined when absent or unparseable.
 */
export function resolvePrice(
  table: PriceTable | undefined,
  provider: string,
  model: string,
): ModelPrice | undefined {
  if (table === undefined) return undefined
  const exact = table[provider + '/' + model]
  if (exact !== undefined) return normalize(exact)
  const wildcard = table[provider + '/*']
  return wildcard === undefined ? undefined : normalize(wildcard)
}

/**
 * Estimate the USD cost of one usage aggregate under a price row.
 * @param price - resolved price row.
 * @param usage - the usage buckets (absent fields count as zero).
 * @returns the USD estimate, or null when no rate applies at all.
 */
export function estimateCost(
  price: ModelPrice | undefined,
  usage: {
    readonly inputTokens?: number
    readonly outputTokens?: number
    readonly cacheReadTokens?: number
    readonly cacheWriteTokens?: number
  },
): number | null {
  const resolved = price
  if (resolved === undefined) return null
  if (noRates(resolved)) return null
  const total = cents(resolved.input, usage.inputTokens ?? 0)
    + cents(resolved.output, usage.outputTokens ?? 0)
    + cents(resolved.cacheRead, usage.cacheReadTokens ?? 0)
    + cents(resolved.cacheWrite, usage.cacheWriteTokens ?? 0)
  return Math.round(total * 1_000_000) / 1_000_000
}

function noRates(price: ModelPrice): boolean {
  return price.input === undefined
    && price.output === undefined
    && price.cacheRead === undefined
    && price.cacheWrite === undefined
}

/** Accept a config value that is not a plain nonnegative-number record as absent. */
function normalize(value: unknown): ModelPrice | undefined {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) return undefined
  const record = value as Record<string, unknown>
  const pick = (key: string): number | undefined => {
    const raw = record[key]
    return typeof raw === 'number' && Number.isFinite(raw) && raw >= 0 ? raw : undefined
  }
  const input = pick('input')
  const output = pick('output')
  const cacheRead = pick('cacheRead')
  const cacheWrite = pick('cacheWrite')
  const price: ModelPrice = {
    ...(input === undefined ? {} : { input }),
    ...(output === undefined ? {} : { output }),
    ...(cacheRead === undefined ? {} : { cacheRead }),
    ...(cacheWrite === undefined ? {} : { cacheWrite }),
  }
  return noRates(price) ? undefined : price
}
