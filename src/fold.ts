/**
 * Single-pass fold of one session's event log into analytics rows.
 *
 * The fold is pure and deterministic: given the stored header and the complete
 * event array (as returned by dsh-session-query's readColdSessionLog), it
 * produces every row the analytics schema persists. Timing semantics mirror
 * dsh-session-stats field by field (step/end is the step-count authority; the
 * first non-empty token delta starts TTFT; decode figures require a valid
 * usage.outputTokens), and tool pairing matches tool/result to tool/call by
 * callId inside one turn.
 *
 * Zero-body rule: prompts, tool arguments, and tool results contribute only
 * counts, character sizes, and recognition labels. No message content is
 * retained.
 *
 * @module session-analytics/fold
 */

import type { SessionEvent, SessionHeader } from '@deepseek-ai/dsh-session'
import { foldSurface } from '@deepseek-ai/dsh-session/surface'
import { expandAssistantStream, type AssistantStreamRecord } from '@deepseek-ai/dsh-llm/assistant-stream'
import type { ContentBlock, StreamChunk } from '@deepseek-ai/dsh-llm'
// Source-level import: the token-meter index does not re-export the pure
// estimator; a package-level `/estimate` export is a formalization candidate
// (see plan appendix A) and this scratch keeps the source path.
import { estimateContent } from '../../../packages/llm/token-meter/src/estimate.ts'

/** One session-level analytics row (sessions table). */
export interface SessionRow {
  sessionId: string
  formatVersion: number
  createdAt: number
  cwd: string | null
  agentPreset: string | null
  parentSession: string | null
  delegationDepth: number | null
  origin: string | null
  isSeeded: boolean
  seedLength: number | null
  title: string | null
  firstSeq: number
  lastSeq: number
  firstTime: number
  lastTime: number
  turnCount: number
  stepCount: number
  endReason: string | null
  promptChars: number
  injectedChars: number
  systemChars: number | null
  toolSchemaCount: number
  headerChangeCount: number
  compactionCount: number
  shadowedEventCount: number
  rejectedAttempts: number
  errorCount: number
  interruptedCount: number
}

/** One step/assistant-message analytics row (steps table). */
export interface StepRow {
  seq: number
  turn: number
  step: number
  provider: string | null
  model: string | null
  contextWindow: number | null
  requestReason: string | null
  startsSeries: boolean
  stepStartTime: number | null
  firstTokenTime: number | null
  completedTime: number
  ttftMs: number | null
  decodeMs: number | null
  throughputTokS: number | null
  inputTokens: number | null
  outputTokens: number | null
  totalTokens: number | null
  cacheReadTokens: number | null
  cacheWriteTokens: number | null
  reasoningTokens: number | null
  interrupted: boolean
  stepDurationMs: number | null
}

/** One tool invocation row (tools table), paired by callId. */
export interface ToolRow {
  callSeq: number
  resultSeq: number | null
  turn: number
  step: number
  callId: string
  name: string
  argChars: number | null
  resultChars: number | null
  durationMs: number | null
  isError: boolean
  errorName: string | null
  errorCode: string | null
  hasMeta: boolean
  skillName: string | null
}

/** One user message / injection row (prompts table). */
export interface PromptRow {
  seq: number
  turn: number
  sourceKind: string | null
  form: string | null
  plugin: string | null
  textBlocks: number
  reasoningBlocks: number
  imageBlocks: number
  fileBlocks: number
  chars: number
  estTokens: number
}

/** One failure row (errors table): turn-level or tool-level. */
export interface ErrorRow {
  seq: number
  channel: 'turn' | 'tool'
  errorName: string | null
  errorCode: string | null
}

/** One request-header snapshot row (headers table). */
export interface HeaderRow {
  seq: number
  reason: string
  provider: string | null
  model: string | null
  reasoningEffort: string | null
  temperature: number | null
  maxTokens: number | null
  systemHash: string | null
  systemChars: number | null
  toolNames: readonly string[]
}

/** One compaction replacement row (compactions table). */
export interface CompactionRow {
  seq: number
  replaceStart: number
  replaceEnd: number
  shadowedCount: number
}

/** Complete result of folding one session. */
export interface SessionAnalyticsRows {
  session: SessionRow
  steps: StepRow[]
  tools: ToolRow[]
  prompts: PromptRow[]
  errors: ErrorRow[]
  headers: HeaderRow[]
  compactions: CompactionRow[]
}

/** Whether a stream chunk carries a non-empty first-token delta. */
function isTokenDelta(chunk: StreamChunk): boolean {
  switch (chunk.type) {
    case 'text-delta':
    case 'reasoning-delta':
      return chunk.text !== ''
    case 'tool-call-delta':
      return chunk.argumentsDelta !== '' || chunk.name !== undefined
    default:
      return false
  }
}

/** Time of the first non-empty token delta in one durable assistant stream. */
export function firstTokenTime(stream: readonly AssistantStreamRecord[]): number | null {
  return expandAssistantStream(stream).find(member => isTokenDelta(member.chunk))?.time ?? null
}

/** Provider-reported completion tokens, guarded like the session-stats fold. */
function usageOutputTokens(usage: unknown): number | null {
  if (typeof usage !== 'object' || usage === null) return null
  const value = (usage as { outputTokens?: unknown }).outputTokens
  return typeof value === 'number' && Number.isFinite(value) && value >= 0 ? value : null
}

const clamp = (value: number): number => Math.max(0, value)

/** Text character total over text and reasoning blocks. */
function contentChars(blocks: readonly ContentBlock[]): number {
  let total = 0
  for (const block of blocks) {
    if (block.type === 'text' || block.type === 'reasoning') total += block.text.length
  }
  return total
}

/** Block-kind counts of one user/assistant content array. */
function blockCounts(blocks: readonly ContentBlock[]): {
  text: number; reasoning: number; image: number; file: number
} {
  const counts = { text: 0, reasoning: 0, image: 0, file: 0 }
  for (const block of blocks) {
    switch (block.type) {
      case 'text': counts.text += 1; break
      case 'reasoning': counts.reasoning += 1; break
      case 'image': counts.image += 1; break
      case 'file': counts.file += 1; break
      case 'tool-call':
      case 'tool-result':
        break
      default:
        break
    }
  }
  return counts
}

/** Fast stable content hash for the header system prompt (FNV-1a 32-bit). */
function hashText(text: string): string {
  let hash = 0x811c9dc5
  for (let index = 0; index < text.length; index += 1) {
    hash ^= text.charCodeAt(index)
    hash = Math.imul(hash, 0x01000193)
  }
  return (hash >>> 0).toString(16).padStart(8, '0')
}

interface PendingCall {
  readonly callSeq: number
  readonly time: number
  readonly turn: number
  readonly step: number
  readonly name: string
  readonly argChars: number
  readonly skillName: string | null
}

interface OpenStep {
  readonly turn: number
  readonly step: number
  readonly startTime: number
  firstTokenTime: number | null
}

interface HeaderState {
  reason: string | null
  provider: string | null
  model: string | null
  contextWindow: number | null
  systemChars: number | null
  systemHash: string | null
  toolNames: readonly string[]
  toolSchemaCount: number
}

/**
 * Fold one session's event log into analytics rows.
 * @param header - the stored session header.
 * @param events - contiguous events in seq order (readColdSessionLog output, closers included).
 * @param seedLength - fork-inherited event count when the header marks isSeeded.
 * @returns the rows the analytics schema persists for this session.
 */
export function foldSessionAnalytics(
  header: SessionHeader,
  events: readonly SessionEvent[],
  seedLength?: number,
): SessionAnalyticsRows {
  // Seed boundary: only events after the last session/end-seed belong to this
  // lifecycle; inherited history is not attributed to the session's activity.
  let liveBase = 0
  for (const event of events) {
    if (event.type === 'session/end-seed') liveBase = Number(event.seq) + 1
  }
  const live = events.filter(event => Number(event.seq) >= liveBase)

  const surface = foldSurface(events)

  const steps: StepRow[] = []
  const tools: ToolRow[] = []
  const prompts: PromptRow[] = []
  const errors: ErrorRow[] = []
  const headers: HeaderRow[] = []

  let openStep: OpenStep | null = null
  let lastCountedTurn: number | null = null
  let turnCount = 0
  let stepCount = 0
  let endReason: string | null = null
  let rejectedAttempts = 0
  let interruptedCount = 0
  let errorCount = 0
  let promptChars = 0
  let injectedChars = 0
  let headerChangeCount = 0
  let lastTurnForPrompt: number | null = null
  const pendingCalls = new Map<string, PendingCall>()
  const headerState: HeaderState = {
    reason: null,
    provider: null,
    model: null,
    contextWindow: null,
    systemChars: null,
    systemHash: null,
    toolNames: [],
    toolSchemaCount: 0,
  }
  for (const event of live) {
    switch (event.type) {
      case 'turn/start':
        lastTurnForPrompt = event.data.turn
        break
      case 'turn/end': {
        endReason = event.data.reason.kind
        if (event.data.reason.kind === 'error') {
          const failure = event.data.reason.error
          errorCount += 1
          errors.push({
            seq: Number(event.seq),
            channel: 'turn',
            // LlmFailure has no name field; its stable routing code is the
            // recognition label (zero body retained).
            errorName: failure.code,
            errorCode: failure.code,
          })
        }
        pendingCalls.clear()
        break
      }
      case 'step/start':
        openStep = {
          turn: event.data.turn,
          step: event.data.step,
          startTime: event.time,
          firstTokenTime: null,
        }
        break
      case 'step/end':
        stepCount += 1
        turnCount += lastCountedTurn === event.data.turn ? 0 : 1
        lastCountedTurn = event.data.turn
        openStep = null
        break
      case 'user/message': {
        const source = event.data.source as { kind?: string; form?: string; plugin?: string }
        const kind = source.kind ?? null
        const form = source.form ?? null
        const plugin = source.kind === 'plugin' ? source.plugin ?? null : kind === 'agent-instructions' ? 'agent-instructions' : null
        const counts = blockCounts(event.data.content)
        const chars = contentChars(event.data.content)
        const isInjected = kind !== 'user' && kind !== null
        if (isInjected) injectedChars += chars
        else promptChars += chars
        prompts.push({
          seq: Number(event.seq),
          turn: lastTurnForPrompt ?? 0,
          sourceKind: kind,
          form,
          plugin,
          textBlocks: counts.text,
          reasoningBlocks: counts.reasoning,
          imageBlocks: counts.image,
          fileBlocks: counts.file,
          chars,
          estTokens: estimateContent(event.data.content),
        })
        break
      }
      case 'assistant/attempt':
        rejectedAttempts += 1
        if (openStep !== null
          && openStep.turn === event.data.turn
          && openStep.step === event.data.step
          && openStep.firstTokenTime === null) {
          openStep.firstTokenTime = firstTokenTime(event.data.stream)
        }
        break
      case 'assistant/message': {
        const open = openStep
        if (open === null || open.turn !== event.data.turn || open.step !== event.data.step) break
        const first = open.firstTokenTime ?? firstTokenTime(event.data.stream)
        const completed = event.time
        const usage = event.data.usage
        const outputTokens = usageOutputTokens(usage)
        const ttft = first !== null ? clamp(first - open.startTime) : null
        const decode = first !== null && outputTokens !== null ? clamp(completed - first) : null
        steps.push({
          seq: Number(event.seq),
          turn: event.data.turn,
          step: event.data.step,
          provider: event.data.message.source.provider,
          model: event.data.message.source.model,
          contextWindow: headerState.contextWindow,
          requestReason: headerState.reason,
          startsSeries: false,
          stepStartTime: open.startTime,
          firstTokenTime: first,
          completedTime: completed,
          ttftMs: ttft,
          decodeMs: decode,
          throughputTokS: decode !== null && decode > 0 && outputTokens !== null
            ? outputTokens / (decode / 1000)
            : null,
          inputTokens: usage?.inputTokens ?? null,
          outputTokens: usage?.outputTokens ?? null,
          totalTokens: usage?.totalTokens ?? null,
          cacheReadTokens: usage?.cacheReadTokens ?? null,
          cacheWriteTokens: usage?.cacheWriteTokens ?? null,
          reasoningTokens: usage?.reasoningTokens ?? null,
          interrupted: event.data.interrupted === true,
          stepDurationMs: clamp(completed - open.startTime),
        })
        if (event.data.interrupted === true) interruptedCount += 1
        openStep = null
        break
      }
      case 'request/header': {
        const config = event.data.header.config
        headerState.reason = event.data.reason
        headerState.systemChars = event.data.header.system?.length ?? null
        headerState.systemHash = event.data.header.system === undefined
          ? null
          : hashText(event.data.header.system)
        headerState.toolNames = event.data.header.tools?.map(tool => tool.name) ?? []
        headerState.toolSchemaCount = event.data.header.tools?.length ?? 0
        if (event.data.reason === 'change' || event.data.reason === 'series') headerChangeCount += 1
        headers.push({
          seq: Number(event.seq),
          reason: event.data.reason,
          provider: config.provider,
          model: config.model,
          reasoningEffort: config.reasoningEffort === undefined
            ? null
            : String(config.reasoningEffort),
          temperature: config.temperature ?? null,
          maxTokens: config.maxTokens ?? null,
          systemHash: headerState.systemHash,
          systemChars: headerState.systemChars,
          toolNames: headerState.toolNames,
        })
        break
      }
      case 'request/context':
        headerState.provider = event.data.provider
        headerState.model = event.data.model
        headerState.contextWindow = event.data.contextWindow ?? null
        break
      case 'tool/call': {
        const skillName = event.data.name === 'skill'
          ? parseSkillName(event.data.arguments)
          : null
        pendingCalls.set(event.data.callId, {
          callSeq: Number(event.seq),
          time: event.time,
          turn: event.data.turn,
          step: event.data.step,
          name: event.data.name,
          argChars: event.data.arguments.length,
          skillName,
        })
        break
      }
      case 'tool/result': {
        const call = pendingCalls.get(event.data.message.source.callId)
        if (call === undefined) break
        pendingCalls.delete(event.data.message.source.callId)
        const blocks = event.data.message.content[0]?.content ?? []
        const resultChars = contentChars(blocks)
        const isError = event.data.error !== undefined
          || event.data.message.content[0]?.isError === true
        tools.push({
          callSeq: call.callSeq,
          resultSeq: Number(event.seq),
          turn: call.turn,
          step: call.step,
          callId: event.data.message.source.callId,
          name: call.name,
          argChars: call.argChars,
          resultChars,
          durationMs: clamp(event.time - call.time),
          isError,
          errorName: event.data.error?.name ?? null,
          errorCode: event.data.error?.code ?? null,
          hasMeta: event.data.meta !== undefined,
          skillName: call.skillName,
        })
        if (isError && event.data.error !== undefined) {
          errorCount += 1
          errors.push({
            seq: Number(event.seq),
            channel: 'tool',
            errorName: event.data.error.name,
            errorCode: event.data.error.code,
          })
        }
        break
      }
      default:
        break
    }
  }

  const first = live[0]
  const last = live.at(-1)
  // foldSurface validates contiguity from baseSeq 0, so it folds the whole
  // array; replacements inside the inherited seed prefix do not count toward
  // this lifecycle's compaction activity.
  const compactions = surface.replacements
    .filter(replacement => Number(replacement.seq) >= liveBase)
    .map(replacement => ({
      seq: Number(replacement.seq),
      replaceStart: Number(replacement.start),
      replaceEnd: Number(replacement.end),
      shadowedCount: replacement.shadowedSeqs.length,
    }))

  const session: SessionRow = {
    sessionId: header.id,
    formatVersion: header.version,
    createdAt: header.createdAt,
    cwd: header.cwd ?? null,
    agentPreset: header.agentPreset ?? null,
    parentSession: header.parentSession ?? null,
    delegationDepth: header.delegationDepth ?? null,
    origin: header.origin ?? null,
    isSeeded: header.isSeeded,
    seedLength: header.isSeeded ? seedLength ?? null : null,
    // Title resolution lands in M1 via @deepseek-ai/dsh-session-title's fold;
    // the session/title event payload is not yet verified at M0.
    title: null,
    firstSeq: first === undefined ? -1 : Number(first.seq),
    lastSeq: last === undefined ? -1 : Number(last.seq),
    firstTime: first?.time ?? 0,
    lastTime: last?.time ?? 0,
    turnCount,
    stepCount,
    endReason,
    promptChars,
    injectedChars,
    systemChars: headerState.systemChars,
    toolSchemaCount: headerState.toolSchemaCount,
    headerChangeCount,
    compactionCount: compactions.length,
    shadowedEventCount: compactions.reduce((sum, row) => sum + row.shadowedCount, 0),
    rejectedAttempts,
    errorCount,
    interruptedCount,
  }

  return {
    session,
    steps,
    tools,
    prompts,
    errors,
    headers,
    compactions,
  }
}

/**
 * Parse the skill name out of a skill tool call's raw arguments JSON.
 * Arguments are model-produced raw JSON strings, so a malformed payload is a
 * missing label, never an aggregation failure.
 */
function parseSkillName(argumentsJson: string): string | null {
  try {
    const parsed = JSON.parse(argumentsJson) as { name?: unknown }
    return typeof parsed.name === 'string' && parsed.name.length > 0 ? parsed.name : null
  } catch {
    return null
  }
}
