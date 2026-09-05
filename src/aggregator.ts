/**
 * Persist folded analytics rows into the derived SQLite model.
 *
 * One session commits as a single transaction: the session row and every
 * detail row are replaced wholesale, so a re-ingest of an updated log yields
 * exactly the current fold (no stale detail rows survive).
 *
 * @module session-analytics/aggregator
 */

import type { DatabaseSync } from 'node:sqlite'
import type { SessionAnalyticsRows } from './fold.ts'

const bool = (value: boolean): number => (value ? 1 : 0)

/** Values node:sqlite accepts as statement bindings. */
type BindValue = null | number | string | bigint | Uint8Array

const SESSION_COLUMNS = [
  'session_id', 'format_version', 'created_at', 'cwd', 'agent_preset', 'parent_session',
  'delegation_depth', 'origin', 'is_seeded', 'seed_length', 'title', 'first_seq', 'last_seq',
  'first_time', 'last_time', 'turn_count', 'step_count', 'end_reason', 'prompt_chars',
  'injected_chars', 'system_chars', 'tool_schema_count', 'header_change_count',
  'compaction_count', 'shadowed_event_count', 'rejected_attempts', 'error_count',
  'interrupted_count',
] as const

const STEP_COLUMNS = [
  'session_id', 'seq', 'turn', 'step', 'provider', 'model', 'context_window',
  'request_reason', 'starts_series', 'step_start_time', 'first_token_time',
  'completed_time', 'ttft_ms', 'decode_ms', 'throughput_tok_s', 'input_tokens',
  'output_tokens', 'total_tokens', 'cache_read_tokens', 'cache_write_tokens',
  'reasoning_tokens', 'interrupted', 'step_duration_ms',
] as const

const TOOL_COLUMNS = [
  'session_id', 'call_seq', 'result_seq', 'turn', 'step', 'call_id', 'name',
  'arg_chars', 'result_chars', 'duration_ms', 'is_error', 'error_name',
  'error_code', 'has_meta', 'skill_name',
] as const

const PROMPT_COLUMNS = [
  'session_id', 'seq', 'turn', 'source_kind', 'form', 'plugin', 'text_blocks',
  'reasoning_blocks', 'image_blocks', 'file_blocks', 'chars', 'est_tokens',
] as const

const ERROR_COLUMNS = ['session_id', 'seq', 'channel', 'error_name', 'error_code'] as const

const HEADER_COLUMNS = [
  'session_id', 'seq', 'reason', 'provider', 'model', 'reasoning_effort',
  'temperature', 'max_tokens', 'system_hash', 'system_chars', 'tool_names',
] as const

const COMPACTION_COLUMNS = [
  'session_id', 'seq', 'replace_start', 'replace_end', 'shadowed_count',
] as const

/** Replace one session's rows under a single transaction. */
export function upsertSessionRows(db: DatabaseSync, sessionId: string, rows: SessionAnalyticsRows): void {
  db.exec('BEGIN')
  try {
    db.prepare('DELETE FROM steps WHERE session_id = ?').run(sessionId)
    db.prepare('DELETE FROM tools WHERE session_id = ?').run(sessionId)
    db.prepare('DELETE FROM prompts WHERE session_id = ?').run(sessionId)
    db.prepare('DELETE FROM errors WHERE session_id = ?').run(sessionId)
    db.prepare('DELETE FROM headers WHERE session_id = ?').run(sessionId)
    db.prepare('DELETE FROM compactions WHERE session_id = ?').run(sessionId)
    db.prepare('DELETE FROM sessions WHERE session_id = ?').run(sessionId)

    const session = rows.session
    insertRow(db, 'sessions', SESSION_COLUMNS, [
      sessionId, session.formatVersion, session.createdAt, session.cwd,
      session.agentPreset, session.parentSession, session.delegationDepth,
      session.origin, bool(session.isSeeded), session.seedLength, session.title,
      session.firstSeq, session.lastSeq, session.firstTime, session.lastTime,
      session.turnCount, session.stepCount, session.endReason, session.promptChars,
      session.injectedChars, session.systemChars, session.toolSchemaCount,
      session.headerChangeCount, session.compactionCount, session.shadowedEventCount,
      session.rejectedAttempts, session.errorCount, session.interruptedCount,
    ])
    for (const step of rows.steps) {
      insertRow(db, 'steps', STEP_COLUMNS, [
        sessionId, step.seq, step.turn, step.step, step.provider, step.model,
        step.contextWindow, step.requestReason, bool(step.startsSeries),
        step.stepStartTime, step.firstTokenTime, step.completedTime, step.ttftMs,
        step.decodeMs, step.throughputTokS, step.inputTokens, step.outputTokens,
        step.totalTokens, step.cacheReadTokens, step.cacheWriteTokens,
        step.reasoningTokens, bool(step.interrupted), step.stepDurationMs,
      ])
    }
    for (const tool of rows.tools) {
      insertRow(db, 'tools', TOOL_COLUMNS, [
        sessionId, tool.callSeq, tool.resultSeq, tool.turn, tool.step, tool.callId,
        tool.name, tool.argChars, tool.resultChars, tool.durationMs,
        bool(tool.isError), tool.errorName, tool.errorCode, bool(tool.hasMeta),
        tool.skillName,
      ])
    }
    for (const prompt of rows.prompts) {
      insertRow(db, 'prompts', PROMPT_COLUMNS, [
        sessionId, prompt.seq, prompt.turn, prompt.sourceKind, prompt.form,
        prompt.plugin, prompt.textBlocks, prompt.reasoningBlocks, prompt.imageBlocks,
        prompt.fileBlocks, prompt.chars, prompt.estTokens,
      ])
    }
    for (const error of rows.errors) {
      insertRow(db, 'errors', ERROR_COLUMNS, [
        sessionId, error.seq, error.channel, error.errorName, error.errorCode,
      ])
    }
    for (const header of rows.headers) {
      insertRow(db, 'headers', HEADER_COLUMNS, [
        sessionId, header.seq, header.reason, header.provider, header.model,
        header.reasoningEffort, header.temperature, header.maxTokens,
        header.systemHash, header.systemChars, JSON.stringify(header.toolNames),
      ])
    }
    for (const compaction of rows.compactions) {
      insertRow(db, 'compactions', COMPACTION_COLUMNS, [
        sessionId, compaction.seq, compaction.replaceStart, compaction.replaceEnd,
        compaction.shadowedCount,
      ])
    }
    db.exec('COMMIT')
  } catch (error) {
    /* v8 ignore next 3 -- rollback arm: a statement failure rethrows with the transaction already restored */
    db.exec('ROLLBACK')
    throw error
  }
}

/** Record one ingest marker in the singleton state table. */
export function recordIngestState(
  db: DatabaseSync,
  sessionsJson: string,
  lastRunAt = Date.now(),
): void {
  db.prepare(
    'INSERT INTO ingest_state (singleton, last_run_at, sessions) VALUES (1, ?, ?) '
    + 'ON CONFLICT(singleton) DO UPDATE SET last_run_at = excluded.last_run_at, sessions = excluded.sessions',
  ).run(lastRunAt, sessionsJson)
}

function insertRow<T extends readonly string[]>(
  db: DatabaseSync,
  table: string,
  columns: T,
  values: readonly BindValue[],
): void {
  const placeholders = columns.map(() => '?').join(', ')
  const sql = 'INSERT INTO ' + table + ' (' + columns.join(', ') + ') VALUES (' + placeholders + ')'
  db.prepare(sql).run(...values)
}
