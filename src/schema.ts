/**
 * SQLite schema for the session-analytics derived read model.
 *
 * The database is a disposable derived index (same contract as
 * dsh-session-query-sqlite): an application id guards against foreign files,
 * SCHEMA_VERSION is monotonic, and a version mismatch rebuilds the whole
 * schema in place. Every table is STRICT. The model stores counts, sizes, and
 * wall times only — zero message bodies ever enter this database.
 *
 * @module session-analytics/schema
 */

import type { DatabaseSync } from 'node:sqlite'
import { mkdir, open } from 'node:fs/promises'
import { dirname, resolve } from 'node:path'

/** Current derived-index schema version; incompatible versions reset in place. */
export const SESSION_ANALYTICS_SCHEMA_VERSION = 1

/** SQLite application id protecting unrelated databases from derived resets. */
export const SESSION_ANALYTICS_APPLICATION_ID = 0x44534141

/** Supported SQLite journal modes, validated against the closed union at open. */
export type JournalMode = 'wal' | 'delete' | 'truncate' | 'persist'

/** Every user table the derived schema owns; anything else is foreign. */
const DERIVED_USER_TABLES = new Set([
  'ingest_state',
  'sessions',
  'steps',
  'tools',
  'prompts',
  'errors',
  'headers',
  'compactions',
])

/**
 * Exclusively create a missing database file with owner-only permissions.
 * Existing files retain their modes; errors other than EEXIST propagate.
 */
async function createDatabaseFile(path: string): Promise<void> {
  try {
    const handle = await open(path, 'wx', 0o600)
    await handle.close()
  } catch (error) {
    /* v8 ignore next 3 -- EEXIST is the only admitted exclusive-create race; other errors propagate */
    if ((error as NodeJS.ErrnoException).code !== 'EEXIST') throw error
  }
}

/**
 * Open, validate, and initialize the analytics database.
 * @param path - dedicated derived-index path or ':memory:'; missing filesystem paths are created owner-only.
 * @param journalMode - validated SQLite journal mode.
 * @returns initialized database handle owned by the analytics service.
 */
export async function openAnalyticsDatabase(path: string, journalMode: JournalMode): Promise<DatabaseSync> {
  const actual = path === ':memory:' ? path : resolve(path)
  if (actual !== ':memory:') {
    await mkdir(dirname(actual), { recursive: true, mode: 0o700 })
    await createDatabaseFile(actual)
  }
  const { DatabaseSync } = await import('node:sqlite')
  const db = new DatabaseSync(actual)
  try {
    const { application_id: applicationId } = db.prepare('PRAGMA application_id').get() as {
      application_id: number
    }
    const { user_version: version } = db.prepare('PRAGMA user_version').get() as { user_version: number }
    const userTables = listUserTables(db)
    if (applicationId !== 0 && applicationId !== SESSION_ANALYTICS_APPLICATION_ID) {
      throw new Error(`analytics database at "${actual}" belongs to another application`)
    }
    if (applicationId === 0 && userTables.length > 0) {
      throw new Error(`analytics database at "${actual}" is not an empty or recognized derived index`)
    }
    if (applicationId === SESSION_ANALYTICS_APPLICATION_ID) {
      assertDerivedUserTables(actual, userTables)
      if (version !== SESSION_ANALYTICS_SCHEMA_VERSION) resetDerivedSchema(db, userTables)
    }
    db.exec(`PRAGMA journal_mode = ${journalMode.toUpperCase()}`)
    ensureSchema(db)
    return db
  } catch (error) {
    db.close()
    throw error
  }
}

function listUserTables(db: DatabaseSync): string[] {
  const rows = db.prepare(
    "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT GLOB 'sqlite_*' ORDER BY name",
  ).all() as Array<{ name: string }>
  return rows.map(row => row.name)
}

function assertDerivedUserTables(path: string, userTables: readonly string[]): void {
  const unknownTables = userTables.filter(name => !DERIVED_USER_TABLES.has(name))
  if (unknownTables.length > 0) {
    /* v8 ignore next 4 -- foreign tables under our application id are rejected as corruption */
    throw new Error(
      `analytics database at "${path}" has unrecognized user tables: ${unknownTables.join(', ')}`,
    )
  }
}

function resetDerivedSchema(db: DatabaseSync, userTables: readonly string[]): void {
  for (const name of userTables) {
    db.exec(`DROP TABLE IF EXISTS ${quoteIdentifier(name)}`)
  }
  db.exec('PRAGMA user_version = 0')
}

function ensureSchema(db: DatabaseSync): void {
  db.exec(`PRAGMA application_id = ${SESSION_ANALYTICS_APPLICATION_ID}`)
  db.exec(`
    CREATE TABLE IF NOT EXISTS ingest_state (
      singleton   INTEGER PRIMARY KEY CHECK (singleton = 1),
      last_run_at INTEGER NOT NULL,
      sessions    TEXT NOT NULL
    ) STRICT
  `)
  db.exec(`
    CREATE TABLE IF NOT EXISTS sessions (
      session_id        TEXT PRIMARY KEY,
      format_version    INTEGER NOT NULL,
      created_at        INTEGER NOT NULL,
      cwd               TEXT,
      agent_preset      TEXT,
      parent_session    TEXT,
      delegation_depth  INTEGER,
      origin            TEXT,
      is_seeded         INTEGER NOT NULL,
      seed_length       INTEGER,
      title             TEXT,
      first_seq         INTEGER NOT NULL,
      last_seq          INTEGER NOT NULL,
      first_time        INTEGER NOT NULL,
      last_time         INTEGER NOT NULL,
      turn_count        INTEGER NOT NULL,
      step_count        INTEGER NOT NULL,
      end_reason        TEXT,
      prompt_chars      INTEGER,
      injected_chars    INTEGER,
      system_chars      INTEGER,
      tool_schema_count INTEGER,
      header_change_count INTEGER,
      compaction_count  INTEGER,
      shadowed_event_count INTEGER,
      rejected_attempts INTEGER,
      error_count       INTEGER,
      interrupted_count INTEGER
    ) STRICT
  `)
  db.exec(`
    CREATE TABLE IF NOT EXISTS steps (
      session_id       TEXT NOT NULL,
      seq              INTEGER NOT NULL,
      turn             INTEGER NOT NULL,
      step             INTEGER NOT NULL,
      provider         TEXT,
      model            TEXT,
      context_window   INTEGER,
      request_reason   TEXT,
      starts_series    INTEGER,
      step_start_time  INTEGER,
      first_token_time INTEGER,
      completed_time   INTEGER,
      ttft_ms          INTEGER,
      decode_ms        INTEGER,
      throughput_tok_s REAL,
      input_tokens     INTEGER,
      output_tokens    INTEGER,
      total_tokens     INTEGER,
      cache_read_tokens INTEGER,
      cache_write_tokens INTEGER,
      reasoning_tokens INTEGER,
      interrupted      INTEGER,
      step_duration_ms INTEGER,
      PRIMARY KEY (session_id, seq)
    ) STRICT
  `)
  db.exec(`
    CREATE TABLE IF NOT EXISTS tools (
      session_id   TEXT NOT NULL,
      call_seq     INTEGER NOT NULL,
      result_seq   INTEGER,
      turn         INTEGER NOT NULL,
      step         INTEGER NOT NULL,
      call_id      TEXT NOT NULL,
      name         TEXT NOT NULL,
      arg_chars    INTEGER,
      result_chars INTEGER,
      duration_ms  INTEGER,
      is_error     INTEGER,
      error_name   TEXT,
      error_code   TEXT,
      has_meta     INTEGER,
      skill_name   TEXT,
      PRIMARY KEY (session_id, call_seq)
    ) STRICT
  `)
  db.exec(`
    CREATE TABLE IF NOT EXISTS prompts (
      session_id      TEXT NOT NULL,
      seq             INTEGER NOT NULL,
      turn            INTEGER NOT NULL,
      source_kind     TEXT,
      form            TEXT,
      plugin          TEXT,
      text_blocks     INTEGER,
      reasoning_blocks INTEGER,
      image_blocks    INTEGER,
      file_blocks     INTEGER,
      chars           INTEGER,
      est_tokens      INTEGER,
      PRIMARY KEY (session_id, seq)
    ) STRICT
  `)
  db.exec(`
    CREATE TABLE IF NOT EXISTS errors (
      session_id TEXT NOT NULL,
      seq        INTEGER NOT NULL,
      channel    TEXT NOT NULL,
      error_name TEXT,
      error_code TEXT,
      PRIMARY KEY (session_id, seq)
    ) STRICT
  `)
  db.exec(`
    CREATE TABLE IF NOT EXISTS headers (
      session_id       TEXT NOT NULL,
      seq              INTEGER NOT NULL,
      reason           TEXT NOT NULL,
      provider         TEXT,
      model            TEXT,
      reasoning_effort TEXT,
      temperature      REAL,
      max_tokens       INTEGER,
      system_hash      TEXT,
      system_chars     INTEGER,
      tool_names       TEXT,
      PRIMARY KEY (session_id, seq)
    ) STRICT
  `)
  db.exec(`
    CREATE TABLE IF NOT EXISTS compactions (
      session_id     TEXT NOT NULL,
      seq            INTEGER NOT NULL,
      replace_start  INTEGER NOT NULL,
      replace_end    INTEGER NOT NULL,
      shadowed_count INTEGER NOT NULL,
      PRIMARY KEY (session_id, seq)
    ) STRICT
  `)
  db.exec(`
    CREATE VIEW IF NOT EXISTS v_by_model AS
      SELECT provider, model, count(*) AS requests,
             sum(output_tokens) AS output_tokens,
             avg(ttft_ms) AS ttft_avg_ms
      FROM steps WHERE provider IS NOT NULL GROUP BY provider, model
  `)
  db.exec(`
    CREATE VIEW IF NOT EXISTS v_by_tool AS
      SELECT name, count(*) AS calls,
             avg(duration_ms) AS duration_avg_ms,
             max(duration_ms) AS duration_max_ms,
             sum(is_error) AS errors
      FROM tools GROUP BY name
  `)
  db.exec(`
    CREATE VIEW IF NOT EXISTS v_errors AS
      SELECT error_name, error_code, count(*) AS n
      FROM errors GROUP BY error_name, error_code
  `)
  db.exec(`
    CREATE VIEW IF NOT EXISTS v_lineage AS
      SELECT child.session_id, child.parent_session, child.delegation_depth
      FROM sessions child
  `)
  db.exec(`PRAGMA user_version = ${SESSION_ANALYTICS_SCHEMA_VERSION}`)
  // v_daily spans sessions#last_time and steps#output/input tokens, so it is
  // created after both base tables; the remaining aggregate views above only
  // read single tables. Steps aggregate per session first so a many-steps
  // session cannot multiply the session-level counts per day.
  db.exec(`
    CREATE VIEW IF NOT EXISTS v_daily AS
      SELECT date(s.last_time / 1000, 'unixepoch') AS day,
             count(*) AS sessions,
             sum(s.turn_count) AS turns,
             sum(s.step_count) AS steps,
             sum(COALESCE(per.output_tokens, 0)) AS output_tokens,
             sum(COALESCE(per.input_tokens, 0)) AS input_tokens,
             sum(COALESCE(per.ttft_ms, 0)) AS ttft_ms,
             sum(COALESCE(per.throughput_tok_s, 0)) AS throughput_tok_s
      FROM sessions s
      LEFT JOIN (
        SELECT session_id,
               sum(output_tokens) AS output_tokens,
               sum(input_tokens) AS input_tokens,
               sum(ttft_ms) AS ttft_ms,
               sum(throughput_tok_s) AS throughput_tok_s
        FROM steps GROUP BY session_id
      ) per USING (session_id)
      GROUP BY day
  `)
}

function quoteIdentifier(value: string): string {
  return `"${value.replaceAll('"', '""')}"`
}
