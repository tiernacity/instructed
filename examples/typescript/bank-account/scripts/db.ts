/**
 * `npm run db` — bring up the isolated docker-compose Postgres in
 * the foreground, apply `sql/instructed.sql` and the read-store
 * schema once it's ready, and stay attached. Ctrl-C tears
 * everything down (container, volume, network) and exits.
 *
 * Usage:
 *
 *   npm run db
 *
 * Once you see "READY", run `npm run workers` in another terminal
 * to start the projection / PM workers, `npm run monitor` to watch
 * the read store, and the `npm run open-account|deposit|transfer`
 * commands to drive the system.
 */

import { spawn, spawnSync } from 'node:child_process'
import type { ChildProcess } from 'node:child_process'
import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'

import { Client } from 'pg'

import { PG_URL } from '../src/common.ts'

const COMPOSE_FILE = resolve(import.meta.dirname, '..', 'docker-compose.yaml')
const INSTRUCTED_SCHEMA = resolve(
  import.meta.dirname,
  '..',
  '..',
  '..',
  '..',
  'sql',
  'instructed.sql',
)
const READ_STORE_SCHEMA = resolve(import.meta.dirname, '..', 'sql', 'read-store.sql')

/** Foreground `docker compose up` -- inherits stdio so the user sees
 *  PG logs in real time. Returns the child process so we can wait /
 *  forward signals. */
function composeUp(): ChildProcess {
  return spawn('docker', ['compose', '-f', COMPOSE_FILE, 'up', '--no-color'], { stdio: 'inherit' })
}

function composeDown(): void {
  // -v removes the named volume; --remove-orphans cleans up any
  // stray containers; --rmi local would also drop locally-built
  // images (we don't build any, so omit).
  spawnSync('docker', ['compose', '-f', COMPOSE_FILE, 'down', '-v', '--remove-orphans'], {
    stdio: 'inherit',
  })
}

async function waitForPg(url: string, timeoutMs = 60_000): Promise<void> {
  const deadline = Date.now() + timeoutMs
  let lastErr: Error | null = null
  while (Date.now() < deadline) {
    const c = new Client({ connectionString: url })
    try {
      await c.connect()
      await c.query('select 1')
      await c.end()
      return
    } catch (err) {
      // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- caught value retained as Error for diagnostics.
      lastErr = err as Error
      try {
        await c.end()
      } catch {
        /* ignore */
      }
      await new Promise((r) => setTimeout(r, 500))
    }
  }
  throw new Error(`postgres not ready after ${timeoutMs}ms: ${lastErr?.message ?? 'unknown'}`)
}

async function applySchema(url: string, file: string): Promise<void> {
  const sql = readFileSync(file, 'utf8')
  const c = new Client({ connectionString: url })
  await c.connect()
  try {
    await c.query(sql)
  } finally {
    await c.end()
  }
}

async function main(): Promise<void> {
  const compose = composeUp()

  // If `docker compose up` exits on its own (e.g. the image fails
  // to start), exit with the same status after cleanup.
  let composeExit: Promise<number> = new Promise((resolveExit) => {
    compose.on('exit', (code) => resolveExit(code ?? 0))
  })

  let stopping = false
  const stop = async (sig: NodeJS.Signals, exitCode = 0) => {
    if (stopping) {
      // Second signal -- bail hard. Any remaining compose child
      // is left for the OS to clean up.
      process.stderr.write(`[${sig}] force exit\n`)
      if (compose.exitCode === null) compose.kill('SIGKILL')
      process.exit(130)
    }
    stopping = true
    process.stderr.write(`\n[${sig}] stopping…\n`)
    // `docker compose down` stops + removes the container, the
    // volume (-v), and any orphaned services. The still-attached
    // `compose up` child sees its container disappear and exits
    // on its own shortly after.
    composeDown()
    if (compose.exitCode === null) {
      // Wait up to 10s for the compose child to exit cleanly; if
      // it lingers, SIGKILL it so we don't leave an orphan that
      // re-attaches to the next run's container.
      const killTimer = setTimeout(() => {
        if (compose.exitCode === null) compose.kill('SIGKILL')
      }, 10_000)
      try {
        await composeExit
      } finally {
        clearTimeout(killTimer)
      }
    }
    process.exit(exitCode)
  }
  process.on('SIGINT', () => void stop('SIGINT'))
  process.on('SIGTERM', () => void stop('SIGTERM'))

  process.stderr.write(`waiting for postgres at ${PG_URL}…\n`)
  // If compose dies during the wait, bail out with its exit code.
  const ready = Promise.race([
    waitForPg(PG_URL).then(() => 'ready' as const),
    composeExit.then((code) => ({ composeExited: code }) as const),
  ])
  const outcome = await ready
  if (typeof outcome !== 'string') {
    process.stderr.write(
      `docker compose up exited (code ${outcome.composeExited}) before postgres became ready\n`,
    )
    composeDown()
    process.exit(outcome.composeExited || 1)
  }

  process.stderr.write(`applying instructed schema (${INSTRUCTED_SCHEMA})…\n`)
  await applySchema(PG_URL, INSTRUCTED_SCHEMA)
  process.stderr.write(`applying read-store schema (${READ_STORE_SCHEMA})…\n`)
  await applySchema(PG_URL, READ_STORE_SCHEMA)

  process.stdout.write('READY -- press Ctrl-C to stop and clean up\n')
  process.stdout.write(`  DATABASE_URL = ${PG_URL}\n`)
  process.stdout.write('  next: npm run workers   (then npm run monitor)\n')

  // Wait for compose to exit on its own (e.g. the container
  // dies); the signal handlers cover Ctrl-C.
  const code = await composeExit
  if (!stopping) {
    process.stderr.write(`docker compose up exited (code ${code})\n`)
    composeDown()
    process.exit(code)
  }
}

main().catch((err) => {
  // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- caught value coerced to Error for diagnostic output.
  process.stderr.write(`start failed: ${(err as Error).stack ?? err}\n`)
  try {
    composeDown()
  } catch {
    /* ignore */
  }
  process.exit(1)
})
