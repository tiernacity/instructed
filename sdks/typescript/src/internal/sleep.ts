/**
 * Interruptible sleep helper.
 *
 * Resolves after `ms` milliseconds, or earlier if the supplied
 * `AbortSignal` fires. Never rejects.
 */
export function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  if (signal?.aborted) return Promise.resolve()
  return new Promise<void>((resolve) => {
    let timer: NodeJS.Timeout | null = null
    const finish = () => {
      if (timer !== null) {
        clearTimeout(timer)
        timer = null
      }
      signal?.removeEventListener('abort', finish)
      resolve()
    }
    timer = setTimeout(finish, ms)
    signal?.addEventListener('abort', finish, { once: true })
  })
}
