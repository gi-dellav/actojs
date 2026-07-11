// actojs — Async-iterable wrapper over a process mailbox.
// Lets you consume messages with `for await…of`.

import * as Proc from './process';

/**
 * Returns an infinite async iterable that yields every message received
 * by the calling process. Loop until the process exits or you `break`.
 *
 * @example
 *   for await (const msg of EventStream.receive()) {
 *     if (msg === 'done') break;
 *   }
 */
export async function* receive<T = unknown>(): AsyncIterable<T> {
  const me = Proc.self();
  while (Proc.alive(me)) {
    yield (await Proc.receive()) as T;
  }
}
