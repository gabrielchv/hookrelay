/**
 * Exponential backoff with full jitter.
 *
 * The cap for attempt N is base * 2^(N-1), bounded by max. The actual delay is
 * drawn uniformly from [1, cap]. Full jitter (rather than "equal jitter" or a
 * fixed schedule) is what prevents a fleet of workers from retrying the same
 * failing endpoint in lockstep after an outage.
 *
 * AWS's own guidance on retry backoff uses exactly this shape:
 * https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
 */
export interface BackoffPolicy {
  baseMs: number;
  maxMs: number;
}

export function backoffCapMs(attempt: number, policy: BackoffPolicy): number {
  if (!Number.isFinite(attempt) || attempt < 1) {
    throw new RangeError('attempt must be a finite number >= 1');
  }
  const raw = policy.baseMs * 2 ** (attempt - 1);
  return Math.min(policy.maxMs, Math.max(policy.baseMs, raw));
}

export function nextDelayMs(
  attempt: number,
  policy: BackoffPolicy,
  random: () => number = Math.random,
): number {
  const cap = backoffCapMs(attempt, policy);
  return Math.floor(random() * cap) + 1;
}
