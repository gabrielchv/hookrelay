/**
 * Opt-in authentication for the `/v1` surface.
 *
 * The service ships open by default because it is meant to be run locally.
 * The moment it is reachable by anything you do not control, set `API_KEY`:
 * every `/v1` request then needs that key, either as
 * `Authorization: Bearer <key>` or as `X-Api-Key: <key>`.
 *
 * `/healthz` deliberately stays unauthenticated so Kubernetes probes and
 * load balancers can reach it without carrying a credential.
 *
 * Comparison hashes both sides first, so `timingSafeEqual` always sees two
 * 32-byte buffers: a wrong key leaks neither its contents nor its length.
 */
import { createHash, timingSafeEqual } from 'node:crypto';
import type { FastifyRequest } from 'fastify';

export const API_KEY_HEADER = 'x-api-key';

export function extractApiKey(headers: FastifyRequest['headers']): string | null {
  const authorization = headers.authorization;
  if (typeof authorization === 'string') {
    const match = /^Bearer[ \t]+(.+)$/i.exec(authorization.trim());
    if (match) return match[1].trim();
  }

  const apiKey = headers[API_KEY_HEADER];
  if (typeof apiKey === 'string' && apiKey.trim().length > 0) {
    return apiKey.trim();
  }

  return null;
}

function digest(value: string): Buffer {
  return createHash('sha256').update(value, 'utf8').digest();
}

export function apiKeyEquals(presented: string, expected: string): boolean {
  return timingSafeEqual(digest(presented), digest(expected));
}
