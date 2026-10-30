/**
 * Stripe-style webhook signatures:
 *   X-Hookrelay-Signature: t=<unix-seconds>,v1=<hex hmac-sha256 of "t.body">
 *
 * Signing the timestamp together with the body binds the signature to a moment
 * in time, so a captured request cannot be replayed forever. Receivers should
 * reject anything outside their tolerance window.
 */
import { createHmac, timingSafeEqual } from 'node:crypto';

export const SIGNATURE_HEADER = 'x-hookrelay-signature';
export const TIMESTAMP_HEADER = 'x-hookrelay-timestamp';
export const DELIVERY_HEADER = 'x-hookrelay-delivery';
export const ATTEMPT_HEADER = 'x-hookrelay-attempt';

export function sign(secret: string, timestampSeconds: number, body: string): string {
  return createHmac('sha256', secret).update(`${timestampSeconds}.${body}`).digest('hex');
}

export function signatureHeader(
  secret: string,
  timestampSeconds: number,
  body: string,
): string {
  return `t=${timestampSeconds},v1=${sign(secret, timestampSeconds, body)}`;
}

export interface ParsedSignature {
  timestamp: number;
  signatures: string[];
}

export function parseSignatureHeader(header: string): ParsedSignature | null {
  const parts = header.split(',');
  let timestamp: number | null = null;
  const signatures: string[] = [];

  for (const part of parts) {
    const separator = part.indexOf('=');
    if (separator === -1) continue;
    const key = part.slice(0, separator).trim();
    const value = part.slice(separator + 1).trim();
    if (key === 't' && /^\d+$/.test(value)) {
      timestamp = Number(value);
    } else if (key === 'v1' && /^[0-9a-f]{64}$/i.test(value)) {
      signatures.push(value.toLowerCase());
    }
  }

  if (timestamp === null || signatures.length === 0) return null;
  return { timestamp, signatures };
}

export interface VerifyOptions {
  secret: string;
  header: string;
  body: string;
  toleranceSeconds?: number;
  nowSeconds?: number;
}

export function verifySignature(options: VerifyOptions): boolean {
  const { secret, header, body, toleranceSeconds = 300 } = options;
  const nowSeconds = options.nowSeconds ?? Math.floor(Date.now() / 1000);

  const parsed = parseSignatureHeader(header);
  if (!parsed) return false;
  if (Math.abs(nowSeconds - parsed.timestamp) > toleranceSeconds) return false;

  const expected = Buffer.from(sign(secret, parsed.timestamp, body), 'hex');
  return parsed.signatures.some((candidate) => {
    const received = Buffer.from(candidate, 'hex');
    return received.length === expected.length && timingSafeEqual(received, expected);
  });
}
