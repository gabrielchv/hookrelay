import { describe, expect, it } from 'vitest';
import {
  parseSignatureHeader,
  sign,
  signatureHeader,
  verifySignature,
} from '../src/core/signature.js';

const secret = 'whsec_test_1234567890';
const body = '{"id":"evt_1","type":"order.created"}';
const now = 1_700_000_000;

describe('signature', () => {
  it('produces a stable HMAC over timestamp and body', () => {
    expect(sign(secret, now, body)).toBe(
      sign(secret, now, body),
    );
    expect(sign(secret, now, body)).not.toBe(sign(secret, now + 1, body));
    expect(sign(secret, now, body)).not.toBe(sign(secret, now, `${body} `));
  });

  it('round-trips through the header format', () => {
    const header = signatureHeader(secret, now, body);
    expect(header.startsWith(`t=${now},v1=`)).toBe(true);

    const parsed = parseSignatureHeader(header);
    expect(parsed?.timestamp).toBe(now);
    expect(parsed?.signatures).toHaveLength(1);
  });

  it('verifies a fresh, correct signature', () => {
    expect(
      verifySignature({
        secret,
        header: signatureHeader(secret, now, body),
        body,
        nowSeconds: now,
      }),
    ).toBe(true);
  });

  it('rejects a tampered body', () => {
    expect(
      verifySignature({
        secret,
        header: signatureHeader(secret, now, body),
        body: body.replace('order.created', 'order.refunded'),
        nowSeconds: now,
      }),
    ).toBe(false);
  });

  it('rejects a wrong secret', () => {
    expect(
      verifySignature({
        secret: 'whsec_other',
        header: signatureHeader(secret, now, body),
        body,
        nowSeconds: now,
      }),
    ).toBe(false);
  });

  it('rejects replayed requests outside the tolerance window', () => {
    expect(
      verifySignature({
        secret,
        header: signatureHeader(secret, now, body),
        body,
        nowSeconds: now + 301,
        toleranceSeconds: 300,
      }),
    ).toBe(false);
  });

  it('accepts multiple v1 signatures during secret rotation', () => {
    const header = `${signatureHeader(secret, now, body)},v1=${sign('whsec_old', now, body)}`;
    expect(verifySignature({ secret, header, body, nowSeconds: now })).toBe(true);
  });

  it('rejects malformed headers', () => {
    expect(parseSignatureHeader('garbage')).toBeNull();
    expect(parseSignatureHeader('t=abc,v1=deadbeef')).toBeNull();
    expect(verifySignature({ secret, header: 'garbage', body, nowSeconds: now })).toBe(false);
  });
});
