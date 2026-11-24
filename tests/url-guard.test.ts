import type { lookup as dnsLookup } from 'node:dns';
import { describe, expect, it } from 'vitest';
import {
  UnsafeUrlError,
  assertDeliverableUrl,
  createGuardedLookup,
  isBlockedAddress,
} from '../src/core/url-guard.js';

type Resolver = typeof dnsLookup;

/** A resolver that answers with a fixed value, so no DNS is needed. */
function fixedResolver(address: string | Array<{ address: string; family: number }>): Resolver {
  return ((_hostname: string, _options: unknown, callback: (...args: unknown[]) => void) => {
    if (Array.isArray(address)) callback(null, address);
    else callback(null, address, 4);
  }) as unknown as Resolver;
}

function collect(lookup: ReturnType<typeof createGuardedLookup>, hostname = 'example.test') {
  return new Promise<{ error: Error | null; address: unknown }>((resolve) => {
    // The third argument is the resolver callback; the options shape is
    // irrelevant when the resolver is injected.
    lookup(hostname, { family: 0 } as never, (error, address) => {
      resolve({ error: error as Error | null, address });
    });
  });
}

describe('isBlockedAddress', () => {
  it.each([
    '127.0.0.1',
    '127.1.2.3',
    '10.0.0.1',
    '172.16.5.4',
    '172.31.255.255',
    '192.168.1.1',
    '169.254.169.254', // cloud metadata
    '100.64.0.1', // CGNAT
    '0.0.0.0',
    '198.18.0.1',
    '224.0.0.1',
    '255.255.255.255',
  ])('refuses IPv4 %s', (address) => {
    expect(isBlockedAddress(address)).toBe(true);
  });

  it.each(['8.8.8.8', '1.1.1.1', '172.32.0.1', '192.169.0.1', '93.184.216.34'])(
    'allows public IPv4 %s',
    (address) => {
      expect(isBlockedAddress(address)).toBe(false);
    },
  );

  it.each([
    '::1',
    '::',
    'fe80::1',
    'fe80::1%eth0', // zone index
    'fc00::1',
    'fd12:3456:789a::1',
    'ff02::1',
    '::ffff:127.0.0.1', // IPv4-mapped, dotted
    '::ffff:7f00:1', // IPv4-mapped, hex
    '64:ff9b::192.168.0.1', // NAT64
    '2002:7f00:1::', // 6to4 wrapping 127.0.0.1
  ])('refuses IPv6 %s', (address) => {
    expect(isBlockedAddress(address)).toBe(true);
  });

  it.each(['2606:4700:4700::1111', '2001:4860:4860::8888', '::ffff:8.8.8.8'])(
    'allows public IPv6 %s',
    (address) => {
      expect(isBlockedAddress(address)).toBe(false);
    },
  );
});

describe('assertDeliverableUrl', () => {
  it('accepts a normal https receiver', () => {
    expect(assertDeliverableUrl('https://example.com/hook').hostname).toBe('example.com');
  });

  it('accepts a public IP literal', () => {
    expect(assertDeliverableUrl('http://93.184.216.34/hook').hostname).toBe('93.184.216.34');
  });

  it.each([
    'http://localhost/hook',
    'http://localhost.localdomain:8080/hook',
    'http://localhost./hook', // fully-qualified spelling of the same host
    'http://127.0.0.1./hook',
  ])('refuses the local hostname %s', (url) => {
    expect(() => assertDeliverableUrl(url)).toThrowError(UnsafeUrlError);
  });

  it('normalises obfuscated IPv4 literals before deciding', () => {
    // WHATWG URL parsing turns these into 127.0.0.1; the guard must see that,
    // not the string the caller wrote.
    for (const url of ['http://2130706433/hook', 'http://0x7f.1/hook', 'http://0177.0.0.1/hook']) {
      expect(() => assertDeliverableUrl(url), url).toThrowError(/private or reserved/);
    }
  });

  it('refuses the metadata endpoint by IP and by name', () => {
    expect(() => assertDeliverableUrl('http://169.254.169.254/latest/meta-data')).toThrowError(
      /private or reserved/,
    );
    expect(() => assertDeliverableUrl('http://[::ffff:169.254.169.254]/')).toThrowError(
      /private or reserved/,
    );
  });

  it('refuses embedded credentials', () => {
    expect(() => assertDeliverableUrl('https://user:pass@example.com/hook')).toThrowError(
      /credentials/,
    );
    expect(() => assertDeliverableUrl('https://good.example@evil.example/hook')).toThrowError(
      /credentials/,
    );
  });

  it('refuses schemes other than http(s)', () => {
    for (const url of ['ftp://example.com/hook', 'file:///etc/passwd', 'gopher://example.com/']) {
      expect(() => assertDeliverableUrl(url), url).toThrowError(/http or https/);
    }
  });

  it('refuses a string that is not an absolute URL', () => {
    expect(() => assertDeliverableUrl('example.com/hook')).toThrowError(/absolute http/);
  });

  it('allows private targets only when explicitly told to', () => {
    expect(assertDeliverableUrl('http://127.0.0.1:3100/hook', { allowPrivate: true }).port).toBe(
      '3100',
    );
    expect(assertDeliverableUrl('http://localhost:3100/hook', { allowPrivate: true }).hostname).toBe(
      'localhost',
    );
  });
});

describe('createGuardedLookup', () => {
  it('rejects a hostname that resolves into a private range', async () => {
    const lookup = createGuardedLookup({}, fixedResolver('127.0.0.1'));
    const outcome = await collect(lookup, 'rebind.test');

    expect(outcome.error).toBeInstanceOf(UnsafeUrlError);
    expect(outcome.error?.message).toMatch(/resolved to 127\.0\.0\.1/);
  });

  it('rejects a resolver answer that includes metadata among public addresses', async () => {
    const lookup = createGuardedLookup(
      {},
      fixedResolver([
        { address: '93.184.216.34', family: 4 },
        { address: '169.254.169.254', family: 4 },
      ]),
    );
    const outcome = await collect(lookup, 'mixed.test');

    expect(outcome.error).toBeInstanceOf(UnsafeUrlError);
  });

  it('passes a public answer through untouched', async () => {
    const lookup = createGuardedLookup({}, fixedResolver('93.184.216.34'));
    const outcome = await collect(lookup, 'example.test');

    expect(outcome.error).toBeNull();
    expect(outcome.address).toBe('93.184.216.34');
  });

  it('passes private answers when allowPrivate is set', async () => {
    const lookup = createGuardedLookup({ allowPrivate: true }, fixedResolver('10.1.2.3'));
    const outcome = await collect(lookup, 'sink');

    expect(outcome.error).toBeNull();
    expect(outcome.address).toBe('10.1.2.3');
  });

  it('forwards resolver failures instead of swallowing them', async () => {
    const failing = ((_hostname: string, _options: unknown, callback: (...args: unknown[]) => void) => {
      const error = Object.assign(new Error('getaddrinfo ENOTFOUND nope.test'), {
        code: 'ENOTFOUND',
      });
      callback(error);
    }) as unknown as Resolver;

    const outcome = await collect(createGuardedLookup({}, failing), 'nope.test');
    expect(outcome.error?.message).toMatch(/ENOTFOUND/);
  });
});
