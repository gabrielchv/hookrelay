import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';

/**
 * The demo stack must not be reachable from the network it runs on.
 *
 * `docker-compose.yml` sets `ALLOW_PRIVATE_ENDPOINTS=true` because the demo
 * delivers to the `sink` container over the private compose network — and, three
 * lines above, carries a comment saying never to default that to true anywhere a
 * caller you do not control can register endpoints. Publishing the API port on
 * all interfaces is exactly that: anyone who can reach the host can register
 * `http://10.0.0.5:5432` and read the outcome back, and with `API_KEY` empty
 * (the local default) they need no account.
 *
 * Binding each published port to 127.0.0.1 keeps the demo intact and removes the
 * exposure. This test exists so a future service added to the stack cannot
 * quietly reintroduce it.
 */

const compose = readFileSync(new URL('../docker-compose.yml', import.meta.url), 'utf8');

/** Every `- "host:container"` or `- "bind:host:container"` mapping in the file. */
const publishedPorts = [...compose.matchAll(/^\s*-\s*"([^"]*:\d+)"\s*$/gm)].map(
  (match) => match[1]!,
);

describe('docker-compose port publishing', () => {
  it('publishes at least one port, so this test is not vacuous', () => {
    expect(publishedPorts.length).toBeGreaterThan(0);
  });

  it('binds every published port to loopback', () => {
    const exposed = publishedPorts.filter((mapping) => !mapping.startsWith('127.0.0.1:'));

    expect(
      exposed,
      `these mappings are reachable off-host: ${exposed.join(', ')}`,
    ).toEqual([]);
  });

  it('still maps the API port the README documents', () => {
    expect(publishedPorts).toContain('127.0.0.1:3110:3100');
  });
});
