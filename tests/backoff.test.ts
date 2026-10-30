import { describe, expect, it } from 'vitest';
import { backoffCapMs, nextDelayMs } from '../src/core/backoff.js';

const policy = { baseMs: 250, maxMs: 4000 };

describe('backoffCapMs', () => {
  it('doubles per attempt starting at the base', () => {
    expect(backoffCapMs(1, policy)).toBe(250);
    expect(backoffCapMs(2, policy)).toBe(500);
    expect(backoffCapMs(3, policy)).toBe(1000);
    expect(backoffCapMs(4, policy)).toBe(2000);
  });

  it('caps at maxMs', () => {
    expect(backoffCapMs(5, policy)).toBe(4000);
    expect(backoffCapMs(50, policy)).toBe(4000);
  });

  it('rejects attempt zero or negative', () => {
    expect(() => backoffCapMs(0, policy)).toThrow(RangeError);
    expect(() => backoffCapMs(-1, policy)).toThrow(RangeError);
  });
});

describe('nextDelayMs', () => {
  it('applies full jitter inside the cap', () => {
    expect(nextDelayMs(3, policy, () => 0)).toBe(1);
    expect(nextDelayMs(3, policy, () => 0.999)).toBe(1000);
    // +1 guarantees a strictly positive delay even when the draw is zero.
    expect(nextDelayMs(3, policy, () => 0.5)).toBe(501);
  });

  it('never returns zero, so a retry is always rescheduled', () => {
    for (let attempt = 1; attempt <= 8; attempt++) {
      expect(nextDelayMs(attempt, policy, () => 0)).toBeGreaterThan(0);
    }
  });

  it('stays bounded by maxMs under adversarial randomness', () => {
    for (let attempt = 1; attempt <= 20; attempt++) {
      expect(nextDelayMs(attempt, policy, () => 0.999999)).toBeLessThanOrEqual(4000);
    }
  });
});
