import { describe, expect, it } from 'vitest';
import type { Config } from '../src/config.js';
import type { DeliveryJob } from '../src/types.js';
import type { Logger } from '../src/logger.js';
import { runWorker } from '../src/worker/worker.js';
import {
  MemoryDeliveryRepo,
  MemoryScheduler,
  makeDelivery,
} from './support/fakes.js';

/**
 * The retry tick's contract.
 *
 * `scheduler.popDue` removes the ids it returns, so every id handed to the tick
 * is already off the schedule. If the tick then fails one id and abandons the
 * batch, those deliveries are gone: their rows stay `queued` with a
 * `nextAttemptAt` in the past and nothing ever reads it again.
 */

const silentLogger = {
  trace() {},
  debug() {},
  info() {},
  warn() {},
  error() {},
  fatal() {},
  child() {
    return silentLogger;
  },
} as unknown as Logger;

// Short enough that the first setInterval tick lands inside the test's window.
const TICK_MS = 25;
const config = {
  SCHEDULER_INTERVAL_MS: TICK_MS,
} as unknown as Config;

/** Broker that records publishes and can be told to fail specific ids. */
class FlakyBroker {
  readonly published: DeliveryJob[] = [];
  handler: ((job: DeliveryJob) => Promise<void>) | null = null;
  failing = new Set<string>();

  async publish(job: DeliveryJob): Promise<void> {
    if (this.failing.has(job.deliveryId)) throw new Error(`broker down for ${job.deliveryId}`);
    this.published.push(job);
  }

  async subscribe(handler: (job: DeliveryJob) => Promise<void>): Promise<void> {
    this.handler = handler;
  }

  async close(): Promise<void> {}
}

/** Starts a worker, runs exactly one scheduler tick, then stops it. */
async function runOneTick(deps: {
  scheduler: MemoryScheduler;
  broker: FlakyBroker;
  deliveries: MemoryDeliveryRepo;
}): Promise<void> {
  const dispatcher = {
    async process() {
      return 'delivered' as const;
    },
  };

  let resolveTick: () => void = () => {};
  const ticked = new Promise<void>((resolve) => {
    resolveTick = resolve;
  });

  const originalPopDue = deps.scheduler.popDue.bind(deps.scheduler);
  let popped = false;
  deps.scheduler.popDue = async (limit: number) => {
    const result = await originalPopDue(limit);
    popped = true;
    return result;
  };

  const stop = await runWorker({
    config,
    broker: deps.broker as never,
    dispatcher: dispatcher as never,
    scheduler: deps.scheduler,
    deliveries: deps.deliveries,
    logger: silentLogger,
  });

  // The tick is scheduled with setInterval; drive it deterministically by
  // waiting until popDue has been called and the publishes settle.
  const deadline = Date.now() + 3000;
  while (!popped && Date.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  await new Promise((resolve) => setTimeout(resolve, TICK_MS * 3));
  void ticked;
  await stop();
}

async function seedQueued(deliveries: MemoryDeliveryRepo, ids: string[]): Promise<void> {
  for (const id of ids) {
    await deliveries.create(makeDelivery({ id, status: 'queued' }));
  }
}

/** Force a scheduler to consider everything due immediately. */
async function scheduleDue(scheduler: MemoryScheduler, ids: string[]): Promise<void> {
  for (const id of ids) {
    await scheduler.schedule(id, Date.now() - 1000);
  }
}

describe('retry scheduler tick', () => {
  it('republishes every due delivery when none fail', async () => {
    const scheduler = new MemoryScheduler();
    const deliveries = new MemoryDeliveryRepo();
    const broker = new FlakyBroker();

    const ids = ['d1', 'd2', 'd3'];
    await seedQueued(deliveries, ids);
    await scheduleDue(scheduler, ids);

    await runOneTick({ scheduler, broker, deliveries });

    expect(broker.published.map((j) => j.deliveryId).sort()).toEqual(ids);
  });

  it('keeps going after one delivery fails to republish', async () => {
    // The regression: a single publisher error used to abort the whole batch,
    // and every id after the failure had already been removed from the schedule.
    const scheduler = new MemoryScheduler();
    const deliveries = new MemoryDeliveryRepo();
    const broker = new FlakyBroker();

    const ids = ['d1', 'd2', 'd3'];
    await seedQueued(deliveries, ids);
    await scheduleDue(scheduler, ids);
    broker.failing.add('d2');

    await runOneTick({ scheduler, broker, deliveries });

    expect(broker.published.map((j) => j.deliveryId).sort()).toEqual(['d1', 'd3']);
  });

  it('puts a failed delivery back on the schedule instead of losing it', async () => {
    const scheduler = new MemoryScheduler();
    const deliveries = new MemoryDeliveryRepo();
    const broker = new FlakyBroker();

    const ids = ['d1', 'd2', 'd3'];
    await seedQueued(deliveries, ids);
    await scheduleDue(scheduler, ids);
    broker.failing.add('d2');

    await runOneTick({ scheduler, broker, deliveries });

    // d1 and d3 were published; d2 must be pending again, not vanished.
    expect(scheduler.scheduled.has('d2')).toBe(true);
    expect(scheduler.scheduled.get('d2')).toBeGreaterThan(Date.now());
    expect(scheduler.scheduled.has('d1')).toBe(false);
    expect(scheduler.scheduled.has('d3')).toBe(false);
  });

  it('skips deliveries that are already delivered or dead', async () => {
    const scheduler = new MemoryScheduler();
    const deliveries = new MemoryDeliveryRepo();
    const broker = new FlakyBroker();

    await deliveries.create(makeDelivery({ id: 'done', status: 'delivered' }));
    await deliveries.create(makeDelivery({ id: 'gone', status: 'dead' }));
    await deliveries.create(makeDelivery({ id: 'live', status: 'queued' }));
    await scheduleDue(scheduler, ['done', 'gone', 'live']);

    await runOneTick({ scheduler, broker, deliveries });

    expect(broker.published.map((j) => j.deliveryId)).toEqual(['live']);
  });

  it('does not reschedule a delivery whose row is missing', async () => {
    const scheduler = new MemoryScheduler();
    const deliveries = new MemoryDeliveryRepo();
    const broker = new FlakyBroker();

    await scheduleDue(scheduler, ['ghost']);

    await runOneTick({ scheduler, broker, deliveries });

    expect(broker.published).toEqual([]);
    expect(scheduler.scheduled.has('ghost')).toBe(false);
  });
});
