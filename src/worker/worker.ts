import type { Config } from '../config.js';
import type { Dispatcher } from '../core/dispatcher.js';
import type { Logger } from '../logger.js';
import type { Broker, DeliveryRepo, RetryScheduler } from '../ports.js';

export interface WorkerDeps {
  config: Config;
  broker: Broker;
  dispatcher: Dispatcher;
  scheduler: RetryScheduler;
  deliveries: DeliveryRepo;
  logger: Logger;
}

/**
 * Two loops share the process:
 *  1. the broker subscription, which handles new deliveries
 *  2. the retry scheduler, which republishes deliveries whose backoff elapsed
 *
 * Redis is the scheduler of record, not the broker, because it gives us
 * per-message due times instead of topic-wide redelivery.
 */
export async function runWorker(deps: WorkerDeps): Promise<() => Promise<void>> {
  let running = true;

  await deps.broker.subscribe(async (job) => {
    const outcome = await deps.dispatcher.process(job);
    deps.logger.debug({ deliveryId: job.deliveryId, outcome }, 'delivery job processed');
  });

  const timer = setInterval(() => {
    if (!running) return;
    void (async () => {
      try {
        const due = await deps.scheduler.popDue(50);
        for (const deliveryId of due) {
          const delivery = await deps.deliveries.get(deliveryId);
          if (!delivery) continue;
          if (delivery.status === 'delivered' || delivery.status === 'dead') continue;
          await deps.broker.publish({
            deliveryId,
            eventId: delivery.eventId,
            endpointId: delivery.endpointId,
            enqueuedAt: Date.now(),
          });
        }
        if (due.length > 0) {
          deps.logger.info({ count: due.length }, 'retries republished');
        }
      } catch (error) {
        deps.logger.error({ err: error }, 'retry scheduler tick failed');
      }
    })();
  }, deps.config.SCHEDULER_INTERVAL_MS);
  timer.unref();

  return async () => {
    running = false;
    clearInterval(timer);
    await deps.broker.close();
  };
}
