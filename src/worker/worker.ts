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
        if (due.length === 0) return;

        // `popDue` already removed these ids from the schedule, so every id in
        // `due` is "in flight" the moment it is returned. Handling them in one
        // try block meant a single failure — a Dynamo timeout on one `get`, or a
        // broker publish error — abandoned the rest of the batch with their
        // schedule entries gone and their rows still `queued`. Nothing reads
        // `nextAttemptAt` afterwards, so those deliveries were lost until a
        // human replayed them.
        //
        // Each id is now independent, and a failure is put back on the schedule
        // rather than dropped, so the tick is at-least-once like the rest of the
        // pipeline.
        let republished = 0;
        let deferred = 0;

        for (const deliveryId of due) {
          try {
            const delivery = await deps.deliveries.get(deliveryId);
            if (!delivery) continue;
            if (delivery.status === 'delivered' || delivery.status === 'dead') continue;
            await deps.broker.publish({
              deliveryId,
              eventId: delivery.eventId,
              endpointId: delivery.endpointId,
              enqueuedAt: Date.now(),
            });
            republished += 1;
          } catch (error) {
            deferred += 1;
            deps.logger.error(
              { err: error, deliveryId },
              'retry republish failed; rescheduling',
            );
            // If this also throws there is nothing left to do but surface it:
            // the tick must keep going for the other ids either way.
            try {
              await deps.scheduler.schedule(
                deliveryId,
                Date.now() + deps.config.SCHEDULER_INTERVAL_MS,
              );
            } catch (scheduleError) {
              deps.logger.error(
                { err: scheduleError, deliveryId },
                'could not reschedule a failed republish',
              );
            }
          }
        }

        if (republished > 0 || deferred > 0) {
          deps.logger.info(
            { republished, deferred },
            'retries republished',
          );
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
