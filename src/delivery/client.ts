/**
 * HTTP delivery adapter. Signs the exact bytes on the wire (we serialize once
 * and send that body) so receivers can verify without re-serializing.
 *
 * It uses `node:http(s).request` rather than `fetch` for one reason: the
 * `lookup` hook. That hook runs inside the connect path, so the address that
 * gets validated is the address that gets dialled — see `core/url-guard.ts`.
 * `fetch` resolves DNS in its own machinery, leaving a window in which a
 * hostname can pass a check and then resolve somewhere else.
 *
 * Redirects are not followed: `http.request` never follows them, and a 3xx is
 * reported as a normal non-2xx attempt so retry/backoff apply as usual.
 */
import { request as httpRequest, type RequestOptions } from 'node:http';
import { request as httpsRequest } from 'node:https';
import {
  ATTEMPT_HEADER,
  DELIVERY_HEADER,
  SIGNATURE_HEADER,
  TIMESTAMP_HEADER,
  signatureHeader,
} from '../core/signature.js';
import {
  UnsafeUrlError,
  assertDeliverableUrl,
  createGuardedLookup,
  type GuardedLookup,
} from '../core/url-guard.js';
import type { DeliveryClient, DeliveryRequest, DeliveryResult } from '../ports.js';

export interface HttpDeliveryClientOptions {
  /** Allow deliveries to loopback/private/link-local addresses. Off by default. */
  allowPrivateEndpoints?: boolean;
  /** Override the guarded DNS lookup. Tests use this; production should not. */
  lookup?: GuardedLookup;
}

/** Walk the shapes Node and happy-eyeballs use to wrap a lookup failure. */
function findUnsafeUrlError(error: unknown): UnsafeUrlError | null {
  if (error instanceof UnsafeUrlError) return error;
  if (error instanceof AggregateError) {
    for (const inner of error.errors) {
      const found = findUnsafeUrlError(inner);
      if (found) return found;
    }
    return null;
  }
  if (error instanceof Error) return findUnsafeUrlError(error.cause);
  return null;
}

export class HttpDeliveryClient implements DeliveryClient {
  private readonly allowPrivate: boolean;
  private readonly lookup: GuardedLookup;

  constructor(options: HttpDeliveryClientOptions = {}) {
    this.allowPrivate = options.allowPrivateEndpoints ?? false;
    this.lookup = options.lookup ?? createGuardedLookup({ allowPrivate: this.allowPrivate });
  }

  async send(request: DeliveryRequest): Promise<DeliveryResult> {
    const startedAt = Date.now();
    const body = JSON.stringify(request.payload);
    const timestamp = Math.floor(Date.now() / 1000);

    let target: URL;
    try {
      target = assertDeliverableUrl(request.url, { allowPrivate: this.allowPrivate });
    } catch (error) {
      return {
        ok: false,
        statusCode: null,
        error: error instanceof Error ? error.message : String(error),
        durationMs: Date.now() - startedAt,
        permanent: true,
      };
    }

    return new Promise<DeliveryResult>((resolve) => {
      let settled = false;
      const finish = (result: DeliveryResult): void => {
        if (settled) return;
        settled = true;
        resolve(result);
      };

      const fail = (error: unknown): void => {
        const unsafe = findUnsafeUrlError(error);
        const message =
          unsafe?.message ??
          (error instanceof Error ? error.message : String(error));
        finish({
          ok: false,
          statusCode: null,
          error: message,
          durationMs: Date.now() - startedAt,
          // A blocked target is a policy decision, not a transient fault:
          // retrying it will never succeed and only burns attempts.
          ...(unsafe ? { permanent: true } : {}),
        });
      };

      const options: RequestOptions = {
        protocol: target.protocol,
        hostname: target.hostname.replace(/^\[|\]$/g, ''),
        port: target.port === '' ? undefined : Number(target.port),
        path: `${target.pathname}${target.search}`,
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          'content-length': Buffer.byteLength(body),
          'user-agent': 'hookrelay/0.1 (+https://github.com/gabrielchv/hookrelay)',
          [SIGNATURE_HEADER]: signatureHeader(request.secret, timestamp, body),
          [TIMESTAMP_HEADER]: String(timestamp),
          [DELIVERY_HEADER]: request.deliveryId,
          [ATTEMPT_HEADER]: String(request.attempt),
        },
        lookup: this.lookup as unknown as RequestOptions['lookup'],
        timeout: request.timeoutMs,
      };

      const send = target.protocol === 'https:' ? httpsRequest : httpRequest;

      let outgoing;
      try {
        outgoing = send(options, (response) => {
          const statusCode = response.statusCode ?? null;
          // Drain so the socket can be reused; we do not interpret the body.
          response.resume();
          finish({
            ok: statusCode !== null && statusCode >= 200 && statusCode < 300,
            statusCode,
            error: null,
            durationMs: Date.now() - startedAt,
          });
        });
      } catch (error) {
        fail(error);
        return;
      }

      outgoing.on('timeout', () => {
        outgoing.destroy(new Error(`timed out after ${request.timeoutMs}ms`));
      });
      outgoing.on('error', fail);

      outgoing.end(body);
    });
  }
}
