/**
 * HTTP delivery adapter. Signs the exact bytes on the wire (we serialize once
 * and send that string) so receivers can verify without re-serializing.
 */
import {
  ATTEMPT_HEADER,
  DELIVERY_HEADER,
  SIGNATURE_HEADER,
  TIMESTAMP_HEADER,
  signatureHeader,
} from '../core/signature.js';
import type { DeliveryClient, DeliveryRequest, DeliveryResult } from '../ports.js';

export class HttpDeliveryClient implements DeliveryClient {
  async send(request: DeliveryRequest): Promise<DeliveryResult> {
    const body = JSON.stringify(request.payload);
    const timestamp = Math.floor(Date.now() / 1000);
    const startedAt = Date.now();

    try {
      const response = await fetch(request.url, {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          'user-agent': 'hookrelay/0.1 (+https://github.com/gabrielchv/hookrelay)',
          [SIGNATURE_HEADER]: signatureHeader(request.secret, timestamp, body),
          [TIMESTAMP_HEADER]: String(timestamp),
          [DELIVERY_HEADER]: request.deliveryId,
          [ATTEMPT_HEADER]: String(request.attempt),
        },
        body,
        signal: AbortSignal.timeout(request.timeoutMs),
        redirect: 'manual',
      });

      // Drain the body so the connection can be reused; we do not interpret it.
      await response.arrayBuffer().catch(() => undefined);

      return {
        ok: response.status >= 200 && response.status < 300,
        statusCode: response.status,
        error: null,
        durationMs: Date.now() - startedAt,
      };
    } catch (error) {
      const message =
        error instanceof Error
          ? error.name === 'TimeoutError'
            ? `timed out after ${request.timeoutMs}ms`
            : error.message
          : String(error);
      return {
        ok: false,
        statusCode: null,
        error: message,
        durationMs: Date.now() - startedAt,
      };
    }
  }
}
