import { channel } from 'node:diagnostics_channel';
import { once } from 'node:events';
import * as http from 'node:http';
import * as net from 'node:net';
import { describe, expect, it } from 'vitest';
import { WebSocketServer } from 'ws';
import { streamOpenAIResponsesWebSocket } from '../src/llm/websocket-openai-responses.js';
import type { FormatAdapter } from '../src/llm/formats/types.js';

const passthroughFormat: FormatAdapter = {
  encodeRequest: () => ({ input: [] }),
  decodeResponse: () => ({ content: { role: 'model', parts: [] } }),
  decodeStreamChunk: (raw) => {
    const record = raw as { delta?: unknown };
    return typeof record.delta === 'string' ? { textDelta: record.delta } : {};
  },
  createStreamState: () => ({}),
};

describe('OpenAI Responses WebSocket undici transport', () => {
  it('consumes a burst of delta events and the terminal event without per-message pacing', async () => {
    const server = new WebSocketServer({ host: '127.0.0.1', port: 0, perMessageDeflate: true });
    await once(server, 'listening');
    const address = server.address();
    if (!address || typeof address === 'string') throw new Error('WebSocket test server did not expose a TCP port');

    let requestedExtensions: string | undefined;
    let negotiatedExtensions: string | undefined;
    const undiciOpenEvents: unknown[] = [];
    const undiciOpenChannel = channel('undici:websocket:open');
    const onUndiciOpen = (event: unknown) => undiciOpenEvents.push(event);
    undiciOpenChannel.subscribe(onUndiciOpen);
    server.once('headers', (_headers, request) => {
      requestedExtensions = request.headers['sec-websocket-extensions'];
    });
    server.once('connection', (socket) => {
      negotiatedExtensions = socket.extensions;
      socket.once('message', () => {
        for (let index = 0; index < 50; index += 1) {
          socket.send(JSON.stringify({
            type: 'response.output_text.delta',
            response_id: 'resp_burst',
            item_id: 'msg_burst',
            output_index: 0,
            content_index: 0,
            sequence_number: index,
            delta: String(index % 10),
          }));
        }
        socket.send(JSON.stringify({
          type: 'response.completed',
          response: { id: 'resp_burst', output: [] },
        }), () => socket.close());
      });
    });

    const startedAt = performance.now();
    let text = '';
    try {
      for await (const chunk of streamOpenAIResponsesWebSocket({
        endpoint: {
          url: `http://127.0.0.1:${address.port}`,
          webSocketUrl: `ws://127.0.0.1:${address.port}`,
          webSocketSessionKey: `transport-test-${Date.now()}-${Math.random()}`,
          headers: {},
        },
        url: `http://127.0.0.1:${address.port}`,
        headers: {},
        body: { input: [] },
        format: passthroughFormat,
      })) {
        text += chunk.textDelta ?? '';
      }
    } finally {
      undiciOpenChannel.unsubscribe(onUndiciOpen);
      for (const client of server.clients) client.terminate();
      await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
    }

    expect(text).toBe('0123456789'.repeat(5));
    const undiciOpen = undiciOpenEvents[0] as { websocket?: unknown; handshakeResponse?: { status?: number; headers?: unknown } } | undefined;
    expect(undiciOpen?.websocket).toBeDefined();
    expect(undiciOpen?.handshakeResponse?.status).toBe(101);
    expect(undiciOpen?.handshakeResponse?.headers).toBeDefined();
    expect(requestedExtensions).toBeUndefined();
    expect(negotiatedExtensions).toBe('');
    expect(performance.now() - startedAt).toBeLessThan(1_000);
  });

  it('reconnects when the proxy configuration changes for the same session key', async () => {
    const server = new WebSocketServer({ host: '127.0.0.1', port: 0, perMessageDeflate: true });
    await once(server, 'listening');
    const address = server.address();
    if (!address || typeof address === 'string') throw new Error('WebSocket test server did not expose a TCP port');

    let targetConnectionCount = 0;
    const requestedExtensions: Array<string | undefined> = [];
    const negotiatedExtensions: string[] = [];
    server.on('headers', (_headers, request) => {
      requestedExtensions.push(request.headers['sec-websocket-extensions']);
    });
    server.on('connection', (socket) => {
      targetConnectionCount += 1;
      negotiatedExtensions.push(socket.extensions);
      socket.on('message', () => {
        socket.send(JSON.stringify({
          type: 'response.output_text.delta',
          response_id: `resp_proxy_${targetConnectionCount}`,
          item_id: `msg_proxy_${targetConnectionCount}`,
          output_index: 0,
          content_index: 0,
          delta: 'ok',
        }));
        socket.send(JSON.stringify({
          type: 'response.completed',
          response: { id: `resp_proxy_${targetConnectionCount}`, output: [] },
        }));
      });
    });

    let proxyConnectCount = 0;
    const proxyServer = http.createServer();
    proxyServer.on('connect', (request, clientSocket, head) => {
      const targetUrl = new URL(`http://${request.url ?? ''}`);
      proxyConnectCount += 1;
      const upstream = net.connect(Number(targetUrl.port), targetUrl.hostname, () => {
        clientSocket.write('HTTP/1.1 200 Connection Established\r\n\r\n');
        if (head.length > 0) upstream.write(head);
        upstream.pipe(clientSocket);
        clientSocket.pipe(upstream);
      });
      upstream.on('error', () => clientSocket.destroy());
    });
    proxyServer.listen(0, '127.0.0.1');
    await once(proxyServer, 'listening');
    const proxyAddress = proxyServer.address();
    if (!proxyAddress || typeof proxyAddress === 'string') throw new Error('Proxy test server did not expose a TCP port');

    const sessionKey = `proxy-switch-test-${Date.now()}-${Math.random()}`;
    const url = `http://127.0.0.1:${address.port}`;
    const run = async (proxy?: string): Promise<string> => {
      let text = '';
      for await (const chunk of streamOpenAIResponsesWebSocket({
        endpoint: {
          url,
          webSocketUrl: `ws://127.0.0.1:${address.port}`,
          webSocketSessionKey: sessionKey,
          headers: {},
          ...(proxy !== undefined ? { proxy } : {}),
        },
        url,
        headers: {},
        body: { input: [] },
        format: passthroughFormat,
      })) {
        text += chunk.textDelta ?? '';
      }
      return text;
    };

    try {
      expect(await run()).toBe('ok');
      expect(proxyConnectCount).toBe(0);
      expect(targetConnectionCount).toBe(1);

      expect(await run()).toBe('ok');
      expect(proxyConnectCount).toBe(0);
      expect(targetConnectionCount).toBe(1);

      expect(await run(`http://proxy-user:proxy-secret@127.0.0.1:${proxyAddress.port}`)).toBe('ok');
      expect(proxyConnectCount).toBe(1);
      expect(targetConnectionCount).toBe(2);

      expect(await run('')).toBe('ok');
      expect(proxyConnectCount).toBe(1);
      expect(targetConnectionCount).toBe(3);
      expect(requestedExtensions).toEqual([undefined, undefined, undefined]);
      expect(negotiatedExtensions).toEqual(['', '', '']);
    } finally {
      for (const client of server.clients) client.terminate();
      await new Promise<void>((resolve, reject) => proxyServer.close((error) => error ? reject(error) : resolve()));
      await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
    }

  });

  it('closes an aborted socket and sends the next turn on a new connection with full local context', async () => {
    const server = new WebSocketServer({ host: '127.0.0.1', port: 0 });
    await once(server, 'listening');
    const address = server.address();
    if (!address || typeof address === 'string') throw new Error('WebSocket test server did not expose a TCP port');

    const payloads: Array<{ connection: number; payload: Record<string, unknown> }> = [];
    let connectionCount = 0;
    let resolveInterruptedRequest!: () => void;
    const interruptedRequestReceived = new Promise<void>((resolve) => { resolveInterruptedRequest = resolve; });
    server.on('connection', (socket) => {
      const connection = ++connectionCount;
      socket.on('message', (data) => {
        const payload = JSON.parse(data.toString()) as Record<string, unknown>;
        payloads.push({ connection, payload });
        if (payloads.length === 1) {
          socket.send(JSON.stringify({ type: 'response.completed', response: { id: 'resp_seed', output: [] } }));
        } else if (payloads.length === 2) {
          resolveInterruptedRequest();
        } else if (payloads.length === 3) {
          socket.send(JSON.stringify({ type: 'response.completed', response: { id: 'resp_after_interrupt', output: [] } }));
        }
      });
    });

    const sessionKey = `abort-reconnect-${Date.now()}-${Math.random()}`;
    const url = `http://127.0.0.1:${address.port}`;
    const inputItem = (text: string) => ({ role: 'user', content: [{ type: 'input_text', text }] });
    const firstInput = inputItem('first');
    const secondInput = inputItem('second');
    const interruptInput = inputItem('[Background command exited] done');
    const consume = async (input: unknown[], signal?: AbortSignal): Promise<void> => {
      for await (const _chunk of streamOpenAIResponsesWebSocket({
        endpoint: {
          url,
          webSocketUrl: `ws://127.0.0.1:${address.port}`,
          webSocketSessionKey: sessionKey,
          headers: {},
        },
        url,
        headers: {},
        body: { input },
        format: passthroughFormat,
        signal,
      })) {
        // Consume until the response completes or the caller aborts it.
      }
    };

    try {
      await consume([firstInput]);

      const controller = new AbortController();
      const interrupted = consume([firstInput, secondInput], controller.signal);
      await interruptedRequestReceived;
      controller.abort(new Error('test interrupt'));
      await expect(interrupted).rejects.toThrow('test interrupt');

      await consume([firstInput, secondInput, interruptInput]);

      expect(connectionCount).toBe(2);
      expect(payloads).toHaveLength(3);
      expect(payloads[0]).toMatchObject({ connection: 1, payload: { input: [firstInput] } });
      expect(payloads[0].payload.previous_response_id).toBeUndefined();
      expect(payloads[1]).toMatchObject({ connection: 1, payload: { input: [secondInput], previous_response_id: 'resp_seed' } });
      expect(payloads[2]).toMatchObject({ connection: 2, payload: { input: [firstInput, secondInput, interruptInput] } });
      expect(payloads[2].payload.previous_response_id).toBeUndefined();
    } finally {
      for (const client of server.clients) client.terminate();
      await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
    }
  });

  it('fails instead of falling back to a direct socket when the proxy is unavailable', async () => {
    const server = new WebSocketServer({ host: '127.0.0.1', port: 0 });
    await once(server, 'listening');
    const address = server.address();
    if (!address || typeof address === 'string') throw new Error('WebSocket test server did not expose a TCP port');

    let targetConnectionCount = 0;
    server.on('connection', () => { targetConnectionCount += 1; });

    const unavailableProxy = http.createServer();
    unavailableProxy.listen(0, '127.0.0.1');
    await once(unavailableProxy, 'listening');
    const proxyAddress = unavailableProxy.address();
    if (!proxyAddress || typeof proxyAddress === 'string') throw new Error('Proxy test server did not expose a TCP port');
    await new Promise<void>((resolve, reject) => unavailableProxy.close((error) => error ? reject(error) : resolve()));

    const url = `http://127.0.0.1:${address.port}`;
    const consume = async (): Promise<void> => {
      for await (const _chunk of streamOpenAIResponsesWebSocket({
        endpoint: {
          url,
          webSocketUrl: `ws://127.0.0.1:${address.port}`,
          webSocketSessionKey: `unavailable-proxy-${Date.now()}-${Math.random()}`,
          headers: {},
          proxy: `http://127.0.0.1:${proxyAddress.port}`,
        },
        url,
        headers: {},
        body: { input: [] },
        format: passthroughFormat,
      })) {
        // Consume the generator until it rejects during connection setup.
      }
    };

    try {
      await expect(consume()).rejects.toThrow();
      expect(targetConnectionCount).toBe(0);
    } finally {
      for (const client of server.clients) client.terminate();
      await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
    }

  });
});
