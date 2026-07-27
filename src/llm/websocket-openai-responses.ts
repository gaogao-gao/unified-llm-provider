/**
 * OpenAI Responses WebSocket transport.
 *
 * This module keeps WebSocket continuation state as a transport optimization only.
 * The caller must still provide the full local context on every request; the
 * transport decides whether it can safely send only the incremental suffix with
 * previous_response_id, and falls back to full input whenever local context no
 * longer matches the connection-local state.
 */

import { createHash } from 'node:crypto';
import { getGlobalDispatcher, WebSocket as UndiciWebSocket } from 'undici';
import type { LLMProxyOption } from '../config/types.js';
import type { LLMRawErrorInfo, LLMRequest, LLMResponse, LLMStreamChunk } from '../types.js';
import type { FormatAdapter } from './formats/types.js';
import { getProxyDispatcher, type EndpointConfig } from './transport.js';

const OPENAI_RESPONSES_WS_MAX_AGE_MS = 55 * 60 * 1000;

interface WebSocketSession {
  key: string;
  connectionFingerprint: string;
  socket?: UndiciWebSocket;
  connectedAt?: number;
  previousResponseId?: string;
  serverInputItems?: unknown[];
  baseSignature?: string;
  lock?: Promise<void>;
}

interface PreparedCreatePayload {
  payload: Record<string, unknown>;
  fullInputItems: unknown[];
  baseSignature: string;
  decision: 'full' | 'incremental';
  decisionReason: string;
  usedPreviousResponseId?: string;
}

export interface StreamedReasoningSignatureRecord {
  itemId?: string;
  outputIndex?: number;
  encryptedContent?: string;
}

export interface OpenAIResponsesWebSocketStreamOptions {
  endpoint: EndpointConfig;
  url: string;
  headers: Record<string, string>;
  body: unknown;
  format: FormatAdapter;
  signal?: AbortSignal;
}

interface QueuedMessage {
  value?: unknown;
  done?: boolean;
  error?: unknown;
}

interface NormalizedWebSocketProxy {
  uri: string;
  headers?: Record<string, string>;
  cacheKey: string;
}

const sessions = new Map<string, WebSocketSession>();
let statelessSessionCounter = 0;

export async function* streamOpenAIResponsesWebSocket(
  options: OpenAIResponsesWebSocketStreamOptions,
): AsyncGenerator<LLMStreamChunk> {
  const fullBody = sanitizeResponsesCreateBody(options.body);
  const session = sessionFor(options.endpoint, options.url, options.headers);
  const release = await acquireSessionLock(session, options.signal);

  try {
    synchronizeSessionConnection(session, options.endpoint, options.url, options.headers);
    let allowIncremental = true;
    for (let attempt = 0; attempt < 2; attempt += 1) {
      const prepared = prepareCreatePayload(session, fullBody, allowIncremental);
      let completedResponse: unknown;
      let responseId = responseIdFromPayload(prepared.payload);
      let shouldRetryFull = false;
      const streamedReasoningSignatures: StreamedReasoningSignatureRecord[] = [];

      const socket = await ensureOpenSocket(session, options, attempt > 0);
      const state = options.format.createStreamState();

      try {
        for await (const raw of sendCreateAndReadEvents(socket, prepared.payload, options.signal)) {
          captureStreamedReasoningSignature(raw, streamedReasoningSignatures);
          responseId = responseIdFromPayload(raw) ?? responseId;
          completedResponse = completedResponseFromPayload(raw) ?? completedResponse;

          if (isProviderErrorPayload(raw)) {
            if (prepared.usedPreviousResponseId) invalidateSessionState(session);
            if (isRecoverableContinuationError(raw) && attempt === 0) {
              shouldRetryFull = true;
              allowIncremental = false;
              closeSessionSocket(session);
              break;
            }
            yield createErrorStreamChunk(errorInfoFromPayload(raw, raw));
            return;
          }

          try {
            yield options.format.decodeStreamChunk(raw, state);
          } catch (err) {
            yield createErrorStreamChunk({
              kind: 'decode_error',
              rawChunk: raw,
              message: stringifyError(err),
            });
          }
        }
      } catch (err) {
        closeSessionSocket(session);
        if (isAbortError(options.signal, err)) throw err;
        yield createErrorStreamChunk({
          kind: 'stream_read_error',
          message: stringifyError(err),
        });
        return;
      }

      if (shouldRetryFull) continue;

      if (responseId) {
        updateSessionAfterComplete(session, options.format, prepared, responseId, completedResponse, streamedReasoningSignatures);
      }
      session.connectedAt = session.connectedAt ?? Date.now();
      return;
    }
  } finally {
    release();
  }
}

function sanitizeResponsesCreateBody(body: unknown): Record<string, unknown> {
  if (!isPlainObject(body)) {
    throw new Error('OpenAI Responses WebSocket body must be a JSON object.');
  }
  const next: Record<string, unknown> = { ...body };
  delete next.type;
  delete next.stream;
  delete next.background;
  delete next.previous_response_id;
  // WebSocket continuation reuses connection-local state. Explicit HTTP prompt-cache
  // breakpoints mutate the last input item on every turn, which prevents stable
  // prefix matching; strip them for websocket transport and keep store=false.
  delete next.prompt_cache_options;
  next.store = false;
  next.input = Array.isArray(next.input) ? next.input.map(stripWebSocketOnlyInputFields) : [];
  return next;
}

function stripWebSocketOnlyInputFields(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(stripWebSocketOnlyInputFields);
  if (!isPlainObject(value)) return value;
  const result: Record<string, unknown> = {};
  for (const [key, child] of Object.entries(value)) {
    if (key === 'prompt_cache_breakpoint') continue;
    result[key] = stripWebSocketOnlyInputFields(child);
  }
  return result;
}

function prepareCreatePayload(
  session: WebSocketSession,
  fullBody: Record<string, unknown>,
  allowIncremental: boolean,
): PreparedCreatePayload {
  const fullInputItems = Array.isArray(fullBody.input) ? [...fullBody.input] : [];
  const baseSignature = requestBaseSignature(fullBody);
  const prefixMismatch = session.serverInputItems ? prefixMismatchReason(fullInputItems, session.serverInputItems) : 'no_cached_server_input';
  const decisionReason = !allowIncremental
    ? 'incremental_disabled_for_retry'
    : !session.previousResponseId
      ? 'no_previous_response_id'
      : !session.serverInputItems
        ? 'no_cached_server_input'
        : session.baseSignature !== baseSignature
          ? 'base_signature_changed'
          : prefixMismatch
            ? prefixMismatch
            : fullInputItems.length <= session.serverInputItems.length
              ? 'no_new_input_suffix'
              : 'matched_prefix';
  const canUsePrevious = decisionReason === 'matched_prefix';

  const body: Record<string, unknown> = {
    ...fullBody,
    input: canUsePrevious ? fullInputItems.slice(session.serverInputItems!.length) : fullInputItems,
  };
  if (canUsePrevious) body.previous_response_id = session.previousResponseId;

  return {
    payload: { type: 'response.create', ...body, store: false },
    fullInputItems,
    baseSignature,
    decision: canUsePrevious ? 'incremental' : 'full',
    decisionReason,
    ...(canUsePrevious ? { usedPreviousResponseId: session.previousResponseId } : {}),
  };
}

function updateSessionAfterComplete(
  session: WebSocketSession,
  format: FormatAdapter,
  prepared: PreparedCreatePayload,
  responseId: string,
  completedResponse: unknown,
  streamedReasoningSignatures: readonly StreamedReasoningSignatureRecord[],
): void {
  const responseInputItems = completedResponse
    ? encodeResponseAsInputItems(format, completedResponse, streamedReasoningSignatures)
    : [];
  session.previousResponseId = responseId;
  session.serverInputItems = [...prepared.fullInputItems, ...responseInputItems];
  session.baseSignature = prepared.baseSignature;
}

function encodeResponseAsInputItems(
  format: FormatAdapter,
  rawResponse: unknown,
  streamedReasoningSignatures: readonly StreamedReasoningSignatureRecord[],
): unknown[] {
  const streamCompatibleResponse = normalizeCompletedResponseForStreamSignatures(rawResponse, streamedReasoningSignatures);
  try {
    const decoded = format.decodeResponse(streamCompatibleResponse) as LLMResponse;
    const request: LLMRequest = { contents: [decoded.content] };
    const encoded = format.encodeRequest(request, false);
    if (isPlainObject(encoded) && Array.isArray(encoded.input)) return encoded.input.map(stripWebSocketOnlyInputFields);
  } catch {
    // Fall back to raw response.output below.
  }

  if (isPlainObject(streamCompatibleResponse) && Array.isArray(streamCompatibleResponse.output)) {
    return streamCompatibleResponse.output.map(stripWebSocketOnlyInputFields);
  }
  return [];
}

function captureStreamedReasoningSignature(
  payload: unknown,
  records: StreamedReasoningSignatureRecord[],
): void {
  if (!isPlainObject(payload) || eventType(payload) !== 'response.output_item.done') return;
  const item = payload.item;
  if (!isPlainObject(item) || item.type !== 'reasoning') return;

  const itemId = typeof item.id === 'string' && item.id.trim() ? item.id.trim() : undefined;
  const outputIndex = typeof payload.output_index === 'number' && Number.isInteger(payload.output_index)
    ? payload.output_index
    : undefined;
  const encryptedContent = typeof item.encrypted_content === 'string' && item.encrypted_content
    ? item.encrypted_content
    : undefined;
  const next: StreamedReasoningSignatureRecord = {
    ...(itemId ? { itemId } : {}),
    ...(outputIndex !== undefined ? { outputIndex } : {}),
    ...(encryptedContent ? { encryptedContent } : {}),
  };
  const existingIndex = records.findIndex((record) =>
    (itemId !== undefined && record.itemId === itemId)
    || (outputIndex !== undefined && record.outputIndex === outputIndex));
  if (existingIndex >= 0) records[existingIndex] = next;
  else records.push(next);
}

export function normalizeCompletedResponseForStreamSignatures(
  rawResponse: unknown,
  streamedReasoningSignatures: readonly StreamedReasoningSignatureRecord[],
): unknown {
  if (!isPlainObject(rawResponse) || !Array.isArray(rawResponse.output)) return rawResponse;

  let reasoningOrdinal = 0;
  const output = rawResponse.output.map((rawItem, outputIndex) => {
    if (!isPlainObject(rawItem) || rawItem.type !== 'reasoning') return rawItem;
    const itemId = typeof rawItem.id === 'string' && rawItem.id.trim() ? rawItem.id.trim() : undefined;
    const matched = streamedReasoningSignatures.find((record) => itemId !== undefined && record.itemId === itemId)
      ?? streamedReasoningSignatures.find((record) => record.outputIndex === outputIndex)
      ?? streamedReasoningSignatures[reasoningOrdinal];
    reasoningOrdinal += 1;

    const normalized: Record<string, unknown> = { ...rawItem };
    delete normalized.encrypted_content;
    if (matched?.encryptedContent) normalized.encrypted_content = matched.encryptedContent;
    return normalized;
  });

  return { ...rawResponse, output };
}

async function ensureOpenSocket(
  session: WebSocketSession,
  options: OpenAIResponsesWebSocketStreamOptions,
  forceNew: boolean,
): Promise<UndiciWebSocket> {
  const expired = isSessionExpired(session);
  if (forceNew || expired || !isSocketOpen(session.socket)) {
    closeSessionSocket(session);
    session.socket = await openSocket(options);
    session.connectedAt = Date.now();
    if (forceNew || expired) invalidateSessionState(session);
  }
  return session.socket!;
}

async function openSocket(options: OpenAIResponsesWebSocketStreamOptions): Promise<UndiciWebSocket> {
  const wsUrl = toWebSocketUrl(options.endpoint.webSocketUrl ?? options.url);
  const headers = webSocketHeaders(options.headers);
  const normalizedProxy = normalizeWebSocketProxy(options.endpoint.proxy);
  const baseDispatcher = normalizedProxy
    ? assertWebSocketDispatcher(await getProxyDispatcher(options.endpoint.proxy))
    : getGlobalDispatcher();
  const dispatcher = webSocketNoCompressionDispatcher(baseDispatcher);

  return new Promise<UndiciWebSocket>((resolve, reject) => {
    if (options.signal?.aborted) {
      reject(errorFromAbortSignal(options.signal));
      return;
    }

    let settled = false;
    const ws = new UndiciWebSocket(wsUrl, {
      headers,
      dispatcher: dispatcher as never,
    });

    const cleanup = () => {
      options.signal?.removeEventListener('abort', onAbort);
      ws.removeEventListener('open', onOpen as never);
      ws.removeEventListener('error', onError as never);
      ws.removeEventListener('close', onClose as never);
    };
    const finishResolve = () => {
      if (settled) return;
      settled = true;
      cleanup();
      resolve(ws);
    };
    const finishReject = (error: unknown) => {
      if (settled) return;
      settled = true;
      cleanup();
      try { ws.close(); } catch { /* noop */ }
      reject(error);
    };
    const onAbort = () => finishReject(errorFromAbortSignal(options.signal!));
    const onOpen = () => finishResolve();
    const onError = (event: Event) => finishReject(errorFromEvent(event));
    const onClose = (event: CloseEvent) => finishReject(
      new Error(`OpenAI Responses WebSocket closed before open: ${event.code} ${event.reason}`.trim()),
    );

    options.signal?.addEventListener('abort', onAbort, { once: true });
    ws.addEventListener('open', onOpen as never, { once: true });
    ws.addEventListener('error', onError as never, { once: true });
    ws.addEventListener('close', onClose as never, { once: true });
  });
}

async function* sendCreateAndReadEvents(
  socket: UndiciWebSocket,
  payload: Record<string, unknown>,
  signal?: AbortSignal,
): AsyncGenerator<unknown> {
  const queue = createAsyncQueue<unknown>(mergeOpenAIResponsesWebSocketEvents);
  let terminalSeen = false;

  const cleanup = () => {
    signal?.removeEventListener('abort', onAbort);
    socket.removeEventListener('message', onMessage as never);
    socket.removeEventListener('error', onError as never);
    socket.removeEventListener('close', onClose as never);
  };
  const finish = () => {
    terminalSeen = true;
    queue.end();
  };
  const onAbort = () => {
    const error = errorFromAbortSignal(signal!);
    try { socket.close(); } catch { /* noop */ }
    queue.fail(error);
  };
  const onMessage = (event: MessageEvent) => {
    const parsed = parseWebSocketData(event.data);
    if (!parsed.ok) {
      queue.push(createErrorPayload('stream_parse_error', parsed.error.message, event.data));
      finish();
      return;
    }
    queue.push(parsed.value);
    if (isTerminalEvent(parsed.value)) finish();
  };
  const onError = (event: Event) => queue.fail(errorFromEvent(event));
  const onClose = (event: CloseEvent) => {
    if (terminalSeen) return;
    queue.fail(new Error(`OpenAI Responses WebSocket closed: ${event.code} ${event.reason}`.trim()));
  };

  if (signal?.aborted) throw errorFromAbortSignal(signal);
  signal?.addEventListener('abort', onAbort, { once: true });
  socket.addEventListener('message', onMessage as never);
  socket.addEventListener('error', onError as never, { once: true });
  socket.addEventListener('close', onClose as never, { once: true });

  try {
    socket.send(JSON.stringify(payload));
    for await (const item of queue) yield item;
  } finally {
    cleanup();
  }
}

function sessionFor(endpoint: EndpointConfig, url: string, headers: Record<string, string>): WebSocketSession {
  const configured = endpoint.webSocketSessionKey?.trim();
  const key = configured || `stateless:${++statelessSessionCounter}:${url}:${headers.authorization ?? headers.Authorization ?? ''}`;
  const connectionFingerprint = webSocketConnectionFingerprint(endpoint, url, headers);
  let session = sessions.get(key);
  if (!session) {
    session = { key, connectionFingerprint };
    sessions.set(key, session);
  }
  return session;
}

function synchronizeSessionConnection(
  session: WebSocketSession,
  endpoint: EndpointConfig,
  url: string,
  headers: Record<string, string>,
): void {
  const connectionFingerprint = webSocketConnectionFingerprint(endpoint, url, headers);
  if (session.connectionFingerprint === connectionFingerprint) return;
  closeSessionSocket(session);
  invalidateSessionState(session);
  session.connectionFingerprint = connectionFingerprint;
}

async function acquireSessionLock(session: WebSocketSession, signal?: AbortSignal): Promise<() => void> {
  const previous = session.lock;
  let releaseCurrent!: () => void;
  session.lock = new Promise<void>((resolve) => { releaseCurrent = resolve; });
  if (previous) await waitWithAbort(previous, signal);
  let released = false;
  return () => {
    if (released) return;
    released = true;
    releaseCurrent();
  };
}

function waitWithAbort(promise: Promise<void>, signal?: AbortSignal): Promise<void> {
  if (!signal) return promise;
  if (signal.aborted) return Promise.reject(errorFromAbortSignal(signal));
  return new Promise<void>((resolve, reject) => {
    const cleanup = () => signal.removeEventListener('abort', onAbort);
    const onAbort = () => {
      cleanup();
      reject(errorFromAbortSignal(signal));
    };
    signal.addEventListener('abort', onAbort, { once: true });
    promise.then(
      () => { cleanup(); resolve(); },
      (error) => { cleanup(); reject(error); },
    );
  });
}

function createAsyncQueue<T>(
  mergeQueued?: (previous: T, next: T) => T | undefined,
): AsyncIterable<T> & { push(value: T): void; end(): void; fail(error: unknown): void } {
  const items: QueuedMessage[] = [];
  const waiters: Array<(item: QueuedMessage) => void> = [];
  let closed = false;

  const emit = (item: QueuedMessage) => {
    const waiter = waiters.shift();
    if (waiter) waiter(item);
    else items.push(item);
  };

  return {
    push(value: T) {
      if (closed) return;
      if (mergeQueued && waiters.length === 0) {
        const previous = items[items.length - 1];
        if (previous && !previous.done && previous.error === undefined) {
          const mergedValue = mergeQueued(previous.value as T, value);
          if (mergedValue !== undefined) {
            previous.value = mergedValue;
            return;
          }
        }
      }
      emit({ value });
    },
    end() {
      if (closed) return;
      closed = true;
      emit({ done: true });
    },
    fail(error: unknown) {
      if (closed) return;
      closed = true;
      emit({ error });
    },
    async *[Symbol.asyncIterator]() {
      while (true) {
        const item = items.shift() ?? await new Promise<QueuedMessage>((resolve) => waiters.push(resolve));
        if (item.error) throw item.error;
        if (item.done) return;
        yield item.value as T;
      }
    },
  };
}

const MERGEABLE_OPENAI_RESPONSES_WS_DELTA_EVENTS = new Set([
  'response.output_text.delta',
  'response.reasoning_summary_text.delta',
  'response.reasoning_text.delta',
  'response.reasoning.delta',
  'response.function_call_arguments.delta',
]);
const OPENAI_RESPONSES_WS_DELTA_IDENTITY_FIELDS = [
  'response_id',
  'item_id',
  'output_index',
  'content_index',
  'summary_index',
] as const;

function isMergeableOpenAIResponsesWebSocketDelta(value: unknown): value is Record<string, unknown> & { delta: string } {
  if (!isPlainObject(value)) return false;
  return MERGEABLE_OPENAI_RESPONSES_WS_DELTA_EVENTS.has(eventType(value)) && typeof value.delta === 'string';
}

export function mergeOpenAIResponsesWebSocketEvents(previous: unknown, next: unknown): unknown | undefined {
  if (!isMergeableOpenAIResponsesWebSocketDelta(previous) || !isMergeableOpenAIResponsesWebSocketDelta(next)) return undefined;
  const type = eventType(previous);
  if (!type || type !== eventType(next)) return undefined;
  for (const field of OPENAI_RESPONSES_WS_DELTA_IDENTITY_FIELDS) {
    if (previous[field] !== next[field]) return undefined;
  }
  return {
    ...previous,
    ...next,
    delta: previous.delta + next.delta,
  };
}

function isSessionExpired(session: WebSocketSession): boolean {
  return session.connectedAt !== undefined && Date.now() - session.connectedAt >= OPENAI_RESPONSES_WS_MAX_AGE_MS;
}

function closeSessionSocket(session: WebSocketSession): void {
  const socket = session.socket;
  session.socket = undefined;
  session.connectedAt = undefined;
  if (!socket) return;
  try { socket.close(); } catch { /* noop */ }
}

function invalidateSessionState(session: WebSocketSession): void {
  session.previousResponseId = undefined;
  session.serverInputItems = undefined;
  session.baseSignature = undefined;
}

function isSocketOpen(socket: UndiciWebSocket | undefined): boolean {
  return !!socket && socket.readyState === UndiciWebSocket.OPEN;
}

function webSocketHeaders(headers: Record<string, string>): Record<string, string> {
  const result: Record<string, string> = {};
  for (const [key, value] of Object.entries(headers)) {
    const normalized = key.toLowerCase();
    if (normalized === 'content-type' || normalized === 'content-length') continue;
    result[key] = value;
  }
  return result;
}

type WebSocketDispatcher = ReturnType<typeof getGlobalDispatcher>;
type WebSocketDispatch = WebSocketDispatcher['dispatch'];
interface WebSocketDispatchOnly {
  dispatch: WebSocketDispatch;
}

const noCompressionDispatcherCache = new WeakMap<object, WebSocketDispatchOnly>();

function assertWebSocketDispatcher(value: unknown): WebSocketDispatcher {
  if (!value || typeof value !== 'object' || typeof (value as { dispatch?: unknown }).dispatch !== 'function') {
    throw new Error('WebSocket proxy dispatcher could not be created.');
  }
  return value as WebSocketDispatcher;
}

function webSocketNoCompressionDispatcher(base: WebSocketDispatcher): WebSocketDispatchOnly {
  const cacheKey = base as object;
  const cached = noCompressionDispatcherCache.get(cacheKey);
  if (cached) return cached;

  const dispatch: WebSocketDispatch = (options, handler) => {
    const headers = stripWebSocketCompressionHeader(options.headers);
    return base.dispatch({ ...options, headers: headers as typeof options.headers }, handler);
  };
  const wrapped = { dispatch };
  noCompressionDispatcherCache.set(cacheKey, wrapped);
  return wrapped;
}

function stripWebSocketCompressionHeader(headers: unknown): unknown {
  if (Array.isArray(headers)) {
    const result = [...headers];
    for (let index = result.length - 2; index >= 0; index -= 2) {
      if (String(result[index]).toLowerCase() === 'sec-websocket-extensions') {
        result.splice(index, 2);
      }
    }
    return result;
  }
  if (!headers || typeof headers !== 'object') return headers;
  const result: Record<string, unknown> = { ...(headers as Record<string, unknown>) };
  for (const key of Object.keys(result)) {
    if (key.toLowerCase() === 'sec-websocket-extensions') delete result[key];
  }
  return result;
}

function normalizeWebSocketProxy(proxy?: LLMProxyOption): NormalizedWebSocketProxy | undefined {
  if (!proxy) return undefined;
  const uri = (typeof proxy === 'string' ? proxy : proxy.url).trim();
  if (!uri) return undefined;
  const headers = typeof proxy === 'string' || !proxy.headers || Object.keys(proxy.headers).length === 0
    ? undefined
    : proxy.headers;
  const sortedHeaders = headers
    ? Object.fromEntries(Object.entries(headers).sort(([left], [right]) => left.localeCompare(right)))
    : undefined;
  return {
    uri,
    ...(headers ? { headers } : {}),
    cacheKey: JSON.stringify({ uri, headers: sortedHeaders }),
  };
}

function webSocketConnectionFingerprint(
  endpoint: EndpointConfig,
  url: string,
  headers: Record<string, string>,
): string {
  const normalizedHeaders = Object.fromEntries(
    Object.entries(webSocketHeaders(headers))
      .map(([key, value]) => [key.toLowerCase(), value] as const)
      .sort(([left], [right]) => left.localeCompare(right)),
  );
  const identity = stableStringify({
    url: toWebSocketUrl(endpoint.webSocketUrl ?? url),
    headers: normalizedHeaders,
    proxy: normalizeWebSocketProxy(endpoint.proxy)?.cacheKey ?? null,
  });
  return createHash('sha256').update(identity).digest('hex');
}

function toWebSocketUrl(url: string): string {
  const parsed = new URL(url);
  if (parsed.protocol === 'https:') parsed.protocol = 'wss:';
  else if (parsed.protocol === 'http:') parsed.protocol = 'ws:';
  return parsed.toString();
}

function requestBaseSignature(body: Record<string, unknown>): string {
  const { input: _input, previous_response_id: _previous, stream: _stream, background: _background, type: _type, ...rest } = body;
  void _input; void _previous; void _stream; void _background; void _type;
  return stableStringify(rest);
}

function prefixMismatchReason(items: unknown[], prefix: unknown[]): string | undefined {
  if (prefix.length > items.length) return `cached_prefix_longer:${prefix.length}>${items.length}`;
  for (let index = 0; index < prefix.length; index += 1) {
    if (stableStringify(items[index]) !== stableStringify(prefix[index])) return `input_prefix_mismatch_at:${index}`;
  }
  return undefined;
}

function stableStringify(value: unknown): string {
  if (value === null || typeof value !== 'object') return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(stableStringify).join(',')}]`;
  const record = value as Record<string, unknown>;
  return `{${Object.keys(record).sort().map((key) => `${JSON.stringify(key)}:${stableStringify(record[key])}`).join(',')}}`;
}

function parseWebSocketData(data: unknown): { ok: true; value: unknown } | { ok: false; error: Error } {
  try {
    if (typeof data === 'string') return { ok: true, value: JSON.parse(data) };
    if (Array.isArray(data) && data.every((part) => Buffer.isBuffer(part))) {
      return { ok: true, value: JSON.parse(Buffer.concat(data).toString('utf8')) };
    }
    if (data instanceof ArrayBuffer) return { ok: true, value: JSON.parse(new TextDecoder().decode(data)) };
    if (ArrayBuffer.isView(data)) return { ok: true, value: JSON.parse(new TextDecoder().decode(data)) };
    return { ok: true, value: JSON.parse(String(data)) };
  } catch (error) {
    return { ok: false, error: error instanceof Error ? error : new Error(String(error)) };
  }
}

function isTerminalEvent(value: unknown): boolean {
  const type = eventType(value);
  return type === 'response.completed'
    || type === 'response.failed'
    || type === 'response.incomplete'
    || type === 'response.cancelled'
    || type === 'error'
    || type.endsWith('.failed')
    || type.endsWith('.incomplete');
}

function completedResponseFromPayload(value: unknown): unknown | undefined {
  if (!isPlainObject(value)) return undefined;
  const type = eventType(value);
  if (type !== 'response.completed') return undefined;
  return isPlainObject(value.response) ? value.response : value;
}

function responseIdFromPayload(value: unknown): string | undefined {
  if (!isPlainObject(value)) return undefined;
  if (typeof value.id === 'string' && value.id.startsWith('resp_')) return value.id;
  if (typeof value.response_id === 'string') return value.response_id;
  const response = value.response;
  if (isPlainObject(response) && typeof response.id === 'string') return response.id;
  return undefined;
}

function isRecoverableContinuationError(value: unknown): boolean {
  const code = errorCode(value);
  return code === 'previous_response_not_found'
    || code === 'websocket_connection_limit_reached';
}

function errorCode(value: unknown): string | undefined {
  if (!isPlainObject(value)) return undefined;
  if (typeof value.code === 'string') return value.code;
  const error = value.error;
  if (isPlainObject(error) && typeof error.code === 'string') return error.code;
  return undefined;
}

function isProviderErrorPayload(payload: unknown): boolean {
  if (!isPlainObject(payload)) return false;
  const type = eventType(payload);
  if (type === 'error' || type.includes('error') || type.includes('failed') || type.includes('incomplete')) return true;
  if ('error' in payload && payload.error !== null && payload.error !== undefined) return true;
  const response = payload.response;
  if (isPlainObject(response)) {
    const status = typeof response.status === 'string' ? response.status.toLowerCase() : '';
    if ((status === 'failed' || status === 'incomplete' || status === 'cancelled') && (response.error || response.incomplete_details)) return true;
  }
  return false;
}

function errorInfoFromPayload(payload: unknown, rawChunk: unknown): LLMRawErrorInfo {
  const record = isPlainObject(payload) ? payload : { data: payload };
  const status = numericField(record.status) ?? numericField(record.status_code);
  const message = nestedPayloadMessage(record);
  return {
    kind: 'stream_error',
    rawChunk,
    event: eventType(payload) || undefined,
    ...(status !== undefined ? { status } : {}),
    ...(isPlainObject(record.headers) ? { headers: record.headers as Record<string, string> } : {}),
    ...(message ? { message } : {}),
    rawBody: payload,
  };
}

function nestedPayloadMessage(value: unknown): string | undefined {
  if (!isPlainObject(value)) return undefined;
  const direct = value.message;
  if (typeof direct === 'string' && direct.trim() && !isGenericErrorLabel(direct)) return direct.trim();
  const error = value.error;
  if (isPlainObject(error)) {
    const message = error.message;
    if (typeof message === 'string' && message.trim() && !isGenericErrorLabel(message)) return message.trim();
  }
  const response = value.response;
  if (isPlainObject(response)) return nestedPayloadMessage(response);
  return undefined;
}

function numericField(value: unknown): number | undefined {
  return typeof value === 'number' && Number.isFinite(value) ? value : undefined;
}

function isGenericErrorLabel(value: string): boolean {
  const normalized = value.trim().toLowerCase();
  return normalized === 'stream_error'
    || normalized === 'upstream_error'
    || normalized === 'http_error'
    || normalized === 'response_error'
    || normalized === 'decode_error'
    || normalized === 'stream_read_error'
    || normalized === 'stream_parse_error'
    || normalized === 'llm_error';
}

function createErrorPayload(kind: LLMRawErrorInfo['kind'], message: string, rawChunk: unknown): Record<string, unknown> {
  return {
    type: 'error',
    error: { code: kind, message },
    rawChunk,
  };
}

function createErrorStreamChunk(error: LLMRawErrorInfo): LLMStreamChunk {
  return {
    error,
    rawChunk: error.rawChunk ?? error.rawBody ?? error.bodyText ?? error.data,
  };
}

function eventType(value: unknown): string {
  if (!isPlainObject(value)) return '';
  return typeof value.type === 'string'
    ? value.type
    : typeof value.event === 'string'
      ? value.event
      : '';
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function stringifyError(error: unknown): string {
  return error instanceof Error ? `${error.name}: ${error.message}` : String(error);
}

function errorFromEvent(event: Event): Error {
  const maybe = event as Event & { error?: unknown; message?: unknown };
  if (maybe.error instanceof Error) return maybe.error;
  if (typeof maybe.message === 'string' && maybe.message) return new Error(maybe.message);
  return new Error(event.type || 'WebSocket error');
}

function errorFromAbortSignal(signal: AbortSignal): Error {
  const reason = signal.reason;
  if (reason instanceof Error) return reason;
  const error = new Error(reason ? String(reason) : 'OpenAI Responses WebSocket request aborted');
  error.name = 'AbortError';
  return error;
}

function isAbortError(signal: AbortSignal | undefined, error: unknown): boolean {
  return signal?.aborted === true || (error instanceof Error && error.name === 'AbortError');
}
