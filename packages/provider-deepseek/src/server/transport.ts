// @effect-diagnostics globalDate:off -- Native fetch/AbortSignal boundary; timestamps measure this HTTP request only.
// @effect-diagnostics globalTimers:off -- Abortable retry delay uses the native timer boundary.
// @effect-diagnostics extendsNativeError:off -- The Promise transport is translated to tagged ProviderAdapterProtocolError at the Effect boundary.
/** Direct official DeepSeek transport. There is deliberately no proxy/CLI fallback. */
export type DeepSeekEffort = "none" | "low" | "high" | "max";
export interface ApiToolCall {
  id: string;
  type: "function";
  function: { name: string; arguments: string };
}
export interface ApiMessage {
  role: "system" | "user" | "assistant" | "tool";
  content: string | null;
  reasoning_content?: string;
  tool_calls?: ApiToolCall[];
  tool_call_id?: string;
}
export interface ApiTool {
  type: "function";
  function: { name: string; description: string; parameters: Record<string, unknown> };
}
export interface ApiUsage {
  prompt_tokens: number;
  completion_tokens: number;
  total_tokens: number;
  completion_tokens_details?: { reasoning_tokens?: number };
}
export class DeepSeekApiError extends Error {
  readonly code: string;
  readonly httpStatus: number | undefined;
  constructor(code: string, httpStatus?: number) {
    super(`DeepSeek API: ${code}${httpStatus === undefined ? "" : ` (HTTP ${httpStatus})`}`);
    this.code = code;
    this.httpStatus = httpStatus;
  }
}

function transportFailure(cause: unknown, stage: "fetch" | "stream-read") {
  const error =
    cause && typeof cause === "object"
      ? (cause as { code?: unknown; syscall?: unknown; cause?: unknown })
      : {};
  const nested =
    error.cause && typeof error.cause === "object" ? (error.cause as typeof error) : {};
  const code =
    [error.code, nested.code].find(
      (value) =>
        typeof value === "string" &&
        /^(?:UND_ERR_(?:CONNECT_TIMEOUT|HEADERS_TIMEOUT|BODY_TIMEOUT|SOCKET)|E(?:CONNRESET|CONNREFUSED|TIMEDOUT|NOTFOUND|AI_AGAIN|NETUNREACH|HOSTUNREACH)|ERR_TLS_CERT_ALTNAME_INVALID|CERT_HAS_EXPIRED|UNABLE_TO_VERIFY_LEAF_SIGNATURE|DEPTH_ZERO_SELF_SIGNED_CERT)$/.test(
          value,
        ),
    ) ?? "unknown";
  const syscall =
    [error.syscall, nested.syscall].find(
      (value) =>
        typeof value === "string" &&
        /^(?:connect|read|write|getaddrinfo|lookup|send|recv|shutdown)$/.test(value),
    ) ?? "none";
  return new DeepSeekApiError(`transport-${stage}:${code}:${syscall}`);
}

function officialEndpoint(path: string): string {
  if (path !== "/chat/completions") {
    throw new DeepSeekApiError("unsupported-endpoint");
  }
  return `https://api.deepseek.com${path}`;
}

export async function retryDelay(signal: AbortSignal, milliseconds = 200): Promise<void> {
  signal.throwIfAborted();
  await new Promise<void>((resolve, reject) => {
    const abort = () => {
      clearTimeout(timer);
      reject(signal.reason);
    };
    const timer = setTimeout(() => {
      signal.removeEventListener("abort", abort);
      resolve();
    }, milliseconds);
    signal.addEventListener("abort", abort, { once: true });
  });
}

/** Read SSE with streaming UTF-8, CRLF, comments and multi-line data fields. */
export async function* sseData(body: ReadableStream<Uint8Array>, signal: AbortSignal) {
  const reader = body.getReader();
  const decoder = new TextDecoder("utf-8", { fatal: true });
  let buffer = "";
  let fields: string[] = [];
  let fieldBytes = 0;
  const encoder = new TextEncoder();
  const aborted = () => void reader.cancel().catch(() => {});
  signal.addEventListener("abort", aborted, { once: true });
  try {
    while (true) {
      signal.throwIfAborted();
      const chunk = await reader.read().catch((cause: unknown) => {
        if (signal.aborted) throw cause;
        throw transportFailure(cause, "stream-read");
      });
      buffer += decoder.decode(chunk.value, { stream: !chunk.done });
      if (buffer.length > 8 * 1024 * 1024) throw new DeepSeekApiError("sse-frame-too-large");
      let end: number;
      while ((end = buffer.indexOf("\n")) >= 0) {
        const line = buffer.slice(0, end).replace(/\r$/, "");
        buffer = buffer.slice(end + 1);
        if (line === "") {
          if (fields.length) yield fields.join("\n");
          fields = [];
          fieldBytes = 0;
        } else if (line.startsWith("data:")) {
          const value = line.slice(5).replace(/^ /, "");
          const nextBytes = fieldBytes + encoder.encode(value).byteLength + (fields.length ? 1 : 0);
          if (nextBytes > 8 * 1024 * 1024) throw new DeepSeekApiError("sse-frame-too-large");
          fields.push(value);
          fieldBytes = nextBytes;
        }
      }
      if (chunk.done) break;
    }
    signal.throwIfAborted();
  } finally {
    signal.removeEventListener("abort", aborted);
    await reader.cancel().catch(() => {});
    reader.releaseLock();
  }
}

export interface CompletionInput {
  apiKey: string;
  model: string;
  effort: DeepSeekEffort;
  messages: readonly ApiMessage[];
  tools: readonly ApiTool[];
  signal: AbortSignal;
  onDelta?: (kind: "text" | "reasoning", value: string) => void;
  /** One caller-owned recovery starts on a new connection; the request body is unchanged. */
  freshConnection?: boolean;
  /** Dependency injection for offline tests; production always uses officialEndpoint. */
  fetch?: typeof globalThis.fetch;
}

export async function complete(input: CompletionInput): Promise<{
  message: ApiMessage;
  usage: ApiUsage | null;
  requestId: string | null;
  responseModel: string;
  startedAt: string;
  completedAt: string;
}> {
  if (!input.apiKey || !input.model) throw new DeepSeekApiError("missing-credentials-or-model");
  if (!["none", "low", "high", "max"].includes(input.effort)) {
    throw new DeepSeekApiError("unsupported-reasoning-effort");
  }
  input.signal.throwIfAborted();
  const startedAt = new Date().toISOString();
  const request: RequestInit = {
    method: "POST",
    redirect: "error",
    headers: {
      Authorization: `Bearer ${input.apiKey}`,
      "Content-Type": "application/json",
      ...(input.freshConnection ? { Connection: "close" } : {}),
    },
    body: JSON.stringify({
      model: input.model,
      messages: input.messages,
      ...(input.tools.length ? { tools: input.tools } : {}),
      thinking: { type: input.effort === "none" ? "disabled" : "enabled" },
      ...(input.effort === "none" ? {} : { reasoning_effort: input.effort }),
      stream: true,
      stream_options: { include_usage: true },
    }),
    signal: input.signal,
  };
  let response: Response | undefined;
  for (let attempt = 0; attempt < 2; attempt++) {
    input.signal.throwIfAborted();
    try {
      response = await (input.fetch ?? globalThis.fetch)(officialEndpoint("/chat/completions"), {
        ...request,
        ...(attempt === 1 ? { headers: { ...request.headers, Connection: "close" } } : {}),
      });
      break;
    } catch (cause: unknown) {
      if (input.signal.aborted) throw cause;
      const failure = transportFailure(cause, "fetch");
      if (attempt !== 0 || !/^transport-fetch:(?:UND_ERR_SOCKET|ECONNRESET):/.test(failure.code)) {
        throw failure;
      }
      await retryDelay(input.signal);
    }
  }
  if (!response) throw new DeepSeekApiError("missing-response");
  // Never include provider error bodies: upstream errors can contain sensitive input.
  if (!response.ok) {
    await response.body?.cancel();
    throw new DeepSeekApiError("request-rejected", response.status);
  }
  if (!response.body) throw new DeepSeekApiError("missing-stream");
  const message: ApiMessage = { role: "assistant", content: "", reasoning_content: "" };
  const calls = new Map<number, ApiToolCall>();
  let usage: ApiUsage | null = null;
  let requestId: string | null = null;
  let responseModel: string | null = null;
  let finish:
    | "stop"
    | "tool_calls"
    | "length"
    | "content_filter"
    | "insufficient_system_resource"
    | "invalid"
    | null = null;
  let done = false;
  for await (const data of sseData(response.body, input.signal)) {
    if (data === "[DONE]") {
      done = true;
      break;
    }
    let frame: any;
    try {
      frame = JSON.parse(data);
    } catch {
      throw new DeepSeekApiError("invalid-stream-json");
    }
    if (frame.error) throw new DeepSeekApiError("stream-error");
    if (typeof frame.model === "string") {
      if (frame.model !== input.model || (responseModel !== null && responseModel !== frame.model))
        throw new DeepSeekApiError("model-identity-mismatch");
      responseModel = frame.model;
    }
    if (typeof frame.id === "string") {
      if (requestId !== null && requestId !== frame.id)
        throw new DeepSeekApiError("request-identity-changed");
      requestId = frame.id;
    }
    if (frame.usage) {
      const u = frame.usage;
      if (
        ![u.prompt_tokens, u.completion_tokens, u.total_tokens].every(
          (n) => Number.isSafeInteger(n) && n >= 0,
        )
      ) {
        throw new DeepSeekApiError("invalid-usage");
      }
      usage = u;
    }
    const choice = frame.choices?.[0];
    if (!choice) continue;
    const delta = choice.delta ?? {};
    for (const [field, kind] of [
      ["content", "text"],
      ["reasoning_content", "reasoning"],
    ] as const) {
      const value = delta[field];
      if (typeof value === "string" && value) {
        message[field] = (message[field] ?? "") + value;
        if ((message[field]?.length ?? 0) > 16 * 1024 * 1024)
          throw new DeepSeekApiError("response-too-large");
        input.onDelta?.(kind, value);
      }
    }
    for (const part of delta.tool_calls ?? []) {
      if (!Number.isSafeInteger(part.index) || part.index < 0 || part.index > 127) {
        throw new DeepSeekApiError("invalid-tool-index");
      }
      const call = calls.get(part.index) ?? {
        id: "",
        type: "function",
        function: { name: "", arguments: "" },
      };
      if (part.id) {
        if (call.id && call.id !== part.id) throw new DeepSeekApiError("tool-identity-changed");
        call.id = part.id;
      }
      if (part.function?.name) call.function.name += part.function.name;
      if (part.function?.arguments) call.function.arguments += part.function.arguments;
      if (call.function.arguments.length > 1024 * 1024)
        throw new DeepSeekApiError("tool-arguments-too-large");
      calls.set(part.index, call);
    }
    if (choice.finish_reason !== undefined && choice.finish_reason !== null) {
      switch (choice.finish_reason) {
        case "stop":
        case "tool_calls":
        case "length":
        case "content_filter":
        case "insufficient_system_resource":
          finish = choice.finish_reason;
          break;
        default:
          finish = "invalid";
      }
    }
  }
  input.signal.throwIfAborted();
  if (!done || !["stop", "tool_calls"].includes(finish ?? "")) {
    const code = !done
      ? "incomplete-response-no-done"
      : finish === "length"
        ? "incomplete-response-length"
        : finish === "content_filter"
          ? "incomplete-response-content-filter"
          : finish === "insufficient_system_resource"
            ? "incomplete-response-insufficient-system-resource"
            : finish === null
              ? "incomplete-response-no-terminal"
              : "incomplete-response-invalid-terminal";
    throw new DeepSeekApiError(code);
  }
  if (responseModel === null) throw new DeepSeekApiError("model-identity-missing");
  if (calls.size) {
    const values = [...calls.entries()].sort(([a], [b]) => a - b).map(([, value]) => value);
    if (
      finish !== "tool_calls" ||
      values.some((c) => !c.id || !c.function.name) ||
      new Set(values.map((c) => c.id)).size !== values.length
    )
      throw new DeepSeekApiError("invalid-tool-calls");
    message.tool_calls = values;
  } else if (finish === "tool_calls") throw new DeepSeekApiError("missing-tool-calls");
  else if (!message.content?.trim()) throw new DeepSeekApiError("empty-final-response");
  return {
    message,
    usage,
    requestId,
    responseModel,
    startedAt,
    completedAt: new Date().toISOString(),
  };
}
