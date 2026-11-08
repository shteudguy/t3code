import { it } from "@effect/vitest";
// @effect-diagnostics nodeBuiltinImport:off -- Disposable real-filesystem provider fixtures.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import { afterEach, describe, expect, vi } from "vite-plus/test";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import * as Schema from "effect/Schema";
import * as C from "@t3tools/contracts";
import * as A from "@t3tools/provider-core/server/ProviderAdapter";
import type * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import { makeAdapter } from "./adapter.ts";
import { Journal } from "./journal.ts";
import type { NativeTool } from "./agent.ts";
const decodeSyncChatImageAttachment = Schema.decodeSync(C.ChatImageAttachment);
const isProviderAdapterV2Event = Schema.is(A.ProviderAdapterV2Event);
const isOrchestrationV2ProviderThread = Schema.is(C.OrchestrationV2ProviderThread);
const isOrchestrationV2ConversationMessage = Schema.is(C.OrchestrationV2ConversationMessage);
const isOrchestrationV2RuntimeRequest = Schema.is(C.OrchestrationV2RuntimeRequest);

const roots: string[] = [];
afterEach(async () => {
  vi.restoreAllMocks();
  await Promise.all(
    roots.splice(0).map((root) => NodeFSP.rm(root, { recursive: true, force: true })),
  );
});
const answer = (
  content: string,
  call?: { name: string; arguments: string },
  usage?: { prompt_tokens: number; completion_tokens: number; total_tokens: number },
) =>
  new Response(
    `data: ${JSON.stringify({ id: "api-request", model: "deepseek-flash", choices: [{ delta: call ? { content, tool_calls: [{ index: 0, id: "call-1", type: "function", function: call }] } : { content }, finish_reason: call ? "tool_calls" : "stop" }], ...(usage ? { usage } : {}) })}\n\ndata: [DONE]\n\n`,
  );

async function fixture(
  interactionMode: "plan" | "default" = "default",
  apiFetch: typeof fetch = async () => answer("готово"),
  toolFactory?: (cwd: string) => Promise<Map<string, NativeTool>>,
  runtimeMode: C.RuntimeMode = "full-access",
  withMcp = false,
) {
  const root = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-ds-adapter-"));
  roots.push(root);
  const instanceId = C.ProviderInstanceId.make("deepseek-api");
  const adapter = makeAdapter({
    instanceId,
    stateDir: root,
    apiKey: async () => "fixture-key",
    apiFetch,
    ...(toolFactory ? { toolFactory } : {}),
    mcpSessions: {
      read: () =>
        withMcp
          ? Effect.succeed({
              environmentId: C.EnvironmentId.make("fixture"),
              threadId: C.ThreadId.make("thread-test"),
              providerSessionId: "session-read",
              providerInstanceId: instanceId,
              endpoint: "http://127.0.0.1:1/mcp",
              authorizationHeader: "Bearer fixture-mcp",
              browserToolsAvailable: false,
            })
          : Effect.undefined,
    } as unknown as McpProviderSessions.McpProviderSessions["Service"],
  });
  const threadId = C.ThreadId.make("thread-test");
  const modelSelection = { instanceId, model: "deepseek-flash", options: [] };
  const runtimePolicy = { cwd: root, runtimeMode, interactionMode };
  return { root, adapter, threadId, modelSelection, runtimePolicy };
}

function start(
  runtime: A.ProviderAdapterV2SessionRuntime,
  f: Awaited<ReturnType<typeof fixture>>,
  thread: C.OrchestrationV2ProviderThread,
  ordinal = 1,
) {
  return runtime.startTurn({
    appThread: {} as C.OrchestrationV2AppThread,
    threadId: f.threadId,
    runId: C.RunId.make(`run-${ordinal}`),
    runOrdinal: ordinal,
    providerTurnOrdinal: ordinal,
    attemptId: C.RunAttemptId.make(`attempt-${ordinal}`),
    rootNodeId: C.NodeId.make(`root-${ordinal}`),
    providerThread: thread,
    modelSelection: f.modelSelection,
    runtimePolicy: f.runtimePolicy,
    message: {
      messageId: C.MessageId.make(`message-${ordinal}`),
      text: "fixture",
      attachments: [],
      createdBy: "user",
      creationSource: "web",
    },
  });
}
const untilTerminal = (runtime: A.ProviderAdapterV2SessionRuntime) =>
  Stream.runCollect(
    runtime.events.pipe(Stream.takeUntil((event) => event.type === "turn.terminal")),
  );

describe("native DeepSeek V2 adapter", () => {
  it.live("keeps distinct assistant messages around the tool that separates API responses", () =>
    Effect.gen(function* () {
      let request = 0;
      const tool: NativeTool = {
        mutating: false,
        definition: {
          type: "function",
          function: { name: "read_file", description: "fixture", parameters: { type: "object" } },
        },
        validate: (value) => value as Record<string, unknown>,
        execute: async () => "contents",
      };
      const f = yield* Effect.promise(() =>
        fixture(
          "default",
          async () =>
            ++request === 1
              ? answer(
                  "BEFORE|",
                  { name: "read_file", arguments: "{}" },
                  { prompt_tokens: 2, completion_tokens: 3, total_tokens: 5 },
                )
              : answer("AFTER", undefined, {
                  prompt_tokens: 4,
                  completion_tokens: 5,
                  total_tokens: 9,
                }),
          async () => new Map([["read_file", tool]]),
        ),
      );
      yield* Effect.scoped(
        Effect.gen(function* () {
          const runtime = yield* f.adapter.openSession({
            threadId: f.threadId,
            providerSessionId: C.ProviderSessionId.make("segments"),
            modelSelection: f.modelSelection,
            runtimePolicy: f.runtimePolicy,
          });
          const thread = yield* runtime.ensureThread({
            threadId: f.threadId,
            modelSelection: f.modelSelection,
            runtimePolicy: f.runtimePolicy,
          });
          yield* start(runtime, f, thread);
          const events = yield* untilTerminal(runtime);
          const items = new Map<string, C.OrchestrationV2TurnItem>();
          for (const event of events)
            if (event.type === "turn_item.updated") items.set(event.turnItem.id, event.turnItem);
          const ordered = [...items.values()].sort((a, b) => a.ordinal - b.ordinal);
          expect(
            ordered.map((item) => (item.type === "assistant_message" ? item.text : item.type)),
          ).toEqual(["BEFORE|", "dynamic_tool", "AFTER"]);
          expect(
            (yield* runtime.readThreadSnapshot({ providerThread: thread })).messages.map(
              (message) => message.text,
            ),
          ).toEqual(["BEFORE|", "AFTER"]);
          const finalTurn = (yield* runtime.readThreadSnapshot({ providerThread: thread }))
            .providerTurns[0];
          expect(finalTurn).toMatchObject({
            turnTokenUsage: { usageStatus: "complete", inputTokens: 6, outputTokens: 8 },
            tokenUsage: { usedTokens: 9, inputTokens: 4, outputTokens: 5 },
          });
        }),
      );
    }),
  );
  it.live("rejects steering attachments before accepting their text", () =>
    Effect.gen(function* () {
      const f = yield* Effect.promise(() => fixture());
      const attachment = decodeSyncChatImageAttachment({
        type: "image",
        id: "fixture-image",
        name: "fixture.png",
        mimeType: "image/png",
        sizeBytes: 1,
      });
      yield* Effect.scoped(
        Effect.gen(function* () {
          const runtime = yield* f.adapter.openSession({
            threadId: f.threadId,
            providerSessionId: C.ProviderSessionId.make("attachment-steer"),
            modelSelection: f.modelSelection,
            runtimePolicy: f.runtimePolicy,
          });
          const thread = yield* runtime.ensureThread({
            threadId: f.threadId,
            modelSelection: f.modelSelection,
            runtimePolicy: f.runtimePolicy,
          });
          const result = yield* runtime
            .steerTurn({
              threadId: f.threadId,
              runId: C.RunId.make("steer-run"),
              providerThread: thread,
              providerTurnId: C.ProviderTurnId.make("any-turn"),
              message: {
                messageId: C.MessageId.make("image-message"),
                text: "use image",
                attachments: [attachment],
                createdBy: "user",
                creationSource: "web",
              },
            })
            .pipe(Effect.result);
          expect(result._tag).toBe("Failure");
          expect(JSON.stringify(result)).toContain("direct-deepseek-attachments-not-supported-yet");
          const snapshot = yield* runtime.readThreadSnapshot({ providerThread: thread });
          expect(snapshot.messages).toHaveLength(0);
        }),
      );
    }),
  );
  it.live("emits schema-valid native lifecycle and message events", () =>
    Effect.gen(function* () {
      const f = yield* Effect.promise(() => fixture());
      const events = yield* Effect.scoped(
        Effect.gen(function* () {
          const runtime = yield* f.adapter.openSession({
            threadId: f.threadId,
            providerSessionId: C.ProviderSessionId.make("session-test"),
            modelSelection: f.modelSelection,
            runtimePolicy: f.runtimePolicy,
          });
          const thread = yield* runtime.ensureThread({
            threadId: f.threadId,
            modelSelection: f.modelSelection,
            runtimePolicy: f.runtimePolicy,
          });
          yield* runtime.startTurn({
            appThread: {} as C.OrchestrationV2AppThread,
            threadId: f.threadId,
            runId: C.RunId.make("run-test"),
            runOrdinal: 1,
            providerTurnOrdinal: 1,
            attemptId: C.RunAttemptId.make("attempt-test"),
            rootNodeId: C.NodeId.make("root-test"),
            providerThread: thread,
            modelSelection: f.modelSelection,
            runtimePolicy: f.runtimePolicy,
            message: {
              messageId: C.MessageId.make("user-test"),
              text: "ответь",
              attachments: [],
              createdBy: "user",
              creationSource: "web",
            },
          });
          return yield* Stream.runCollect(
            runtime.events.pipe(Stream.takeUntil((event) => event.type === "turn.terminal")),
          );
        }),
      );
      expect(events.length).toBeGreaterThan(3);
      for (const event of events)
        expect(isProviderAdapterV2Event(event), JSON.stringify(event)).toBe(true);
      expect(events.at(-1)).toMatchObject({ type: "turn.terminal", status: "completed" });
      expect(
        events.some(
          (event) =>
            event.type === "message.updated" &&
            event.message.text === "готово" &&
            !event.message.streaming,
        ),
      ).toBe(true);
      expect(
        events.some(
          (event) =>
            event.type === "turn_item.updated" &&
            event.turnItem.type === "assistant_message" &&
            event.turnItem.text === "готово" &&
            !event.turnItem.streaming &&
            event.turnItem.status === "completed",
        ),
      ).toBe(true);
    }),
  );
  it.live("refuses unsupported Plan before a model request or tool startup", () =>
    Effect.gen(function* () {
      let requests = 0;
      const f = yield* Effect.promise(() =>
        fixture("plan", async () => {
          requests++;
          return answer("unused");
        }),
      );
      yield* Effect.scoped(
        Effect.gen(function* () {
          const runtime = yield* f.adapter.openSession({
            threadId: f.threadId,
            providerSessionId: C.ProviderSessionId.make("plan-refused"),
            modelSelection: f.modelSelection,
            runtimePolicy: f.runtimePolicy,
          });
          const thread = yield* runtime.ensureThread({
            threadId: f.threadId,
            modelSelection: f.modelSelection,
            runtimePolicy: f.runtimePolicy,
          });
          const result = yield* start(runtime, f, thread).pipe(Effect.result);
          expect(result._tag).toBe("Failure");
          expect(JSON.stringify(result)).toContain("direct-deepseek-plan-mode-not-supported");
          expect(
            (yield* runtime.readThreadSnapshot({ providerThread: thread })).providerTurns,
          ).toHaveLength(0);
        }),
      );
      expect(requests).toBe(0);
    }),
  );
  it.live("declines native history injection so the common inline summary is retained", () =>
    Effect.gen(function* () {
      const f = yield* Effect.promise(() => fixture());
      yield* Effect.scoped(
        Effect.gen(function* () {
          const runtime = yield* f.adapter.openSession({
            threadId: f.threadId,
            providerSessionId: C.ProviderSessionId.make("handoff-fallback"),
            modelSelection: f.modelSelection,
            runtimePolicy: f.runtimePolicy,
          });
          const thread = yield* runtime.ensureThread({
            threadId: f.threadId,
            modelSelection: f.modelSelection,
            runtimePolicy: f.runtimePolicy,
          });
          expect(
            yield* runtime.injectHistory!({
              providerThread: thread,
              messages: [],
              context: "summary-only handoff must remain inline",
            }),
          ).toBe(false);
          expect(
            (yield* runtime.readThreadSnapshot({ providerThread: thread })).messages,
          ).toHaveLength(0);
        }),
      );
    }),
  );
  it.live(
    "fresh native recovery changes journal identity while retaining the app row identity",
    () =>
      Effect.gen(function* () {
        const requests: any[] = [];
        const f = yield* Effect.promise(() =>
          fixture("default", async (_url, init) => {
            requests.push(JSON.parse(String(init?.body)));
            return answer(requests.length === 1 ? "old native context" : "fresh native context");
          }),
        );
        let fresh!: C.OrchestrationV2ProviderThread;
        yield* Effect.scoped(
          Effect.gen(function* () {
            const runtime = yield* f.adapter.openSession({
              threadId: f.threadId,
              providerSessionId: C.ProviderSessionId.make("fresh-native"),
              modelSelection: f.modelSelection,
              runtimePolicy: f.runtimePolicy,
            });
            const old = yield* runtime.ensureThread({
              threadId: f.threadId,
              modelSelection: f.modelSelection,
              runtimePolicy: f.runtimePolicy,
            });
            yield* start(runtime, f, old);
            yield* untilTerminal(runtime);
            fresh = yield* runtime.ensureThread({
              threadId: f.threadId,
              modelSelection: f.modelSelection,
              runtimePolicy: f.runtimePolicy,
              existingProviderThread: { ...old, nativeThreadRef: null },
              forceNewNativeThread: true,
            });
            expect(fresh.id).toBe(old.id);
            expect(fresh.nativeThreadRef?.nativeId).not.toBe(old.nativeThreadRef?.nativeId);
            expect(
              (yield* runtime.readThreadSnapshot({ providerThread: fresh })).providerTurns,
            ).toHaveLength(0);
            yield* start(runtime, f, fresh, 2);
            yield* untilTerminal(runtime);
          }),
        );
        expect(JSON.stringify(requests[1].messages)).not.toContain("old native context");
        yield* Effect.scoped(
          Effect.gen(function* () {
            const runtime = yield* f.adapter.openSession({
              threadId: f.threadId,
              providerSessionId: C.ProviderSessionId.make("fresh-reopened"),
              modelSelection: f.modelSelection,
              runtimePolicy: f.runtimePolicy,
            });
            const reopened = yield* runtime.resumeThread({
              providerThread: fresh,
              threadId: f.threadId,
            });
            expect(reopened.nativeThreadRef).toEqual(fresh.nativeThreadRef);
            expect(
              (yield* runtime.readThreadSnapshot({ providerThread: reopened })).messages[0]?.text,
            ).toBe("fresh native context");
          }),
        );
      }),
  );
  it.live("executes a read tool through a complete native provider turn", () =>
    Effect.gen(function* () {
      const mcpFetch = vi
        .spyOn(globalThis, "fetch")
        .mockRejectedValueOnce(new TypeError("fixture-connect-failed"));
      let requests = 0;
      const f = yield* Effect.promise(() =>
        fixture(
          "default",
          async (_url, init) => {
            const body = JSON.parse(String(init?.body));
            requests++;
            if (requests === 1)
              return answer("", { name: "read_file", arguments: '{"path":"sample.txt"}' });
            expect(
              body.messages.some(
                (m: any) => m.role === "tool" && m.content.includes("native fixture"),
              ),
            ).toBe(true);
            return answer("прочитано");
          },
          undefined,
          "full-access",
          true,
        ),
      );
      yield* Effect.promise(() =>
        NodeFSP.writeFile(NodePath.join(f.root, "sample.txt"), "native fixture"),
      );
      yield* Effect.scoped(
        Effect.gen(function* () {
          const runtime = yield* f.adapter.openSession({
            threadId: f.threadId,
            providerSessionId: C.ProviderSessionId.make("session-read"),
            modelSelection: f.modelSelection,
            runtimePolicy: f.runtimePolicy,
          });
          const thread = yield* runtime.ensureThread({
            threadId: f.threadId,
            modelSelection: f.modelSelection,
            runtimePolicy: f.runtimePolicy,
          });
          yield* runtime.startTurn({
            appThread: {} as C.OrchestrationV2AppThread,
            threadId: f.threadId,
            runId: C.RunId.make("run-read"),
            runOrdinal: 1,
            providerTurnOrdinal: 1,
            attemptId: C.RunAttemptId.make("attempt-read"),
            rootNodeId: C.NodeId.make("root-read"),
            providerThread: thread,
            modelSelection: f.modelSelection,
            runtimePolicy: f.runtimePolicy,
            message: {
              messageId: C.MessageId.make("user-read"),
              text: "read",
              attachments: [],
              createdBy: "user",
              creationSource: "web",
            },
          });
          const events = yield* Stream.runCollect(
            runtime.events.pipe(Stream.takeUntil((event) => event.type === "turn.terminal")),
          );
          for (const event of events)
            expect(isProviderAdapterV2Event(event), JSON.stringify(event)).toBe(true);
          expect(events.at(-1)).toMatchObject({ status: "completed" });
          const toolDone = events.findIndex(
            (event) =>
              event.type === "turn_item.updated" &&
              event.turnItem.type === "dynamic_tool" &&
              event.turnItem.status === "completed",
          );
          const finalAnswer = events.findIndex(
            (event) =>
              event.type === "turn_item.updated" && event.turnItem.type === "assistant_message",
          );
          expect(finalAnswer).toBeGreaterThan(toolDone);
        }),
      );
      expect(requests).toBe(2);
      expect(mcpFetch).toHaveBeenCalledTimes(1);
    }),
  );
  it.live("round-trips the public native snapshot through JSON and a new session", () =>
    Effect.gen(function* () {
      let requestCount = 0;
      const f = yield* Effect.promise(() =>
        fixture("default", async () => {
          requestCount++;
          return answer("готово");
        }),
      );
      let previous!: C.OrchestrationV2ProviderThread;
      yield* Effect.scoped(
        Effect.gen(function* () {
          const runtime = yield* f.adapter.openSession({
            threadId: f.threadId,
            providerSessionId: C.ProviderSessionId.make("before-restart"),
            modelSelection: f.modelSelection,
            runtimePolicy: f.runtimePolicy,
          });
          previous = yield* runtime.ensureThread({
            threadId: f.threadId,
            modelSelection: f.modelSelection,
            runtimePolicy: f.runtimePolicy,
          });
          yield* start(runtime, f, previous);
          yield* untilTerminal(runtime);
        }),
      );
      yield* Effect.scoped(
        Effect.gen(function* () {
          const runtime = yield* f.adapter.openSession({
            threadId: f.threadId,
            providerSessionId: C.ProviderSessionId.make("after-restart"),
            modelSelection: f.modelSelection,
            runtimePolicy: f.runtimePolicy,
          });
          const thread = yield* runtime.resumeThread({
            providerThread: previous,
            threadId: f.threadId,
          });
          const snapshot = yield* runtime.readThreadSnapshot({ providerThread: thread });
          expect(snapshot.providerTurns).toHaveLength(1);
          expect(snapshot.messages[0]?.text).toBe("готово");
          expect(isOrchestrationV2ProviderThread(thread)).toBe(true);
          expect(isOrchestrationV2ConversationMessage(snapshot.messages[0])).toBe(true);
          const repeated = yield* start(runtime, f, thread).pipe(Effect.result);
          expect(repeated._tag).toBe("Failure");
          expect(JSON.stringify(repeated)).toContain("automatic-task-replay-refused");
        }),
      );
      expect(requestCount).toBe(1);
    }),
  );
  it.live("aborts an active SSE read and settles rows before the interrupted terminal", () =>
    Effect.gen(function* () {
      const reading = Promise.withResolvers<void>();
      let cancelled = false;
      const f = yield* Effect.promise(() =>
        fixture("default", async () => {
          let sent = false;
          return new Response(
            new ReadableStream<Uint8Array>({
              pull(controller) {
                if (sent) {
                  reading.resolve();
                  return;
                }
                sent = true;
                controller.enqueue(
                  new TextEncoder().encode(
                    `data: ${JSON.stringify({ id: "abort-request", model: "deepseek-flash", choices: [{ delta: { content: "partial" }, finish_reason: null }] })}\n\n`,
                  ),
                );
              },
              cancel() {
                cancelled = true;
              },
            }),
          );
        }),
      );
      yield* Effect.scoped(
        Effect.gen(function* () {
          const runtime = yield* f.adapter.openSession({
            threadId: f.threadId,
            providerSessionId: C.ProviderSessionId.make("stop-stream"),
            modelSelection: f.modelSelection,
            runtimePolicy: f.runtimePolicy,
          });
          const thread = yield* runtime.ensureThread({
            threadId: f.threadId,
            modelSelection: f.modelSelection,
            runtimePolicy: f.runtimePolicy,
          });
          yield* start(runtime, f, thread);
          yield* Effect.promise(() => reading.promise);
          const turn = (yield* runtime.readThreadSnapshot({ providerThread: thread }))
            .providerTurns[0]!;
          yield* runtime.interruptTurn({ providerThread: thread, providerTurnId: turn.id });
          const events = yield* untilTerminal(runtime);
          expect(events.at(-1)).toMatchObject({ status: "interrupted", failure: null });
          expect(events.filter((event) => event.type === "turn.terminal")).toHaveLength(1);
          expect(events.filter((event) => event.type === "message.updated").at(-1)).toMatchObject({
            message: { streaming: false },
          });
          expect(cancelled).toBe(true);
        }),
      );
    }),
  );
  it.live.each([402, 429, 401, 503])(
    "reports HTTP %s without retrying or leaking the response body",
    (status) =>
      Effect.gen(function* () {
        let calls = 0;
        const f = yield* Effect.promise(() =>
          fixture("default", async () => {
            calls++;
            return new Response("fixture-key private upstream error", { status });
          }),
        );
        const events = yield* Effect.scoped(
          Effect.gen(function* () {
            const runtime = yield* f.adapter.openSession({
              threadId: f.threadId,
              providerSessionId: C.ProviderSessionId.make("http-error"),
              modelSelection: f.modelSelection,
              runtimePolicy: f.runtimePolicy,
            });
            const thread = yield* runtime.ensureThread({
              threadId: f.threadId,
              modelSelection: f.modelSelection,
              runtimePolicy: f.runtimePolicy,
            });
            yield* start(runtime, f, thread);
            return yield* untilTerminal(runtime);
          }),
        );
        expect(events.at(-1)).toMatchObject({
          status: "failed",
          failure: {
            class:
              status === 402 || status === 429
                ? "usage_limit"
                : status === 401
                  ? "permission_error"
                  : "provider_error",
            retryable: false,
          },
        });
        expect(JSON.stringify(events)).not.toContain("fixture-key");
        expect(calls).toBe(1);
      }),
  );
  it.live("cleans session ownership and Stop when final persistence fails", () =>
    Effect.gen(function* () {
      const original = Journal.prototype.save;
      let injected = false;
      vi.spyOn(Journal.prototype, "save").mockImplementation(async function (this: Journal) {
        const view = this.state.view as { turns?: { status: string }[] } | undefined;
        if (!injected && view?.turns?.at(-1)?.status === "completed") {
          injected = true;
          throw Object.assign(Error("fixture EIO"), { code: "EIO" });
        }
        return original.call(this);
      });
      const f = yield* Effect.promise(() => fixture());
      yield* Effect.scoped(
        Effect.gen(function* () {
          const runtime = yield* f.adapter.openSession({
            threadId: f.threadId,
            providerSessionId: C.ProviderSessionId.make("eio-session"),
            modelSelection: f.modelSelection,
            runtimePolicy: f.runtimePolicy,
          });
          const thread = yield* runtime.ensureThread({
            threadId: f.threadId,
            modelSelection: f.modelSelection,
            runtimePolicy: f.runtimePolicy,
          });
          yield* start(runtime, f, thread);
          const first = yield* untilTerminal(runtime);
          expect(first.at(-1)).toMatchObject({
            status: "failed",
            failure: { message: "native-journal-persistence-failed" },
          });
          const turn = (yield* runtime.readThreadSnapshot({ providerThread: thread }))
            .providerTurns[0]!;
          yield* runtime.interruptTurn({ providerThread: thread, providerTurnId: turn.id });
          yield* start(runtime, f, thread, 2);
          expect((yield* untilTerminal(runtime)).at(-1)).toMatchObject({ status: "completed" });
        }),
      );
      expect(injected).toBe(true);
    }),
  );
  it.live("finishes tool and streaming rows before the single failed terminal", () =>
    Effect.gen(function* () {
      const tool: NativeTool = {
        mutating: true,
        definition: {
          type: "function",
          function: {
            name: "fixture_mutation",
            description: "fixture",
            parameters: { type: "object" },
          },
        },
        validate: (value) => value as Record<string, unknown>,
        execute: async () => {
          throw Error("fixture-side-effect-uncertain");
        },
      };
      const f = yield* Effect.promise(() =>
        fixture(
          "default",
          async () => answer("partial explanation", { name: "fixture_mutation", arguments: "{}" }),
          async () => new Map([["fixture_mutation", tool]]),
        ),
      );
      yield* Effect.scoped(
        Effect.gen(function* () {
          const runtime = yield* f.adapter.openSession({
            threadId: f.threadId,
            providerSessionId: C.ProviderSessionId.make("failure-session"),
            modelSelection: f.modelSelection,
            runtimePolicy: f.runtimePolicy,
          });
          const thread = yield* runtime.ensureThread({
            threadId: f.threadId,
            modelSelection: f.modelSelection,
            runtimePolicy: f.runtimePolicy,
          });
          yield* start(runtime, f, thread);
          const events = yield* untilTerminal(runtime);
          expect(events.at(-1)).toMatchObject({ type: "turn.terminal", status: "failed" });
          const nodes = events
            .filter((event) => event.type === "node.updated")
            .map((event) => event.node);
          expect(nodes.at(-1)).toMatchObject({ kind: "tool_call", status: "failed" });
          const tools = events.flatMap((event) =>
            event.type === "turn_item.updated" && event.turnItem.type === "dynamic_tool"
              ? [event.turnItem]
              : [],
          );
          expect(tools.at(-1)).toMatchObject({ status: "failed" });
          expect(events.filter((event) => event.type === "message.updated").at(-1)).toMatchObject({
            message: { streaming: false },
          });
          for (const event of events)
            expect(isProviderAdapterV2Event(event), JSON.stringify(event)).toBe(true);
        }),
      );
    }),
  );
  it.live("restores a pending approval as expired through the public JSON snapshot codec", () =>
    Effect.gen(function* () {
      let requests = 0;
      const f = yield* Effect.promise(() =>
        fixture(
          "default",
          async () =>
            ++requests === 1
              ? answer("", {
                  name: "write_file",
                  arguments: '{"path":"denied.txt","content":"no","expectedSha256":"new"}',
                })
              : answer("отклонено"),
          undefined,
          "approval-required",
        ),
      );
      let previous!: C.OrchestrationV2ProviderThread;
      yield* Effect.scoped(
        Effect.gen(function* () {
          const runtime = yield* f.adapter.openSession({
            threadId: f.threadId,
            providerSessionId: C.ProviderSessionId.make("approval-before"),
            modelSelection: f.modelSelection,
            runtimePolicy: f.runtimePolicy,
          });
          previous = yield* runtime.ensureThread({
            threadId: f.threadId,
            modelSelection: f.modelSelection,
            runtimePolicy: f.runtimePolicy,
          });
          yield* start(runtime, f, previous);
          yield* Stream.runCollect(
            runtime.events.pipe(
              Stream.tap((event) =>
                event.type === "runtime_request.updated" &&
                event.runtimeRequest.status === "pending"
                  ? runtime.respondToRuntimeRequest({
                      requestId: event.runtimeRequest.id,
                      decision: "decline",
                    })
                  : Effect.void,
              ),
              Stream.takeUntil((event) => event.type === "turn.terminal"),
            ),
          );
        }),
      );
      const folder = NodePath.join(f.root, "deepseek-api", "deepseek-api");
      const file = NodePath.join(
        folder,
        (yield* Effect.promise(() => NodeFSP.readdir(folder))).find((name) =>
          name.endsWith(".json"),
        )!,
      );
      const disk = JSON.parse(yield* Effect.promise(() => NodeFSP.readFile(file, "utf8")));
      disk.view.requests[0].status = "pending";
      disk.view.requests[0].resolvedAt = null;
      yield* Effect.promise(() => NodeFSP.writeFile(file, JSON.stringify(disk)));
      yield* Effect.scoped(
        Effect.gen(function* () {
          const runtime = yield* f.adapter.openSession({
            threadId: f.threadId,
            providerSessionId: C.ProviderSessionId.make("approval-after"),
            modelSelection: f.modelSelection,
            runtimePolicy: f.runtimePolicy,
          });
          const thread = yield* runtime.resumeThread({
            providerThread: previous,
            threadId: f.threadId,
          });
          const snapshot = yield* runtime.readThreadSnapshot({ providerThread: thread });
          expect(snapshot.runtimeRequests[0]).toMatchObject({
            status: "expired",
            responseCapability: { type: "not_resumable" },
          });
          expect(isOrchestrationV2RuntimeRequest(snapshot.runtimeRequests[0])).toBe(true);
        }),
      );
    }),
  );
  it.live("releases ownership if persistence fails before turn startup", () =>
    Effect.gen(function* () {
      const original = Journal.prototype.save;
      let injected = false;
      vi.spyOn(Journal.prototype, "save").mockImplementation(async function (this: Journal) {
        const view = this.state.view as { turns?: { status: string }[] } | undefined;
        if (!injected && view?.turns?.at(-1)?.status === "running") {
          injected = true;
          throw Error("fixture startup EIO");
        }
        return original.call(this);
      });
      const f = yield* Effect.promise(() => fixture());
      yield* Effect.scoped(
        Effect.gen(function* () {
          const runtime = yield* f.adapter.openSession({
            threadId: f.threadId,
            providerSessionId: C.ProviderSessionId.make("startup-eio"),
            modelSelection: f.modelSelection,
            runtimePolicy: f.runtimePolicy,
          });
          const thread = yield* runtime.ensureThread({
            threadId: f.threadId,
            modelSelection: f.modelSelection,
            runtimePolicy: f.runtimePolicy,
          });
          const failed = yield* start(runtime, f, thread).pipe(Effect.result);
          expect(failed._tag).toBe("Failure");
          yield* start(runtime, f, thread, 2);
          expect((yield* untilTerminal(runtime)).at(-1)).toMatchObject({ status: "completed" });
        }),
      );
    }),
  );
});
