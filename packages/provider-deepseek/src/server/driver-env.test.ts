// @effect-diagnostics nodeBuiltinImport:off -- Disposable real filesystem fixture for the native driver boundary.
// @effect-diagnostics processEnvInEffect:off -- Assert the real parent marker remains unchanged after driver setup.
import { it, expect } from "@effect/vitest";
import { afterEach, vi } from "vite-plus/test";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as C from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as ProviderHost from "@t3tools/provider-core/server/ProviderHost";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as Schema from "effect/Schema";
import { DeepSeekApiDriver } from "./driver.ts";
import { DeepSeekSettings } from "../settings.ts";
import * as Tools from "./tools.ts";

const roots: string[] = [];
afterEach(async () => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  vi.unstubAllEnvs();
  await Promise.all(roots.splice(0).map((p) => NodeFSP.rm(p, { recursive: true, force: true })));
});
it.effect(
  "passes the filtered host environment and instance overrides through the actual driver",
  () =>
    Effect.gen(function* () {
      const root = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-ds-policy-")),
      );
      roots.push(root);
      vi.stubEnv("T3CODE_CODEX_LAUNCH_ARGS", "fixture-blocked");
      let captured: NodeJS.ProcessEnv | undefined;
      vi.spyOn(Tools, "nativeTools").mockImplementation(async (_cwd, env) => {
        captured = env;
        return new Map();
      });
      vi.stubGlobal(
        "fetch",
        async () =>
          new Response(
            'data: {"id":"fixture","model":"deepseek-flash","choices":[{"delta":{"content":"done"},"finish_reason":"stop"}]}\n\ndata: [DONE]\n\n',
          ),
      );
      const host = Layer.mock(ProviderHost.ProviderHost)({
        paths: {
          cwd: root,
          baseDir: root,
          stateDir: NodePath.join(root, "state"),
          providerStatusCacheDir: NodePath.join(root, "cache"),
          attachmentsDir: NodePath.join(root, "attachments"),
        },
        settings: {
          get: Effect.succeed(C.DEFAULT_SERVER_SETTINGS),
          changes: Stream.empty,
          subscribe: Effect.succeed(Stream.empty),
          withSnapshot: (use) => use(C.DEFAULT_SERVER_SETTINGS),
        },
        shouldRunBackgroundWork: () => Effect.succeed(false),
        resolveAttachmentPath: () => null,
      });
      yield* Effect.scoped(
        Effect.gen(function* () {
          const instanceId = C.ProviderInstanceId.make("env-fixture");
          const instance = yield* DeepSeekApiDriver.create({
            instanceId,
            displayName: undefined,
            enabled: true,
            environment: [{ name: "T3_DS_OVERRIDE", value: "instance", sensitive: false }],
            config: yield* Schema.decodeEffect(DeepSeekSettings)({ apiKey: "fixture-key" }),
          });
          const threadId = C.ThreadId.make("env-fixture"),
            modelSelection = { instanceId, model: "deepseek-flash" },
            runtimePolicy = {
              cwd: root,
              runtimeMode: "full-access" as const,
              interactionMode: "default" as const,
            };
          const runtime = yield* instance.orchestrationAdapter.openSession({
            threadId,
            providerSessionId: C.ProviderSessionId.make("env-fixture"),
            modelSelection,
            runtimePolicy,
          });
          const thread = yield* runtime.ensureThread({ threadId, modelSelection, runtimePolicy });
          yield* runtime.startTurn({
            appThread: {} as C.OrchestrationV2AppThread,
            threadId,
            runId: C.RunId.make("env-run"),
            runOrdinal: 1,
            providerTurnOrdinal: 1,
            attemptId: C.RunAttemptId.make("env-attempt"),
            rootNodeId: C.NodeId.make("env-root"),
            providerThread: thread,
            modelSelection,
            runtimePolicy,
            message: {
              messageId: C.MessageId.make("env-message"),
              text: "fixture",
              attachments: [],
              createdBy: "user",
              creationSource: "web",
            },
          });
          yield* Stream.runCollect(
            runtime.events.pipe(Stream.takeUntil((e) => e.type === "turn.terminal")),
          );
        }).pipe(
          Effect.provide(
            Layer.mergeAll(
              host,
              Layer.mock(McpProviderSessions.McpProviderSessions)({ read: () => Effect.undefined }),
            ),
          ),
          Effect.provideService(HostProcess.Environment, { T3_DS_ALLOWED: "filtered" }),
        ),
      );
      expect(captured).toEqual({ T3_DS_ALLOWED: "filtered", T3_DS_OVERRIDE: "instance" });
      expect(process.env.T3CODE_CODEX_LAUNCH_ARGS).toBe("fixture-blocked");
    }),
);
