import { describe, expect, it } from "@effect/vitest";
import {
  DEFAULT_SERVER_SETTINGS,
  ProviderInstanceId,
  ProviderSessionId,
  SecretProviderId,
  ThreadId,
} from "@t3tools/contracts";
import { DeepSeekApiDriver } from "@t3tools/provider-deepseek/server";
import * as ProviderHost from "@t3tools/provider-core/server/ProviderHost";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as SecretResolver from "@t3tools/secret-provider-core/server/SecretResolver";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import { BUILT_IN_DRIVERS } from "./builtInDrivers.ts";
import { makeProviderInstanceRegistry } from "./ProviderInstanceRegistry.ts";
import { deriveProviderInstanceConfigMap } from "./ProviderInstanceRegistryHydration.ts";

const host = Layer.mock(ProviderHost.ProviderHost)({
  paths: {
    cwd: "/fixture",
    baseDir: "/fixture",
    stateDir: "/fixture/state",
    providerStatusCacheDir: "/fixture/cache",
    attachmentsDir: "/fixture/attachments",
  },
  settings: {
    get: Effect.succeed(DEFAULT_SERVER_SETTINGS),
    changes: Stream.empty,
    subscribe: Effect.succeed(Stream.empty),
    withSnapshot: (use) => use(DEFAULT_SERVER_SETTINGS),
  },
  shouldRunBackgroundWork: () => Effect.succeed(false),
  resolveAttachmentPath: () => null,
});
const mcp = Layer.mock(McpProviderSessions.McpProviderSessions)({
  read: () => Effect.undefined,
});
const instanceId = ProviderInstanceId.make("deepseek-api");

describe("native DeepSeek registration", () => {
  it.effect("hydrates a disabled instance without reading a secret or opening an API session", () =>
    Effect.gen(function* () {
      expect(BUILT_IN_DRIVERS).toContain(DeepSeekApiDriver);
      const map = deriveProviderInstanceConfigMap(DEFAULT_SERVER_SETTINGS);
      expect(map[instanceId]?.driver).toBe("deepseek-api");
      const { registry } = yield* makeProviderInstanceRegistry({
        drivers: [DeepSeekApiDriver],
        configMap: {
          [instanceId]: {
            ...map[instanceId]!,
            config: {
              apiKey: { provider: SecretProviderId.make("fixture"), reference: "locked-disabled" },
            },
          },
        },
      });
      const instance = (yield* registry.getInstance(instanceId))!;
      expect(yield* instance.snapshot.getSnapshot).toMatchObject({
        status: "disabled",
        enabled: false,
        installed: true,
      });
      const opened = yield* instance.orchestrationAdapter
        .openSession({
          threadId: ThreadId.make("fixture"),
          providerSessionId: ProviderSessionId.make("fixture"),
          modelSelection: { instanceId, model: "deepseek-flash" },
          runtimePolicy: {
            cwd: "/fixture",
            runtimeMode: "full-access",
            interactionMode: "default",
          },
        })
        .pipe(Effect.result);
      expect(opened._tag).toBe("Failure");
      expect(JSON.stringify(opened)).toContain("provider-disabled");
    }).pipe(
      Effect.provide(
        Layer.mergeAll(
          host,
          mcp,
          Layer.mock(SecretResolver.SecretResolver)({
            read: () => Effect.die("Disabled instances must not read secrets"),
          }),
        ),
      ),
      Effect.scoped,
    ),
  );

  it.effect("resolves per-instance keys and leaves an unreadable reference unavailable", () =>
    Effect.gen(function* () {
      const reference = (name: string) => ({
        provider: SecretProviderId.make("fixture"),
        reference: name,
      });
      const { registry, mutator } = yield* makeProviderInstanceRegistry({
        drivers: [DeepSeekApiDriver],
        configMap: {},
      });
      const second = ProviderInstanceId.make("deepseek_other");
      yield* mutator.reconcile({
        [instanceId]: {
          driver: DeepSeekApiDriver.driverKind,
          enabled: true,
          config: { apiKey: reference("readable") },
        },
        [second]: {
          driver: DeepSeekApiDriver.driverKind,
          enabled: true,
          config: { apiKey: reference("locked") },
        },
      });
      const instance = (yield* registry.getInstance(instanceId))!;
      expect(instance.enabled).toBe(true);
      const snapshot = yield* instance.snapshot.getSnapshot;
      expect(snapshot).toMatchObject({
        status: "ready",
        auth: { status: "unknown", label: "API key configured; not verified" },
      });
      expect(snapshot.models.map((model) => model.slug)).toContain("deepseek-flash");
      expect(snapshot.models[0]?.capabilities?.optionDescriptors?.[0]).toMatchObject({
        options: [{ id: "none" }, { id: "low" }, { id: "high" }, { id: "max" }],
      });
      expect(JSON.stringify(snapshot)).not.toContain("fixture-native-key");
      expect(yield* registry.getInstance(second)).toBeUndefined();
      expect(yield* registry.listUnavailable).toEqual(
        expect.arrayContaining([expect.objectContaining({ instanceId: second })]),
      );
    }).pipe(
      Effect.provide(
        Layer.mergeAll(
          host,
          mcp,
          Layer.mock(SecretResolver.SecretResolver)({
            read: (references) =>
              Effect.succeed(
                references.map((ref) =>
                  ref.reference === "readable" ? "fixture-native-key" : undefined,
                ),
              ),
          }),
        ),
      ),
      Effect.scoped,
    ),
  );
});
