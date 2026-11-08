import * as C from "@t3tools/contracts";
import type { ProviderDriver } from "@t3tools/provider-core/server/driver";
import { defaultProviderContinuationIdentity } from "@t3tools/provider-core/server/driver";
import { ProviderDriverError } from "@t3tools/provider-core/server/errors";
import { makeManagedServerProvider } from "@t3tools/provider-core/server/managedProvider";
import { makeManualOnlyProviderMaintenanceCapabilities } from "@t3tools/provider-core/server/maintenanceResolver";
import * as ProviderHost from "@t3tools/provider-core/server/ProviderHost";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { DeepSeekSettings } from "../settings.ts";
import { mergeProviderInstanceEnvironment } from "@t3tools/provider-core/server/instanceEnvironment";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { DRIVER, makeAdapter } from "./adapter.ts";

export type DeepSeekDriverEnv = ProviderHost.ProviderHost | McpProviderSessions.McpProviderSessions;
const decodeSettings = Schema.decodeSync(DeepSeekSettings);
const efforts = ["none", "low", "high", "max"].map((id) => ({
  id,
  label: id === "none" ? "Off" : id,
  isDefault: id === "max",
}));

// Documented API IDs, not Harness/ACP or OpenCode model IDs. Health checks never run inference.
const models: ReadonlyArray<C.ServerProviderModel> = [
  ["deepseek-flash", "DeepSeek Flash"],
  ["deepseek-v4-pro", "DeepSeek V4 Pro"],
].map(([slug, name]) => ({
  slug: slug!,
  name: name!,
  isCustom: false,
  isDefault: slug === "deepseek-flash",
  capabilities: {
    optionDescriptors: [
      {
        id: "reasoningEffort",
        label: "Reasoning effort",
        type: "select",
        options: efforts,
        currentValue: "max",
      },
    ],
  },
}));

export const DeepSeekApiDriver: ProviderDriver<DeepSeekSettings, DeepSeekDriverEnv> = {
  driverKind: DRIVER,
  metadata: { displayName: "DeepSeek API", supportsMultipleInstances: true },
  configSchema: DeepSeekSettings,
  defaultConfig: () => decodeSettings({}),
  create: Effect.fn("DeepSeekApiDriver.create")(function* ({
    instanceId,
    displayName,
    accentColor,
    environment,
    enabled,
    config,
  }) {
    const host = yield* ProviderHost.ProviderHost;
    const mcpSessions = yield* McpProviderSessions.McpProviderSessions;
    const processEnvironment = yield* mergeProviderInstanceEnvironment(
      environment,
      yield* HostProcess.Environment,
    );
    // The registry resolves secret references. Refuse an unresolved reference rather than
    // reading an ambient key or accidentally switching accounts.
    if (enabled && typeof config.apiKey !== "string") {
      return yield* new ProviderDriverError({
        driver: DRIVER,
        instanceId,
        detail:
          "The DeepSeek API key could not be resolved. Check the secret provider in Settings.",
      });
    }
    const apiKey = typeof config.apiKey === "string" ? config.apiKey : "";
    const continuationIdentity = defaultProviderContinuationIdentity({
      driverKind: DRIVER,
      instanceId,
    });
    const checkProvider = DateTime.now.pipe(
      Effect.map((now): C.ServerProvider => ({
        instanceId,
        driver: DRIVER,
        enabled,
        installed: true,
        version: null,
        ...(displayName ? { displayName } : {}),
        ...(accentColor ? { accentColor } : {}),
        continuation: { groupKey: continuationIdentity.continuationKey },
        status: !enabled ? "disabled" : apiKey ? "ready" : "warning",
        auth: {
          status: apiKey ? "unknown" : "unauthenticated",
          label: apiKey ? "API key configured; not verified" : "API key required",
        },
        ...(!apiKey && enabled
          ? { message: "Set an official DeepSeek API key in this provider's settings." }
          : {}),
        checkedAt: DateTime.formatIso(now),
        models,
        slashCommands: [],
        skills: [],
        showInteractionModeToggle: false,
        supportedRuntimeModes: ["approval-required", "auto-accept-edits", "full-access"],
        supportsTextGeneration: false,
        setup: {
          canAuthenticate: false,
          canInstall: false,
          documentationUrl: "https://api-docs.deepseek.com/",
        },
      })),
    );
    const snapshot = yield* makeManagedServerProvider({
      resolveMaintenance: () =>
        Effect.succeed(
          makeManualOnlyProviderMaintenanceCapabilities({ provider: DRIVER, packageName: null }),
        ),
      getSettings: Effect.succeed(config),
      streamSettings: Stream.empty,
      haveSettingsChanged: () => false,
      initialSnapshot: () => checkProvider,
      checkProvider,
      refreshOnInterval: false,
    }).pipe(
      Effect.mapError(
        () =>
          new ProviderDriverError({
            driver: DRIVER,
            instanceId,
            detail: "Could not initialize the DeepSeek provider.",
          }),
      ),
    );
    const unavailable = () =>
      Effect.fail(
        new C.TextGenerationError({
          operation: "deepseek-api",
          detail: "This provider does not support background text generation.",
        }),
      );
    return {
      instanceId,
      driverKind: DRIVER,
      displayName,
      accentColor,
      enabled,
      continuationIdentity,
      snapshot,
      orchestrationAdapter: makeAdapter({
        instanceId,
        stateDir: host.paths.stateDir,
        enabled,
        apiKey: async () => apiKey,
        mcpSessions,
        environment: processEnvironment,
      }),
      textGeneration: {
        translateReasoning: unavailable,
        generateCommitMessage: unavailable,
        generatePrContent: unavailable,
        generateBranchName: unavailable,
        generateThreadTitle: unavailable,
        generateReviewGuide: unavailable,
        generateTranscriptionPostProcessing: unavailable,
      },
    };
  }),
};
