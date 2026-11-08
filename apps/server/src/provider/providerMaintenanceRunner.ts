import {
  defaultInstanceIdForDriver,
  ProviderDriverKind,
  ServerProviderUpdateError,
  type ProviderInstanceId,
  type ServerProvider,
  type ServerProviderUpdatedPayload,
  type ServerProviderUpdateState,
} from "@t3tools/contracts";
import { resolveSpawnCommand } from "@t3tools/shared/shell";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { HttpClient } from "effect/http";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import * as NodeUtil from "node:util";

import * as ModelManifest from "./ModelManifest.ts";
import { resolveProviderCompatibility } from "./providerCompatibility.ts";
import * as ProviderRegistry from "./ProviderRegistry.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as ProviderSessionManager from "../orchestration-v2/ProviderSessionManager.ts";
import { makeProviderUpdateDrainQueue } from "./providerUpdateQueue.ts";
import type { ProviderMaintenanceInstallPin } from "@t3tools/provider-core/server/maintenance";
import { makeProviderMaintenanceCommandCoordinator } from "./providerMaintenanceCommandCoordinator.ts";
import {
  enrichProviderSnapshotWithVersionAdvisory,
  invalidateLatestProviderVersion,
  makeTargetedProviderUpdateAction,
  normalizeCommandPath,
  resolveLatestProviderUpdateAction,
  resolveLatestProviderVersion,
  type ProviderMaintenanceCommandAction,
} from "@t3tools/provider-core/server/maintenanceResolver";
import * as ProviderLatestVersions from "@t3tools/provider-core/server/ProviderLatestVersions";
import type { ProviderMaintenanceCapabilities } from "@t3tools/provider-core/server/maintenanceResolver";
import { collectUint8StreamText } from "@t3tools/provider-core/server/collectStreamText";

// Native updaters (`claude update`) download a ~300 MB binary without printing
// progress, and killing one mid-download leaves a truncated binary that the
// next run treats as installed. Allow a slow connection to finish.
const UPDATE_TIMEOUT_MINUTES = 30;
const UPDATE_TIMEOUT_MS = UPDATE_TIMEOUT_MINUTES * 60_000;
const UPDATE_OUTPUT_MAX_BYTES = 10_000;
// Sample installer output because every progress update resends the provider list to all clients.
const UPDATE_PROGRESS_INTERVAL = Duration.seconds(1);
const UPDATE_PROGRESS_MAX_LENGTH = 200;
// An installer may write for minutes without a line ending; only its tail matters.
const UPDATE_PARTIAL_LINE_MAX_LENGTH = 4_096;

export interface ProviderMaintenanceCommandResult {
  readonly stdout: string;
  readonly stderr: string;
  readonly exitCode: number | null;
  readonly timedOut: boolean;
  readonly stdoutTruncated: boolean;
  readonly stderrTruncated: boolean;
}

export interface ProviderMaintenanceRunnerShape {
  readonly updateProvider: (
    target:
      | ProviderDriverKind
      | {
          readonly provider: ProviderDriverKind;
          readonly instanceId?: ProviderInstanceId | undefined;
          readonly targetVersion?: string | undefined;
        },
  ) => Effect.Effect<ServerProviderUpdatedPayload, ServerProviderUpdateError>;
}

export class ProviderMaintenanceRunner extends Context.Service<
  ProviderMaintenanceRunner,
  ProviderMaintenanceRunnerShape
>()("t3/provider/providerMaintenanceRunner") {}

class ProviderMaintenanceCommandError extends Schema.TaggedError<ProviderMaintenanceCommandError>()(
  "ProviderMaintenanceCommandError",
  {
    message: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {}

interface VerifiedProviderRefresh {
  readonly providers: ReadonlyArray<ServerProvider>;
  readonly verifiedProviders: ReadonlyArray<ServerProvider>;
  readonly advisoryEnriched: boolean;
}

const nowIso = Effect.map(DateTime.now, DateTime.formatIso);

const runProviderMaintenanceCommandWithSpawner = Effect.fn("ProviderMaintenanceRunner.runCommand")(
  function* (input: {
    readonly spawner: ChildProcessSpawner.ChildProcessSpawner["Service"];
    readonly command: string;
    readonly args: ReadonlyArray<string>;
    readonly env?: NodeJS.ProcessEnv;
    readonly onProgress?: (line: string) => Effect.Effect<void>;
  }) {
    const collectCommandResult = Effect.fn("ProviderMaintenanceRunner.collectCommandResult")(
      function* () {
        const env = input.env ? { env: input.env, extendEnv: true } : {};
        // Windows batch shims require shell dispatch through resolveSpawnCommand.
        const resolved = yield* resolveSpawnCommand(input.command, input.args, env);
        const child = yield* input.spawner
          .spawn(
            ChildProcess.make(resolved.command, resolved.args, {
              shell: resolved.shell,
              ...env,
            }),
          )
          .pipe(
            Effect.mapError(
              (cause) =>
                new ProviderMaintenanceCommandError({
                  message: `Failed to run update command ${input.command}: ${cause.message}`,
                  cause,
                }),
            ),
          );
        yield* Effect.addFinalizer(() => child.kill().pipe(Effect.ignore));

        const pendingProgress = yield* Ref.make<string | null>(null);
        const onProgress = input.onProgress;
        if (onProgress) {
          yield* Ref.getAndSet(pendingProgress, null).pipe(
            Effect.flatMap((line) => (line === null ? Effect.void : onProgress(line))),
            Effect.repeat(Schedule.spaced(UPDATE_PROGRESS_INTERVAL)),
            Effect.forkScoped,
          );
        }
        const trackProgress = <E>(stream: Stream.Stream<Uint8Array, E>) => {
          if (!onProgress) {
            return stream;
          }
          const decoder = new TextDecoder();
          let partialLine = "";
          return stream.pipe(
            Stream.tap((chunk) => {
              const split = splitOutputLines(partialLine, decoder.decode(chunk, { stream: true }));
              partialLine = split.partialLine;
              const line =
                toProgressLine(split.partialLine) ??
                split.lines.map(toProgressLine).findLast((value) => value !== null);
              return line ? Ref.set(pendingProgress, line) : Effect.void;
            }),
          );
        };

        const [stdout, stderr, exitCode] = yield* Effect.all(
          [
            collectUint8StreamText({
              stream: trackProgress(child.stdout),
              maxBytes: UPDATE_OUTPUT_MAX_BYTES,
            }),
            collectUint8StreamText({
              stream: trackProgress(child.stderr),
              maxBytes: UPDATE_OUTPUT_MAX_BYTES,
            }),
            child.exitCode,
          ],
          { concurrency: "unbounded" },
        ).pipe(
          Effect.mapError(
            (cause) =>
              new ProviderMaintenanceCommandError({
                message: cause instanceof Error ? cause.message : "Update command failed to run.",
                cause,
              }),
          ),
        );

        return {
          stdout: stdout.text,
          stderr: stderr.text,
          exitCode: Number(exitCode),
          timedOut: false,
          stdoutTruncated: stdout.truncated,
          stderrTruncated: stderr.truncated,
        } satisfies ProviderMaintenanceCommandResult;
      },
    );

    return yield* collectCommandResult().pipe(
      Effect.scoped,
      Effect.timeoutOption(Duration.millis(UPDATE_TIMEOUT_MS)),
      Effect.map((result) =>
        Option.match(result, {
          onSome: (value) => value,
          onNone: () =>
            ({
              stdout: "",
              stderr: "",
              exitCode: null,
              timedOut: true,
              stdoutTruncated: false,
              stderrTruncated: false,
            }) satisfies ProviderMaintenanceCommandResult,
        }),
      ),
    );
  },
);

export function splitOutputLines(
  partialLine: string,
  text: string,
): { readonly lines: ReadonlyArray<string>; readonly partialLine: string } {
  const parts = (partialLine + text).replace(/\x1b\[(?:0*1?G|999D)/g, "\r").split(/\r\n|\r|\n/);
  return { partialLine: (parts.pop() ?? "").slice(-UPDATE_PARTIAL_LINE_MAX_LENGTH), lines: parts };
}

export function toProgressLine(line: string): string | null {
  if (/\x1b(?:\[[0-9]*)?$/.test(line)) {
    return null;
  }
  const text = NodeUtil.stripVTControlCharacters(line).replace(/\s+/g, " ").trim();
  if (text.length === 0) {
    return null;
  }
  return text.length <= UPDATE_PROGRESS_MAX_LENGTH
    ? text
    : `${text.slice(0, UPDATE_PROGRESS_MAX_LENGTH - 1)}…`;
}

function describeCommand(command: ProviderMaintenanceCommandAction): string {
  const executable = command.executable.split(/[\\/]/).pop() || command.executable;
  return [executable, ...command.args].join(" ");
}

function trimNullable(value: string): string | null {
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

function truncateText(value: string, maxLength: number): string {
  return value.length <= maxLength ? value : value.slice(0, maxLength);
}

function commandOutput(result: ProviderMaintenanceCommandResult): string | null {
  const output = trimNullable([result.stderr, result.stdout].filter(Boolean).join("\n\n"));
  if (!output) {
    return null;
  }
  return truncateText(output, UPDATE_OUTPUT_MAX_BYTES);
}

// Updaters print their reason last, usually on stderr.
function lastOutputLine(result: ProviderMaintenanceCommandResult): string | null {
  for (const output of [result.stderr, result.stdout]) {
    const line = output
      .split(/\r\n|\r|\n/)
      .map(toProgressLine)
      .findLast(Boolean);
    if (line) {
      return line;
    }
  }
  return null;
}

function failureMessage(result: ProviderMaintenanceCommandResult): string {
  if (result.timedOut) {
    return `Update timed out after ${UPDATE_TIMEOUT_MINUTES} minutes.`;
  }
  const summary =
    result.exitCode !== null && result.exitCode !== 0
      ? `Update command exited with code ${result.exitCode}`
      : "Update command failed";
  const reason = lastOutputLine(result);
  return reason ? `${summary}: ${reason}` : `${summary}.`;
}

function isOutdatedProvider(provider: ServerProvider | undefined): boolean {
  return provider?.versionAdvisory?.status === "behind_latest";
}

function isStillInstalled(provider: ServerProvider): boolean {
  return provider.installed;
}

function makeUpdateState(input: {
  readonly status: ServerProviderUpdateState["status"];
  readonly startedAt: string | null;
  readonly finishedAt: string | null;
  readonly message: string | null;
  readonly output?: string | null;
}): ServerProviderUpdateState {
  return {
    status: input.status,
    startedAt: input.startedAt,
    finishedAt: input.finishedAt,
    message: input.message,
    output: input.output ?? null,
  };
}

/** @public */
export const make = Effect.fn("ProviderMaintenanceRunner.make")(function* () {
  const serviceClock = yield* Clock.Clock;
  const providerRegistry = yield* ProviderRegistry.ProviderRegistry;
  const manifestService = yield* ModelManifest.ModelManifest;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const httpClient = yield* HttpClient.HttpClient;
  const latestVersions = yield* ProviderLatestVersions.ProviderLatestVersions;
  const settingsService = yield* Effect.serviceOption(ServerSettings.ServerSettingsService);
  const providerSessions = yield* Effect.serviceOption(
    ProviderSessionManager.ProviderSessionManagerV2,
  );
  const runMaintenanceCommand = (
    update: ProviderMaintenanceCommandAction,
    onProgress: (line: string) => Effect.Effect<void>,
  ) =>
    runProviderMaintenanceCommandWithSpawner({
      spawner,
      command: update.executable,
      args: update.args,
      onProgress,
      ...(update.env ? { env: update.env } : {}),
    });
  const commandCoordinator = yield* makeProviderMaintenanceCommandCoordinator({
    makeAlreadyRunningError: () =>
      new ServerProviderUpdateError({
        provider: ProviderDriverKind.make("unknown"),
        reason: "An update is already running for this provider.",
      }),
  });
  const updateQueue = yield* makeProviderUpdateDrainQueue();

  const sameUpdateTarget = (
    left: ProviderMaintenanceCommandAction,
    right: ProviderMaintenanceCommandAction,
  ) => {
    if (left.lockKey !== right.lockKey) return false;
    // A pinned updater writes its managed destination, which two source paths may share.
    if (left.pin || right.pin) {
      return (
        left.pin !== undefined &&
        right.pin !== undefined &&
        normalizeCommandPath(left.pin.binaryPath) === normalizeCommandPath(right.pin.binaryPath)
      );
    }
    return normalizeCommandPath(left.executable) === normalizeCommandPath(right.executable);
  };

  const sameUpdateAction = (
    left: ProviderMaintenanceCommandAction,
    right: ProviderMaintenanceCommandAction,
  ) => {
    if (
      !sameUpdateTarget(left, right) ||
      left.command !== right.command ||
      left.executable !== right.executable ||
      (left.pin?.versionArgs ?? []).length !== (right.pin?.versionArgs ?? []).length ||
      !(left.pin?.versionArgs ?? []).every(
        (arg, index) => arg === right.pin?.versionArgs?.[index],
      ) ||
      left.args.length !== right.args.length ||
      !left.args.every((arg, index) => arg === right.args[index])
    )
      return false;
    const leftEnv = left.env ?? {};
    const rightEnv = right.env ?? {};
    const leftKeys = Object.keys(leftEnv);
    const rightKeys = Object.keys(rightEnv);
    return (
      leftKeys.length === rightKeys.length &&
      leftKeys.every((key) => leftEnv[key] === rightEnv[key])
    );
  };

  interface SharedUpdateMember {
    readonly instanceId: ProviderInstanceId;
    readonly update: ProviderMaintenanceCommandAction;
    readonly installPath: string;
  }

  const resolveSharedUpdateMembers = Effect.fn("ProviderMaintenanceRunner.sharedUpdateMembers")(
    function* (
      targetInstanceId: ProviderInstanceId,
      provider: ProviderDriverKind,
      targetUpdate: ProviderMaintenanceCommandAction,
      options?: { readonly fresh?: boolean },
    ) {
      const providers = yield* providerRegistry.getProviders;
      const candidateInstanceIds = providers
        .filter((candidate) => candidate.driver === provider)
        .map((candidate) => candidate.instanceId);
      const members: Array<SharedUpdateMember> = [];
      for (const candidateInstanceId of candidateInstanceIds) {
        const capabilities = yield* providerRegistry.getProviderMaintenanceCapabilitiesForInstance(
          candidateInstanceId,
          provider,
          options ?? {},
        );
        const update = capabilities.update;
        if (!update || !sameUpdateTarget(update, targetUpdate)) continue;
        members.push({
          instanceId: candidateInstanceId,
          update,
          installPath: capabilities.installPath ?? update.executable,
        });
      }
      if (!members.some((member) => member.instanceId === targetInstanceId)) {
        members.push({
          instanceId: targetInstanceId,
          update: targetUpdate,
          installPath: targetUpdate.executable,
        });
      }
      return members.sort((left, right) =>
        String(left.instanceId).localeCompare(String(right.instanceId)),
      );
    },
  );

  const configuredBinaryPath = (config: unknown): string => {
    if (!config || typeof config !== "object" || Array.isArray(config)) return "";
    const binaryPath = (config as Record<string, unknown>).binaryPath;
    return typeof binaryPath === "string" ? binaryPath.trim() : "";
  };

  const settingsError = (cause: unknown) =>
    new ProviderMaintenanceCommandError({
      message: cause instanceof Error ? cause.message : "Could not update provider settings.",
      cause,
    });

  const readConfiguredBinaryPaths = Effect.fn(
    "ProviderMaintenanceRunner.readConfiguredBinaryPaths",
  )(function* (instanceIds: ReadonlyArray<ProviderInstanceId>) {
    const settings = Option.getOrUndefined(settingsService);
    if (!settings) return null;
    const current = yield* settings.getSettings.pipe(Effect.mapError(settingsError));
    return new Map(
      instanceIds.map((instanceId) => {
        const entry = current.providerInstances[instanceId];
        return [instanceId, configuredBinaryPath(entry?.config)] as const;
      }),
    );
  });

  const switchConfiguredBinaryPaths = Effect.fn(
    "ProviderMaintenanceRunner.switchConfiguredBinaryPaths",
  )(function* (
    changes: ReadonlyArray<{
      readonly instanceId: ProviderInstanceId;
      readonly expectedBinaryPath: string;
      readonly binaryPath: string;
    }>,
  ) {
    const settings = Option.getOrUndefined(settingsService);
    const switchPaths = settings?.switchProviderBinaryPaths;
    if (!switchPaths) {
      return yield* new ProviderMaintenanceCommandError({
        message: "Cannot switch provider pins without atomic settings support.",
      });
    }
    return yield* switchPaths(changes).pipe(Effect.mapError(settingsError));
  });

  const probePinnedBinary = Effect.fn("ProviderMaintenanceRunner.probePinnedBinary")(function* (
    pin: ProviderMaintenanceInstallPin,
  ) {
    const result = yield* runProviderMaintenanceCommandWithSpawner({
      spawner,
      command: pin.binaryPath,
      args: pin.versionArgs ?? ["--version"],
    }).pipe(Effect.timeoutOption(Duration.seconds(15)));
    const value = Option.getOrUndefined(result);
    if (!value || value.timedOut || value.exitCode !== 0) {
      return {
        ok: false as const,
        version: null,
        message:
          value?.timedOut === true
            ? "The installed copy did not answer --version before the timeout."
            : "The installed copy failed its --version check.",
      };
    }
    const output = [value.stdout, value.stderr].filter(Boolean).join("\n");
    const version = /(?:^|\s)v?(\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?)/.exec(output)?.[1] ?? null;
    if (!version) {
      return {
        ok: false as const,
        version: null,
        message: "The installed copy did not report a readable version.",
      };
    }
    return { ok: true as const, version, message: null };
  });

  const normalizedVersion = (version: string) => version.trim().replace(/^v/, "");

  interface PinRestoreResult {
    readonly restored: boolean;
    readonly failedInstanceIds: ReadonlyArray<ProviderInstanceId>;
    readonly failureMessage: string | null;
  }

  const restoreConfiguredBinaryPaths = Effect.fn(
    "ProviderMaintenanceRunner.restoreConfiguredBinaryPaths",
  )(function* (input: {
    readonly initialPaths: ReadonlyMap<ProviderInstanceId, string>;
    readonly pinnedPath: string;
    readonly instanceIds: ReadonlyArray<ProviderInstanceId>;
  }) {
    const failed: Array<ProviderInstanceId> = [];
    let failureMessage: string | null = null;
    for (const instanceId of input.instanceIds) {
      const outcome = yield* switchConfiguredBinaryPaths([
        {
          instanceId,
          expectedBinaryPath: input.pinnedPath,
          binaryPath: input.initialPaths.get(instanceId) ?? "",
        },
      ]).pipe(Effect.exit);
      if (Exit.isFailure(outcome)) {
        const cause = Cause.squash(outcome.cause);
        failureMessage =
          cause instanceof Error ? cause.message : "Could not restore the previous pin.";
        failed.push(instanceId);
        continue;
      }
      if (outcome.value.status === "conflict") failed.push(instanceId);
    }
    return {
      restored: failed.length === 0,
      failedInstanceIds: failed,
      failureMessage,
    } satisfies PinRestoreResult;
  });

  const pinRestoreFailureMessage = (result: PinRestoreResult): string => {
    const failed = result.failedInstanceIds.join(", ");
    return result.failureMessage
      ? `The new copy could not be verified, and restoring the previous provider pin failed for ${failed}: ${result.failureMessage}. Review provider settings.`
      : `The new copy could not be verified, and the previous provider pin was not restored for ${failed}. Review provider settings.`;
  };

  const verifyFromRefresh = (
    refresh: Effect.Effect<ReadonlyArray<ServerProvider>>,
    provider: ProviderDriverKind,
    maintenanceCapabilities: ProviderMaintenanceCapabilities,
    instanceIds: ReadonlyArray<ProviderInstanceId>,
    phase: "pre-update" | "post-update",
  ): Effect.Effect<VerifiedProviderRefresh> =>
    refresh.pipe(
      Effect.flatMap((providers) => {
        const refreshedById = new Map(
          providers
            .filter(
              (candidate) =>
                candidate.driver === provider && instanceIds.includes(candidate.instanceId),
            )
            .map((candidate) => [candidate.instanceId, candidate] as const),
        );
        return Effect.gen(function* () {
          for (const instanceId of instanceIds) {
            if (refreshedById.has(instanceId)) continue;
            const refreshed = yield* providerRegistry.refreshInstance(instanceId);
            const candidate = refreshed.find(
              (entry) => entry.driver === provider && entry.instanceId === instanceId,
            );
            if (candidate) refreshedById.set(instanceId, candidate);
          }
          const refreshedProviders = [...refreshedById.values()];
          if (refreshedProviders.length === 0) {
            return {
              providers,
              verifiedProviders: [],
              advisoryEnriched: true,
            } satisfies VerifiedProviderRefresh;
          }
          return yield* Effect.forEach(
            refreshedProviders,
            (refreshedProvider) =>
              enrichProviderSnapshotWithVersionAdvisory(
                refreshedProvider,
                maintenanceCapabilities,
              ).pipe(
                Effect.provideService(HttpClient.HttpClient, httpClient),
                Effect.provideService(
                  ProviderLatestVersions.ProviderLatestVersions,
                  latestVersions,
                ),
              ),
            { concurrency: "unbounded" },
          ).pipe(
            Effect.map((verifiedProviders): VerifiedProviderRefresh => ({
              providers,
              verifiedProviders,
              advisoryEnriched: true,
            })),
            Effect.catchCause((cause) =>
              Effect.logWarning("Provider version verification failed", {
                provider,
                phase,
                cause: Cause.pretty(cause),
              }).pipe(
                Effect.as<VerifiedProviderRefresh>({
                  providers,
                  verifiedProviders: refreshedProviders,
                  advisoryEnriched: false,
                }),
              ),
            ),
          );
        });
      }),
    );

  const updateProvider: ProviderMaintenanceRunnerShape["updateProvider"] = Effect.fn(
    "ProviderMaintenanceRunner.updateProvider",
  )(function* (target) {
    const provider = typeof target === "string" ? target : target.provider;
    const instanceId =
      typeof target === "string"
        ? defaultInstanceIdForDriver(provider)
        : (target.instanceId ?? defaultInstanceIdForDriver(provider));
    const targetVersion = typeof target === "string" ? undefined : target.targetVersion;
    const capabilities = yield* providerRegistry.getProviderMaintenanceCapabilitiesForInstance(
      instanceId,
      provider,
    );
    const update = capabilities.update;
    if (!update) {
      return yield* new ServerProviderUpdateError({
        provider,
        reason: "This provider does not support one-click updates.",
      });
    }
    if (update.pin && !Option.isSome(settingsService)) {
      return yield* new ServerProviderUpdateError({
        provider,
        reason: "This update needs a verified provider pin, but server settings are unavailable.",
      });
    }

    const members = yield* resolveSharedUpdateMembers(instanceId, provider, update);
    const groupIds = members.map((member) => member.instanceId);
    const initialInstallPaths = new Map(
      members.map((member) => [member.instanceId, member.installPath] as const),
    );
    const initialConfiguredPaths = update.pin
      ? yield* readConfiguredBinaryPaths(groupIds).pipe(
          Effect.mapError(
            (cause) =>
              new ServerProviderUpdateError({
                provider,
                reason: cause.message,
                cause,
              }),
          ),
        )
      : null;
    if (update.pin && initialConfiguredPaths === null) {
      return yield* new ServerProviderUpdateError({
        provider,
        reason: "Could not read provider settings before queuing the pin update.",
      });
    }

    const revalidateQueuedTarget = Effect.fn("ProviderMaintenanceRunner.revalidateQueuedTarget")(
      function* () {
        const fresh = yield* providerRegistry.getProviderMaintenanceCapabilitiesForInstance(
          instanceId,
          provider,
          { fresh: true },
        );
        const freshMembers = fresh.update
          ? yield* resolveSharedUpdateMembers(instanceId, provider, fresh.update, { fresh: true })
          : null;
        const freshGroupIds = freshMembers?.map((member) => member.instanceId) ?? [];
        const sameGroup =
          freshMembers !== null &&
          freshGroupIds.length === groupIds.length &&
          freshGroupIds.every((freshId) => groupIds.includes(freshId));
        const sameInstallPaths =
          freshMembers !== null &&
          freshMembers.every(
            (member) => initialInstallPaths.get(member.instanceId) === member.installPath,
          );
        const currentConfiguredPaths = update.pin
          ? yield* readConfiguredBinaryPaths(groupIds)
          : null;
        const pinComparison =
          update.pin && initialConfiguredPaths !== null
            ? yield* switchConfiguredBinaryPaths(
                groupIds.map((groupInstanceId) => ({
                  instanceId: groupInstanceId,
                  expectedBinaryPath: initialConfiguredPaths.get(groupInstanceId) ?? "",
                  binaryPath: initialConfiguredPaths.get(groupInstanceId) ?? "",
                })),
              )
            : null;
        const sameConfiguration =
          !update.pin ||
          (pinComparison?.status === "switched" &&
            initialConfiguredPaths !== null &&
            currentConfiguredPaths !== null &&
            groupIds.every(
              (groupInstanceId) =>
                initialConfiguredPaths.get(groupInstanceId) ===
                currentConfiguredPaths.get(groupInstanceId),
            ));
        return {
          fresh,
          valid:
            fresh.update !== null &&
            fresh.update !== undefined &&
            sameUpdateAction(fresh.update, update) &&
            sameGroup &&
            sameInstallPaths &&
            sameConfiguration,
        };
      },
    );

    const setGroupState = Effect.fn("ProviderMaintenanceRunner.setGroupState")(function* (
      state: ServerProviderUpdateState | null,
    ) {
      let providers = yield* providerRegistry.getProviders;
      for (const groupInstanceId of groupIds) {
        providers = yield* providerRegistry.setProviderMaintenanceActionState({
          instanceId: groupInstanceId,
          action: "update",
          state,
        });
      }
      return providers;
    });
    const setQueuedState = setGroupState(
      makeUpdateState({
        status: "queued",
        startedAt: null,
        finishedAt: null,
        message: "Waiting for active provider work to finish before updating.",
      }),
    ).pipe(Effect.map((providers) => ({ providers })));

    const runProviderUpdate = Effect.fn("ProviderMaintenanceRunner.runProviderUpdate")(
      function* () {
        const finish = (state: ServerProviderUpdateState) =>
          setGroupState(state).pipe(Effect.map((providers) => ({ providers })));
        const startedAtRef = yield* Ref.make<string | null>(null);
        const pinStateRef = yield* Ref.make<{
          readonly initialPaths: ReadonlyMap<ProviderInstanceId, string>;
          readonly pinnedPath: string;
        } | null>(null);

        const runCommandAndVerify = Effect.fn("ProviderMaintenanceRunner.runCommandAndVerify")(
          function* () {
            const startedAt = yield* nowIso;
            yield* Ref.set(startedAtRef, startedAt);
            const setRunningMessage = (message: string) =>
              setGroupState(
                makeUpdateState({
                  status: "running",
                  startedAt,
                  finishedAt: null,
                  message,
                }),
              ).pipe(Effect.asVoid);
            yield* setRunningMessage("Checking for the latest version");

            // Recheck the full alias group, install paths, command identity, and provider-pin
            // configuration after the drain. Any change aborts before invoking the vendor updater.
            const queuedTarget = yield* revalidateQueuedTarget();
            if (!queuedTarget.valid || !queuedTarget.fresh.update) {
              return yield* finish(
                makeUpdateState({
                  status: "failed",
                  startedAt,
                  finishedAt: yield* nowIso,
                  message:
                    "Provider settings or installation group changed while waiting. Refresh and try again.",
                }),
              );
            }
            const fresh = queuedTarget.fresh;

            const manifest = yield* manifestService.current;
            // Check compatibility against what an unpinned command would install now.
            const candidateVersion =
              targetVersion ??
              (yield* invalidateLatestProviderVersion(fresh).pipe(
                Effect.andThen(resolveLatestProviderVersion(fresh)),
                Effect.provideService(HttpClient.HttpClient, httpClient),
                Effect.provideService(
                  ProviderLatestVersions.ProviderLatestVersions,
                  latestVersions,
                ),
              ));
            const advisory =
              resolveProviderCompatibility(manifest.compatibility, provider, candidateVersion) ??
              resolveProviderCompatibility(
                ModelManifest.BUNDLED_MODEL_MANIFEST.compatibility,
                provider,
                candidateVersion,
              );
            const command =
              targetVersion !== undefined
                ? makeTargetedProviderUpdateAction(fresh, targetVersion)
                : resolveLatestProviderUpdateAction(fresh, candidateVersion);
            const rejected =
              targetVersion !== undefined
                ? !command ||
                  advisory?.recommendedVersion !== targetVersion ||
                  advisory.status !== "supported"
                : advisory?.status === "broken" || advisory?.status === "unsupported";
            if (rejected || !command) {
              return yield* finish(
                makeUpdateState({
                  status: "failed",
                  startedAt,
                  finishedAt: yield* nowIso,
                  message:
                    targetVersion !== undefined
                      ? "This version is no longer recommended or this installer cannot install a specific version. Refresh provider settings."
                      : "The latest provider version is incompatible with this T3 Code release. Review provider settings.",
                }),
              );
            }
            if (command.pin && initialConfiguredPaths === null) {
              return yield* finish(
                makeUpdateState({
                  status: "failed",
                  startedAt,
                  finishedAt: yield* nowIso,
                  message: "Could not read provider settings for the pin switch.",
                }),
              );
            }

            if (targetVersion === undefined) {
              const priorById = new Map(
                (yield* providerRegistry.getProviders).map(
                  (candidate) => [candidate.instanceId, candidate] as const,
                ),
              );
              const precheck = yield* verifyFromRefresh(
                providerRegistry.refreshInstance(instanceId),
                provider,
                fresh,
                groupIds,
                "pre-update",
              );
              const alreadyCurrent =
                precheck.advisoryEnriched &&
                precheck.verifiedProviders.length === groupIds.length &&
                precheck.verifiedProviders.every(
                  (verified) =>
                    priorById.has(verified.instanceId) &&
                    verified.checkedAt !== priorById.get(verified.instanceId)!.checkedAt &&
                    verified.installed &&
                    verified.status !== "error" &&
                    verified.versionAdvisory?.status === "current",
                );
              if (alreadyCurrent) {
                return yield* finish(
                  makeUpdateState({
                    status: "succeeded",
                    startedAt,
                    finishedAt: yield* nowIso,
                    message: "Already up to date.",
                  }),
                );
              }
            }

            yield* setRunningMessage(`Running ${describeCommand(command)}`);
            const finalTarget = yield* revalidateQueuedTarget();
            if (!finalTarget.valid || !finalTarget.fresh.update) {
              return yield* finish(
                makeUpdateState({
                  status: "failed",
                  startedAt,
                  finishedAt: yield* nowIso,
                  message:
                    "Provider settings or installation group changed before execution. Refresh and try again.",
                }),
              );
            }
            const result = yield* runMaintenanceCommand(command, setRunningMessage);
            const finishedAt = yield* nowIso;
            if (result.timedOut || result.exitCode !== 0) {
              yield* Effect.logWarning("Provider update command failed", {
                provider,
                command: describeCommand(command),
                exitCode: result.exitCode,
                timedOut: result.timedOut,
                output: commandOutput(result),
              });
              return yield* finish(
                makeUpdateState({
                  status: "failed",
                  startedAt,
                  finishedAt,
                  message: failureMessage(result),
                  output: commandOutput(result),
                }),
              );
            }

            let pinConfirmed = false;
            if (command.pin) {
              yield* setRunningMessage("Verifying the new installation");
              const probe = yield* probePinnedBinary(command.pin);
              if (!probe.ok) {
                return yield* finish(
                  makeUpdateState({
                    status: "failed",
                    startedAt,
                    finishedAt: yield* nowIso,
                    message: probe.message,
                    output: commandOutput(result),
                  }),
                );
              }
              const expectedVersion = targetVersion ?? candidateVersion;
              if (
                expectedVersion !== null &&
                expectedVersion !== undefined &&
                normalizedVersion(probe.version) !== normalizedVersion(expectedVersion)
              ) {
                return yield* finish(
                  makeUpdateState({
                    status: "failed",
                    startedAt,
                    finishedAt: yield* nowIso,
                    message: `The installer produced ${normalizedVersion(probe.version)}, but ${normalizedVersion(expectedVersion)} was expected. The previous provider pin was kept.`,
                    output: commandOutput(result),
                  }),
                );
              }

              yield* setRunningMessage("Switching the provider pin");
              const switched = yield* switchConfiguredBinaryPaths(
                groupIds.map((groupInstanceId) => ({
                  instanceId: groupInstanceId,
                  expectedBinaryPath: initialConfiguredPaths!.get(groupInstanceId) ?? "",
                  binaryPath: command.pin!.binaryPath,
                })),
              );
              if (switched.status === "conflict") {
                return yield* finish(
                  makeUpdateState({
                    status: "failed",
                    startedAt,
                    finishedAt: yield* nowIso,
                    message:
                      "Provider settings changed while updating. The previous provider pins were kept. Refresh and try again.",
                    output: commandOutput(result),
                  }),
                );
              }
              yield* Ref.set(pinStateRef, {
                initialPaths: initialConfiguredPaths!,
                pinnedPath: command.pin.binaryPath,
              });
              for (let attempt = 0; attempt < 50; attempt += 1) {
                const pinned = yield* Effect.forEach(
                  groupIds,
                  (groupInstanceId) =>
                    providerRegistry
                      .getProviderMaintenanceCapabilitiesForInstance(groupInstanceId, provider, {
                        fresh: true,
                      })
                      .pipe(
                        Effect.map(
                          (candidate) =>
                            candidate.installPath !== undefined &&
                            normalizeCommandPath(candidate.installPath) ===
                              normalizeCommandPath(command.pin!.binaryPath),
                        ),
                      ),
                  { concurrency: "unbounded" },
                );
                if (pinned.every(Boolean)) {
                  pinConfirmed = true;
                  break;
                }
                yield* Effect.sleep(Duration.millis(100));
              }
            }

            yield* setRunningMessage("Verifying the installed version");
            const verified = yield* providerRegistry.getProviderMaintenanceCapabilitiesForInstance(
              instanceId,
              provider,
              { fresh: true },
            );
            yield* invalidateLatestProviderVersion(verified).pipe(
              Effect.provideService(ProviderLatestVersions.ProviderLatestVersions, latestVersions),
            );
            const { verifiedProviders } = yield* verifyFromRefresh(
              providerRegistry.refreshAfterUpdate(provider, instanceId),
              provider,
              verified,
              groupIds,
              "post-update",
            );
            // Cursor about probes can fail transiently; missing versions alone do not imply failure.
            const couldNotVerify =
              verifiedProviders.length !== groupIds.length ||
              verifiedProviders.some((verifiedProvider) => {
                const installedVersion =
                  verifiedProvider.installedVersion !== undefined
                    ? verifiedProvider.installedVersion
                    : verifiedProvider.version;
                return (
                  !isStillInstalled(verifiedProvider) ||
                  !installedVersion ||
                  (verifiedProvider.status === "error" &&
                    (verifiedProvider.auth.status !== "unauthenticated" ||
                      !verifiedProvider.version)) ||
                  (targetVersion !== undefined &&
                    installedVersion?.replace(/^v/, "") !== targetVersion)
                );
              });
            const stillOutdated =
              targetVersion === undefined &&
              verifiedProviders.some((verifiedProvider) => isOutdatedProvider(verifiedProvider));

            if (command.pin && (couldNotVerify || stillOutdated) && pinConfirmed) {
              const rollback = yield* restoreConfiguredBinaryPaths({
                initialPaths: initialConfiguredPaths!,
                pinnedPath: command.pin.binaryPath,
                instanceIds: groupIds,
              }).pipe(
                Effect.catchCause((cause) => {
                  const rollbackFailure = Cause.squash(cause);
                  return Effect.logError("Provider pin rollback failed", {
                    provider,
                    cause: Cause.pretty(cause),
                  }).pipe(
                    Effect.as<PinRestoreResult>({
                      restored: false,
                      failedInstanceIds: groupIds,
                      failureMessage:
                        rollbackFailure instanceof Error
                          ? rollbackFailure.message
                          : "Could not restore the previous pin.",
                    }),
                  );
                }),
              );
              yield* Ref.set(pinStateRef, null);
              return yield* finish(
                makeUpdateState({
                  status: "failed",
                  startedAt,
                  finishedAt: yield* nowIso,
                  message: rollback.restored
                    ? "The new copy could not be verified. The previous provider pin and installation were kept."
                    : pinRestoreFailureMessage(rollback),
                  output: commandOutput(result),
                }),
              );
            }

            const pinPending = command.pin !== undefined && !pinConfirmed;
            return yield* finish(
              makeUpdateState({
                status: pinPending || couldNotVerify || stillOutdated ? "unchanged" : "succeeded",
                startedAt,
                finishedAt,
                message: pinPending
                  ? "Provider installed and its pin was switched; refresh provider settings to load the new version."
                  : couldNotVerify
                    ? "Update command completed, but T3 Code could not verify the provider version."
                    : stillOutdated
                      ? "Update command completed, but T3 Code still detects an outdated provider version."
                      : "Provider updated.",
                output: commandOutput(result),
              }),
            );
          },
        );

        const recordFailedUpdate = Effect.fn("ProviderMaintenanceRunner.recordFailedUpdate")(
          function* (cause: Cause.Cause<unknown>) {
            const failure = Cause.squash(cause);
            const pinState = yield* Ref.get(pinStateRef);
            const rollback = pinState
              ? yield* restoreConfiguredBinaryPaths({
                  initialPaths: pinState.initialPaths,
                  pinnedPath: pinState.pinnedPath,
                  instanceIds: groupIds,
                }).pipe(
                  Effect.catchCause((rollbackCause) => {
                    const rollbackFailure = Cause.squash(rollbackCause);
                    return Effect.logError("Provider pin rollback failed", {
                      provider,
                      cause: Cause.pretty(rollbackCause),
                    }).pipe(
                      Effect.as<PinRestoreResult>({
                        restored: false,
                        failedInstanceIds: groupIds,
                        failureMessage:
                          rollbackFailure instanceof Error
                            ? rollbackFailure.message
                            : "Could not restore the previous pin.",
                      }),
                    );
                  }),
                )
              : null;
            const startedAt = yield* Ref.get(startedAtRef);
            const updateMessage =
              failure instanceof Error ? failure.message : "Update command failed.";
            return yield* finish(
              makeUpdateState({
                status: "failed",
                startedAt,
                finishedAt: yield* nowIso,
                message:
                  rollback === null
                    ? updateMessage
                    : rollback.restored
                      ? `${updateMessage} The previous provider pin was restored.`
                      : `${updateMessage} Restoring the previous provider pin failed for ${rollback.failedInstanceIds.join(", ")}${rollback.failureMessage ? `: ${rollback.failureMessage}` : ""}. Review provider settings.`,
                output: null,
              }),
            );
          },
        );

        return yield* runCommandAndVerify().pipe(Effect.catchCause(recordFailedUpdate));
      },
    );

    const sessions = Option.getOrUndefined(providerSessions);
    const reserveMaintenance =
      sessions !== undefined && Object.hasOwn(sessions, "reserveMaintenance")
        ? sessions.reserveMaintenance
        : undefined;
    if (reserveMaintenance === undefined) {
      return yield* new ServerProviderUpdateError({
        provider,
        reason: "Provider maintenance admission is unavailable.",
      });
    }

    // Keep the established coordinator lock. The queue key covers all shared aliases, so a
    // repeated click attaches to the existing status instead of invoking a second installer.
    const targetKey = update.pin
      ? `pin:${update.lockKey}:${normalizeCommandPath(update.pin.binaryPath)}`
      : `install:${provider}:${update.lockKey}:${normalizeCommandPath(update.executable)}`;
    const runJob = (reservation: ProviderSessionManager.ProviderMaintenanceReservation) =>
      reservation
        .withUse(
          commandCoordinator.withCommandLock({
            targetKey,
            lockKey: update.lockKey,
            run: runProviderUpdate().pipe(Effect.provideService(Clock.Clock, serviceClock)),
          }),
        )
        .pipe(Effect.provideService(Clock.Clock, serviceClock), Effect.asVoid);

    return yield* updateQueue
      .start({
        key: targetKey,
        instanceIds: groupIds,
        reserve: reserveMaintenance(groupIds),
        setQueued: setQueuedState,
        snapshot: providerRegistry.getProviders.pipe(Effect.map((providers) => ({ providers }))),
        run: runJob,
        release: (reservation) => reservation.release,
        onFailure: Effect.fn("ProviderMaintenanceRunner.onFailure")(
          function* (cause) {
            const failure = Cause.squash(cause);
            yield* setGroupState(
              makeUpdateState({
                status: "failed",
                startedAt: null,
                finishedAt: yield* nowIso,
                message: failure instanceof Error ? failure.message : "Provider update failed.",
              }),
            );
          },
          Effect.provideService(Clock.Clock, serviceClock),
        ),
      })
      .pipe(
        Effect.mapError(
          (cause) =>
            new ServerProviderUpdateError({
              provider,
              reason:
                cause instanceof Error
                  ? cause.message
                  : "Could not reserve the provider install group.",
              cause,
            }),
        ),
      );
  });

  return ProviderMaintenanceRunner.of({
    updateProvider,
  });
});

export const layer = Layer.effect(ProviderMaintenanceRunner, make());
