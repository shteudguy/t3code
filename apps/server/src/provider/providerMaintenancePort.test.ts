import { assert, it } from "@effect/vitest";
import {
  ProviderDriverKind,
  ProviderInstanceId,
  type ServerProvider,
  type ServerProviderUpdateState,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import { HttpClient, HttpClientResponse } from "effect/http";
import { ChildProcessSpawner } from "effect/process";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as ProviderLatestVersions from "@t3tools/provider-core/server/ProviderLatestVersions";
import { type ProviderMaintenanceCapabilities } from "@t3tools/provider-core/server/maintenance";
import * as ProviderRegistry from "./ProviderRegistry.ts";
import * as ModelManifest from "./ModelManifest.ts";
import * as Runner from "./providerMaintenanceRunner.ts";
import * as SessionManager from "../orchestration-v2/ProviderSessionManager.ts";
import * as ServerSettings from "../serverSettings.ts";
import { makeProviderMaintenanceAdmission } from "./providerUpdateQueue.ts";

const cliA = ProviderDriverKind.make("nativeCliA");
const cliB = ProviderDriverKind.make("nativeCliB");
const idA = ProviderInstanceId.make("cli-a");
const aliasA = ProviderInstanceId.make("cli-a-alias");
const idB = ProviderInstanceId.make("cli-b");
const direct = ProviderInstanceId.make("direct-api");
const directDriver = ProviderDriverKind.make("deepseek");
const snapshot = (instanceId: ProviderInstanceId, driver: ProviderDriverKind): ServerProvider => ({
  instanceId,
  driver,
  enabled: true,
  installed: true,
  version: "1.0.0",
  status: "ready",
  auth: { status: "authenticated" },
  checkedAt: "2026-01-01T00:00:00Z",
  models: [],
  slashCommands: [],
  skills: [],
});

const harness = Effect.gen(function* () {
  const states = yield* Queue.unbounded<{
    instanceId: ProviderInstanceId;
    state: ServerProviderUpdateState;
  }>();
  const activityChecked = yield* Queue.unbounded<ReadonlyArray<ProviderInstanceId>>();
  const busy = new Set<ProviderInstanceId>();
  const providers = yield* Ref.make<ReadonlyArray<ServerProvider>>([
    snapshot(idA, cliA),
    snapshot(aliasA, cliA),
    snapshot(idB, cliB),
    snapshot(direct, directDriver),
  ]);
  let blockedBeforeReady: Deferred.Deferred<void> | undefined;
  const admission = yield* makeProviderMaintenanceAdmission<ProviderInstanceId, string>({
    makeBusyError: () => "reserved",
    isBusy: (ids) =>
      Queue.offer(activityChecked, ids).pipe(Effect.as(ids.some((id) => busy.has(id)))),
  });
  const sessions: SessionManager.ProviderSessionManagerV2["Service"] = {
    shutdown: Effect.void,
    open: () => Effect.die("unused open"),
    get: () => Effect.die("unused get"),
    isLive: () => Effect.succeed(false),
    close: () => Effect.void,
    closeInstance: () => Effect.void,
    release: () => Effect.void,
    detach: () => Effect.void,
    releaseUnlessBusy: () => Effect.succeed(true),
    otherWorkingThreads: () => Effect.succeed([]),
    withTurnStart: admission.withTurnStart,
    reserveMaintenance: (ids) =>
      admission.reserveMaintenance(ids).pipe(
        Effect.orDie,
        Effect.map((reservation) => ({
          instanceIds: reservation.instanceIds,
          release: reservation.release,
          withUse: (use) =>
            reservation.awaitReady.pipe(
              Effect.andThen(
                Effect.suspend(() =>
                  blockedBeforeReady === undefined
                    ? Effect.void
                    : Deferred.await(blockedBeforeReady),
                ),
              ),
              Effect.andThen(use),
            ),
        })),
      ),
  };
  const calls: string[] = [];
  const installs = new Map<ProviderInstanceId, string>([
    [idA, "/cli/a"],
    [aliasA, "/cli/a"],
    [idB, "/cli/b"],
  ]);
  const pinned = new Set<ProviderInstanceId>();
  const envRevisions = new Map<ProviderInstanceId, string>();
  let onRunning: Effect.Effect<void> = Effect.void;
  let onRefresh: Effect.Effect<void> = Effect.void;
  let failVerification = false;
  let missingVersion = false;
  const pinContext = yield* Layer.build(
    ServerSettings.layerTest({
      providerInstances: {
        [idA]: { driver: cliA, config: { binaryPath: "/cli/a", preserved: "a" } },
        [aliasA]: { driver: cliA, config: { binaryPath: "/cli/a", preserved: "alias" } },
      },
    }),
  );
  const pinSettings = Context.get(pinContext, ServerSettings.ServerSettingsService);
  const synchronizePins = pinSettings.getSettings.pipe(
    Effect.tap((settings) =>
      Effect.sync(() => {
        for (const id of [idA, aliasA]) {
          const config = settings.providerInstances[id]?.config as Record<string, unknown>;
          if (typeof config.binaryPath === "string") installs.set(id, config.binaryPath);
        }
      }),
    ),
  );
  const settingsService: ServerSettings.ServerSettingsService["Service"] = {
    ...pinSettings,
    switchProviderBinaryPaths: (changes) =>
      pinSettings
        .switchProviderBinaryPaths(changes)
        .pipe(
          Effect.tap((result) => (result.status === "switched" ? synchronizePins : Effect.void)),
        ),
  };
  const capabilities = (
    id: ProviderInstanceId,
    driver: ProviderDriverKind,
  ): ProviderMaintenanceCapabilities => ({
    provider: driver,
    packageName: null,
    latestVersion: "2.0.0",
    ...(installs.has(id) ? { installPath: installs.get(id)! } : {}),
    update:
      id === direct
        ? null
        : {
            command: `update ${id === idB ? "b" : "a"}`,
            executable: "fixture-package-manager",
            args: ["update", id === idB ? "b" : "a"],
            lockKey: "shared-package-manager",
            env: { FIXTURE_INSTALL: envRevisions.get(id) ?? (id === idB ? "b" : "a") },
            ...(pinned.has(id)
              ? { pin: { binaryPath: "/managed/a", versionArgs: ["--version"] } }
              : {}),
          },
  });
  const registry: ProviderRegistry.ProviderRegistry["Service"] = {
    getProviders: Ref.get(providers),
    refresh: () => Ref.get(providers),
    refreshInstance: () => Ref.get(providers),
    reloadSecretsAndRefresh: () => Ref.get(providers),
    refreshWorkspaceSnapshot: () => Ref.get(providers),
    refreshAfterUpdate: (driver) =>
      Effect.suspend(() => onRefresh).pipe(
        Effect.andThen(
          Ref.updateAndGet(providers, (rows) =>
            rows.map((row) =>
              row.driver === driver
                ? {
                    ...row,
                    version: missingVersion ? null : failVerification ? "1.0.0" : "2.0.0",
                    checkedAt: "2026-01-02T00:00:00Z",
                  }
                : row,
            ),
          ),
        ),
      ),
    invalidateModelCaches: () => Effect.void,
    getProviderMaintenanceCapabilitiesForInstance: (id, driver) =>
      Effect.sync(() => capabilities(id, driver)),
    setProviderMaintenanceActionState: ({ instanceId, state }) =>
      Ref.updateAndGet(providers, (rows) =>
        rows.map((row) =>
          row.instanceId === instanceId
            ? { ...row, ...(state ? { updateState: state } : {}) }
            : row,
        ),
      ).pipe(
        Effect.tap(() =>
          state === null ? Effect.void : Queue.offer(states, { instanceId, state }),
        ),
        Effect.tap(() =>
          state?.message?.startsWith("Running ") ? Effect.suspend(() => onRunning) : Effect.void,
        ),
      ),
    streamChanges: Stream.empty,
  };
  const spawnGate = yield* Deferred.make<void>();
  let blockInstaller = false;
  let beforeSpawn: (() => void) | undefined;
  const encoder = new TextEncoder();
  const spawner = ChildProcessSpawner.make((command) => {
    const child = command as unknown as { command: string; args: ReadonlyArray<string> };
    beforeSpawn?.();
    calls.push(`${child.command} ${child.args.join(" ")}`);
    return Effect.succeed(
      ChildProcessSpawner.makeHandle({
        pid: ChildProcessSpawner.ProcessId(1),
        exitCode: Effect.suspend(() =>
          blockInstaller
            ? Deferred.await(spawnGate).pipe(Effect.as(ChildProcessSpawner.ExitCode(0)))
            : Effect.succeed(ChildProcessSpawner.ExitCode(0)),
        ),
        isRunning: Effect.succeed(false),
        kill: () => Effect.void,
        unref: Effect.succeed(Effect.void),
        stdin: Sink.drain,
        stdout: Stream.make(encoder.encode(child.args.includes("--version") ? "2.0.0" : "updated")),
        stderr: Stream.empty,
        all: Stream.empty,
        getInputFd: () => Sink.drain,
        getOutputFd: () => Stream.empty,
      }),
    );
  });
  const manifest: ModelManifest.ModelManifestData = {
    version: 1,
    currentModels: {},
    compatibility: [],
  };
  const runner = yield* Runner.make().pipe(
    Effect.provideService(ProviderRegistry.ProviderRegistry, registry),
    Effect.provideService(SessionManager.ProviderSessionManagerV2, sessions),
    Effect.provideService(ServerSettings.ServerSettingsService, settingsService),
    Effect.provideService(ModelManifest.ModelManifest, {
      current: Effect.succeed(manifest),
      refresh: Effect.succeed(manifest),
      forceRefresh: Effect.succeed(manifest),
      refreshInBackground: Effect.void,
    }),
    Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
    Effect.provideService(
      HttpClient.HttpClient,
      HttpClient.make((request) =>
        Effect.succeed(HttpClientResponse.fromWeb(request, Response.json({ version: "2.0.0" }))),
      ),
    ),
    Effect.provideService(HostProcess.Platform, "linux"),
    Effect.provide(ProviderLatestVersions.layer),
  );
  const finished = (id: ProviderInstanceId) =>
    Queue.take(states).pipe(
      Effect.repeat({
        until: (entry) =>
          entry.instanceId === id &&
          ["succeeded", "failed", "unchanged"].includes(entry.state.status),
      }),
    );
  return {
    runner,
    calls,
    busy,
    installs,
    providers,
    states,
    activityChecked,
    admission,
    finished,
    pinned,
    pinSettings,
    envRevisions,
    onRunning: (effect: Effect.Effect<void>) => {
      onRunning = effect;
    },
    onRefresh: (effect: Effect.Effect<void>) => {
      onRefresh = effect;
    },
    failVerification: () => {
      failVerification = true;
    },
    missingVersion: () => {
      missingVersion = true;
    },
    blockBeforeReady: (gate: Deferred.Deferred<void>) => {
      blockedBeforeReady = gate;
    },
    setBeforeSpawn: (fn: () => void) => {
      beforeSpawn = fn;
    },
    blockSpawn: () => {
      blockInstaller = true;
    },
    spawnGate,
  };
});

it.effect(
  "Q2: a draining group never holds the package-manager lock; alias clicks dedupe and API drivers stay manual",
  () =>
    Effect.gen(function* () {
      const h = yield* harness;
      h.busy.add(idA);
      const queued = yield* h.runner.updateProvider({ provider: cliA, instanceId: idA });
      assert.strictEqual(
        queued.providers.find((p) => p.instanceId === idA)?.updateState?.status,
        "queued",
      );
      yield* Queue.take(h.activityChecked);
      const second = yield* h.runner.updateProvider({ provider: cliA, instanceId: aliasA });
      assert.strictEqual(
        second.providers.find((p) => p.instanceId === aliasA)?.updateState?.status,
        "queued",
      );
      yield* h.runner.updateProvider({ provider: cliB, instanceId: idB });
      const b = yield* h.finished(idB);
      assert.strictEqual(b.state.status, "succeeded");
      assert.deepStrictEqual(h.calls, ["fixture-package-manager update b"]);
      const unsupported = yield* h.runner
        .updateProvider({ provider: directDriver, instanceId: direct })
        .pipe(Effect.exit);
      assert.strictEqual(unsupported._tag, "Failure");
      assert.deepStrictEqual(h.calls, ["fixture-package-manager update b"]);
      h.busy.clear();
      yield* h.admission.notifyActivityChanged;
      const a = yield* h.finished(idA);
      assert.strictEqual(a.state.status, "succeeded");
      assert.deepStrictEqual(h.calls, [
        "fixture-package-manager update b",
        "fixture-package-manager update a",
      ]);
      assert.strictEqual(
        (yield* Ref.get(h.providers)).find((p) => p.instanceId === aliasA)?.version,
        "2.0.0",
      );
    }),
);

it.effect("the final check rejects a changed command environment before any installer runs", () =>
  Effect.gen(function* () {
    const h = yield* harness;
    h.onRunning(
      Effect.sync(() => {
        h.envRevisions.set(idA, "foreign-env");
      }),
    );
    yield* h.runner.updateProvider({ provider: cliA, instanceId: idA });
    const done = yield* h.finished(idA);
    assert.strictEqual(done.state.status, "failed");
    assert.deepStrictEqual(h.calls, []);
  }),
);

it.effect("a pinned update verifies the actual binary and switches every alias pin", () =>
  Effect.gen(function* () {
    const h = yield* harness;
    h.pinned.add(idA);
    h.pinned.add(aliasA);
    yield* h.runner.updateProvider({ provider: cliA, instanceId: idA });
    const done = yield* h.finished(idA);
    assert.strictEqual(done.state.status, "succeeded");
    assert.deepStrictEqual(h.calls, ["fixture-package-manager update a", "/managed/a --version"]);
    const settings = yield* h.pinSettings.getSettings;
    for (const id of [idA, aliasA]) {
      assert.strictEqual(
        (settings.providerInstances[id]?.config as Record<string, unknown>).binaryPath,
        "/managed/a",
      );
    }
    assert.strictEqual(
      (settings.providerInstances[aliasA]?.config as Record<string, unknown>).preserved,
      "alias",
    );
  }),
);

it.effect("the last pin comparison rejects a foreign pin before invoking the installer", () =>
  Effect.gen(function* () {
    const h = yield* harness;
    h.pinned.add(idA);
    h.pinned.add(aliasA);
    h.onRunning(
      h.pinSettings
        .switchProviderBinaryPaths([
          { instanceId: aliasA, expectedBinaryPath: "/cli/a", binaryPath: "/foreign" },
        ])
        .pipe(Effect.orDie, Effect.asVoid),
    );
    yield* h.runner.updateProvider({ provider: cliA, instanceId: idA });
    const done = yield* h.finished(idA);
    assert.strictEqual(done.state.status, "failed");
    assert.deepStrictEqual(h.calls, []);
    const settings = yield* h.pinSettings.getSettings;
    assert.strictEqual(
      (settings.providerInstances[aliasA]?.config as Record<string, unknown>).binaryPath,
      "/foreign",
    );
  }),
);

it.effect("a missing actual CLI version cannot verify a successful update", () =>
  Effect.gen(function* () {
    const h = yield* harness;
    h.missingVersion();
    yield* h.runner.updateProvider({ provider: cliA, instanceId: idA });
    const done = yield* h.finished(idA);
    assert.strictEqual(done.state.status, "unchanged");
    assert.include(done.state.message ?? "", "could not verify");
  }),
);

it.effect(
  "failed pin verification rolls back conditionally and preserves a foreign alias pin",
  () =>
    Effect.gen(function* () {
      const h = yield* harness;
      h.pinned.add(idA);
      h.pinned.add(aliasA);
      h.failVerification();
      h.onRefresh(
        h.pinSettings
          .switchProviderBinaryPaths([
            { instanceId: aliasA, expectedBinaryPath: "/managed/a", binaryPath: "/foreign" },
          ])
          .pipe(Effect.orDie, Effect.asVoid),
      );
      yield* h.runner.updateProvider({ provider: cliA, instanceId: idA });
      const done = yield* h.finished(idA);
      assert.strictEqual(done.state.status, "failed");
      const settings = yield* h.pinSettings.getSettings;
      assert.strictEqual(
        (settings.providerInstances[idA]?.config as Record<string, unknown>).binaryPath,
        "/cli/a",
      );
      assert.strictEqual(
        (settings.providerInstances[aliasA]?.config as Record<string, unknown>).binaryPath,
        "/foreign",
      );
      assert.strictEqual(
        (settings.providerInstances[aliasA]?.config as Record<string, unknown>).preserved,
        "alias",
      );
    }),
);

it.effect(
  "the runner rechecks the install target under lock after drain and refuses a changed target",
  () =>
    Effect.gen(function* () {
      const h = yield* harness;
      h.busy.add(idA);
      yield* h.runner.updateProvider({ provider: cliA, instanceId: idA });
      yield* Queue.take(h.activityChecked);
      h.installs.set(idA, "/foreign/a");
      h.busy.clear();
      yield* h.admission.notifyActivityChanged;
      const done = yield* h.finished(idA);
      assert.strictEqual(done.state.status, "failed");
      assert.deepStrictEqual(h.calls, []);
    }),
);
