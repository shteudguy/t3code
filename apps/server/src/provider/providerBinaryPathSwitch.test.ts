import { assert, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  DEFAULT_SERVER_SETTINGS,
  ProviderDriverKind,
  ProviderInstanceId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";
import * as ServerSettings from "../serverSettings.ts";
import * as ServerConfig from "../config.ts";
import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import * as ManagedPolicy from "../policy/ManagedPolicy.ts";
import { prepareProviderBinaryPathSwitch } from "./providerBinaryPathSwitch.ts";

const a = ProviderInstanceId.make("codex_a");
const b = ProviderInstanceId.make("codex_b");
const driver = ProviderDriverKind.make("codex");
const settings = {
  ...DEFAULT_SERVER_SETTINGS,
  providerInstances: {
    [a]: { driver, displayName: "A", config: { binaryPath: "/old/a", extra: "preserved" } },
    [b]: { driver, displayName: "B", config: { binaryPath: "/old/b" } },
  },
};

it("validates the entire batch and leaves every original object untouched on rejection", () => {
  const original = JSON.stringify(settings);
  const result = prepareProviderBinaryPathSwitch(settings, [
    { instanceId: a, expectedBinaryPath: "/old/a", binaryPath: "/new" },
    { instanceId: b, expectedBinaryPath: "/foreign", binaryPath: "/new" },
  ]);
  assert.strictEqual(result.status, "conflict");
  assert.strictEqual(JSON.stringify(settings), original);
  const changed = prepareProviderBinaryPathSwitch(settings, [
    { instanceId: a, expectedBinaryPath: "/old/a", binaryPath: "/new" },
    { instanceId: b, expectedBinaryPath: "/old/b", binaryPath: "/new" },
  ]);
  assert.strictEqual(changed.status, "switched");
  if (changed.status !== "switched") return;
  assert.strictEqual(
    (changed.settings.providerInstances[a]?.config as Record<string, unknown>).binaryPath,
    "/new",
  );
  assert.strictEqual(
    (changed.settings.providerInstances[b]?.config as Record<string, unknown>).binaryPath,
    "/new",
  );
  assert.strictEqual(
    (changed.settings.providerInstances[a]?.config as Record<string, unknown>).extra,
    "preserved",
  );
  assert.strictEqual(changed.settings.providerModelPreferences, settings.providerModelPreferences);
  assert.strictEqual(changed.settings.defaultModelSelection, settings.defaultModelSelection);
  assert.strictEqual(JSON.stringify(settings), original);
});

const harness = Effect.gen(function* () {
  const actualFs = yield* FileSystem.FileSystem;
  const cfgContext = yield* Layer.build(
    ServerConfig.layerTest(process.cwd(), { prefix: "maintenance-cas-" }),
  );
  const config = Context.get(cfgContext, ServerConfig.ServerConfig);
  const operations: string[] = [];
  const secretValues = new Map<string, Uint8Array>();
  const secrets = ServerSecretStore.ServerSecretStore.of({
    get: (key) =>
      Effect.sync(() => {
        operations.push("secret.get");
        return Option.fromUndefinedOr(secretValues.get(key));
      }),
    set: (key, value) =>
      Effect.sync(() => {
        operations.push("secret.set");
        secretValues.set(key, value);
      }),
    create: (key, value) =>
      Effect.sync(() => {
        operations.push("secret.create");
        secretValues.set(key, value);
      }),
    getOrCreateRandom: () =>
      Effect.sync(() => {
        operations.push("secret.random");
        return new Uint8Array();
      }),
    remove: (key) =>
      Effect.sync(() => {
        operations.push("secret.remove");
        secretValues.delete(key);
      }),
  });
  // SQL is a read-only external boundary; these SELECTs find no historical provider/project rows.
  const sql = (() => Effect.succeed([])) as unknown as SqlClient.SqlClient;
  const trackedFs = FileSystem.FileSystem.of({
    ...actualFs,
    writeFileString: (...args) =>
      Effect.sync(() => {
        operations.push("fs.write");
      }).pipe(Effect.andThen(actualFs.writeFileString(...args))),
    rename: (...args) =>
      Effect.sync(() => {
        operations.push("fs.rename");
      }).pipe(Effect.andThen(actualFs.rename(...args))),
    remove: (...args) =>
      Effect.sync(() => {
        operations.push("fs.remove");
      }).pipe(Effect.andThen(actualFs.remove(...args))),
  });
  const context = yield* Layer.build(
    ServerSettings.layer.pipe(
      Layer.provide(ManagedPolicy.layerTest()),
      Layer.provide(Layer.succeed(ServerConfig.ServerConfig, config)),
      Layer.provide(Layer.succeed(SqlClient.SqlClient, sql)),
      Layer.provide(Layer.succeed(ServerSecretStore.ServerSecretStore, secrets)),
      Layer.provide(Layer.succeed(FileSystem.FileSystem, trackedFs)),
    ),
  );
  const service = Context.get(context, ServerSettings.ServerSettingsService);
  return { actualFs, config, service, operations, secretValues };
});

it.effect(
  "rejected/empty/NOOP batches perform zero migration, secret, persistence and event writes",
  () =>
    Effect.gen(function* () {
      const h = yield* harness;
      const original = JSON.stringify({
        providerInstances: settings.providerInstances,
        // Both current inline and retired source-control migrations are pending on first load.
        github: { tokens: { "github.com": "synthetic-fixture-only" } },
        projectSettingsFolded: false,
      });
      yield* h.actualFs.writeFileString(h.config.settingsPath, original);
      const changes = yield* h.service.subscribeChanges;
      const emitted = yield* Deferred.make<void>();
      let events = 0;
      yield* Stream.runForEach(changes, () =>
        Effect.sync(() => {
          events++;
        }).pipe(Effect.andThen(Deferred.succeed(emitted, undefined))),
      ).pipe(Effect.forkScoped);
      const first = { instanceId: a, expectedBinaryPath: "/old/a", binaryPath: "/new" };
      const batches = [
        [first, { instanceId: b, expectedBinaryPath: "/wrong", binaryPath: "/new" }],
        [
          first,
          {
            instanceId: ProviderInstanceId.make("unknown"),
            expectedBinaryPath: "",
            binaryPath: "/new",
          },
        ],
        [first, first],
        [first, { instanceId: b, expectedBinaryPath: "/old/b", binaryPath: null }],
        [],
        [{ instanceId: a, expectedBinaryPath: "/old/a", binaryPath: "/old/a" }],
      ];
      for (let index = 0; index < batches.length; index++) {
        const exit = yield* h.service
          .switchProviderBinaryPaths(
            batches[index] as Parameters<typeof h.service.switchProviderBinaryPaths>[0],
          )
          .pipe(Effect.exit);
        if (index === 0)
          assert.deepStrictEqual(Exit.isSuccess(exit) && exit.value, { status: "conflict" });
        else if (index < 4) assert.isTrue(Exit.isFailure(exit));
        else assert.deepStrictEqual(Exit.isSuccess(exit) && exit.value, { status: "switched" });
        assert.deepStrictEqual(h.operations, []);
        assert.strictEqual(events, 0);
        assert.strictEqual(yield* h.actualFs.readFileString(h.config.settingsPath), original);
      }
      yield* h.service.switchProviderBinaryPaths([
        first,
        { instanceId: b, expectedBinaryPath: "/old/b", binaryPath: "/new" },
      ]);
      yield* Deferred.await(emitted);
      assert.strictEqual(events, 1);
      assert.isTrue(
        h.operations.includes("secret.set"),
        "changed batch must take the real pending migration/persistence path",
      );
      assert.isTrue(h.operations.includes("fs.write"));
      assert.isTrue(h.operations.includes("fs.rename"));
      const saved = JSON.parse(yield* h.actualFs.readFileString(h.config.settingsPath));
      assert.strictEqual(saved.providerInstances[a].config.binaryPath, "/new");
      assert.strictEqual(saved.providerInstances[b].config.binaryPath, "/new");
      assert.strictEqual(saved.providerInstances[a].config.extra, "preserved");
      assert.notInclude(JSON.stringify(saved), "synthetic-fixture-only");
    }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect(
  "native persistence preserves non-pin settings, profile data, models, defaults and secret bytes",
  () =>
    Effect.gen(function* () {
      const h = yield* harness;
      yield* h.actualFs.writeFileString(
        h.config.settingsPath,
        JSON.stringify({
          providerInstances: {
            [a]: {
              ...settings.providerInstances[a],
              environment: [{ name: "FIXTURE_SECRET", value: "synthetic-only", sensitive: true }],
              config: {
                binaryPath: "/old/a",
                customModels: ["fixture-model"],
                customModelOptions: { "fixture-model": { maxOutputTokens: 7 } },
              },
            },
            [b]: settings.providerInstances[b],
          },
          defaultModelSelection: {
            instanceId: a,
            model: "fixture-model",
            options: [{ id: "fixture-option", value: true }],
          },
          projectSettingsFolded: true,
        }),
      );
      yield* h.service.updateSettings({});
      const before = JSON.parse(yield* h.actualFs.readFileString(h.config.settingsPath));
      const beforeSecrets = [...h.secretValues].map(([key, bytes]) => [key, [...bytes]]);
      h.operations.length = 0;
      yield* h.service.switchProviderBinaryPaths([
        { instanceId: a, expectedBinaryPath: "/old/a", binaryPath: "/new" },
        { instanceId: b, expectedBinaryPath: "/old/b", binaryPath: "/new" },
      ]);
      const after = JSON.parse(yield* h.actualFs.readFileString(h.config.settingsPath));
      before.providerInstances[a].config.binaryPath = "/new";
      before.providerInstances[b].config.binaryPath = "/new";
      assert.deepStrictEqual(after, before);
      assert.deepStrictEqual(
        [...h.secretValues].map(([key, bytes]) => [key, [...bytes]]),
        beforeSecrets,
      );
      assert.isFalse(h.operations.includes("secret.set"));
      assert.isFalse(h.operations.includes("secret.remove"));
    }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect(
  "concurrent CAS uses fresh disk state under the existing semaphore and rollback keeps foreign pins",
  () =>
    Effect.gen(function* () {
      const h = yield* harness;
      yield* h.actualFs.writeFileString(
        h.config.settingsPath,
        JSON.stringify({
          providerInstances: settings.providerInstances,
          projectSettingsFolded: true,
        }),
      );
      const results = yield* Effect.forEach(
        ["/one", "/two"],
        (binaryPath) =>
          h.service.switchProviderBinaryPaths([
            { instanceId: a, expectedBinaryPath: "/old/a", binaryPath },
            { instanceId: b, expectedBinaryPath: "/old/b", binaryPath },
          ]),
        { concurrency: "unbounded" },
      );
      assert.strictEqual(results.filter((r) => r.status === "switched").length, 1);
      assert.strictEqual(results.filter((r) => r.status === "conflict").length, 1);
      const current = JSON.parse(yield* h.actualFs.readFileString(h.config.settingsPath));
      const installed = current.providerInstances[a].config.binaryPath;
      assert.strictEqual(current.providerInstances[b].config.binaryPath, installed);
      current.providerInstances[b].config.binaryPath = "/foreign";
      yield* h.actualFs.writeFileString(h.config.settingsPath, JSON.stringify(current));
      const before = h.operations.length;
      const rollback = yield* h.service.switchProviderBinaryPaths([
        { instanceId: b, expectedBinaryPath: installed, binaryPath: "/old/b" },
      ]);
      assert.strictEqual(rollback.status, "conflict");
      assert.strictEqual(h.operations.length, before);
      const saved = JSON.parse(yield* h.actualFs.readFileString(h.config.settingsPath));
      assert.strictEqual(saved.providerInstances[b].config.binaryPath, "/foreign");
      assert.strictEqual(saved.providerInstances[a].config.binaryPath, installed);
    }).pipe(Effect.provide(NodeServices.layer)),
);
