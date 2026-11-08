import { assert, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Scope from "effect/Scope";
import {
  makeProviderMaintenanceAdmission,
  makeProviderUpdateDrainQueue,
} from "./providerUpdateQueue.ts";

const admission = () =>
  makeProviderMaintenanceAdmission<string, string>({
    isBusy: () => Effect.succeed(false),
    makeBusyError: () => "reserved",
  });

it.effect(
  "Q1: a reservation cannot deadlock an admitted outer startup's nested open/startTurn",
  () =>
    Effect.gen(function* () {
      const gate = yield* admission();
      const entered = yield* Deferred.make<void>();
      const proceed = yield* Deferred.make<void>();
      const events: string[] = [];
      const outer = yield* gate
        .withTurnStart(
          "cli-a",
          Effect.gen(function* () {
            yield* Deferred.succeed(entered, undefined);
            yield* Deferred.await(proceed);
            yield* gate.withTurnStart(
              "cli-a",
              Effect.sync(() => events.push("open")),
            );
            yield* gate.withTurnStart(
              "cli-a",
              Effect.sync(() => events.push("startTurn")),
            );
          }),
        )
        .pipe(Effect.forkScoped);
      yield* Deferred.await(entered);
      const reserved = yield* gate.reserveMaintenance(["cli-a", "cli-alias"]);
      yield* Deferred.succeed(proceed, undefined);
      yield* Fiber.join(outer);
      yield* reserved.awaitReady;
      assert.deepStrictEqual(events, ["open", "startTurn"]);
      const waiter = yield* gate
        .withTurnStart(
          "cli-alias",
          Effect.sync(() => events.push("later")),
        )
        .pipe(Effect.forkScoped);
      yield* reserved.release;
      yield* Fiber.join(waiter);
      assert.deepStrictEqual(events, ["open", "startTurn", "later"]);
    }),
);

it.effect("Q4: interrupting a new admission blocked by maintenance completes before release", () =>
  Effect.gen(function* () {
    const gate = yield* admission();
    const reserved = yield* gate.reserveMaintenance(["cli-a"]);
    const trying = yield* Deferred.make<void>();
    let entered = false;
    const waiter = yield* Deferred.succeed(trying, undefined).pipe(
      Effect.andThen(
        gate.withTurnStart(
          "cli-a",
          Effect.sync(() => {
            entered = true;
          }),
        ),
      ),
      Effect.forkScoped,
    );
    yield* Deferred.await(trying);
    yield* Fiber.interrupt(waiter);
    const exit = yield* Fiber.await(waiter);
    assert.isTrue(Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause));
    assert.isFalse(entered);
    yield* reserved.release;
    yield* gate.withTurnStart("cli-a", Effect.void);
  }),
);

it.effect("a fork cannot reuse an expired inherited admission lease", () =>
  Effect.gen(function* () {
    const gate = yield* admission();
    const scope = yield* Effect.scope;
    const proceed = yield* Deferred.make<void>();
    const trying = yield* Deferred.make<void>();
    let entered = false;
    const child = yield* gate.withTurnStart(
      "cli-a",
      Effect.forkIn(
        Deferred.await(proceed).pipe(
          Effect.andThen(Deferred.succeed(trying, undefined)),
          Effect.andThen(
            gate.withTurnStart(
              "cli-a",
              Effect.sync(() => {
                entered = true;
              }),
            ),
          ),
        ),
        scope,
      ),
    );
    const reserved = yield* gate.reserveMaintenance(["cli-a"]);
    yield* Deferred.succeed(proceed, undefined);
    yield* Deferred.await(trying);
    yield* Fiber.interrupt(child);
    const exit = yield* Fiber.await(child);
    assert.isTrue(Exit.isFailure(exit));
    assert.isFalse(entered);
    yield* reserved.release;
  }),
);

it.effect("Q3: cancellation before the queued handoff releases its reservation exactly once", () =>
  Effect.gen(function* () {
    const queue = yield* makeProviderUpdateDrainQueue();
    const publishing = yield* Deferred.make<void>();
    let releases = 0;
    let runs = 0;
    let queued = 0;
    const starter = yield* queue
      .start({
        key: "physical-a",
        instanceIds: ["cli-a"],
        reserve: Effect.succeed({ lease: true }),
        setQueued: Effect.sync(() => {
          queued++;
        }).pipe(
          Effect.andThen(Deferred.succeed(publishing, undefined)),
          Effect.andThen(Effect.never),
        ),
        snapshot: Effect.succeed("snapshot"),
        run: () =>
          Effect.sync(() => {
            runs++;
          }),
        release: () =>
          Effect.sync(() => {
            releases++;
          }),
        onFailure: () => Effect.void,
      })
      .pipe(Effect.forkScoped);
    yield* Deferred.await(publishing);
    yield* queue.cancel("physical-a");
    const result = yield* Fiber.await(starter);
    assert.isTrue(Exit.isFailure(result));
    assert.strictEqual(releases, 1);
    assert.strictEqual(runs, 0);
    assert.strictEqual(queued, 1);
  }),
);

it.effect("Q3: cancellation waits for installer finalizers before admitting another turn", () =>
  Effect.gen(function* () {
    const gate = yield* admission();
    const queue = yield* makeProviderUpdateDrainQueue();
    const running = yield* Deferred.make<void>();
    const finalizing = yield* Deferred.make<void>();
    const finishFinalizer = yield* Deferred.make<void>();
    let releases = 0;
    let entered = false;
    yield* queue.start({
      key: "physical-a",
      instanceIds: ["cli-a"],
      reserve: gate.reserveMaintenance(["cli-a"]),
      setQueued: Effect.succeed("queued"),
      snapshot: Effect.succeed("snapshot"),
      run: (reservation) =>
        reservation.awaitReady.pipe(
          Effect.andThen(Deferred.succeed(running, undefined)),
          Effect.andThen(Effect.never),
          Effect.ensuring(
            Deferred.succeed(finalizing, undefined).pipe(
              Effect.andThen(Deferred.await(finishFinalizer)),
            ),
          ),
        ),
      release: (reservation) =>
        Effect.sync(() => {
          releases++;
        }).pipe(Effect.andThen(reservation.release)),
      onFailure: () => Effect.void,
    });
    yield* Deferred.await(running);
    const cancel = yield* queue.cancel("physical-a").pipe(Effect.forkScoped);
    yield* Deferred.await(finalizing);
    const next = yield* gate
      .withTurnStart(
        "cli-a",
        Effect.sync(() => {
          entered = true;
        }),
      )
      .pipe(Effect.forkScoped);
    assert.strictEqual(releases, 0);
    assert.isFalse(entered);
    yield* Deferred.succeed(finishFinalizer, undefined);
    yield* Fiber.join(cancel);
    yield* Fiber.join(next);
    assert.strictEqual(releases, 1);
    assert.isTrue(entered);
  }),
);

it.effect("Q3: a service scope closed during reservation cannot orphan a lease or start work", () =>
  Effect.gen(function* () {
    const owner = yield* Scope.make();
    const queue = yield* makeProviderUpdateDrainQueue().pipe(
      Effect.provideService(Scope.Scope, owner),
    );
    const reserved = yield* Deferred.make<void>();
    const handoff = yield* Deferred.make<void>();
    let releases = 0;
    let runs = 0;
    const starter = yield* queue
      .start({
        key: "physical-a",
        instanceIds: ["cli-a"],
        reserve: Deferred.succeed(reserved, undefined).pipe(
          Effect.andThen(Deferred.await(handoff)),
        ),
        setQueued: Effect.succeed("queued"),
        snapshot: Effect.succeed("snapshot"),
        run: () =>
          Effect.sync(() => {
            runs++;
          }),
        release: () =>
          Effect.sync(() => {
            releases++;
          }),
        onFailure: () => Effect.void,
      })
      .pipe(Effect.forkScoped);
    yield* Deferred.await(reserved);
    yield* Scope.close(owner, Exit.void);
    yield* Deferred.succeed(handoff, undefined);
    const exit = yield* Fiber.await(starter);
    assert.isTrue(Exit.isFailure(exit));
    assert.strictEqual(releases, 1);
    assert.strictEqual(runs, 0);
    const later = yield* queue
      .start({
        key: "later",
        instanceIds: [],
        reserve: Effect.void,
        setQueued: Effect.void,
        snapshot: Effect.void,
        run: () => Effect.void,
        release: () =>
          Effect.sync(() => {
            releases++;
          }),
        onFailure: () => Effect.void,
      })
      .pipe(Effect.exit);
    assert.isTrue(Exit.isFailure(later));
    assert.strictEqual(releases, 1);
  }),
);
