import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";

export interface ProviderMaintenanceReservation<Id> {
  readonly instanceIds: ReadonlyArray<Id>;
  /** Waits interruptibly until existing turns and provider background work drain. */
  readonly awaitReady: Effect.Effect<void>;
  /** Releases the admission barrier and wakes later turn starts. Idempotent. */
  readonly release: Effect.Effect<void>;
}

export interface ProviderMaintenanceAdmission<Id, E> {
  readonly withTurnStart: <A, E2, R>(
    instanceId: Id,
    use: Effect.Effect<A, E2, R>,
  ) => Effect.Effect<A, E2, R>;
  readonly reserveMaintenance: (
    instanceIds: ReadonlyArray<Id>,
  ) => Effect.Effect<ProviderMaintenanceReservation<Id>, E>;
  /** Wakes drain waiters after session activity changes; timed rechecks remain as a fallback. */
  readonly notifyActivityChanged: Effect.Effect<void>;
}

/**
 * Admission is independent of session storage and SQLite. A reservation is published under the
 * mutex before its owner waits, so new opens and turn starts cannot pass while old work drains.
 */
export const makeProviderMaintenanceAdmission = <Id, E>(input: {
  readonly isBusy: (instanceIds: ReadonlyArray<Id>) => Effect.Effect<boolean>;
  readonly makeBusyError: (instanceIds: ReadonlyArray<Id>) => E;
}): Effect.Effect<ProviderMaintenanceAdmission<Id, E>> =>
  Effect.gen(function* () {
    const mutex = yield* Semaphore.make(1);
    const leaseOwner = {};
    const readers = new Map<Id, number>();
    const reserved = new Map<Id, Deferred.Deferred<void>>();
    const changedRef = yield* Ref.make(yield* Deferred.make<void>());

    const signalChangedUnderMutex = Effect.gen(function* () {
      const previous = yield* Ref.get(changedRef);
      const next = yield* Deferred.make<void>();
      yield* Ref.set(changedRef, next);
      yield* Deferred.succeed(previous, undefined);
    });

    const notifyActivityChanged = mutex.withPermits(1)(signalChangedUnderMutex);

    interface AdmittedTurnLease {
      readonly owner: object;
      readonly instanceId: unknown;
      readonly active: Ref.Ref<boolean>;
    }

    class AdmittedTurn extends Context.Service<AdmittedTurn, AdmittedTurnLease>()(
      "t3/provider/providerUpdateQueue/AdmittedTurn",
    ) {}

    const acquireTurn = (instanceId: Id): Effect.Effect<AdmittedTurnLease> =>
      Effect.uninterruptibleMask(() =>
        Effect.gen(function* () {
          const inherited = Option.getOrUndefined(yield* Effect.serviceOption(AdmittedTurn));
          const admission = yield* mutex.withPermits(1)(
            Effect.gen(function* () {
              const nested =
                inherited !== undefined &&
                inherited.owner === leaseOwner &&
                inherited.instanceId === instanceId &&
                (yield* Ref.get(inherited.active));
              const barrier = reserved.get(instanceId);
              if (barrier !== undefined && !nested) return { barrier, lease: null };
              readers.set(instanceId, (readers.get(instanceId) ?? 0) + 1);
              const active = yield* Ref.make(true);
              return { barrier: null, lease: { owner: leaseOwner, instanceId, active } };
            }),
          );
          if (admission.lease !== null) return admission.lease;
          yield* Effect.interruptible(Deferred.await(admission.barrier));
          return yield* acquireTurn(instanceId);
        }),
      );

    const withTurnStart: ProviderMaintenanceAdmission<Id, E>["withTurnStart"] = (instanceId, use) =>
      Effect.acquireUseRelease(
        acquireTurn(instanceId),
        (lease) => use.pipe(Effect.provideService(AdmittedTurn, lease)),
        (lease) =>
          mutex.withPermits(1)(
            Effect.gen(function* () {
              if (!(yield* Ref.getAndSet(lease.active, false))) return;
              const count = (readers.get(instanceId) ?? 1) - 1;
              if (count === 0) readers.delete(instanceId);
              else readers.set(instanceId, count);
              yield* signalChangedUnderMutex;
            }),
          ),
      );

    const reserveMaintenance: ProviderMaintenanceAdmission<Id, E>["reserveMaintenance"] = (
      instanceIdsInput,
    ) =>
      Effect.uninterruptible(
        Effect.gen(function* () {
          const instanceIds = [...new Set(instanceIdsInput)];
          const barrier = yield* mutex.withPermits(1)(
            Effect.gen(function* () {
              if (
                instanceIds.length === 0 ||
                instanceIds.some((instanceId) => reserved.has(instanceId))
              ) {
                return yield* Effect.fail(input.makeBusyError(instanceIds));
              }
              const next = yield* Deferred.make<void>();
              for (const instanceId of instanceIds) reserved.set(instanceId, next);
              return next;
            }),
          );
          const released = yield* Ref.make(false);

          const awaitReady = Effect.gen(function* () {
            while (true) {
              // Capture the signal and reader state atomically. A release between this snapshot
              // and the idle check completes the captured Deferred, so no wakeup can be lost.
              const snapshot = yield* mutex.withPermits(1)(
                Effect.gen(function* () {
                  return {
                    readersIdle: instanceIds.every(
                      (instanceId) => (readers.get(instanceId) ?? 0) === 0,
                    ),
                    changed: yield* Ref.get(changedRef),
                  };
                }),
              );
              const busy = snapshot.readersIdle ? yield* input.isBusy(instanceIds) : true;
              if (snapshot.readersIdle && !busy) return;

              // Activity notifications are the fast path. The timeout also rechecks providers
              // whose background-work state changes without a session event.
              yield* Deferred.await(snapshot.changed).pipe(
                Effect.timeoutOption(Duration.millis(100)),
                Effect.asVoid,
              );
            }
          });

          const release = Effect.uninterruptible(
            mutex.withPermits(1)(
              Effect.gen(function* () {
                if (yield* Ref.getAndSet(released, true)) return;
                for (const instanceId of instanceIds) {
                  if (reserved.get(instanceId) === barrier) reserved.delete(instanceId);
                }
                yield* Deferred.succeed(barrier, undefined);
                yield* signalChangedUnderMutex;
              }),
            ),
          );

          return { instanceIds, awaitReady, release };
        }),
      );

    return { withTurnStart, reserveMaintenance, notifyActivityChanged };
  });

export interface ProviderUpdateDrainQueueStartInput<Payload, Lease, E, R, RunError> {
  /** Stable update target shared by alias requests for the same installation. */
  readonly key: string;
  readonly instanceIds: ReadonlyArray<string>;
  /** Must reserve every affected instance immediately and must not wait for drain. */
  readonly reserve: Effect.Effect<Lease, E, R>;
  readonly setQueued: Effect.Effect<Payload, never, R>;
  /** Read without changing state when a second click matches a live job. */
  readonly snapshot: Effect.Effect<Payload, never, R>;
  readonly run: (lease: Lease) => Effect.Effect<unknown, RunError, R>;
  readonly release: (lease: Lease) => Effect.Effect<void, never, R>;
  readonly onFailure: (cause: Cause.Cause<RunError>) => Effect.Effect<void, never, R>;
}

export interface ProviderUpdateDrainQueue {
  readonly start: <Payload, Lease, E, R, RunError>(
    input: ProviderUpdateDrainQueueStartInput<Payload, Lease, E, R, RunError>,
  ) => Effect.Effect<Payload, E, R>;
  /** Used for explicit job cancellation; service-scope shutdown interrupts the same fibers. */
  readonly cancel: (key: string) => Effect.Effect<void>;
}

interface ActiveUpdateJob {
  readonly token: object;
  readonly instanceIds: ReadonlyArray<string>;
  readonly fiber: Fiber.Fiber<void, never> | null;
  readonly cancelled: Ref.Ref<boolean>;
  readonly cleanup: Effect.Effect<void>;
}

/** A registered owner exists before forking; workers cannot run before their handle is published. */
export const makeProviderUpdateDrainQueue = Effect.fn("ProviderUpdateDrainQueue.make")(
  function* () {
    const serviceScope = yield* Effect.scope;
    const gate = yield* Semaphore.make(1);
    const closed = yield* Ref.make(false);
    const jobsRef = yield* Ref.make<ReadonlyMap<string, ActiveUpdateJob>>(new Map());

    const removeJob = (key: string, token: object) =>
      Ref.update(jobsRef, (jobs) => {
        if (jobs.get(key)?.token !== token) return jobs;
        const next = new Map(jobs);
        next.delete(key);
        return next;
      });

    const stopJob = (key: string, job: ActiveUpdateJob) =>
      Effect.uninterruptible(
        Effect.gen(function* () {
          yield* Ref.set(job.cancelled, true);
          const current = (yield* Ref.get(jobsRef)).get(key);
          const fiber = current?.token === job.token ? current.fiber : job.fiber;
          // A published fiber must finish its interruption/finalizers before admission is released.
          if (fiber !== null) yield* Fiber.interrupt(fiber).pipe(Effect.asVoid);
          yield* job.cleanup;
        }),
      );

    yield* Scope.addFinalizer(
      serviceScope,
      Effect.uninterruptible(
        Effect.gen(function* () {
          yield* Ref.set(closed, true);
          const jobs = yield* Ref.get(jobsRef);
          yield* Effect.forEach([...jobs], ([key, job]) => stopJob(key, job), {
            concurrency: "unbounded",
            discard: true,
          });
        }),
      ),
    );

    const start: ProviderUpdateDrainQueue["start"] = <Payload, Lease, E, R, RunError>(
      input: ProviderUpdateDrainQueueStartInput<Payload, Lease, E, R, RunError>,
    ) =>
      Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          const callerContext = yield* Effect.context<R>();
          const serviceContext = Context.add(callerContext, Scope.Scope, serviceScope);
          const payload = yield* Deferred.make<Payload>();
          const startGate = yield* Deferred.make<void>();
          const cancelled = yield* Ref.make(false);
          const released = yield* Ref.make(false);
          const token = {};
          const admitted = yield* gate.withPermits(1)(
            Effect.gen(function* () {
              if (yield* Ref.get(closed)) return yield* Effect.interrupt;
              if ((yield* Ref.get(jobsRef)).has(input.key)) return null;
              const lease = yield* input.reserve;
              const cleanup: Effect.Effect<void> = Effect.uninterruptible(
                Ref.getAndSet(released, true).pipe(
                  Effect.flatMap((alreadyReleased) =>
                    alreadyReleased
                      ? Effect.void
                      : Deferred.interrupt(payload).pipe(
                          Effect.andThen(input.release(lease)),
                          Effect.ensuring(removeJob(input.key, token)),
                          Effect.asVoid,
                        ),
                  ),
                ),
              ).pipe(Effect.provideContext(serviceContext));
              const entry: ActiveUpdateJob = {
                token,
                instanceIds: [...new Set(input.instanceIds)],
                fiber: null,
                cancelled,
                cleanup,
              };
              // Functional Ref updates also serialize correctly with cancellation outside gate.
              yield* Ref.update(jobsRef, (jobs) => new Map(jobs).set(input.key, entry));
              if (yield* Ref.get(closed)) {
                yield* Ref.set(cancelled, true);
                yield* cleanup;
                return yield* Effect.interrupt;
              }
              return { lease, entry };
            }),
          );
          if (admitted === null) return yield* restore(input.snapshot);
          const job = Effect.interruptible(
            Effect.gen(function* () {
              yield* Deferred.await(startGate);
              if (yield* Ref.get(cancelled)) return yield* Effect.interrupt;
              const queued = yield* input.setQueued;
              yield* Deferred.succeed(payload, queued);
              if (yield* Ref.get(cancelled)) return yield* Effect.interrupt;
              yield* input.run(admitted.lease);
            }),
          ).pipe(
            Effect.catchCause((cause) =>
              Deferred.interrupt(payload).pipe(
                Effect.andThen(
                  Cause.hasInterruptsOnly(cause)
                    ? Effect.void
                    : input.onFailure(cause).pipe(Effect.ignoreCause),
                ),
                Effect.asVoid,
              ),
            ),
            Effect.ensuring(admitted.entry.cleanup),
            Effect.provideContext(serviceContext),
          );
          const fiber = yield* Effect.forkIn(job, serviceScope);
          yield* Ref.update(jobsRef, (jobs) => {
            if (jobs.get(input.key)?.token !== token) return jobs;
            return new Map(jobs).set(input.key, { ...admitted.entry, fiber });
          });
          if ((yield* Ref.get(cancelled)) || (yield* Ref.get(closed))) {
            yield* Fiber.interrupt(fiber).pipe(Effect.asVoid);
            yield* admitted.entry.cleanup;
          } else {
            yield* Deferred.succeed(startGate, undefined);
          }
          // RPC cancellation only cancels this wait, after the server-owned job has been armed.
          return yield* restore(Deferred.await(payload));
        }),
      );

    const cancel: ProviderUpdateDrainQueue["cancel"] = Effect.fn("ProviderUpdateDrainQueue.cancel")(
      function* (key) {
        const job = (yield* Ref.get(jobsRef)).get(key);
        if (job !== undefined) yield* stopJob(key, job);
      },
    );

    return { start, cancel };
  },
);
