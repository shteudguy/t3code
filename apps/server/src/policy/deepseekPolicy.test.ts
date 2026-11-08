import { describe, it, expect } from "@effect/vitest";
import {
  DEFAULT_SERVER_SETTINGS,
  ProviderDriverKind,
  ProviderInstanceId,
} from "@t3tools/contracts";
import { knownDriverLabels, makeManagedProviders } from "./managedProviders.ts";
describe("native DeepSeek managed policy", () => {
  it("recognizes the driver and enforces pinned/ceiling instance environments", () => {
    const driver = ProviderDriverKind.make("deepseek-api"),
      id = ProviderInstanceId.make("deepseek-api");
    expect(knownDriverLabels()).toContainEqual({ driver, label: "DeepSeek API" });
    const result = makeManagedProviders(
      { allowedDrivers: [driver], instances: { [driver]: { locked: true, environment: [] } } },
      "approval-required",
      "win32",
    );
    expect(result._tag).toBe("Valid");
    if (result._tag !== "Valid") throw Error(result.reason);
    const next = result.providers.applyLimits({
      ...DEFAULT_SERVER_SETTINGS,
      providerInstances: {
        [id]: {
          driver,
          enabled: true,
          environment: [{ name: "NODE_OPTIONS", value: "fixture-blocked", sensitive: false }],
          config: { apiKey: "fixture-key" },
        },
      },
    });
    expect(next.providerInstances[id]).toBeDefined();
    expect(next.providerInstances[id]?.environment ?? []).toEqual([]);
    expect(result.providers.lockedInstanceFields[driver]).toContain("environment");
  });
});
