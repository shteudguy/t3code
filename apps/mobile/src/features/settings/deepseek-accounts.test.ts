import { describe, expect, it } from "vite-plus/test";
import { ProviderInstanceId } from "@t3tools/contracts";
import { DEEPSEEK_DRIVER, planDeepSeekKeySave } from "./deepseek-accounts";
describe("mobile DeepSeek key settings", () => {
  it("preserves instance configuration and environment when replacing a key", () => {
    const id = ProviderInstanceId.make("deepseek-team");
    const current = {
      driver: DEEPSEEK_DRIVER,
      enabled: false,
      displayName: "Team",
      environment: [{ name: "FIXTURE", value: "kept", sensitive: false }],
      config: { apiKey: "old", other: "kept" },
    };
    const result = planDeepSeekKeySave({
      providerInstances: { [id]: current },
      policy: undefined,
      instanceId: id,
      apiKey: " new-fixture-key ",
    });
    expect("mutation" in result).toBe(true);
    if ("mutation" in result && result.mutation.operation === "upsert")
      expect(result.mutation.instance).toEqual({
        ...current,
        enabled: true,
        config: { ...current.config, enabled: true, apiKey: "new-fixture-key" },
      });
  });
  it("rejects empty and multiline keys without exposing them", () => {
    for (const apiKey of ["", "fixture\nsecret"]) {
      const result = planDeepSeekKeySave({ providerInstances: {}, policy: undefined, apiKey });
      expect(result).toEqual({ error: "Enter a single-line DeepSeek API key." });
    }
  });
});
