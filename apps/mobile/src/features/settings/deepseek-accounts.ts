import {
  defaultInstanceIdForDriver,
  isManagedDriverAllowed,
  isManagedSettingPath,
  ProviderDriverKind,
  type ManagedPolicySummary,
  type ProviderInstanceId,
  type ProviderInstanceMutation,
  type ServerSettings,
} from "@t3tools/contracts";

export const DEEPSEEK_DRIVER = ProviderDriverKind.make("deepseek-api");

export function planDeepSeekKeySave(input: {
  readonly providerInstances: ServerSettings["providerInstances"];
  readonly policy: ManagedPolicySummary | undefined;
  readonly instanceId?: ProviderInstanceId;
  readonly apiKey: string;
}): { readonly mutation: ProviderInstanceMutation } | { readonly error: string } {
  const instanceId = input.instanceId ?? defaultInstanceIdForDriver(DEEPSEEK_DRIVER);
  if (!isManagedDriverAllowed(input.policy, DEEPSEEK_DRIVER))
    return { error: "DeepSeek API is not allowed by this environment's policy." };
  if (
    input.policy &&
    ["", ".enabled", ".config.enabled", ".config.apiKey"].some((suffix) =>
      isManagedSettingPath(input.policy!, `providerInstances.${instanceId}${suffix}`),
    )
  )
    return { error: `Managed by ${input.policy.organization}.` };
  const apiKey = input.apiKey.trim();
  if (!apiKey || apiKey.length > 16384 || /[\r\n]/.test(apiKey))
    return { error: "Enter a single-line DeepSeek API key." };
  const current = input.providerInstances[instanceId];
  if (current && current.driver !== DEEPSEEK_DRIVER)
    return { error: "This instance belongs to another provider." };
  const config = current?.config && typeof current.config === "object" ? current.config : {};
  return {
    mutation: {
      operation: "upsert",
      instanceId,
      instance: {
        ...current,
        driver: DEEPSEEK_DRIVER,
        enabled: true,
        config: { ...config, enabled: true, apiKey },
      },
    },
  };
}
