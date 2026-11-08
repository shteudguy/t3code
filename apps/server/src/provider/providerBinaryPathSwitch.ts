import type { ProviderInstanceId, ServerSettings } from "@t3tools/contracts";

export interface ProviderBinaryPathSwitchChange {
  readonly instanceId: ProviderInstanceId;
  readonly expectedBinaryPath: string;
  readonly binaryPath: string;
}

export type ProviderBinaryPathSwitchPreparation =
  | { readonly status: "invalid"; readonly reason: string }
  | { readonly status: "conflict" }
  | {
      readonly status: "switched";
      readonly settings: ServerSettings;
      readonly changed: boolean;
    };

const isRecord = (value: unknown): value is Record<string, unknown> =>
  value !== null && typeof value === "object" && !Array.isArray(value);

const configuredBinaryPath = (config: unknown): string => {
  if (!isRecord(config)) return "";
  const binaryPath = config.binaryPath;
  return typeof binaryPath === "string" ? binaryPath.trim() : "";
};

/** Validate the entire compare-and-swap batch before producing any new settings. */
export function prepareProviderBinaryPathSwitch(
  settings: ServerSettings,
  changes: ReadonlyArray<ProviderBinaryPathSwitchChange>,
): ProviderBinaryPathSwitchPreparation {
  if (!Array.isArray(changes)) {
    return { status: "invalid", reason: "Provider binary-path changes must be an array." };
  }

  const seen = new Set<string>();
  for (const change of changes) {
    if (
      !change ||
      typeof change !== "object" ||
      typeof change.instanceId !== "string" ||
      typeof change.expectedBinaryPath !== "string" ||
      typeof change.binaryPath !== "string"
    ) {
      return { status: "invalid", reason: "A provider binary-path change is malformed." };
    }
    if (seen.has(change.instanceId)) {
      return { status: "invalid", reason: "Duplicate provider instance id in batch." };
    }
    seen.add(change.instanceId);

    if (!Object.hasOwn(settings.providerInstances, change.instanceId)) {
      return { status: "invalid", reason: "Unknown provider instance id in batch." };
    }
    const instance = settings.providerInstances[change.instanceId as ProviderInstanceId];
    if (instance === undefined || (instance.config !== undefined && !isRecord(instance.config))) {
      return { status: "invalid", reason: "Provider instance configuration is malformed." };
    }
  }

  for (const change of changes) {
    const instance = settings.providerInstances[change.instanceId]!;
    if (configuredBinaryPath(instance.config) !== change.expectedBinaryPath) {
      return { status: "conflict" };
    }
  }

  if (changes.length === 0) {
    return { status: "switched", settings, changed: false };
  }

  const providerInstances = { ...settings.providerInstances };
  let changed = false;
  for (const change of changes) {
    const instance = settings.providerInstances[change.instanceId]!;
    const currentPath = configuredBinaryPath(instance.config);
    const nextPath = change.binaryPath.trim();
    if (currentPath === nextPath) continue;

    const config =
      instance.config === undefined ? {} : { ...(instance.config as Record<string, unknown>) };
    if (nextPath.length === 0) {
      delete config.binaryPath;
    } else {
      config.binaryPath = change.binaryPath;
    }
    providerInstances[change.instanceId] = { ...instance, config };
    changed = true;
  }

  return {
    status: "switched",
    settings: changed ? { ...settings, providerInstances } : settings,
    changed,
  };
}
