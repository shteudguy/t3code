import { isAtomCommandInterrupted } from "@t3tools/client-runtime/state/runtime";
import type { ServerProvider } from "@t3tools/contracts";
import { useRef, useState } from "react";
import { TextInput, View } from "react-native";
import { AppText as Text } from "../../components/ui/AppText";
import { ControlPill } from "../../components/ui/ControlPill";
import { ListActionRow } from "../../components/ui/ListActionRow";
import { serverEnvironment } from "../../state/server";
import { useAtomCommand } from "../../state/use-atom-command";
import { planDeepSeekKeySave } from "./deepseek-accounts";
import type { SettingsTarget } from "./settings-environment-filter";

export function DeepSeekAccount({
  environment,
  provider,
}: {
  readonly environment: SettingsTarget;
  readonly provider?: ServerProvider;
}) {
  const [editing, setEditing] = useState(false);
  const [apiKey, setApiKey] = useState("");
  const [pending, setPending] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const pendingRef = useRef(false);
  const mutate = useAtomCommand(serverEnvironment.mutateProviderInstance, {
    reportFailure: false,
    reportDefect: false,
  });
  async function save() {
    if (pendingRef.current) return;
    const plan = planDeepSeekKeySave({
      providerInstances: environment.serverConfig.settings.providerInstances,
      policy: environment.serverConfig.managedPolicy,
      instanceId: provider?.instanceId,
      apiKey,
    });
    if ("error" in plan) {
      setError(plan.error);
      return;
    }
    pendingRef.current = true;
    setPending(true);
    setError(null);
    try {
      const result = await mutate({
        environmentId: environment.environmentId,
        input: { patch: {}, providerInstanceMutation: plan.mutation },
      });
      if (result._tag === "Success") {
        setApiKey("");
        setEditing(false);
      } else if (!isAtomCommandInterrupted(result))
        setError("Could not save the DeepSeek API key.");
    } catch {
      setError("Could not save the DeepSeek API key.");
    } finally {
      pendingRef.current = false;
      setPending(false);
    }
  }
  return (
    <View className="border-t border-border-subtle">
      <ListActionRow
        icon="person.crop.circle"
        label={provider?.displayName ?? "DeepSeek API"}
        description={
          provider?.enabled
            ? provider.auth.label
            : "Set an official API key to enable this provider."
        }
        disabled={pending}
        onPress={() => setEditing(true)}
      />
      {editing ? (
        <View className="gap-3 px-4 pb-4">
          <TextInput
            accessibilityLabel="DeepSeek API key"
            secureTextEntry
            autoCapitalize="none"
            autoCorrect={false}
            maxLength={16384}
            editable={!pending}
            value={apiKey}
            onChangeText={setApiKey}
            placeholder="API key"
            className="rounded-lg border border-border-subtle px-3 py-2 text-base text-foreground"
          />
          <Text className="text-sm text-foreground-muted">
            Stored securely on {environment.label}. API usage is billed to this key.
          </Text>
          <View className="flex-row gap-3">
            <ControlPill
              label={pending ? "Saving…" : "Save and enable"}
              disabled={pending}
              onPress={() => {
                void save();
              }}
            />
            <ControlPill
              label="Cancel"
              disabled={pending}
              onPress={() => {
                setApiKey("");
                setError(null);
                setEditing(false);
              }}
            />
          </View>
        </View>
      ) : null}
      {error ? (
        <Text accessibilityRole="alert" className="px-4 pb-3 text-danger-foreground">
          {error}
        </Text>
      ) : null}
    </View>
  );
}
