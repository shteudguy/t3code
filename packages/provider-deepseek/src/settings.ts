import { makeProviderSettingsSchema, SettingsSecretValue } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

export const DeepSeekSettings = makeProviderSettingsSchema(
  {
    enabled: Schema.Boolean.pipe(
      Schema.withDecodingDefault(Effect.succeed(false)),
      Schema.annotateKey({ providerSettingsForm: { hidden: true } }),
    ),
    apiKey: SettingsSecretValue.pipe(
      Schema.withDecodingDefault(Effect.succeed("")),
      Schema.annotateKey({
        title: "API key",
        description: "Key for the official DeepSeek API, stored securely on this environment.",
        providerSettingsForm: { clearWhenEmpty: "omit" },
      }),
    ),
  },
  { order: ["apiKey"] },
);
export type DeepSeekSettings = typeof DeepSeekSettings.Type;
