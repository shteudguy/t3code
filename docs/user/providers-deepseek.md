# DeepSeek API

Use **DeepSeek API** to run a coding agent with an official DeepSeek API key. It connects
directly to DeepSeek and is separate from DeepSeek Harness in ACP Registry or models in OpenCode.
API usage is billed to the key's account.

In **Settings → Providers**, enable DeepSeek API on the environment that will run the agent,
then set its **API key**. The key is stored in that environment's secret store; you can also
select a configured secret provider. Additional instances can use separate keys. New instances
are disabled until you enable them. A configured key is not reported as verified authentication.

On mobile, open **Settings → Providers → DeepSeek API**, enter a new key and choose **Save and enable**.
Existing instances keep their other configuration when you replace a key. Create additional named instances
or choose a secret-provider reference from web or desktop Settings.

Select `deepseek-flash` or `deepseek-v4-pro` and a reasoning effort: **Off**, **low**, **high**,
or **max**. The default is **max**. T3 sends the selected model and effort without substitutions.
See the [official API documentation](https://api-docs.deepseek.com/guides/thinking_mode/).

The agent can read and list workspace files, write files, run commands, and use T3 tools when
available. **Supervised** asks before writes and commands; **Auto-accept edits** permits file
writes and asks before commands; **Full access** permits both. Automatic review is unavailable.
These modes govern this agent's tool execution; shell commands are not an OS sandbox.

Stop cancels the current request and active tool. If a tool's outcome is uncertain after a stop
or crash, the conversation requires recovery instead of replaying that tool or restarting the
task automatically. Check the workspace before continuing in a new thread. A command that
explicitly starts an independent background process can outlive its shell.

Attachments, structured Plan mode, conversation rollback, native forks, and background text
generation are currently unavailable. On Windows, file writes use an exclusive handle and
retain a preimage backup. On other hosts, the file-writing tool creates new files; edits to
existing files can be made with a command under the thread's permission mode.
