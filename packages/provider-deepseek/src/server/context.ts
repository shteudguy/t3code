import type { ApiMessage, ApiTool } from "./transport.ts";

const bytes = (value: unknown) => new TextEncoder().encode(JSON.stringify(value)).length;
// A byte bound is deliberately conservative: reserve room below the 1M-token
// context for message framing, the request envelope and generated output.
export const MAX_REQUEST_CONTEXT_BYTES = 750_000;

export interface ContextWindowMetadata {
  sourceMessageCount: number;
  retainedMessageCount: number;
  omittedMessageCount: number;
  bytesBefore: number;
  bytesAfter: number;
  archivePath: string;
  archiveRevision: number;
}

/** Select a request window without changing the durable transcript or tool ledger. */
export function prepareContextWindow(
  messages: readonly ApiMessage[],
  tools: readonly ApiTool[],
  archive: { path: string; revision: number },
  maximumBytes = MAX_REQUEST_CONTEXT_BYTES,
): { messages: ApiMessage[]; compacted: boolean; metadata?: ContextWindowMetadata } {
  if (!Number.isSafeInteger(maximumBytes) || maximumBytes <= 0) throw Error("invalid-context-byte-budget");
  const before = bytes({ messages, tools });
  if (before <= maximumBytes) return { messages: [...messages], compacted: false };

  // Each assistant tool-call message and all its responses form one atomic group.
  const groups: number[][] = [];
  let pending = new Set<string>();
  for (let i = 0; i < messages.length; i++) {
    const message = messages[i]!;
    if (message.role === "tool") {
      if (!message.tool_call_id || !pending.delete(message.tool_call_id)) throw Error("context-tool-history-invalid");
      groups[groups.length - 1]!.push(i);
    } else {
      if (pending.size) throw Error("context-tool-history-incomplete");
      groups.push([i]);
      if (message.role === "assistant") {
        const ids = (message.tool_calls ?? []).map((call) => call.id);
        pending = new Set(ids);
        if (pending.size !== ids.length) throw Error("context-tool-history-invalid");
      }
    }
  }
  if (pending.size) throw Error("context-tool-history-incomplete");
  let latestUser = -1;
  messages.forEach((message, i) => { if (message.role === "user") latestUser = i; });
  if (latestUser < 0) throw Error("context-user-anchor-missing");
  const retained = new Set<number>([latestUser]);
  // Preserve every operator message as well as all system instructions; older
  // constraints must not disappear when completed assistant/tool history moves out.
  messages.forEach((message, i) => { if (message.role === "system" || message.role === "user") retained.add(i); });
  const notice: ApiMessage = { role: "system", content:
    "Native context-window notice. Earlier conversation and completed tool results remain in the durable journal " +
    JSON.stringify(archive.path) + ". " +
    "This request retains all system instructions, all user messages in their original order, and recent complete tool exchanges. " +
    "Earlier context is archived, not deleted. Consult the saved task checkpoint and relevant archived context " +
    "before further changes. No tool execution has been repeated and this notice grants no new authorization." };
  const render = () => {
    const selected = [...retained].sort((a, b) => a - b).map((i) => messages[i]!);
    const position = selected[0]?.role === "system" ? 1 : 0;
    return [...selected.slice(0, position), notice, ...selected.slice(position)];
  };
  if (bytes({ messages: render(), tools }) > maximumBytes) throw Error("context-anchor-too-large");
  for (let g = groups.length - 1; g >= 0; g--) {
    const added = groups[g]!.filter((index) => !retained.has(index));
    added.forEach((index) => retained.add(index));
    if (bytes({ messages: render(), tools }) > maximumBytes) {
      added.forEach((index) => retained.delete(index));
      break;
    }
  }
  const result = render();
  return { messages: result, compacted: true, metadata: {
    sourceMessageCount: messages.length, retainedMessageCount: result.length - 1,
    omittedMessageCount: messages.length - retained.size, bytesBefore: before,
    bytesAfter: bytes({ messages: result, tools }), archivePath: archive.path, archiveRevision: archive.revision,
  } };
}
