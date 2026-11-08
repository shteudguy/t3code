import {
  complete,
  DeepSeekApiError,
  retryDelay,
  type ApiTool,
  type ApiToolCall,
  type CompletionInput,
} from "./transport.ts";
import { Journal } from "./journal.ts";
import { prepareContextWindow } from "./context.ts";

export type AgentRequestRecovery = {
  type: "stream-lost";
  /** 1 = failed original response, 2 = the single bounded recovery request. */
  attempt: 1 | 2;
  code: string;
  action: "recovering" | "exhausted";
};

/** A body-stream socket loss happens before complete() can return any assistant/tool message. */
const recoverableStreamLoss = (error: unknown): error is DeepSeekApiError =>
  error instanceof DeepSeekApiError && /^transport-stream-read:(?:UND_ERR_SOCKET|ECONNRESET):/.test(error.code);

export interface NativeTool {
  definition: ApiTool;
  mutating: boolean;
  /** Validate before recording an executable intent or asking for approval. */
  validate: (input: unknown) => Record<string, unknown>;
  execute: (input: Record<string, unknown>, signal: AbortSignal) => Promise<string>;
}
export interface AgentInput extends Omit<CompletionInput, "messages" | "tools"> {
  journal: Journal;
  prompt: string;
  instructions: string;
  nativeTools: Map<string, NativeTool>;
  authorize: (
    tool: NativeTool,
    call: ApiToolCall,
    input: Record<string, unknown>,
  ) => Promise<boolean>;
  onTool?: (call: ApiToolCall, state: "running" | "completed" | "failed", output?: string) => void;
  onRequest?: (result: Awaited<ReturnType<typeof complete>>) => void;
  /** Reports the one side-effect-free recovery of a response whose SSE body was lost. */
  onRequestRecovery?: (event: AgentRequestRecovery) => void;
  /** Steering is admitted by T3; consume each accepted message once at a model boundary. */
  /** Must synchronously close admission before returning true. */
  tryFinish?: () => boolean;
}

export async function runAgent(input: AgentInput) {
  const { journal } = input;
  if (journal.uncertain) throw new Error("transcript-recovery-required");
  await journal.settleUnexecuted();
  if (journal.state.messages[0]?.role === "system")
    journal.state.messages[0] = { role: "system", content: input.instructions };
  else journal.state.messages.unshift({ role: "system", content: input.instructions });
  await journal.flushSteering();
  journal.state.messages.push({ role: "user", content: input.prompt });
  journal.state.interrupted = false;
  await journal.save();
  try {
    while (true) {
      input.signal.throwIfAborted();
      await journal.flushSteering();
      const definitions = [...input.nativeTools.values()].map((tool) => tool.definition);
      const context = prepareContextWindow(journal.state.messages, definitions, {
        path: journal.file,
        revision: journal.state.revision,
      });
      if (context.compacted && context.metadata) {
        journal.state.contextWindow = context.metadata;
        await journal.save();
      }
      const completion = async () => {
        const messagesBefore = journal.state.messages.length;
        const toolsBefore = Object.keys(journal.state.tools).length;
        let recovered = false;
        for (;;) {
          try {
            return await complete({
              ...input,
              messages: context.messages,
              tools: definitions,
              freshConnection: recovered,
            });
          } catch (error) {
            if (!recoverableStreamLoss(error)) throw error;
            if (recovered) {
              input.onRequestRecovery?.({
                type: "stream-lost",
                attempt: 2,
                code: error.code,
                action: "exhausted",
              });
              throw error;
            }
            if (
              journal.state.messages.length !== messagesBefore ||
              Object.keys(journal.state.tools).length !== toolsBefore ||
              journal.uncertain
            ) {
              throw error;
            }
            input.onRequestRecovery?.({
              type: "stream-lost",
              attempt: 1,
              code: error.code,
              action: "recovering",
            });
            recovered = true;
            await retryDelay(input.signal);
            input.signal.throwIfAborted();
          }
        }
      };
      const result = await completion();
      // Store all reasoning_content for subsequent tool-enabled API requests.
      journal.state.messages.push(result.message);
      if (result.usage) (journal.state.usage ??= []).push(result.usage);
      await journal.save();
      input.onRequest?.(result);
      const calls = result.message.tool_calls ?? [];
      if (!calls.length) {
        if (journal.hasSteering || input.tryFinish?.() === false) continue;
        return result;
      }
      for (const call of calls) {
        input.signal.throwIfAborted();
        const tool = input.nativeTools.get(call.function.name);
        if (!tool) throw new Error("unadvertised-tool");
        let args: Record<string, unknown>;
        try {
          args = tool.validate(JSON.parse(call.function.arguments));
        } catch (error) {
          // Validation runs before approval and before any side effect, so this result
          // is a proven no-effect rejection rather than an unknown action.
          const code =
            error instanceof Error && /^[a-z0-9-]{1,40}$/.test(error.message)
              ? error.message
              : null;
          const output = JSON.stringify({
            error: "invalid-tool-arguments",
            detail:
              "Arguments failed validation. No tool was executed. Fix the arguments and retry.",
            notExecuted: true,
            ...(code ? { code } : {}),
          });
          input.onTool?.(call, "failed", output);
          journal.state.messages.push({ role: "tool", tool_call_id: call.id, content: output });
          await journal.save();
          continue;
        }
        const allowed = await input.authorize(tool, call, args);
        input.signal.throwIfAborted();
        const output = allowed
          ? await journal.execute(call, tool.mutating, async () => {
              input.onTool?.(call, "running");
              return tool.execute(args, input.signal);
            })
          : "Tool execution denied by T3 runtime policy or user. No action performed.";
        let failed = !allowed;
        try {
          const value = JSON.parse(output);
          failed ||= Boolean(
            value?.error ||
            value?.isError ||
            value?.timedOut === true ||
            value?.exitCode === null ||
            (typeof value?.signal === "string" && value.signal.length > 0) ||
            (typeof value?.exitCode === "number" && value.exitCode !== 0),
          );
        } catch {
          /* Plain tool text is a valid result. */
        }
        input.onTool?.(call, failed ? "failed" : "completed", output);
        journal.state.messages.push({ role: "tool", tool_call_id: call.id, content: output });
        await journal.save();
      }
    }
  } catch (error) {
    journal.state.interrupted = true;
    await journal.settleUnexecuted();
    await journal.save();
    throw error;
  }
}
