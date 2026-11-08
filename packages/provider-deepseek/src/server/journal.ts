// @effect-diagnostics nodeBuiltinImport:off -- Durable native tool journal uses fsync and atomic rename before side effects.
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import type { ApiMessage, ApiToolCall, ApiUsage } from "./transport.ts";
import type { ContextWindowMetadata } from "./context.ts";

export interface ToolIntent {
  call: ApiToolCall;
  status: "prepared" | "running" | "done" | "unknown";
  mutating: boolean;
  output?: string;
}
export interface Transcript {
  version: 1;
  id: string;
  messages: ApiMessage[];
  tools: Record<string, ToolIntent>;
  interrupted: boolean;
  revision: number;
  view?: unknown;
  usage?: ApiUsage[];
  contextWindow?: ContextWindowMetadata;
  pendingSteering?: { id: string; message: ApiMessage }[];
  appliedSteering?: string[];
}

/** One owner per transcript. Readers never run or replay a stored tool intent. */
export class Journal {
  readonly file: string;
  readonly state: Transcript;
  private writes: Promise<void> = Promise.resolve();
  private constructor(file: string, state: Transcript) {
    this.file = file;
    this.state = state;
  }

  static async load(root: string, id: string) {
    if (!/^[a-zA-Z0-9_-]{1,160}$/.test(id)) throw new Error("invalid-transcript-identity");
    await NodeFSP.mkdir(root, { recursive: true, mode: 0o700 });
    const rootStat = await NodeFSP.lstat(root);
    if (rootStat.isSymbolicLink() || !rootStat.isDirectory())
      throw new Error("invalid-transcript-root");
    const file = NodePath.join(root, `${id}.json`);
    let state: Transcript;
    try {
      const stat = await NodeFSP.lstat(file);
      if (!stat.isFile() || stat.isSymbolicLink() || stat.size > 64 * 1024 * 1024)
        throw new Error("invalid-transcript-file");
      state = JSON.parse(await NodeFSP.readFile(file, "utf8"));
      if (
        state.version !== 1 ||
        state.id !== id ||
        !Array.isArray(state.messages) ||
        !state.tools ||
        typeof state.tools !== "object" ||
        !Number.isSafeInteger(state.revision)
      ) {
        throw new Error("invalid-transcript-schema");
      }
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
      state = { version: 1, id, messages: [], tools: {}, interrupted: false, revision: 0 };
    }
    const journal = new Journal(file, state);
    let recovered = false;
    for (const intent of Object.values(state.tools)) {
      if (intent.status === "running") {
        intent.status = "unknown";
        recovered = true;
      } else if (intent.status === "prepared") {
        // execute() fsyncs "running" before entering the tool. Prepared means no action began.
        intent.status = "done";
        intent.output = "Provider restarted before tool execution. No action was performed.";
        recovered = true;
      }
    }
    const answered = new Set(
      state.messages
        .filter((message) => message.role === "tool")
        .map((message) => message.tool_call_id),
    );
    const pending = state.messages
      .flatMap((message) => message.tool_calls ?? [])
      .filter((call) => !answered.has(call.id));
    if (!journal.uncertain) {
      for (const call of pending) {
        const intent = Object.hasOwn(state.tools, call.id) ? state.tools[call.id] : undefined;
        state.messages.push({
          role: "tool",
          tool_call_id: call.id,
          content:
            intent?.output ??
            "Provider restarted before preparing tool execution. No action was performed.",
        });
        recovered = true;
      }
    }
    if (recovered) {
      state.interrupted = true;
      await journal.save();
    }
    return journal;
  }

  get uncertain() {
    return Object.values(this.state.tools).some((tool) => tool.status === "unknown");
  }
  get hasSteering() {
    return (this.state.pendingSteering?.length ?? 0) > 0;
  }

  async save() {
    const operation = this.writes.then(() => this.write());
    this.writes = operation.catch(() => {});
    await operation;
  }

  private async write() {
    const temp = `${this.file}.${NodeCrypto.randomUUID()}.tmp`;
    const fd = await NodeFSP.open(temp, "wx", 0o600);
    try {
      this.state.revision += 1;
      await fd.writeFile(JSON.stringify(this.state));
      await fd.sync();
    } finally {
      await fd.close();
    }
    try {
      await NodeFSP.rename(temp, this.file);
    } catch (error) {
      await NodeFSP.unlink(temp).catch(() => {});
      throw error;
    }
  }

  async enqueueSteering(id: string, message: ApiMessage) {
    if (
      this.state.appliedSteering?.includes(id) ||
      this.state.pendingSteering?.some((item) => item.id === id)
    ) {
      // A previous save may have failed after mutating memory. A duplicate still
      // needs a successful durable save before acknowledging delivery to T3.
      await this.save();
      return;
    }
    (this.state.pendingSteering ??= []).push({ id, message });
    await this.save();
  }

  async flushSteering() {
    const pending = (this.state.pendingSteering ??= []).splice(0);
    if (!pending.length) return 0;
    this.state.messages.push(...pending.map((item) => item.message));
    (this.state.appliedSteering ??= []).push(...pending.map((item) => item.id));
    await this.save();
    return pending.length;
  }

  /** Close known-unexecuted calls after Stop as well as after process restart. */
  async settleUnexecuted() {
    const answered = new Set(
      this.state.messages
        .filter((message) => message.role === "tool")
        .map((message) => message.tool_call_id),
    );
    let changed = false;
    for (const call of this.state.messages.flatMap((message) => message.tool_calls ?? [])) {
      if (answered.has(call.id)) continue;
      const intent = Object.hasOwn(this.state.tools, call.id)
        ? this.state.tools[call.id]
        : undefined;
      if (intent?.status === "running" || intent?.status === "unknown") continue;
      const output =
        intent?.output ?? "Tool was cancelled before execution. No action was performed.";
      if (intent?.status === "prepared") {
        intent.status = "done";
        intent.output = output;
      }
      this.state.messages.push({ role: "tool", tool_call_id: call.id, content: output });
      answered.add(call.id);
      changed = true;
    }
    if (changed) await this.save();
  }

  async prepare(call: ApiToolCall, mutating: boolean) {
    const previous = Object.hasOwn(this.state.tools, call.id)
      ? this.state.tools[call.id]
      : undefined;
    if (previous) {
      if (JSON.stringify(previous.call) !== JSON.stringify(call))
        throw new Error("tool-identity-collision");
      if (previous.status !== "done") throw new Error("tool-delivery-unknown");
      return previous.output!;
    }
    if (this.uncertain) throw new Error("transcript-recovery-required");
    Object.defineProperty(this.state.tools, call.id, {
      value: { call, status: "prepared", mutating },
      enumerable: true,
      writable: true,
      configurable: true,
    });
    await this.save();
    return undefined;
  }

  async execute(call: ApiToolCall, mutating: boolean, run: () => Promise<string>) {
    const previous = await this.prepare(call, mutating);
    if (previous !== undefined) return previous;
    const intent = this.state.tools[call.id]!;
    intent.status = "running";
    await this.save();
    let output: string;
    try {
      output = await run();
    } catch (error) {
      if (!mutating) {
        intent.status = "done";
        intent.output = JSON.stringify({
          error: "read-tool-failed",
          code: (error as NodeJS.ErrnoException).code ?? "read-failed",
        });
        await this.save();
        return intent.output;
      }
      // A thrown transport/process error is not proof that a side effect did not happen.
      intent.status = "unknown";
      await this.save();
      throw error;
    }
    intent.status = "done";
    intent.output = output;
    await this.save();
    return output;
  }
}
