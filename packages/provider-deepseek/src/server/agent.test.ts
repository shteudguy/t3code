// @effect-diagnostics nodeBuiltinImport:off -- Disposable filesystem fixtures test the durable native journal.
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { afterEach, describe, expect, it, vi } from "vite-plus/test";
import { runAgent, type AgentInput, type NativeTool } from "./agent.ts";
import { Journal } from "./journal.ts";
import { nativeTools } from "./tools.ts";
import type { ApiMessage } from "./transport.ts";

const roots: string[] = [];
afterEach(async () => {
  vi.restoreAllMocks();
  await Promise.all(
    roots.splice(0).map((root) => NodeFSP.rm(root, { recursive: true, force: true })),
  );
});
async function journal() {
  const root = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-deepseek-"));
  roots.push(root);
  return Journal.load(root, "thread-a");
}
const call = {
  id: "call-1",
  type: "function" as const,
  function: { name: "read_file", arguments: '{"path":"sample"}' },
};
const tool: NativeTool = {
  definition: {
    type: "function",
    function: { name: "read_file", description: "fixture", parameters: { type: "object" } },
  },
  mutating: false,
  validate: (value) => {
    if (!value || typeof value !== "object") throw Error();
    return value as Record<string, unknown>;
  },
  execute: async () => "file contents",
};
const stream = (message: object, finish: string) =>
  new Response(
    `data: ${JSON.stringify({ id: "req", model: "deepseek-flash", choices: [{ delta: message, finish_reason: finish }] })}\n\ndata: [DONE]\n\n`,
  );

describe("direct DeepSeek durable agent loop", () => {
  it("reports timed out, signalled, and unknown exit results as failed tool calls", async () => {
    for (const result of [
      { exitCode: null, timedOut: true, signal: "SIGKILL" },
      { exitCode: 0, timedOut: true },
      { exitCode: 0, signal: "SIGTERM" },
      { exitCode: null, timedOut: false },
    ]) {
      const state = await journal();
      let requests = 0;
      const states: string[] = [];
      await runAgent({
        apiKey: "fixture",
        model: "deepseek-flash",
        effort: "max",
        signal: new AbortController().signal,
        journal: state,
        prompt: "fixture",
        instructions: "fixture",
        nativeTools: new Map([
          ["read_file", { ...tool, execute: async () => JSON.stringify(result) }],
        ]),
        authorize: async () => true,
        onTool: (_call, status) => states.push(status),
        fetch: async () =>
          ++requests === 1
            ? stream({ tool_calls: [{ index: 0, ...call }] }, "tool_calls")
            : stream({ content: "handled failure" }, "stop"),
      });
      expect(states).toEqual(["running", "failed"]);
    }
  });
  it("does not acknowledge retried steering from memory after a failed save", async () => {
    const state = await journal();
    await state.save();
    const save = vi
      .spyOn(state, "save")
      .mockRejectedValueOnce(Object.assign(Error("fixture EIO"), { code: "EIO" }));
    const message = { role: "user" as const, content: "must survive restart" };
    await expect(state.enqueueSteering("s1", message)).rejects.toThrow("fixture EIO");
    await state.enqueueSteering("s1", message);
    expect(save).toHaveBeenCalledTimes(2);
    expect(JSON.parse(await NodeFSP.readFile(state.file, "utf8")).pendingSteering).toEqual([
      { id: "s1", message },
    ]);
    const restored = await Journal.load(roots.at(-1)!, "thread-a");
    expect(await restored.flushSteering()).toBe(1);
    expect(restored.state.messages.at(-1)).toEqual(message);
  });
  it("returns native tool results to the API while retaining reasoning", async () => {
    const state = await journal();
    const requests: any[] = [];
    const input: AgentInput = {
      apiKey: "fixture",
      model: "deepseek-flash",
      effort: "max",
      signal: new AbortController().signal,
      journal: state,
      prompt: "read",
      instructions: "fixture",
      nativeTools: new Map([["read_file", tool]]),
      authorize: async () => true,
      fetch: async (_url, init) => {
        requests.push(JSON.parse(String(init?.body)));
        return requests.length === 1
          ? stream(
              { reasoning_content: "provider reasoning", tool_calls: [{ index: 0, ...call }] },
              "tool_calls",
            )
          : stream({ content: "done" }, "stop");
      },
    };
    await runAgent(input);
    expect(requests).toHaveLength(2);
    expect(requests[1].messages).toContainEqual({
      role: "assistant",
      content: "",
      reasoning_content: "provider reasoning",
      tool_calls: [call],
    });
    expect(requests[1].messages).toContainEqual({
      role: "tool",
      tool_call_id: "call-1",
      content: "file contents",
    });
  });
  it("does not execute a denied tool", async () => {
    const state = await journal();
    let executed = 0;
    let requests = 0;
    await runAgent({
      apiKey: "fixture",
      model: "deepseek-flash",
      effort: "none",
      signal: new AbortController().signal,
      journal: state,
      prompt: "read",
      instructions: "fixture",
      nativeTools: new Map([
        [
          "read_file",
          {
            ...tool,
            execute: async () => {
              executed++;
              return "bad";
            },
          },
        ],
      ]),
      authorize: async () => false,
      fetch: async () =>
        ++requests === 1
          ? stream({ tool_calls: [{ index: 0, ...call }] }, "tool_calls")
          : stream({ content: "denied" }, "stop"),
    });
    expect(executed).toBe(0);
  });
  it("does not repeat a tool whose result was durably recorded", async () => {
    const state = await journal();
    let executed = 0;
    expect(
      await state.execute(call, true, async () => {
        executed++;
        return "ok";
      }),
    ).toBe("ok");
    expect(
      await state.execute(call, true, async () => {
        executed++;
        return "bad";
      }),
    ).toBe("ok");
    expect(executed).toBe(1);
  });
  it("quarantines an in-flight action after restart and never replays it", async () => {
    const state = await journal();
    await state.prepare(call, true);
    state.state.tools[call.id]!.status = "running";
    await state.save();
    const root = roots[roots.length - 1]!;
    const restored = await Journal.load(root, "thread-a");
    expect(restored.uncertain).toBe(true);
    await expect(restored.execute(call, true, async () => "must not run")).rejects.toThrow(
      "tool-delivery-unknown",
    );
  });
  it("keeps uncertainty visible when a side effect throws", async () => {
    const state = await journal();
    await expect(
      state.execute(call, true, async () => {
        throw Error("connection lost");
      }),
    ).rejects.toThrow("connection lost");
    expect(state.uncertain).toBe(true);
  });
  it("restores a committed result missing from the conversation without executing it", async () => {
    const state = await journal();
    state.state.messages.push({ role: "assistant", content: "", tool_calls: [call] });
    await state.execute(call, true, async () => "committed-result");
    const restored = await Journal.load(roots[roots.length - 1]!, "thread-a");
    expect(restored.uncertain).toBe(false);
    expect(restored.state.messages.at(-1)).toEqual({
      role: "tool",
      tool_call_id: call.id,
      content: "committed-result",
    });
  });
  it("does not confuse an upstream tool ID with an object prototype", async () => {
    const state = await journal();
    const unusual = { ...call, id: "__proto__" };
    expect(await state.execute(unusual, false, async () => "safe")).toBe("safe");
    expect(Object.hasOwn(state.state.tools, "__proto__")).toBe(true);
  });
  it("replaces stale system context on the next turn", async () => {
    const state = await journal();
    const systems: string[] = [];
    const base = {
      apiKey: "fixture",
      model: "deepseek-flash",
      effort: "none" as const,
      signal: new AbortController().signal,
      journal: state,
      prompt: "hello",
      nativeTools: new Map<string, NativeTool>(),
      authorize: async () => false,
      fetch: async (_url: unknown, init: RequestInit | undefined) => {
        systems.push(JSON.parse(String(init?.body)).messages[0].content);
        return stream({ content: "ok" }, "stop");
      },
    };
    await runAgent({ ...base, instructions: "mode=plan" });
    await runAgent({ ...base, instructions: "mode=build NEW-CONTEXT" });
    expect(systems).toEqual(["mode=plan", "mode=build NEW-CONTEXT"]);
  });
  it("closes unexecuted tool calls after Stop before reusing the same journal", async () => {
    const state = await journal();
    const controller = new AbortController();
    const base = {
      apiKey: "fixture",
      model: "deepseek-flash",
      effort: "none" as const,
      journal: state,
      prompt: "hello",
      instructions: "fixture",
      nativeTools: new Map([["read_file", tool]]),
      authorize: async () => true,
    };
    await expect(
      runAgent({
        ...base,
        signal: controller.signal,
        onRequest: () => controller.abort(),
        fetch: async () => stream({ tool_calls: [{ index: 0, ...call }] }, "tool_calls"),
      }),
    ).rejects.toThrow();
    let body: any;
    await runAgent({
      ...base,
      signal: new AbortController().signal,
      fetch: async (_url, init) => {
        body = JSON.parse(String(init?.body));
        return stream({ content: "resumed" }, "stop");
      },
    });
    expect(
      body.messages.some(
        (message: ApiMessage) => message.role === "tool" && message.tool_call_id === call.id,
      ),
    ).toBe(true);
    expect(state.uncertain).toBe(false);
  });
  it("returns a read error as a tool result rather than quarantining the conversation", async () => {
    const state = await journal();
    const output = await state.execute(call, false, async () => {
      throw Object.assign(Error("missing"), { code: "ENOENT" });
    });
    expect(JSON.parse(output).code).toBe("ENOENT");
    expect(state.uncertain).toBe(false);
  });
  it("delivers accepted late steering even when the current request returns a final answer", async () => {
    const state = await journal();
    let release!: () => void, started!: () => void;
    const waiting = new Promise<void>((resolve) => {
      started = resolve;
    });
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const bodies: any[] = [];
    const result = runAgent({
      apiKey: "fixture",
      model: "deepseek-flash",
      effort: "none",
      signal: new AbortController().signal,
      journal: state,
      prompt: "first",
      instructions: "fixture",
      nativeTools: new Map(),
      authorize: async () => false,
      fetch: async (_url, init) => {
        bodies.push(JSON.parse(String(init?.body)));
        if (bodies.length === 1) {
          started();
          await gate;
        }
        return stream({ content: "ok" }, "stop");
      },
    });
    await waiting;
    await state.enqueueSteering("steer-1", { role: "user", content: "late correction" });
    release();
    await result;
    expect(bodies).toHaveLength(2);
    expect(bodies[1].messages.at(-1)).toEqual({ role: "user", content: "late correction" });
    expect(state.hasSteering).toBe(false);
  });

  it("closes a proven pre-effect write rejection and continues to the next model turn", async () => {
    const workspace = await NodeFSP.mkdtemp(
      NodePath.join(NodeOS.tmpdir(), "t3-deepseek-workspace-"),
    );
    roots.push(workspace);
    const state = await journal();
    const requests: any[] = [];
    const seen: string[] = [];
    const write = {
      id: "call-write",
      type: "function" as const,
      function: {
        name: "write_file",
        arguments: JSON.stringify({
          path: "android-origin-diagnose/probe-1.ps1",
          content: "Write-Output 1",
          expectedSha256: "new",
        }),
      },
    };
    await runAgent({
      apiKey: "fixture",
      model: "deepseek-flash",
      effort: "none",
      signal: new AbortController().signal,
      journal: state,
      prompt: "write",
      instructions: "fixture",
      nativeTools: await nativeTools(workspace),
      authorize: async () => true,
      onTool: (_call, status, output) => {
        seen.push(
          output
            ? `${status}:${String((JSON.parse(output) as { error?: string }).error ?? "ok")}`
            : status,
        );
      },
      fetch: async (_url, init) => {
        requests.push(JSON.parse(String(init?.body)));
        return requests.length === 1
          ? stream({ tool_calls: [{ index: 0, ...write }] }, "tool_calls")
          : stream({ content: "done" }, "stop");
      },
    });
    expect(requests).toHaveLength(2);
    const result = requests[1].messages.find(
      (message: ApiMessage) => message.role === "tool" && message.tool_call_id === "call-write",
    );
    expect(JSON.parse(String(result.content))).toMatchObject({
      error: "parent-directory-missing",
      notExecuted: true,
    });
    expect(seen).toEqual(["running", "failed:parent-directory-missing"]);
    expect(state.uncertain).toBe(false);
    expect(state.state.tools["call-write"]).toMatchObject({ status: "done", mutating: true });
    await expect(
      NodeFSP.stat(NodePath.join(workspace, "android-origin-diagnose")),
    ).rejects.toMatchObject({
      code: "ENOENT",
    });
    expect((await Journal.load(NodePath.dirname(state.file), "thread-a")).uncertain).toBe(false);
  });
  it("closes invalid tool arguments as a known no-effect result", async () => {
    const workspace = await NodeFSP.mkdtemp(
      NodePath.join(NodeOS.tmpdir(), "t3-deepseek-workspace-"),
    );
    roots.push(workspace);
    const state = await journal();
    const requests: any[] = [];
    const callA = {
      id: "call-json",
      type: "function" as const,
      function: { name: "write_file", arguments: '{"path":' },
    };
    const callB = {
      id: "call-hash",
      type: "function" as const,
      function: { name: "write_file", arguments: '{"path":"a.txt","content":"x"}' },
    };
    await runAgent({
      apiKey: "fixture",
      model: "deepseek-flash",
      effort: "none",
      signal: new AbortController().signal,
      journal: state,
      prompt: "write",
      instructions: "fixture",
      nativeTools: await nativeTools(workspace),
      authorize: async () => true,
      fetch: async (_url, init) => {
        requests.push(JSON.parse(String(init?.body)));
        return requests.length === 1
          ? stream(
              {
                tool_calls: [
                  { index: 0, ...callA },
                  { index: 1, ...callB },
                ],
              },
              "tool_calls",
            )
          : stream({ content: "done" }, "stop");
      },
    });
    expect(requests).toHaveLength(2);
    const messages: ApiMessage[] = requests[1].messages;
    const results = messages
      .filter((message) => message.role === "tool")
      .map(
        (message) =>
          JSON.parse(String(message.content)) as {
            error?: string;
            code?: string;
            notExecuted?: boolean;
          },
      );
    expect(results.map((value) => value.error)).toEqual([
      "invalid-tool-arguments",
      "invalid-tool-arguments",
    ]);
    expect(results.map((value) => value.notExecuted)).toEqual([true, true]);
    expect(results[0]!.code).toBeUndefined();
    expect(results[1]!.code).toBe("invalid-arguments");
    await expect(NodeFSP.stat(NodePath.join(workspace, "a.txt"))).rejects.toMatchObject({
      code: "ENOENT",
    });
    expect(state.uncertain).toBe(false);
  });
  it("keeps a mutating action unknown even when the thrown error claims it was not executed", async () => {
    const state = await journal();
    const error = Object.assign(Error("mcp transport lost"), {
      notExecuted: true,
      code: "ECONNRESET",
    });
    await expect(
      state.execute(call, true, async () => {
        throw error;
      }),
    ).rejects.toThrow("mcp transport lost");
    expect(state.uncertain).toBe(true);
    await expect(state.execute(call, true, async () => "must not run")).rejects.toThrow(
      "tool-delivery-unknown",
    );
  });
});
