// @effect-diagnostics nodeBuiltinImport:off -- Disposable filesystem fixtures test the durable native journal.
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { afterEach, describe, expect, it } from "vite-plus/test";
import { runAgent } from "./agent.ts";
import { Journal } from "./journal.ts";
import type { NativeTool } from "./agent.ts";

const roots: string[] = [];
afterEach(async () => {
  await Promise.all(roots.splice(0).map((root) => NodeFSP.rm(root, { recursive: true, force: true })));
});

async function journal() {
  const root = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-deepseek-"));
  roots.push(root);
  return Journal.load(root, "thread-a");
}

const tool: NativeTool = {
  definition: {
    type: "function",
    function: { name: "read_file", description: "fixture", parameters: { type: "object" } },
  },
  mutating: false,
  validate: (value) => (value && typeof value === "object" ? (value as Record<string, unknown>) : {}),
  execute: async () => "file contents",
};

function resetResponse() {
  const encoder = new TextEncoder();
  return new Response(
    new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(
          encoder.encode(
            'data: {"id":"req","model":"deepseek-flash","choices":[{"delta":{"content":"partial"}}]}\n\n',
          ),
        );
        controller.error(Object.assign(new Error("socket reset"), { code: "UND_ERR_SOCKET" }));
      },
    }),
  );
}

describe("DeepSeek SSE body recovery", () => {
  it("does not replay tools after a persistent SSE body reset", async () => {
    const state = await journal();
    let requests = 0;
    let executed = 0;
    await expect(
      runAgent({
        apiKey: "fixture",
        model: "deepseek-flash",
        effort: "max",
        signal: new AbortController().signal,
        journal: state,
        prompt: "read",
        instructions: "fixture",
        nativeTools: new Map([
          ["read_file", { ...tool, execute: async () => (executed++, "file contents") }],
        ]),
        authorize: async () => true,
        fetch: async () => {
          requests++;
          return resetResponse();
        },
      }),
    ).rejects.toThrow(/transport-stream-read:UND_ERR_SOCKET/);
    expect(requests).toBe(2);
    expect(executed).toBe(0);
    expect(state.state.messages.map((message) => message.role)).toEqual(["system", "user"]);
    expect(Object.keys(state.state.tools)).toHaveLength(0);
  });
});