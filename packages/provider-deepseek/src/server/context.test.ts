import { describe, expect, it } from "vite-plus/test";
import { prepareContextWindow } from "./context.ts";
import type { ApiMessage, ApiTool } from "./transport.ts";

const tool: ApiTool = {
  type: "function",
  function: { name: "read_file", description: "fixture", parameters: { type: "object" } },
};

describe("prepareContextWindow", () => {
  it("returns a copy without compaction when the request fits", () => {
    const messages: ApiMessage[] = [
      { role: "system", content: "instructions" },
      { role: "user", content: "hello" },
    ];
    const result = prepareContextWindow(messages, [tool], { path: "/tmp/journal.json", revision: 3 });
    expect(result.compacted).toBe(false);
    expect(result.messages).toEqual(messages);
    expect(result.messages).not.toBe(messages);
    expect(result.metadata).toBeUndefined();
  });

  it("compacts oversized history while retaining anchors and metadata", () => {
    const messages: ApiMessage[] = [
      { role: "system", content: "system instruction" },
      { role: "user", content: "first user" },
      {
        role: "assistant",
        content: "",
        tool_calls: [{ id: "call-1", type: "function", function: { name: "read_file", arguments: "{}" } }],
      },
      { role: "tool", content: "x".repeat(100_000), tool_call_id: "call-1" },
      { role: "assistant", content: "y".repeat(700_000) },
      { role: "user", content: "latest user" },
    ];
    const result = prepareContextWindow(messages, [tool], { path: "/tmp/journal.json", revision: 3 });
    expect(result.compacted).toBe(true);
    expect(result.messages.map((message) => message.role)).toEqual(["system", "system", "user", "assistant", "user"]);
    expect(result.messages[1]?.content).toContain("/tmp/journal.json");
    expect(result.metadata).toMatchObject({
      sourceMessageCount: 6,
      retainedMessageCount: 4,
      omittedMessageCount: 2,
      archivePath: "/tmp/journal.json",
      archiveRevision: 3,
    });
    expect(result.metadata!.bytesAfter).toBeLessThanOrEqual(750_000);
  });

  it("rejects tool history that cannot be grouped atomically", () => {
    const messages: ApiMessage[] = [
      { role: "system", content: "instructions" },
      { role: "tool", content: "orphan", tool_call_id: "missing" },
      { role: "user", content: "latest" },
    ];
    expect(() => prepareContextWindow(messages, [tool], { path: "/tmp/journal.json", revision: 1 }, 1)).toThrow(
      "context-tool-history-invalid",
    );
  });
});
