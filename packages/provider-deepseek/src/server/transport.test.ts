import { describe, expect, it } from "vite-plus/test";
import { complete, type CompletionInput } from "./transport.ts";

const frame = (delta: object, finish_reason: string | null = null) =>
  `data: ${JSON.stringify({ id: "req-1", model: "deepseek-flash", choices: [{ delta, finish_reason }] })}\r\n\r\n`;
const response = (text: string) =>
  new Response(
    new ReadableStream({
      start(controller) {
        const bytes = new TextEncoder().encode(text);
        for (let i = 0; i < bytes.length; i += 3) controller.enqueue(bytes.slice(i, i + 3));
        controller.close();
      },
    }),
    { headers: { "content-type": "text/event-stream" } },
  );
const input = (text: string): CompletionInput => ({
  apiKey: "fixture-secret",
  model: "deepseek-flash",
  effort: "max",
  messages: [],
  tools: [],
  signal: new AbortController().signal,
  fetch: async () => response(text),
});

describe("official DeepSeek streaming transport", () => {
  it("preserves fragmented UTF-8 reasoning and content separately", async () => {
    const result = await complete(
      input(
        frame({ reasoning_content: "Мысль" }) +
          frame({ content: "Ответ" }, "stop") +
          "data: [DONE]\n\n",
      ),
    );
    expect(result.message).toMatchObject({ content: "Ответ", reasoning_content: "Мысль" });
  });
  it("assembles tool arguments across chunks and preserves their IDs", async () => {
    const result = await complete(
      input(
        frame({
          tool_calls: [
            { index: 0, id: "call-a", function: { name: "read_file", arguments: '{"path":' } },
          ],
        }) +
          frame({ tool_calls: [{ index: 0, function: { arguments: '"a.txt"}' } }] }, "tool_calls") +
          "data: [DONE]\n\n",
      ),
    );
    expect(result.message.tool_calls?.[0]).toEqual({
      id: "call-a",
      type: "function",
      function: { name: "read_file", arguments: '{"path":"a.txt"}' },
    });
  });
  it("rejects a disconnected stream rather than accepting partial output", async () => {
    await expect(complete(input(frame({ content: "partial" })))).rejects.toThrow(
      "incomplete-response",
    );
  });
  it("rejects truncation even if the SSE stream terminates", async () => {
    await expect(
      complete(input(frame({ content: "partial" }, "length") + "data: [DONE]\n\n")),
    ).rejects.toThrow("incomplete-response-length");
  });
  it("does not expose an upstream error body or API key", async () => {
    const value = input("");
    value.fetch = async () => new Response("fixture-secret private prompt", { status: 401 });
    await expect(complete(value)).rejects.toThrow("HTTP 401");
    await expect(complete(value)).rejects.not.toThrow("fixture-secret");
  });
  it("honors cancellation before making a request", async () => {
    const controller = new AbortController();
    controller.abort();
    let called = false;
    const value = input("");
    value.signal = controller.signal;
    value.fetch = async () => {
      called = true;
      return response("");
    };
    await expect(complete(value)).rejects.toThrow();
    expect(called).toBe(false);
  });
  it.each(["none", "low", "high", "max"] as const)(
    "uses only the official endpoint and preserves effort %s",
    async (effort) => {
      const value = input("");
      value.effort = effort;
      let body: any;
      value.fetch = async (url, init) => {
        expect(url).toBe("https://api.deepseek.com/chat/completions");
        body = JSON.parse(String(init?.body));
        return response(frame({ content: "ok" }, "stop") + "data: [DONE]\n\n");
      };
      await complete(value);
      expect(body.reasoning_effort).toBe(effort === "none" ? undefined : effort);
      expect(body.thinking.type).toBe(effort === "none" ? "disabled" : "enabled");
    },
  );
  it("rejects an unexpected model instead of relabeling its output", async () => {
    const value = frame({ content: "wrong provider model" }, "stop").replace(
      '"deepseek-flash"',
      '"deepseek-v4-pro"',
    );
    await expect(complete(input(value + "data: [DONE]\n\n"))).rejects.toThrow(
      "model-identity-mismatch",
    );
  });
  it("does not accept an empty final response as task completion", async () => {
    await expect(complete(input(frame({}, "stop") + "data: [DONE]\n\n"))).rejects.toThrow(
      "empty-final-response",
    );
  });
});
