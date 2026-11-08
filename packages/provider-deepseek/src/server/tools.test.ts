import * as HostProcess from "@t3tools/shared/HostProcess";
// @effect-diagnostics nodeBuiltinImport:off -- Disposable filesystem fixtures test the native tool path preflight.
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeProcess from "node:process";
import { afterEach, describe, expect, it } from "vite-plus/test";
import { nativeTools, T3McpClient } from "./tools.ts";

const roots: string[] = [];
afterEach(async () => {
  await Promise.all(
    roots.splice(0).map((root) => NodeFSP.rm(root, { recursive: true, force: true })),
  );
});
async function workspace() {
  const root = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-native-tools-"));
  roots.push(root);
  return root;
}
async function run(root: string, name: string, args: Record<string, unknown>) {
  const tools = await nativeTools(root);
  return JSON.parse(await tools.get(name)!.execute(args, new AbortController().signal)) as Record<
    string,
    unknown
  >;
}

describe("native MCP client handshake", () => {
  it("keeps command environments separate without changing the parent environment", async () => {
    const root = await workspace();
    const before = NodeProcess.env.T3_DS_ENV_FIXTURE;
    for (const value of ["instance-a", "instance-b"]) {
      const tools = await nativeTools(root, { ...NodeProcess.env, T3_DS_ENV_FIXTURE: value });
      const result = JSON.parse(
        await tools
          .get("exec_command")!
          .execute(
            {
              command:
                HostProcess.Platform.defaultValue() === "win32"
                  ? "Write-Output $env:T3_DS_ENV_FIXTURE"
                  : "printf '%s' \"$T3_DS_ENV_FIXTURE\"",
              timeoutMs: 10000,
            },
            new AbortController().signal,
          ),
      );
      expect(result.exitCode).toBe(0);
      expect(result.output.trim()).toBe(value);
    }
    expect(NodeProcess.env.T3_DS_ENV_FIXTURE).toBe(before);
  });
  it("uses the negotiated protocol and session header on subsequent requests", async () => {
    const requests: { body: any; headers: Headers }[] = [];
    const client = new T3McpClient(
      "http://127.0.0.1:1234/mcp",
      "Bearer fixture-only",
      async (_url, options) => {
        const body = JSON.parse(String(options?.body)),
          headers = new Headers(options?.headers);
        requests.push({ body, headers });
        if (body.method === "initialize")
          return Response.json(
            { jsonrpc: "2.0", id: body.id, result: { protocolVersion: "2025-11-25" } },
            { headers: { "mcp-session-id": "fixture-session" } },
          );
        expect(headers.get("mcp-protocol-version")).toBe("2025-11-25");
        expect(headers.get("mcp-session-id")).toBe("fixture-session");
        if (body.method === "notifications/initialized") {
          expect(body.params).toBeUndefined();
          return new Response(null, { status: 202 });
        }
        return Response.json({ jsonrpc: "2.0", id: body.id, result: { tools: [] } });
      },
    );
    expect((await client.tools(new AbortController().signal)).size).toBe(0);
    expect(requests.map((request) => request.body.method)).toEqual([
      "initialize",
      "notifications/initialized",
      "tools/list",
    ]);
  });
  it("does not echo a tool error body into a thrown exception", async () => {
    let count = 0;
    const client = new T3McpClient(
      "http://127.0.0.1:1234/mcp",
      "Bearer fixture-only",
      async (_url, options) => {
        const body = JSON.parse(String(options?.body));
        count++;
        if (body.method === "initialize")
          return Response.json({ id: body.id, result: { protocolVersion: "2025-06-18" } });
        if (body.method === "notifications/initialized") return new Response(null, { status: 202 });
        if (body.method === "tools/list")
          return Response.json({
            id: body.id,
            result: { tools: [{ name: "fixture_tool", inputSchema: { type: "object" } }] },
          });
        return new Response("private user data", { status: 500 });
      },
    );
    const tools = await client.tools(new AbortController().signal);
    await expect(
      tools.get("fixture_tool")!.execute({}, new AbortController().signal),
    ).rejects.toThrow("t3-mcp-tools-call-http-500");
    expect(count).toBe(4);
  });
});

describe("native tool path preflight", () => {
  it("reports a missing parent as a closed pre-effect rejection and creates nothing", async () => {
    const root = await workspace();
    const result = await run(root, "write_file", {
      path: "android-origin-diagnose/probe-1.ps1",
      content: "Write-Output 1",
      expectedSha256: "new",
    });
    expect(result).toMatchObject({
      error: "parent-directory-missing",
      notExecuted: true,
      code: "ENOENT",
    });
    await expect(
      NodeFSP.stat(NodePath.join(root, "android-origin-diagnose")),
    ).rejects.toMatchObject({
      code: "ENOENT",
    });
  });
  it("refuses a path outside the workspace before any host write", async () => {
    const root = await workspace(),
      outside = `../escape-${NodePath.basename(root)}.txt`;
    const result = await run(root, "write_file", {
      path: outside,
      content: "x",
      expectedSha256: "new",
    });
    expect(result).toMatchObject({ error: "path-outside-workspace", notExecuted: true });
    await expect(NodeFSP.stat(NodePath.join(root, outside))).rejects.toMatchObject({
      code: "ENOENT",
    });
  });
  it("still writes a new file when the parent exists", async () => {
    const root = await workspace();
    const result = await run(root, "write_file", {
      path: "probe-1.ps1",
      content: "Write-Output 1",
      expectedSha256: "new",
    });
    expect(result).toMatchObject({
      path: await NodeFSP.realpath(NodePath.join(root, "probe-1.ps1")),
    });
    expect(result.error).toBeUndefined();
    expect(await NodeFSP.readFile(NodePath.join(root, "probe-1.ps1"), "utf8")).toBe(
      "Write-Output 1",
    );
  });
  it("reports a missing read target and still reads and lists an existing one", async () => {
    const root = await workspace();
    expect(await run(root, "read_file", { path: "absent.txt" })).toMatchObject({
      error: "path-not-found",
      notExecuted: true,
      code: "ENOENT",
    });
    expect(await run(root, "list_files", { path: "absent" })).toMatchObject({
      error: "path-not-found",
      notExecuted: true,
    });
    await NodeFSP.writeFile(NodePath.join(root, "sample.txt"), "hello");
    expect(await run(root, "read_file", { path: "sample.txt" })).toMatchObject({
      path: await NodeFSP.realpath(NodePath.join(root, "sample.txt")),
      text: "hello",
      size: 5,
    });
    expect(await run(root, "list_files", { path: "." })).toMatchObject({
      entries: [{ name: "sample.txt", directory: false, symlink: false }],
    });
  });
});

describe("native Windows shell exit handling", () => {
  it.skipIf(HostProcess.Platform.defaultValue() !== "win32")(
    "requires explicit propagation of a native program failure",
    async () => {
      const root = await workspace();
      const executable = NodeProcess.execPath.replaceAll("'", "''");
      const command = `& '${executable}' -e 'process.exit(7)'`;
      const unpropagated = await run(root, "exec_command", { command });
      expect(unpropagated.exitCode).toBe(0);
      const propagated = await run(root, "exec_command", {
        command: `${command}\n$taskNativeExit = $LASTEXITCODE\nif ($taskNativeExit -ne 0) { exit $taskNativeExit }`,
      });
      expect(propagated.exitCode).toBe(7);
    },
  );
});
