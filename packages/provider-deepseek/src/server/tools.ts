import * as HostProcess from "@t3tools/shared/HostProcess";
// @effect-diagnostics nodeBuiltinImport:off,globalTimers:off,globalFetch:off -- Native tool boundary uses AbortSignal and owns the exact spawned PID.
// @effect-diagnostics globalTimers:off -- Native child timeout is cancelled when that exact process exits.
// @effect-diagnostics globalFetch:off -- MCP HTTP lifetime follows the provider's AbortSignal.
import * as NodeCrypto from "node:crypto";
import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import type { NativeTool } from "./agent.ts";
import { sseData } from "./transport.ts";
import { runWindowsTool } from "./windowsHost.ts";

const MAX_OUTPUT = 128 * 1024;
const object = (value: unknown): Record<string, unknown> => {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw Error("expected-object");
  return value as Record<string, unknown>;
};
function argumentsFor(required: readonly string[], optional: readonly string[] = []) {
  return (value: unknown) => {
    const args = object(value);
    if (
      Object.keys(args).some((key) => ![...required, ...optional].includes(key)) ||
      required.some((key) => typeof args[key] !== "string" || !args[key])
    )
      throw Error("invalid-arguments");
    return args;
  };
}
const hash = (value: string | Buffer) =>
  NodeCrypto.createHash("sha256").update(value).digest("hex");
/**
 * Reports a failure that is proven to precede any write or child process, so the
 * transcript can close the call instead of quarantining a side effect that never began.
 */
const notExecuted = (error: string, detail: string, code?: string) =>
  JSON.stringify({ error, detail, notExecuted: true, ...(code ? { code } : {}) });
type PathResolution = { ok: true; path: string } | { ok: false; output: string };
const isWindows = HostProcess.Platform.defaultValue() === "win32";

export async function nativeTools(
  cwd: string,
  environment: NodeJS.ProcessEnv = process.env,
): Promise<Map<string, NativeTool>> {
  const processEnvironment = { ...environment };
  const root = await NodeFSP.realpath(cwd);
  const within = (path: string) => {
    const rel = NodePath.relative(root, path);
    return (
      rel !== ".." && !rel.startsWith(`..${isWindows ? "\\" : "/"}`) && !NodePath.isAbsolute(rel)
    );
  };
  // Resolution is read-only and runs before the Windows host starts, so every
  // failure here is pre-effect and can be reported instead of quarantined.
  const resolveTarget = async (input: string, create = false): Promise<PathResolution> => {
    const path = NodePath.resolve(root, input);
    if (!within(path))
      return {
        ok: false,
        output: notExecuted(
          "path-outside-workspace",
          `Path resolves outside the workspace root ${root}. Use a path inside the workspace.`,
        ),
      };
    let canonical: string;
    try {
      canonical = create
        ? NodePath.resolve(await NodeFSP.realpath(NodePath.dirname(path)), NodePath.basename(path))
        : await NodeFSP.realpath(path);
      if (create && !(await NodeFSP.lstat(NodePath.dirname(canonical))).isDirectory())
        return {
          ok: false,
          output: notExecuted(
            "parent-not-a-directory",
            "The parent of this path is not a directory. Nothing was written.",
          ),
        };
    } catch (cause) {
      const code = (cause as NodeJS.ErrnoException).code;
      if (create && code === "ENOENT")
        return {
          ok: false,
          output: notExecuted(
            "parent-directory-missing",
            "The parent directory does not exist. Create it explicitly with exec_command (New-Item -ItemType Directory on Windows, mkdir -p on /bin/sh), then retry. Nothing was written.",
            code,
          ),
        };
      if (create && code === "ENOTDIR")
        return {
          ok: false,
          output: notExecuted(
            "parent-not-a-directory",
            "The parent of this path is not a directory. Nothing was written.",
            code,
          ),
        };
      if (!create && code === "ENOENT")
        return {
          ok: false,
          output: notExecuted("path-not-found", "The path does not exist.", code),
        };
      return {
        ok: false,
        output: notExecuted(
          "path-preflight-failed",
          "The path could not be resolved before any action. Check it and retry.",
          code,
        ),
      };
    }
    if (!within(canonical))
      return {
        ok: false,
        output: notExecuted(
          "symlink-outside-workspace",
          "Path resolves through a symlink outside the workspace root. Nothing was written.",
        ),
      };
    return { ok: true, path: canonical };
  };
  const tools = new Map<string, NativeTool>();
  const add = (
    name: string,
    description: string,
    properties: Record<string, unknown>,
    required: string[],
    mutating: boolean,
    validate: NativeTool["validate"],
    execute: NativeTool["execute"],
  ) => {
    tools.set(name, {
      definition: {
        type: "function",
        function: {
          name,
          description,
          parameters: { type: "object", properties, required, additionalProperties: false },
        },
      },
      mutating,
      validate,
      execute,
    });
  };
  add(
    "read_file",
    "Read a UTF-8 workspace file. Returns SHA256 for conditional editing. Use offset/limit for large files.",
    {
      path: { type: "string" },
      offset: { type: "integer", minimum: 0 },
      limit: { type: "integer", minimum: 1, maximum: MAX_OUTPUT },
    },
    ["path"],
    false,
    (value) => {
      const a = argumentsFor(["path"], ["offset", "limit"])(value);
      for (const k of ["offset", "limit"])
        if (
          a[k] !== undefined &&
          (!Number.isSafeInteger(a[k]) || Number(a[k]) < (k === "offset" ? 0 : 1))
        )
          throw Error("invalid-range");
      if (Number(a.limit ?? MAX_OUTPUT) > MAX_OUTPUT) throw Error("limit-exceeded");
      return a;
    },
    async (a, signal) => {
      signal.throwIfAborted();
      const resolved = await resolveTarget(String(a.path));
      if (!resolved.ok) return resolved.output;
      const path = resolved.path,
        st = await NodeFSP.lstat(path);
      if (!st.isFile() || st.size > 16 * 1024 * 1024)
        return JSON.stringify({ error: "file-not-regular-or-too-large" });
      const data = await NodeFSP.readFile(path);
      signal.throwIfAborted();
      const offset = Number(a.offset ?? 0),
        limit = Number(a.limit ?? MAX_OUTPUT);
      return JSON.stringify({
        path,
        sha256: hash(data),
        size: data.length,
        offset,
        text: data.subarray(offset, offset + limit).toString("utf8"),
        truncated: offset + limit < data.length,
      });
    },
  );
  add(
    "list_files",
    "List one workspace directory, with at most 1000 entries.",
    { path: { type: "string" } },
    ["path"],
    false,
    argumentsFor(["path"]),
    async (a, signal) => {
      signal.throwIfAborted();
      const resolved = await resolveTarget(String(a.path));
      if (!resolved.ok) return resolved.output;
      const entries = await NodeFSP.readdir(resolved.path, { withFileTypes: true });
      return JSON.stringify({
        path: resolved.path,
        entries: entries
          .slice(0, 1000)
          .map((x) => ({ name: x.name, directory: x.isDirectory(), symlink: x.isSymbolicLink() })),
        truncated: entries.length > 1000,
      });
    },
  );
  add(
    "write_file",
    "Write a workspace file. Windows uses an exclusive OS handle and retains a durable preimage backup. Existing files require expectedSha256; new files require 'new'. Other hosts support creation only. Parent directory must exist.",
    { path: { type: "string" }, content: { type: "string" }, expectedSha256: { type: "string" } },
    ["path", "content", "expectedSha256"],
    true,
    (value) => {
      const a = argumentsFor(["path", "expectedSha256"], ["content"])(value);
      if (
        typeof a.content !== "string" ||
        a.content.length > 2 * 1024 * 1024 ||
        !/^(new|[a-f0-9]{64})$/.test(String(a.expectedSha256))
      )
        throw Error("invalid-write");
      return a;
    },
    async (a, signal) => {
      signal.throwIfAborted();
      const resolved = await resolveTarget(String(a.path), true);
      if (!resolved.ok) return resolved.output;
      const path = resolved.path;
      let exists = false;
      try {
        const stat = await NodeFSP.lstat(path);
        if (!stat.isFile() || stat.isSymbolicLink())
          return notExecuted(
            "target-not-regular",
            "The target exists but is not a regular file, or is a symlink. Nothing was written.",
          );
        exists = true;
      } catch (error) {
        const code = (error as NodeJS.ErrnoException).code;
        if (code !== "ENOENT")
          return notExecuted(
            "target-stat-failed",
            "The target could not be inspected before any write. Nothing was written.",
            code,
          );
      }
      if (isWindows)
        return runWindowsTool(
          { mode: "write", path, content: a.content, expectedSha256: a.expectedSha256 },
          signal,
          processEnvironment,
        );
      if (exists || a.expectedSha256 !== "new")
        return notExecuted(
          "exclusive-existing-write-not-supported-on-this-host",
          "This host only creates new files; another expectedSha256 is refused. Nothing was written.",
        );
      const temp = `${path}.${NodeCrypto.randomUUID()}.tmp`;
      const opened = await NodeFSP.open(temp, "wx", 0o600).catch(
        (error: NodeJS.ErrnoException) => error,
      );
      if (opened instanceof Error)
        return notExecuted(
          "temp-file-create-failed",
          "A temporary file could not be created; the target was not touched.",
          opened.code,
        );
      const fd = opened;
      try {
        await fd.writeFile(String(a.content));
        await fd.sync();
      } finally {
        await fd.close();
      }
      try {
        signal.throwIfAborted();
        await NodeFSP.link(temp, path);
        await NodeFSP.unlink(temp);
      } catch (error) {
        await NodeFSP.unlink(temp).catch(() => {});
        throw error;
      }
      return JSON.stringify({ path, sha256: hash(String(a.content)) });
    },
  );
  add(
    "exec_command",
    isWindows
      ? "Run a Windows PowerShell 5.1 command (powershell.exe -NoLogo -NoProfile -NonInteractive) in the workspace. Search text with rg and files with rg --files, scoped to explicitly named directories. If a path is unknown, list only its immediate parent. Never use broad Get-ChildItem -Recurse over work, .t3, the user profile or node_modules. Keep each command bounded and set timeoutMs explicitly for longer work. After an unknown stop outcome, inspect state before any new action. Write direct cmdlets and simple pipelines: Get-Content -Raw, New-Item -ItemType Directory, Test-Path. '&&' and '||' are not valid PowerShell 5.1 syntax. Do not start a nested powershell.exe with a double-quoted -Command. Native executable failures are not automatically propagated: this wrapper can report exitCode 0 even when the executable failed. Immediately after each native executable capture $taskNativeExit = $LASTEXITCODE; if ($taskNativeExit -ne 0) { exit $taskNativeExit } when a non-zero result means failure. Capture it before another executable runs. Do not infer native success from tool exitCode 0 without this explicit check. Requires Build mode and T3 permission. Output is bounded; timeout is at most 60 seconds."
      : "Run a /bin/sh -c command in the workspace. Requires Build mode and T3 permission. Output is bounded; timeout is at most 60 seconds.",
    { command: { type: "string" }, timeoutMs: { type: "integer", minimum: 1, maximum: 60000 } },
    ["command"],
    true,
    (value) => {
      const a = argumentsFor(["command"], ["timeoutMs"])(value);
      if (
        String(a.command).length > 32768 ||
        (a.timeoutMs !== undefined &&
          (!Number.isSafeInteger(a.timeoutMs) ||
            Number(a.timeoutMs) < 1 ||
            Number(a.timeoutMs) > 60000))
      )
        throw Error("invalid-command");
      return a;
    },
    async (a, signal) =>
      isWindows
        ? runWindowsTool(
            {
              mode: "exec",
              command: a.command,
              cwd: root,
              timeoutMs: Number(a.timeoutMs ?? 30000),
            },
            signal,
            processEnvironment,
          )
        : new Promise<string>((accept, reject) => {
            signal.throwIfAborted();
            const child = NodeChildProcess.spawn("/bin/sh", ["-c", String(a.command)], {
              cwd: root,
              env: processEnvironment,
              detached: true,
              stdio: ["ignore", "pipe", "pipe"],
            });
            let output = "",
              timedOut = false,
              stopped = false;
            const append = (data: Buffer) => {
              if (output.length < MAX_OUTPUT)
                output += data.toString("utf8").slice(0, MAX_OUTPUT - output.length);
            };
            child.stdout.on("data", append);
            child.stderr.on("data", append);
            const stop = () => {
              if (stopped || !child.pid) return;
              stopped = true;
              // PID is captured from this spawn, never found by a process-name pattern.
              try {
                process.kill(-child.pid, "SIGKILL");
              } catch {
                child.kill("SIGKILL");
              }
            };
            const timer = setTimeout(
              () => {
                timedOut = true;
                stop();
              },
              Number(a.timeoutMs ?? 30000),
            );
            signal.addEventListener("abort", stop, { once: true });
            const cleanup = () => {
              clearTimeout(timer);
              signal.removeEventListener("abort", stop);
            };
            child.on("error", (error) => {
              cleanup();
              reject(error);
            });
            child.on("close", (code, terminationSignal) => {
              cleanup();
              if (signal.aborted) reject(signal.reason ?? Error("aborted"));
              else
                accept(
                  JSON.stringify({
                    exitCode: code,
                    timedOut,
                    signal: terminationSignal,
                    output,
                    truncated: output.length >= MAX_OUTPUT,
                  }),
                );
            });
          }),
  );
  return tools;
}

/** Uses the same scoped MCP credential as other T3 providers. No new server/bridge. */
export class T3McpClient {
  private sessionId: string | null = null;
  private serial = 0;
  private protocolVersion = "2025-06-18";
  private readonly endpoint: string;
  private readonly authorization: string;
  private readonly requester: typeof globalThis.fetch;
  constructor(
    endpoint: string,
    authorization: string,
    requester: typeof globalThis.fetch = globalThis.fetch,
  ) {
    this.endpoint = endpoint;
    this.authorization = authorization;
    this.requester = requester;
    const url = new URL(endpoint);
    if (!["127.0.0.1", "localhost", "[::1]"].includes(url.hostname))
      throw Error("mcp-origin-not-loopback");
  }
  private async request(
    method: string,
    params: object,
    signal: AbortSignal,
    notification = false,
  ): Promise<any> {
    const id = ++this.serial;
    const response = await this.requester(this.endpoint, {
      method: "POST",
      redirect: "error",
      signal,
      headers: {
        Authorization: this.authorization,
        "Content-Type": "application/json",
        Accept: "application/json, text/event-stream",
        "MCP-Protocol-Version": this.protocolVersion,
        ...(this.sessionId ? { "Mcp-Session-Id": this.sessionId } : {}),
      },
      body: JSON.stringify({
        jsonrpc: "2.0",
        ...(notification ? {} : { id }),
        method,
        ...(notification ? {} : { params }),
      }),
    });
    if (!response.ok) {
      // Only initialization/list failures can contribute protocol diagnostics: tool arguments/results never do.
      let detail = "";
      if (method !== "tools/call") {
        const body = await response.text();
        try {
          const parsed = JSON.parse(body);
          const error = parsed.error;
          if (error && typeof error.code === "number") detail = `-rpc-${error.code}`;
          if (typeof error?.message === "string")
            detail += `-${error.message.slice(0, 180).replace(/[^a-zA-Z0-9 .,():_-]/g, "?")}`;
          if (typeof error === "string" && /^[a-zA-Z0-9_-]{1,100}$/.test(error))
            detail += `-${error}`;
          if (typeof parsed.message === "string")
            detail += `-${parsed.message.slice(0, 180).replace(/[^a-zA-Z0-9 .,():_-]/g, "?")}`;
        } catch {
          /* Do not echo unstructured upstream response bodies. */
        }
      } else await response.body?.cancel();
      throw Error(`t3-mcp-${method.replaceAll("/", "-")}-http-${response.status}${detail}`);
    }
    this.sessionId = response.headers.get("mcp-session-id") ?? this.sessionId;
    if (notification || response.status === 202) {
      await response.body?.cancel();
      return {};
    }
    let message: any;
    if (response.headers.get("content-type")?.includes("text/event-stream")) {
      if (!response.body) throw Error("t3-mcp-empty-stream");
      for await (const data of sseData(response.body, signal)) {
        const frame = JSON.parse(data);
        if (frame.id === id) {
          message = frame;
          break;
        }
      }
    } else message = await response.json();
    if (!message || message.id !== id || message.error) throw Error("t3-mcp-request-failed");
    return message.result;
  }
  async tools(signal: AbortSignal) {
    const initialized = await this.request(
      "initialize",
      {
        protocolVersion: this.protocolVersion,
        capabilities: {},
        clientInfo: { name: "t3-deepseek-api", version: "1" },
      },
      signal,
    );
    if (
      typeof initialized?.protocolVersion !== "string" ||
      !/^\d{4}-\d{2}-\d{2}$/.test(initialized.protocolVersion)
    )
      throw Error("invalid-mcp-protocol-negotiation");
    this.protocolVersion = initialized.protocolVersion;
    await this.request("notifications/initialized", {}, signal, true);
    const tools = new Map<string, NativeTool>();
    let cursor: string | undefined;
    const seen = new Set<string>();
    do {
      const result = await this.request("tools/list", cursor ? { cursor } : {}, signal);
      for (const item of result.tools ?? []) {
        if (
          typeof item.name !== "string" ||
          !/^[a-zA-Z0-9_-]{1,64}$/.test(item.name) ||
          tools.has(item.name)
        )
          throw Error("invalid-mcp-tool-catalog");
        const name = item.name;
        tools.set(name, {
          definition: {
            type: "function",
            function: {
              name,
              description: String(item.description ?? name),
              parameters: item.inputSchema,
            },
          },
          // The T3 MCP server enforces the fine-grained native access policy.
          mutating: item.annotations?.readOnlyHint !== true,
          validate: object,
          execute: async (args, callSignal) =>
            JSON.stringify(await this.request("tools/call", { name, arguments: args }, callSignal)),
        });
      }
      cursor = result.nextCursor;
      if ((cursor && seen.has(cursor)) || tools.size > 1000) throw Error("invalid-mcp-pagination");
      if (cursor) seen.add(cursor);
    } while (cursor);
    return tools;
  }
  async close() {
    if (!this.sessionId) return;
    const response = await this.requester(this.endpoint, {
      method: "DELETE",
      redirect: "error",
      signal: AbortSignal.timeout(1000),
      headers: {
        Authorization: this.authorization,
        "Mcp-Session-Id": this.sessionId,
        "MCP-Protocol-Version": this.protocolVersion,
      },
    });
    await response.body?.cancel();
    this.sessionId = null;
    if (![200, 202, 204, 404, 405].includes(response.status))
      throw Error(`t3-mcp-close-http-${response.status}`);
  }
}
