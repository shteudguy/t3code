import * as HostProcess from "@t3tools/shared/HostProcess";
// @effect-diagnostics nodeBuiltinImport:off -- Disposable real Windows OS boundary tests.
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import { afterEach, describe, expect, it } from "vite-plus/test";
import { runWindowsTool, windowsHostSource } from "./windowsHost.ts";

const roots: string[] = [];
afterEach(async () => {
  await Promise.all(
    roots.splice(0).map((root) => NodeFSP.rm(root, { recursive: true, force: true })),
  );
});
async function workspace() {
  const root = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-native-host-"));
  roots.push(root);
  return root;
}
const sha = (text: string) => NodeCrypto.createHash("sha256").update(text).digest("hex");
function marker(root: string, name: string) {
  return new Promise<void>((accept, reject) => {
    const observer = NodeFS.watch(root, (_event, file) => {
      if (String(file) === name) {
        observer.close();
        accept();
      }
    });
    observer.on("error", reject);
  });
}

describe.skipIf(HostProcess.Platform.defaultValue() !== "win32")("native Windows tool host", () => {
  it("fits the static Windows command-line limit without model content in argv", () => {
    expect(Buffer.from(windowsHostSource, "utf16le").toString("base64").length).toBeLessThan(31000);
  });
  it("conditionally edits under an exclusive handle and retains the complete original", async () => {
    const root = await workspace(),
      path = NodePath.join(root, "sample.txt");
    await NodeFSP.writeFile(path, "original");
    const result = JSON.parse(
      await runWindowsTool(
        { mode: "write", path, content: "изменено", expectedSha256: sha("original") },
        new AbortController().signal,
      ),
    );
    expect(result.error).toBeUndefined();
    expect(await NodeFSP.readFile(path, "utf8")).toBe("изменено");
    expect(await NodeFSP.readFile(result.backupPath, "utf8")).toBe("original");
  });
  it("does not replace a changed file or an existing new-file target", async () => {
    const root = await workspace(),
      path = NodePath.join(root, "sample.txt");
    await NodeFSP.writeFile(path, "concurrent-user-edit");
    const result = JSON.parse(
      await runWindowsTool(
        { mode: "write", path, content: "must not win", expectedSha256: sha("old") },
        new AbortController().signal,
      ),
    );
    expect(result.error).toBe("file-changed");
    expect(await NodeFSP.readFile(path, "utf8")).toBe("concurrent-user-edit");
    const creation = JSON.parse(
      await runWindowsTool(
        { mode: "write", path, content: "must not win", expectedSha256: "new" },
        new AbortController().signal,
      ),
    );
    expect(creation.error).toBeTruthy();
    expect(await NodeFSP.readFile(path, "utf8")).toBe("concurrent-user-edit");
  });
  it("creates a backup with the protected source DACL instead of directory permissions", async () => {
    const root = await workspace(),
      path = NodePath.join(root, "private.txt");
    await NodeFSP.writeFile(path, "private-original");
    const setup = JSON.parse(
      await runWindowsTool(
        {
          mode: "exec",
          cwd: root,
          timeoutMs: 10000,
          command:
            "$everyone=New-Object Security.Principal.SecurityIdentifier('S-1-1-0');$sid=[Security.Principal.WindowsIdentity]::GetCurrent().User;" +
            "$dir=Get-Acl '.';$dir.AddAccessRule((New-Object Security.AccessControl.FileSystemAccessRule($everyone,'ReadAndExecute','ContainerInherit,ObjectInherit','None','Allow')));Set-Acl '.' $dir;" +
            "$acl=New-Object Security.AccessControl.FileSecurity;$acl.SetAccessRuleProtection($true,$false);$acl.AddAccessRule((New-Object Security.AccessControl.FileSystemAccessRule($sid,'FullControl','Allow')));Set-Acl 'private.txt' $acl;",
        },
        new AbortController().signal,
      ),
    );
    expect(setup.exitCode).toBe(0);
    const written = JSON.parse(
      await runWindowsTool(
        { mode: "write", path, content: "changed", expectedSha256: sha("private-original") },
        new AbortController().signal,
      ),
    );
    expect(written.error).toBeUndefined();
    const check = JSON.parse(
      await runWindowsTool(
        {
          mode: "exec",
          cwd: root,
          timeoutMs: 10000,
          command:
            "$a=Get-Acl 'private.txt';$b=Get-Acl '" +
            String(written.backupPath).replaceAll("'", "''") +
            "';" +
            "@{source=$a.GetSecurityDescriptorSddlForm([Security.AccessControl.AccessControlSections]::Access);backup=$b.GetSecurityDescriptorSddlForm([Security.AccessControl.AccessControlSections]::Access);protected=$b.AreAccessRulesProtected}|ConvertTo-Json -Compress",
        },
        new AbortController().signal,
      ),
    );
    expect(check.exitCode).toBe(0);
    const acls = JSON.parse(check.output);
    expect(acls.protected).toBe(true);
    // Auto-inheritance bookkeeping flags may differ; every access-control entry must match.
    expect(acls.backup.slice(acls.backup.indexOf("("))).toBe(
      acls.source.slice(acls.source.indexOf("(")),
    );
    expect(await NodeFSP.readFile(written.backupPath, "utf8")).toBe("private-original");
  });
  it("runs a command in its own job and reports the closed tree", async () => {
    const root = await workspace();
    const result = JSON.parse(
      await runWindowsTool(
        {
          mode: "exec",
          cwd: root,
          command: "[Console]::Out.Write('native-job-ok')",
          timeoutMs: 10000,
        },
        new AbortController().signal,
      ),
    );
    expect(result.error).toBeUndefined();
    expect(result.exitCode).toBe(0);
    expect(result.output).toContain("native-job-ok");
    expect(result.processTreeClosed).toBe(true);
  });
  it("bounds a timed-out command using the job object", async () => {
    const root = await workspace();
    const result = JSON.parse(
      await runWindowsTool(
        { mode: "exec", cwd: root, command: "Start-Sleep -Seconds 30", timeoutMs: 100 },
        new AbortController().signal,
      ),
    );
    expect(result.error).toBeUndefined();
    expect(result.timedOut).toBe(true);
    expect(result.processTreeClosed).toBe(true);
  });
  it("rejects replacement while another writer owns the exclusive file handle", async () => {
    const root = await workspace(),
      path = NodePath.join(root, "sample.txt");
    await NodeFSP.writeFile(path, "user-edit");
    const ready = marker(root, "locked");
    const controller = new AbortController();
    const owner = runWindowsTool(
      {
        mode: "exec",
        cwd: root,
        timeoutMs: 30000,
        command:
          "$f=[IO.File]::Open('sample.txt',[IO.FileMode]::Open,[IO.FileAccess]::ReadWrite,[IO.FileShare]::None);try{[IO.File]::WriteAllText('locked','yes');Start-Sleep -Seconds 30}finally{$f.Dispose()}",
      },
      controller.signal,
    );
    try {
      await ready;
      const result = JSON.parse(
        await runWindowsTool(
          { mode: "write", path, content: "must not win", expectedSha256: sha("user-edit") },
          new AbortController().signal,
        ),
      );
      expect(result.error).toBe("file-busy-or-changed");
    } finally {
      controller.abort();
      await expect(owner).rejects.toThrow();
    }
    expect(await NodeFSP.readFile(path, "utf8")).toBe("user-edit");
  });
  it("Stop closes the actual descendant process, not just the PowerShell parent", async () => {
    const root = await workspace(),
      ready = marker(root, "descendant");
    const controller = new AbortController();
    const owner = runWindowsTool(
      {
        mode: "exec",
        cwd: root,
        timeoutMs: 30000,
        command:
          "$child=Start-Process -FilePath (Join-Path $PSHOME 'powershell.exe') -ArgumentList '-NoLogo -NoProfile -NonInteractive -Command Start-Sleep -Seconds 60' -PassThru -WindowStyle Hidden;[IO.File]::WriteAllText('descendant',[string]$child.Id);Start-Sleep -Seconds 60",
      },
      controller.signal,
    );
    await ready;
    const pid = Number(await NodeFSP.readFile(NodePath.join(root, "descendant"), "utf8"));
    expect(Number.isInteger(pid) && pid > 0).toBe(true);
    controller.abort();
    await expect(owner).rejects.toThrow();
    let alive = true;
    try {
      process.kill(pid, 0);
    } catch {
      alive = false;
    }
    expect(alive).toBe(false);
  });
});
