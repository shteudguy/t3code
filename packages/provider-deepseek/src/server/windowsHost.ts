import * as HostProcess from "@t3tools/shared/HostProcess";
// @effect-diagnostics nodeBuiltinImport:off -- Windows file sharing and Job Objects are native OS boundaries.
// @effect-diagnostics globalTimers:off -- Bounded deadline for terminating the exact owned host process.
import * as NodeChildProcess from "node:child_process";
import * as NodePath from "node:path";

/** Static host only; model-supplied commands/content travel on stdin, never in argv. */
export const windowsHostSource = String.raw`
$ErrorActionPreference = 'Stop'
[Console]::InputEncoding = New-Object System.Text.UTF8Encoding
[Console]::OutputEncoding = New-Object System.Text.UTF8Encoding
$payload = [Console]::In.ReadToEnd() | ConvertFrom-Json
try {
Add-Type -TypeDefinition @'
using System;
using System.IO;
using System.Text;
using System.Diagnostics;
using System.Threading.Tasks;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Collections.Generic;
using System.Security.AccessControl;
public static class T3NativeTools {
  [StructLayout(LayoutKind.Sequential)] struct BasicLimits {
    public long ProcessTime, JobTime; public uint Flags; public UIntPtr Minimum, Maximum;
    public uint Active; public UIntPtr Affinity; public uint Priority, Scheduling;
  }
  [StructLayout(LayoutKind.Sequential)] struct Io { public ulong A,B,C,D,E,F; }
  [StructLayout(LayoutKind.Sequential)] struct Limits {
    public BasicLimits Basic; public Io Io; public UIntPtr ProcessMemory, JobMemory, PeakProcess, PeakJob;
  }
  [DllImport("kernel32.dll", CharSet=CharSet.Unicode, SetLastError=true)] static extern IntPtr CreateJobObject(IntPtr attributes,string name);
  [DllImport("kernel32.dll", SetLastError=true)] static extern bool SetInformationJobObject(IntPtr job,int kind,IntPtr info,uint size);
  [DllImport("kernel32.dll", SetLastError=true)] static extern bool AssignProcessToJobObject(IntPtr job,IntPtr process);
  [DllImport("kernel32.dll", SetLastError=true)] static extern bool TerminateJobObject(IntPtr job,uint code);
  [DllImport("kernel32.dll")] static extern bool CloseHandle(IntPtr handle);
  static string Hash(Stream stream) {
    using(var hash=SHA256.Create()) return BitConverter.ToString(hash.ComputeHash(stream)).Replace("-","").ToLowerInvariant();
  }
  static Dictionary<string,object> Result(string error) {
    return new Dictionary<string,object>{{"error",error},{"unknown",false}};
  }
  public static object Write(string path,string expected,string text) {
    FileStream file;
    try { file=new FileStream(path, expected=="new"?FileMode.CreateNew:FileMode.Open,
      FileAccess.ReadWrite,FileShare.None); }
    catch(IOException) { return Result("file-busy-or-changed"); }
    catch(UnauthorizedAccessException) { return Result("file-not-writable"); }
    string backup=null;
    bool started=false;
    using(file) {
      try {
        if(expected!="new") {
          if(Hash(file)!=expected) return Result("file-changed");
          backup=path+".t3-backup-"+Guid.NewGuid().ToString("N");
          var security=new FileSecurity();
          security.SetSecurityDescriptorSddlForm(file.GetAccessControl().GetSecurityDescriptorSddlForm(AccessControlSections.Access),AccessControlSections.Access);
          security.SetAccessRuleProtection(true,true);
          // Supply the DACL at creation: closed contents never inherit broader directory access.
          using(var copy=new FileStream(backup,FileMode.CreateNew,FileSystemRights.Write,FileShare.None,4096,FileOptions.None,security)) {
            file.Position=0; file.CopyTo(copy); copy.Flush(true);
          }
        }
        var bytes=new UTF8Encoding(false).GetBytes(text);
        file.Position=0; started=true; file.Write(bytes,0,bytes.Length); file.SetLength(bytes.Length); file.Flush(true);
        file.Position=0;
        return new Dictionary<string,object>{{"path",path},{"sha256",Hash(file)},{"backupPath",backup},{"unknown",false}};
      } catch {
        return new Dictionary<string,object>{{"error","guarded-write-failed"},{"unknown",started},{"backupPath",backup}};
      }
    }
  }
  static Task Drain(StreamReader reader,StringBuilder output,object sync) {
    return Task.Run(()=>{ var buffer=new char[4096]; int count;
      while((count=reader.Read(buffer,0,buffer.Length))>0) lock(sync) {
        int available=131072-output.Length; if(available>0) output.Append(buffer,0,Math.Min(count,available));
      }
    });
  }
  public static object Exec(string command,string cwd,int timeout) {
    IntPtr job=CreateJobObject(IntPtr.Zero,null);
    if(job==IntPtr.Zero) return Result("job-create-failed");
    Process child=null;
    bool assigned=false, started=false;
    var output=new StringBuilder(); var sync=new object();
    try {
      var limits=new Limits(); limits.Basic.Flags=0x2000;
      int size=Marshal.SizeOf(limits); IntPtr memory=Marshal.AllocHGlobal(size);
      try { Marshal.StructureToPtr(limits,memory,false);
        if(!SetInformationJobObject(job,9,memory,(uint)size)) return Result("job-policy-failed");
      } finally { Marshal.FreeHGlobal(memory); }
      // The child blocks on stdin until it has been assigned to the kill-on-close job.
      string reader="$ProgressPreference='SilentlyContinue';$ErrorActionPreference='Stop';[Console]::InputEncoding=New-Object Text.UTF8Encoding;[Console]::OutputEncoding=New-Object Text.UTF8Encoding;$command=[Console]::In.ReadToEnd();Invoke-Expression $command; if(-not $?) {exit 1}";
      var info=new ProcessStartInfo(Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.System),"WindowsPowerShell", "v1.0", "powershell.exe"));
      info.Arguments="-NoLogo -NoProfile -NonInteractive -EncodedCommand "+Convert.ToBase64String(Encoding.Unicode.GetBytes(reader));
      info.WorkingDirectory=cwd; info.UseShellExecute=false; info.CreateNoWindow=true;
      info.RedirectStandardInput=true; info.RedirectStandardOutput=true; info.RedirectStandardError=true;
      info.StandardOutputEncoding=new UTF8Encoding(false); info.StandardErrorEncoding=new UTF8Encoding(false);
      child=Process.Start(info);
      if(!AssignProcessToJobObject(job,child.Handle)) { child.Kill(); child.WaitForExit(5000); return Result("job-assign-failed"); }
      assigned=true;
      var stdout=Drain(child.StandardOutput,output,sync); var stderr=Drain(child.StandardError,output,sync);
      child.StandardInput.Write(command); child.StandardInput.Close(); started=true;
      bool exited=child.WaitForExit(timeout);
      if(!exited && !TerminateJobObject(job,124)) return new Dictionary<string,object>{{"error","job-stop-failed"},{"unknown",true}};
      if(!child.WaitForExit(5000)) return new Dictionary<string,object>{{"error","job-stop-timeout"},{"unknown",true}};
      int code=exited?child.ExitCode:124;
      // Kill background descendants before awaiting EOF on inherited output pipes.
      if(!CloseHandle(job)) return new Dictionary<string,object>{{"error","job-close-failed"},{"unknown",true}};
      job=IntPtr.Zero;
      if(!Task.WaitAll(new[]{stdout,stderr},5000)) return new Dictionary<string,object>{{"error","job-output-close-failed"},{"unknown",true}};
      return new Dictionary<string,object>{{"exitCode",code},{"timedOut",!exited},{"output",output.ToString()},
        {"truncated",output.Length>=131072},{"unknown",false},{"processTreeClosed",true}};
    } catch { return new Dictionary<string,object>{{"error","native-command-failed"},{"unknown",started}}; }
    finally { if(job!=IntPtr.Zero) CloseHandle(job); if(child!=null) { if(!assigned) {try {if(!child.HasExited) child.Kill();}catch{}} child.Dispose(); } }
  }
}
'@
if($payload.mode -eq 'write') {
  $result = [T3NativeTools]::Write([string]$payload.path,[string]$payload.expectedSha256,[string]$payload.content)
} elseif($payload.mode -eq 'exec') {
  $result = [T3NativeTools]::Exec([string]$payload.command,[string]$payload.cwd,[int]$payload.timeoutMs)
} else { throw 'unsupported native tool' }
$result | ConvertTo-Json -Compress -Depth 5
} catch {
  [Console]::Out.WriteLine('{"error":"native-tool-host-failed","unknown":false}')
  exit 1
}
`;

const encodedHost = Buffer.from(windowsHostSource, "utf16le").toString("base64");

export async function runWindowsTool(
  payload: Record<string, unknown>,
  signal: AbortSignal,
  environment: NodeJS.ProcessEnv = process.env,
): Promise<string> {
  if (HostProcess.Platform.defaultValue() !== "win32")
    throw Error("windows-native-tool-on-other-platform");
  signal.throwIfAborted();
  return new Promise((accept, reject) => {
    const shell = NodePath.join(
      process.env.SystemRoot ?? "C:\\Windows",
      "System32",
      "WindowsPowerShell",
      "v1.0",
      "powershell.exe",
    );
    const child = NodeChildProcess.spawn(
      shell,
      ["-NoLogo", "-NoProfile", "-NonInteractive", "-EncodedCommand", encodedHost],
      { windowsHide: true, stdio: ["pipe", "pipe", "pipe"], env: { ...environment } },
    );
    let output = "",
      stopped = false,
      stopDeadline: ReturnType<typeof setTimeout> | undefined;
    const cleanup = () => {
      if (stopDeadline) clearTimeout(stopDeadline);
      signal.removeEventListener("abort", stop);
    };
    const stop = () => {
      if (stopped) return;
      stopped = true;
      // Closing this exact host closes the kernel Job Object; no taskkill/name search is involved.
      if (!child.kill()) {
        cleanup();
        reject(Error("native-host-stop-rejected"));
        return;
      }
      stopDeadline = setTimeout(() => {
        cleanup();
        reject(Error("native-host-stop-unconfirmed"));
      }, 5000);
    };
    signal.addEventListener("abort", stop, { once: true });
    child.stdout.on("data", (data: Buffer) => {
      if (output.length < 1024 * 1024) output += data.toString("utf8");
    });
    child.stderr.resume(); // Static compiler/runtime diagnostics never become model-visible command output.
    child.on("error", () => {
      cleanup();
      reject(Error("native-host-launch-failed"));
    });
    child.on("close", (code) => {
      cleanup();
      if (signal.aborted) {
        reject(signal.reason ?? Error("aborted"));
        return;
      }
      try {
        const result = JSON.parse(output.trim());
        if (result.unknown)
          reject(
            Error(
              `native-action-outcome-unknown:${result.error ?? "failed"}:${result.backupPath ?? ""}`,
            ),
          );
        else if (code && !result.error) reject(Error("native-host-exit-without-result"));
        else accept(JSON.stringify(result));
      } catch {
        reject(Error("native-host-invalid-result"));
      }
    });
    child.stdin.on("error", () => {});
    child.stdin.end(JSON.stringify(payload));
    if (signal.aborted) stop();
  });
}
