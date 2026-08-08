using System;
using System.ComponentModel;
using System.Runtime.InteropServices;
using System.Text;

namespace FuturesRebuild
{
    public sealed class ProbeResult
    {
        public uint ChildProcessId { get; set; }
        public uint ChildExitCode { get; set; }
    }

    public static class DurableJobProcess
    {
        private const uint CREATE_SUSPENDED = 0x00000004;
        private const uint CREATE_NO_WINDOW = 0x08000000;
        private const uint INFINITE = 0xFFFFFFFF;
        private const uint WAIT_OBJECT_0 = 0;
        private const uint STILL_ACTIVE = 259;
        private const uint JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x00002000;
        private const int JobObjectExtendedLimitInformation = 9;

        [StructLayout(LayoutKind.Sequential)]
        private struct SECURITY_ATTRIBUTES
        {
            public uint nLength;
            public IntPtr lpSecurityDescriptor;
            public int bInheritHandle;
        }

        [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Unicode)]
        private struct STARTUPINFO
        {
            public uint cb;
            public string lpReserved;
            public string lpDesktop;
            public string lpTitle;
            public uint dwX;
            public uint dwY;
            public uint dwXSize;
            public uint dwYSize;
            public uint dwXCountChars;
            public uint dwYCountChars;
            public uint dwFillAttribute;
            public uint dwFlags;
            public short wShowWindow;
            public short cbReserved2;
            public IntPtr lpReserved2;
            public IntPtr hStdInput;
            public IntPtr hStdOutput;
            public IntPtr hStdError;
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct PROCESS_INFORMATION
        {
            public IntPtr hProcess;
            public IntPtr hThread;
            public uint dwProcessId;
            public uint dwThreadId;
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct JOBOBJECT_BASIC_LIMIT_INFORMATION
        {
            public long PerProcessUserTimeLimit;
            public long PerJobUserTimeLimit;
            public uint LimitFlags;
            public UIntPtr MinimumWorkingSetSize;
            public UIntPtr MaximumWorkingSetSize;
            public uint ActiveProcessLimit;
            public UIntPtr Affinity;
            public uint PriorityClass;
            public uint SchedulingClass;
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct IO_COUNTERS
        {
            public ulong ReadOperationCount;
            public ulong WriteOperationCount;
            public ulong OtherOperationCount;
            public ulong ReadTransferCount;
            public ulong WriteTransferCount;
            public ulong OtherTransferCount;
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct JOBOBJECT_EXTENDED_LIMIT_INFORMATION
        {
            public JOBOBJECT_BASIC_LIMIT_INFORMATION BasicLimitInformation;
            public IO_COUNTERS IoInfo;
            public UIntPtr ProcessMemoryLimit;
            public UIntPtr JobMemoryLimit;
            public UIntPtr PeakProcessMemoryUsed;
            public UIntPtr PeakJobMemoryUsed;
        }

        [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
        private static extern IntPtr CreateJobObject(
            IntPtr lpJobAttributes,
            string lpName
        );

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern bool SetInformationJobObject(
            IntPtr hJob,
            int infoType,
            ref JOBOBJECT_EXTENDED_LIMIT_INFORMATION lpJobObjectInfo,
            uint cbJobObjectInfoLength
        );

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern bool AssignProcessToJobObject(
            IntPtr hJob,
            IntPtr hProcess
        );

        [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
        private static extern bool CreateProcess(
            string lpApplicationName,
            StringBuilder lpCommandLine,
            IntPtr lpProcessAttributes,
            IntPtr lpThreadAttributes,
            bool bInheritHandles,
            uint dwCreationFlags,
            IntPtr lpEnvironment,
            string lpCurrentDirectory,
            ref STARTUPINFO lpStartupInfo,
            out PROCESS_INFORMATION lpProcessInformation
        );

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern uint ResumeThread(IntPtr hThread);

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern uint WaitForSingleObject(
            IntPtr hHandle,
            uint dwMilliseconds
        );

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern bool GetExitCodeProcess(
            IntPtr hProcess,
            out uint lpExitCode
        );

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern bool CloseHandle(IntPtr hObject);

        private static Win32Exception Error(string operation)
        {
            return new Win32Exception(
                Marshal.GetLastWin32Error(),
                operation + " failed"
            );
        }

        private static IntPtr NewKillOnCloseJob()
        {
            IntPtr job = CreateJobObject(IntPtr.Zero, null);
            if (job == IntPtr.Zero)
            {
                throw Error("CreateJobObject");
            }
            var limits = new JOBOBJECT_EXTENDED_LIMIT_INFORMATION();
            limits.BasicLimitInformation.LimitFlags =
                JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
            if (!SetInformationJobObject(
                job,
                JobObjectExtendedLimitInformation,
                ref limits,
                (uint)Marshal.SizeOf(limits)
            ))
            {
                int error = Marshal.GetLastWin32Error();
                CloseHandle(job);
                throw new Win32Exception(
                    error,
                    "SetInformationJobObject failed"
                );
            }
            return job;
        }

        private static PROCESS_INFORMATION StartSuspended(
            string executable,
            string arguments,
            string workingDirectory
        )
        {
            var startup = new STARTUPINFO();
            startup.cb = (uint)Marshal.SizeOf(startup);
            string quotedExecutable = "\"" + executable.Replace("\"", "\\\"") + "\"";
            var commandLine = new StringBuilder(
                quotedExecutable + " " + arguments
            );
            PROCESS_INFORMATION process;
            if (!CreateProcess(
                executable,
                commandLine,
                IntPtr.Zero,
                IntPtr.Zero,
                false,
                CREATE_SUSPENDED | CREATE_NO_WINDOW,
                IntPtr.Zero,
                workingDirectory,
                ref startup,
                out process
            ))
            {
                throw Error("CreateProcess");
            }
            return process;
        }

        public static int RunContained(
            string executable,
            string arguments,
            string workingDirectory
        )
        {
            IntPtr job = IntPtr.Zero;
            PROCESS_INFORMATION process = new PROCESS_INFORMATION();
            try
            {
                job = NewKillOnCloseJob();
                process = StartSuspended(executable, arguments, workingDirectory);
                if (!AssignProcessToJobObject(job, process.hProcess))
                {
                    throw Error("AssignProcessToJobObject");
                }
                if (ResumeThread(process.hThread) == 0xFFFFFFFF)
                {
                    throw Error("ResumeThread");
                }
                if (WaitForSingleObject(process.hProcess, INFINITE) != WAIT_OBJECT_0)
                {
                    throw Error("WaitForSingleObject");
                }
                uint exitCode;
                if (!GetExitCodeProcess(process.hProcess, out exitCode))
                {
                    throw Error("GetExitCodeProcess");
                }
                return unchecked((int)exitCode);
            }
            finally
            {
                if (process.hThread != IntPtr.Zero)
                {
                    CloseHandle(process.hThread);
                }
                if (process.hProcess != IntPtr.Zero)
                {
                    CloseHandle(process.hProcess);
                }
                if (job != IntPtr.Zero)
                {
                    CloseHandle(job);
                }
            }
        }

        public static ProbeResult ProbeKillOnClose(
            string commandInterpreter,
            string workingDirectory
        )
        {
            IntPtr job = IntPtr.Zero;
            PROCESS_INFORMATION process = new PROCESS_INFORMATION();
            try
            {
                job = NewKillOnCloseJob();
                process = StartSuspended(
                    commandInterpreter,
                    "/d /q /c ping -n 60 127.0.0.1 >nul",
                    workingDirectory
                );
                if (!AssignProcessToJobObject(job, process.hProcess))
                {
                    throw Error("AssignProcessToJobObject(probe)");
                }
                if (ResumeThread(process.hThread) == 0xFFFFFFFF)
                {
                    throw Error("ResumeThread(probe)");
                }
                System.Threading.Thread.Sleep(100);
                uint beforeClose;
                if (!GetExitCodeProcess(process.hProcess, out beforeClose))
                {
                    throw Error("GetExitCodeProcess(probe before close)");
                }
                if (beforeClose != STILL_ACTIVE)
                {
                    throw new InvalidOperationException(
                        "Containment probe exited before job close."
                    );
                }
                if (!CloseHandle(job))
                {
                    throw Error("CloseHandle(probe job)");
                }
                job = IntPtr.Zero;
                if (WaitForSingleObject(process.hProcess, 10000) != WAIT_OBJECT_0)
                {
                    throw new InvalidOperationException(
                        "Containment probe survived kill-on-close."
                    );
                }
                uint exitCode;
                if (!GetExitCodeProcess(process.hProcess, out exitCode))
                {
                    throw Error("GetExitCodeProcess(probe after close)");
                }
                return new ProbeResult
                {
                    ChildProcessId = process.dwProcessId,
                    ChildExitCode = exitCode
                };
            }
            finally
            {
                if (process.hThread != IntPtr.Zero)
                {
                    CloseHandle(process.hThread);
                }
                if (process.hProcess != IntPtr.Zero)
                {
                    CloseHandle(process.hProcess);
                }
                if (job != IntPtr.Zero)
                {
                    CloseHandle(job);
                }
            }
        }
    }
}
