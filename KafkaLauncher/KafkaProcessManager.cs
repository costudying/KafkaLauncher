using System;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace KafkaLauncher
{
    public class CliExecutionResult
    {
        public int ExitCode { get; set; }
        public string StdOut { get; set; }
        public string StdErr { get; set; }
        public bool TimedOut { get; set; }
        public bool Success => !TimedOut && ExitCode == 0;
    }

    public class KRaftFormatResult
    {
        public bool Success { get; set; }
        public string ClusterId { get; set; }
        public string Message { get; set; }
    }

    public class JavaCheckResult
    {
        public bool IsAvailable { get; set; }
        public string Version { get; set; } = string.Empty;
        public string JavaHomePath { get; set; } = string.Empty;
        public string JavaExePath { get; set; } = string.Empty;
        public string Source { get; set; } = string.Empty;
        public string Message { get; set; } = string.Empty;
    }

    public class KafkaProcessManager
    {
        private readonly object _syncLock = new object();
        private Process _zkProc;
        private Process _kafkaProc;

        public event EventHandler<string> OutputReceived;
        public event EventHandler<string> ErrorReceived;
        public event EventHandler<string> StateChanged;
        public event EventHandler<bool> ZookeeperStateChanged;
        public event EventHandler<bool> KafkaStateChanged;

        public bool IsZookeeperRunning
        {
            get
            {
                lock (_syncLock)
                {
                    return IsProcessAlive(_zkProc);
                }
            }
        }

        public bool IsKafkaRunning
        {
            get
            {
                lock (_syncLock)
                {
                    return IsProcessAlive(_kafkaProc);
                }
            }
        }

        private void OnOutput(string line) => OutputReceived?.Invoke(this, line);
        private void OnError(string line) => ErrorReceived?.Invoke(this, line);
        private void OnState(string state) => StateChanged?.Invoke(this, state);
        private void OnZookeeperState(bool running) => ZookeeperStateChanged?.Invoke(this, running);
        private void OnKafkaState(bool running) => KafkaStateChanged?.Invoke(this, running);

        private static bool IsProcessAlive(Process proc)
        {
            if (proc == null) return false;
            try
            {
                return !proc.HasExited;
            }
            catch
            {
                return false;
            }
        }

        public Task StartZookeeperAsync(string zkBat, string zkConfig, bool newWindow, string customJavaHome = null)
        {
            return Task.Run(() =>
            {
                lock (_syncLock)
                {
                    if (IsProcessAlive(_zkProc))
                    {
                        OnOutput("Zookeeper가 이미 실행 중입니다.");
                        return;
                    }
                    CleanupExitedProcess(ref _zkProc);
                }

                string workDir = Path.GetDirectoryName(zkBat);
                string resolvedJavaHome = NormalizeJavaHome(customJavaHome);
                OnState("Zookeeper 시작 중...");

                Process proc = CreateServerProcess(
                    batPath: zkBat,
                    configPath: zkConfig,
                    heapOpts: null,
                    workDir: workDir,
                    newWindow: newWindow,
                    label: "Zookeeper",
                    customJavaHome: resolvedJavaHome);

                proc.Exited += (s, e) =>
                {
                    lock (_syncLock)
                    {
                        if (ReferenceEquals(_zkProc, proc))
                        {
                            _zkProc = null;
                        }
                    }
                    try { proc.Dispose(); } catch { }
                    OnOutput("Zookeeper 프로세스가 종료되었습니다.");
                    OnZookeeperState(false);
                    PublishCombinedState();
                };

                try
                {
                    proc.Start();
                    if (!newWindow)
                    {
                        proc.BeginOutputReadLine();
                        proc.BeginErrorReadLine();
                    }

                    lock (_syncLock)
                    {
                        _zkProc = proc;
                    }

                    OnOutput(newWindow
                        ? $"[Zookeeper] 새 콘솔창에서 시작됨: {zkBat} \"{zkConfig}\""
                        : $"[Zookeeper] 시작됨 (PID={proc.Id}): {zkBat} \"{zkConfig}\"");
                    OnZookeeperState(true);
                    PublishCombinedState();
                }
                catch
                {
                    try { proc.Dispose(); } catch { }
                    OnZookeeperState(false);
                    PublishCombinedState();
                    throw;
                }
            });
        }

        public Task StartKafkaAsync(string kafkaBat, string kafkaConfig, string heapOpts, bool newWindow, KafkaRunMode mode, string customJavaHome = null)
        {
            return Task.Run(() =>
            {
                lock (_syncLock)
                {
                    if (IsProcessAlive(_kafkaProc))
                    {
                        OnOutput("Kafka가 이미 실행 중입니다.");
                        return;
                    }
                    CleanupExitedProcess(ref _kafkaProc);
                }

                string workDir = Path.GetDirectoryName(kafkaBat);
                string resolvedJavaHome = NormalizeJavaHome(customJavaHome);
                string modeLabel = mode == KafkaRunMode.KRaft ? "Kafka (KRaft)" : "Kafka";
                OnState($"{modeLabel} 시작 중...");

                Process proc = CreateServerProcess(
                    batPath: kafkaBat,
                    configPath: kafkaConfig,
                    heapOpts: heapOpts,
                    workDir: workDir,
                    newWindow: newWindow,
                    label: modeLabel,
                    customJavaHome: resolvedJavaHome);

                proc.Exited += (s, e) =>
                {
                    lock (_syncLock)
                    {
                        if (ReferenceEquals(_kafkaProc, proc))
                        {
                            _kafkaProc = null;
                        }
                    }
                    try { proc.Dispose(); } catch { }
                    OnOutput($"{modeLabel} 프로세스가 종료되었습니다.");
                    OnKafkaState(false);
                    PublishCombinedState();
                };

                try
                {
                    proc.Start();
                    if (!newWindow)
                    {
                        proc.BeginOutputReadLine();
                        proc.BeginErrorReadLine();
                    }

                    lock (_syncLock)
                    {
                        _kafkaProc = proc;
                    }

                    OnOutput(newWindow
                        ? $"[{modeLabel}] 새 콘솔창에서 시작됨: {kafkaBat} \"{kafkaConfig}\" (HEAP={heapOpts})"
                        : $"[{modeLabel}] 시작됨 (PID={proc.Id}): {kafkaBat} \"{kafkaConfig}\" (HEAP={heapOpts})");
                    OnKafkaState(true);
                    PublishCombinedState();
                }
                catch
                {
                    try { proc.Dispose(); } catch { }
                    OnKafkaState(false);
                    PublishCombinedState();
                    throw;
                }
            });
        }

        public Task StopZookeeperAsync()
        {
            return Task.Run(() =>
            {
                StopSingleProcess(isKafka: false);
                PublishCombinedState();
            });
        }

        public Task StopKafkaAsync()
        {
            return Task.Run(() =>
            {
                StopSingleProcess(isKafka: true);
                PublishCombinedState();
            });
        }

        public Task StopAllAsync()
        {
            return Task.Run(() => StopAllSync());
        }

        public void StopAllSync()
        {
            int killed = 0;
            // Kafka를 먼저 종료한 뒤 Zookeeper를 종료
            killed += StopSingleProcess(isKafka: true);
            killed += StopSingleProcess(isKafka: false);

            OnState(killed > 0 ? "모든 프로세스 종료 완료" : "실행 중인 프로세스 없음");
        }

        public async Task<KRaftFormatResult> FormatKRaftStorageAsync(
            string kafkaRoot,
            string kraftConfigPath,
            string clusterId = null,
            bool ignoreFormatted = true,
            string customJavaHome = null)
        {
            string storageBat = Path.Combine(kafkaRoot, "bin", "windows", "kafka-storage.bat");
            if (!File.Exists(storageBat))
            {
                return new KRaftFormatResult
                {
                    Success = false,
                    Message = $"kafka-storage.bat을 찾을 수 없습니다 (KRaft는 Kafka 3.3+ 필요): {storageBat}"
                };
            }

            if (!File.Exists(kraftConfigPath))
            {
                return new KRaftFormatResult
                {
                    Success = false,
                    Message = $"KRaft 설정 파일을 찾을 수 없습니다: {kraftConfigPath}"
                };
            }

            string resolvedJavaHome = NormalizeJavaHome(customJavaHome);
            OnState("KRaft 스토리지 포맷 중...");
            string resolvedId = string.IsNullOrWhiteSpace(clusterId) ? null : clusterId.Trim();

            if (string.IsNullOrEmpty(resolvedId))
            {
                OnOutput("[KRaft] 클러스터 UUID 생성 중 (kafka-storage.bat random-uuid)...");
                var uuidResult = await RunCliCommandAsync(storageBat, "random-uuid", 20000, customJavaHome: resolvedJavaHome);
                if (!uuidResult.Success)
                {
                    string err = string.IsNullOrWhiteSpace(uuidResult.StdErr) ? uuidResult.StdOut : uuidResult.StdErr;
                    OnError("[KRaft] UUID 생성 실패: " + err);
                    PublishCombinedState();
                    return new KRaftFormatResult
                    {
                        Success = false,
                        Message = "클러스터 UUID 생성에 실패했습니다: " + err
                    };
                }

                // 마지막 비어있지 않은 줄을 UUID로 추출
                var lines = (uuidResult.StdOut ?? string.Empty).Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);
                resolvedId = lines.Length > 0 ? lines[lines.Length - 1].Trim() : string.Empty;

                if (string.IsNullOrEmpty(resolvedId))
                {
                    PublishCombinedState();
                    return new KRaftFormatResult
                    {
                        Success = false,
                        Message = "생성된 클러스터 UUID를 파싱하지 못했습니다."
                    };
                }

                OnOutput($"[KRaft] 생성된 Cluster ID: {resolvedId}");
            }

            string ignoreFlag = ignoreFormatted ? " --ignore-formatted" : string.Empty;
            string formatArgs = $"format -t {resolvedId} -c \"{kraftConfigPath}\"{ignoreFlag}";
            OnOutput($"[KRaft] 스토리지 포맷 실행: kafka-storage.bat {formatArgs}");

            var formatResult = await RunCliCommandAsync(storageBat, formatArgs, 30000, customJavaHome: resolvedJavaHome);
            if (!string.IsNullOrWhiteSpace(formatResult.StdOut))
                OnOutput("[KRaft] " + formatResult.StdOut.Trim());
            if (!string.IsNullOrWhiteSpace(formatResult.StdErr))
                OnError("[KRaft] " + formatResult.StdErr.Trim());

            PublishCombinedState();

            if (formatResult.Success)
            {
                return new KRaftFormatResult
                {
                    Success = true,
                    ClusterId = resolvedId,
                    Message = string.IsNullOrWhiteSpace(formatResult.StdOut)
                        ? $"KRaft 스토리지 포맷 완료 (Cluster ID: {resolvedId})"
                        : formatResult.StdOut.Trim()
                };
            }

            return new KRaftFormatResult
            {
                Success = false,
                ClusterId = resolvedId,
                Message = string.IsNullOrWhiteSpace(formatResult.StdErr)
                    ? (formatResult.StdOut ?? "KRaft 스토리지 포맷 실패").Trim()
                    : formatResult.StdErr.Trim()
            };
        }

        public static string NormalizeJavaHome(string inputPath)
        {
            if (string.IsNullOrWhiteSpace(inputPath))
                return string.Empty;

            string trimmed = inputPath.Trim().Trim('"').TrimEnd('\\', '/');
            if (string.IsNullOrEmpty(trimmed))
                return string.Empty;

            // 사용자가 java.exe 파일 자체를 입력한 경우 상위 폴더 2단계 이동
            if (trimmed.EndsWith("java.exe", StringComparison.OrdinalIgnoreCase))
            {
                string binDir = Path.GetDirectoryName(trimmed);
                if (!string.IsNullOrEmpty(binDir))
                    trimmed = Path.GetDirectoryName(binDir) ?? binDir;
            }
            // 사용자가 bin 폴더를 선택한 경우 상위 JDK 루트로 보정
            else if (string.Equals(Path.GetFileName(trimmed), "bin", StringComparison.OrdinalIgnoreCase) &&
                     File.Exists(Path.Combine(trimmed, "java.exe")))
            {
                trimmed = Path.GetDirectoryName(trimmed) ?? trimmed;
            }

            return trimmed;
        }

        public static Task<JavaCheckResult> DetectJavaAsync(string customJavaHome = null)
        {
            return Task.Run(() =>
            {
                string normalizedCustom = NormalizeJavaHome(customJavaHome);
                if (!string.IsNullOrEmpty(normalizedCustom))
                {
                    string customExe = Path.Combine(normalizedCustom, "bin", "java.exe");
                    if (!File.Exists(customExe))
                    {
                        return new JavaCheckResult
                        {
                            IsAvailable = false,
                            JavaHomePath = normalizedCustom,
                            JavaExePath = customExe,
                            Source = "Portable JDK",
                            Message = $"지정한 JDK 경로에 bin\\java.exe가 없습니다: {customExe}"
                        };
                    }

                    string ver = ReadJavaVersion(customExe);
                    return new JavaCheckResult
                    {
                        IsAvailable = !string.IsNullOrEmpty(ver),
                        Version = ver,
                        JavaHomePath = normalizedCustom,
                        JavaExePath = customExe,
                        Source = "Portable JDK",
                        Message = string.IsNullOrEmpty(ver)
                            ? $"java.exe 실행에 실패했습니다: {customExe}"
                            : $"Portable JDK 감지됨: {ver} ({normalizedCustom})"
                    };
                }

                // 1) 시스템 JAVA_HOME 확인
                string envJavaHome = NormalizeJavaHome(Environment.GetEnvironmentVariable("JAVA_HOME"));
                if (!string.IsNullOrEmpty(envJavaHome))
                {
                    string envExe = Path.Combine(envJavaHome, "bin", "java.exe");
                    if (File.Exists(envExe))
                    {
                        string ver = ReadJavaVersion(envExe);
                        if (!string.IsNullOrEmpty(ver))
                        {
                            return new JavaCheckResult
                            {
                                IsAvailable = true,
                                Version = ver,
                                JavaHomePath = envJavaHome,
                                JavaExePath = envExe,
                                Source = "JAVA_HOME",
                                Message = $"시스템 JAVA_HOME 감지됨: {ver} ({envJavaHome})"
                            };
                        }
                    }
                }

                // 2) PATH에서 java.exe 검색
                string pathJavaExe = FindExecutableOnPath("java.exe");
                if (!string.IsNullOrEmpty(pathJavaExe) && File.Exists(pathJavaExe))
                {
                    string ver = ReadJavaVersion(pathJavaExe);
                    if (!string.IsNullOrEmpty(ver))
                    {
                        string inferredHome = NormalizeJavaHome(pathJavaExe);
                        return new JavaCheckResult
                        {
                            IsAvailable = true,
                            Version = ver,
                            JavaHomePath = inferredHome,
                            JavaExePath = pathJavaExe,
                            Source = "System PATH",
                            Message = $"시스템 PATH Java 감지됨: {ver} ({pathJavaExe})"
                        };
                    }
                }

                return new JavaCheckResult
                {
                    IsAvailable = false,
                    Source = "미설치",
                    Message = "시스템에 Java(JDK)가 감지되지 않았습니다. 로컬 Kafka 실행 시 Portable JDK 폴더를 지정해 주세요. (Offset Explorer 탭은 Java 없이 사용 가능)"
                };
            });
        }

        private static string FindExecutableOnPath(string exeName)
        {
            string pathEnv = Environment.GetEnvironmentVariable("PATH") ?? string.Empty;
            foreach (string rawDir in pathEnv.Split(new[] { ';' }, StringSplitOptions.RemoveEmptyEntries))
            {
                try
                {
                    string dir = rawDir.Trim().Trim('"');
                    if (string.IsNullOrEmpty(dir)) continue;
                    string candidate = Path.Combine(dir, exeName);
                    if (File.Exists(candidate))
                        return candidate;
                }
                catch { }
            }
            return null;
        }

        private static string ReadJavaVersion(string javaExePath)
        {
            try
            {
                var psi = new ProcessStartInfo
                {
                    FileName = javaExePath,
                    Arguments = "-version",
                    UseShellExecute = false,
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    CreateNoWindow = true
                };

                using (var proc = Process.Start(psi))
                {
                    if (proc == null) return string.Empty;
                    string err = proc.StandardError.ReadToEnd();
                    string outTxt = proc.StandardOutput.ReadToEnd();
                    proc.WaitForExit(5000);

                    string combined = string.IsNullOrWhiteSpace(err) ? outTxt : err;
                    var firstLine = (combined ?? string.Empty)
                        .Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);
                    if (firstLine.Length == 0) return string.Empty;

                    // 예: openjdk version "17.0.10" 2024-01-16 → 17.0.10
                    string line = firstLine[0].Trim();
                    int q1 = line.IndexOf('"');
                    int q2 = q1 >= 0 ? line.IndexOf('"', q1 + 1) : -1;
                    if (q1 >= 0 && q2 > q1 + 1)
                        return line.Substring(q1 + 1, q2 - q1 - 1);

                    return line;
                }
            }
            catch
            {
                return string.Empty;
            }
        }

        public static Task<CliExecutionResult> RunCliCommandAsync(
            string batPath,
            string arguments,
            int timeoutMs = 30000,
            string stdinInput = null,
            string customJavaHome = null)
        {
            return Task.Run(() =>
            {
                var stdout = new StringBuilder();
                var stderr = new StringBuilder();

                var psi = new ProcessStartInfo
                {
                    FileName = "cmd.exe",
                    Arguments = $"/c \"\"{batPath}\" {arguments}\"",
                    WorkingDirectory = Path.GetDirectoryName(batPath) ?? Environment.CurrentDirectory,
                    UseShellExecute = false,
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    RedirectStandardInput = stdinInput != null,
                    StandardOutputEncoding = Encoding.UTF8,
                    StandardErrorEncoding = Encoding.UTF8,
                    CreateNoWindow = true
                };

                ApplyJavaHomeEnvironment(psi, customJavaHome);

                string existingKafkaOpts = psi.EnvironmentVariables.ContainsKey("KAFKA_OPTS")
                    ? psi.EnvironmentVariables["KAFKA_OPTS"]
                    : string.Empty;
                if (existingKafkaOpts.IndexOf("file.encoding", StringComparison.OrdinalIgnoreCase) < 0)
                {
                    psi.EnvironmentVariables["KAFKA_OPTS"] = string.IsNullOrWhiteSpace(existingKafkaOpts)
                        ? "-Dfile.encoding=UTF-8"
                        : existingKafkaOpts + " -Dfile.encoding=UTF-8";
                }

                using (var proc = new Process { StartInfo = psi })
                {
                    proc.OutputDataReceived += (s, e) =>
                    {
                        if (e.Data != null)
                        {
                            lock (stdout) { stdout.AppendLine(e.Data); }
                        }
                    };
                    proc.ErrorDataReceived += (s, e) =>
                    {
                        if (e.Data != null)
                        {
                            lock (stderr) { stderr.AppendLine(e.Data); }
                        }
                    };

                    proc.Start();
                    proc.BeginOutputReadLine();
                    proc.BeginErrorReadLine();

                    if (stdinInput != null)
                    {
                        using (var utf8Writer = new StreamWriter(proc.StandardInput.BaseStream, new UTF8Encoding(false)))
                        {
                            utf8Writer.WriteLine(stdinInput);
                            utf8Writer.Flush();
                        }
                    }

                    bool exited = proc.WaitForExit(timeoutMs);
                    if (!exited)
                    {
                        KillProcessTree(proc);
                        return new CliExecutionResult
                        {
                            ExitCode = -1,
                            StdOut = stdout.ToString(),
                            StdErr = stderr.ToString() + Environment.NewLine + $"명령 실행 시간 초과 ({timeoutMs / 1000}초)",
                            TimedOut = true
                        };
                    }

                    // 비동기 출력 버퍼가 모두 플러시될 때까지 대기
                    proc.WaitForExit();

                    return new CliExecutionResult
                    {
                        ExitCode = proc.ExitCode,
                        StdOut = stdout.ToString(),
                        StdErr = stderr.ToString(),
                        TimedOut = false
                    };
                }
            });
        }

        private static void ApplyJavaHomeEnvironment(ProcessStartInfo psi, string customJavaHome)
        {
            string normalized = NormalizeJavaHome(customJavaHome);
            if (string.IsNullOrEmpty(normalized)) return;

            psi.EnvironmentVariables["JAVA_HOME"] = normalized;
            string binDir = Path.Combine(normalized, "bin");
            string currentPath = psi.EnvironmentVariables.ContainsKey("PATH")
                ? psi.EnvironmentVariables["PATH"]
                : (Environment.GetEnvironmentVariable("PATH") ?? string.Empty);

            psi.EnvironmentVariables["PATH"] = binDir + ";" + currentPath;
        }

        private Process CreateServerProcess(
            string batPath,
            string configPath,
            string heapOpts,
            string workDir,
            bool newWindow,
            string label,
            string customJavaHome = null)
        {
            string normalizedJavaHome = NormalizeJavaHome(customJavaHome);
            ProcessStartInfo psi;
            if (newWindow)
            {
                // cmd.exe /k 따옴표 규칙: 바깥쪽 "..."가 제거된 뒤 내부 명령이 실행됨
                var parts = new System.Collections.Generic.List<string>();
                if (!string.IsNullOrEmpty(normalizedJavaHome))
                {
                    string javaBin = Path.Combine(normalizedJavaHome, "bin");
                    parts.Add($"set \"JAVA_HOME={normalizedJavaHome}\"");
                    parts.Add($"set \"PATH={javaBin};%PATH%\"");
                }
                if (!string.IsNullOrWhiteSpace(heapOpts))
                {
                    parts.Add($"set \"KAFKA_HEAP_OPTS={heapOpts}\"");
                }
                parts.Add($"call \"{batPath}\" \"{configPath}\"");

                string cmdArgs = "/k \"" + string.Join(" && ", parts) + "\"";

                psi = new ProcessStartInfo
                {
                    FileName = "cmd.exe",
                    Arguments = cmdArgs,
                    WorkingDirectory = workDir,
                    UseShellExecute = true,
                    CreateNoWindow = false,
                    WindowStyle = ProcessWindowStyle.Normal
                };
            }
            else
            {
                psi = new ProcessStartInfo
                {
                    FileName = "cmd.exe",
                    Arguments = $"/c \"\"{batPath}\" \"{configPath}\"\"",
                    WorkingDirectory = workDir,
                    UseShellExecute = false,
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    CreateNoWindow = true
                };

                ApplyJavaHomeEnvironment(psi, normalizedJavaHome);
                if (!string.IsNullOrWhiteSpace(heapOpts))
                {
                    psi.EnvironmentVariables["KAFKA_HEAP_OPTS"] = heapOpts;
                }
            }

            var proc = new Process { StartInfo = psi, EnableRaisingEvents = true };
            if (!newWindow)
            {
                proc.OutputDataReceived += (s, e) =>
                {
                    if (e.Data != null) OnOutput($"[{label}] {e.Data}");
                };
                proc.ErrorDataReceived += (s, e) =>
                {
                    if (e.Data != null) OnError($"[{label}] {e.Data}");
                };
            }

            return proc;
        }

        private int StopSingleProcess(bool isKafka)
        {
            Process target;
            string name = isKafka ? "Kafka" : "Zookeeper";

            lock (_syncLock)
            {
                target = isKafka ? _kafkaProc : _zkProc;
                if (isKafka) _kafkaProc = null;
                else _zkProc = null;
            }

            if (target == null)
            {
                if (isKafka) OnKafkaState(false);
                else OnZookeeperState(false);
                return 0;
            }

            try
            {
                if (!IsProcessAlive(target))
                {
                    try { target.Dispose(); } catch { }
                    if (isKafka) OnKafkaState(false);
                    else OnZookeeperState(false);
                    return 0;
                }

                int pid = target.Id;
                OnOutput($"{name} 프로세스 트리 종료 시도 (PID={pid})...");
                KillProcessTree(target);
                OnOutput($"{name} 종료 완료 (자식 Java 프로세스 포함).");

                try { target.Dispose(); } catch { }
                if (isKafka) OnKafkaState(false);
                else OnZookeeperState(false);
                return 1;
            }
            catch (Exception ex)
            {
                OnError($"{name} 종료 오류: {ex.Message}");
                if (isKafka) OnKafkaState(IsKafkaRunning);
                else OnZookeeperState(IsZookeeperRunning);
                return 0;
            }
        }

        internal static void KillProcessTree(Process proc)
        {
            if (proc == null) return;

            int pid;
            try
            {
                if (proc.HasExited) return;
                pid = proc.Id;
            }
            catch
            {
                return;
            }

            // taskkill /T /F 로 cmd.exe 및 하위 java.exe 프로세스 트리 전체 종료
            try
            {
                var killPsi = new ProcessStartInfo
                {
                    FileName = "taskkill.exe",
                    Arguments = $"/PID {pid} /T /F",
                    UseShellExecute = false,
                    CreateNoWindow = true,
                    RedirectStandardOutput = true,
                    RedirectStandardError = true
                };

                using (var killProc = Process.Start(killPsi))
                {
                    killProc?.WaitForExit(5000);
                }
            }
            catch
            {
                // taskkill 실패 시 아래 fallback 수행
            }

            try
            {
                if (!proc.HasExited)
                {
                    proc.Kill();
                    proc.WaitForExit(5000);
                }
            }
            catch
            {
                // 이미 종료된 경우 무시
            }
        }

        private static void CleanupExitedProcess(ref Process proc)
        {
            if (proc != null && !IsProcessAlive(proc))
            {
                try { proc.Dispose(); } catch { }
                proc = null;
            }
        }

        private void PublishCombinedState()
        {
            bool zk = IsZookeeperRunning;
            bool kafka = IsKafkaRunning;

            if (zk && kafka)
                OnState("Zookeeper + Kafka 실행 중");
            else if (kafka)
                OnState("Kafka 실행 중");
            else if (zk)
                OnState("Zookeeper 실행 중");
            else
                OnState("대기");
        }
    }
}
