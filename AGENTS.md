# KafkaLauncher Project Rules (`AGENTS.md`)

This repository is a C# / WPF (.NET Framework 4.7.2) Windows desktop application combining a local Apache Kafka launcher (Zookeeper & KRaft) with a native `Confluent.Kafka` Offset Explorer, packaged into a single standalone EXE via `Costura.Fody`.

## Technical Constraints & Invariants

1. **Target Framework & Project Format**
   - Classic `.NET Framework 4.7.2` `.csproj` using `<RestoreProjectStyle>PackageReference</RestoreProjectStyle>`.
   - Build with Visual Studio 2022 `MSBuild.exe` (not `dotnet build` alone, due to WPF `.NET Framework` targets).
   - Whenever adding or removing `.cs` / `.xaml` / resource files, update `KafkaLauncher/KafkaLauncher.csproj` explicitly.

2. **Single-EXE Packaging (`Costura.Fody` + Native `librdkafka`)**
   - Managed dependencies are embedded by `Costura.Fody` (`FodyWeavers.xml`).
   - Native `librdkafka` DLLs (`x64`/`x86`) are zipped at build time by the `EmbedLibrdkafkaNativeZip` target in `KafkaLauncher.csproj` and extracted once at startup by `NativeKafkaLoader.EnsureLoaded()` in `App.xaml.cs`.
   - Do not remove `NativeKafkaLoader` or re-expose `librdkafka\` DLLs into `bin\Release\` / `bin\Debug\`.

3. **Process Lifecycle & Safety (`KafkaProcessManager.cs`)**
   - Always terminate `.bat` server processes via `KillProcessTree` (`taskkill /PID <pid> /T /F`) so child `java.exe` JVM processes are never orphaned.
   - Always propagate `CustomJavaHome` (`JAVA_HOME` + `%JAVA_HOME%\bin` prepended to `PATH`) for both hidden execution and `cmd.exe /k` new-window execution, as well as `kafka-storage.bat` KRaft formatting.
   - Auto-detect Kafka 4.x dynamic quorum (`controller.quorum.bootstrap.servers` without `controller.quorum.voters`) and pass `--standalone` to `kafka-storage.bat format`.
   - Keep `CleanDataDirectoriesAsync` guarded by `IsSafeDataDirectory` (never allow deleting drive roots or OS/system folders) and block cleanup while Zookeeper or Kafka is running.
   - Keep `ToggleButton` state transitions driven by `Click` handlers + actual manager state (`IsZookeeperRunning` / `IsKafkaRunning`) — never attach side-effectful `Checked`/`Unchecked` handlers that can cascade on programmatic `IsChecked` updates.

4. **Native Kafka Explorer & Dashboard (`KafkaExplorerService.cs`)**
   - Use `Confluent.Kafka` (`AdminClient`, `Consumer`, `Producer`) directly — avoid shelling out to Kafka `.bat` tools for topic/message exploration so the Offset Explorer tab remains fast and usable on machines without Java.
   - For `KafkaLiveConsumer`, use manual partition `Assign` instead of `Subscribe` to avoid consumer-group rebalance delays.
   - Never block `RefreshClusterAsync` button re-enabling on full dashboard watermark scans; for large remote clusters (>40 partitions), load Broker/Topic/Partition metadata immediately and make full message counting opt-in (parallelized) to avoid VPN latency stalls.
   - Keep `ExtractFieldAuto` and `FormatPretty` supporting both JSON and XML (including namespace-agnostic XPath / dot paths) and qualify `Newtonsoft.Json.Formatting` explicitly when `System.Xml` is imported.
   - Batch high-frequency log and live-stream UI updates via `DispatcherTimer` (`DispatcherPriority.Background`, 100ms), guard background UI updates with `SafeUiInvoke`, and cap in-memory log/message buffers.

5. **XAML & Styling (`MainWindow.xaml`)**
   - Never include `--` inside XML comments (`<!-- ... -->`) in XAML files (causes `MC3000` build errors).
   - Always set `LastChildFill="False"` on header `DockPanel`s that align left and right children.
   - Maintain the dark theme palette (`#0F1115`, `#151922`, `#4F8EF7`, `#34C38F`), keep `TxtLog` `TextWrapping="NoWrap"`, and avoid nested `ScrollViewer` around `TextBox` / `DataGrid`.

6. **Privacy & GitHub Hygiene**
   - Do not commit `bin/`, `obj/`, `.vs/`, or machine-specific secrets/credentials.
   - Avoid introducing new hardcoded personal filesystem paths; persist user settings in `%LocalAppData%\KafkaLauncher\settings.xml` via `LauncherSettings`.
