---
name: kafka-launcher-dev
description: >-
  Use this skill when adding or modifying features in KafkaLauncher, including WPF UI tabs,
  Zookeeper/KRaft process management, Portable JDK injection, or Confluent.Kafka Offset Explorer features.
---

# KafkaLauncher Development Guide

## Codebase Map

| File | Responsibility |
|---|---|
| `KafkaLauncher/App.xaml.cs` | Calls `NativeKafkaLoader.EnsureLoaded()` during `OnStartup`. |
| `KafkaLauncher/NativeKafkaLoader.cs` | Extracts architecture-matched `librdkafka` native DLLs from embedded `KafkaLauncher.librdkafka_native.zip` into `%LocalAppData%\KafkaLauncher\native\` and calls `SetDllDirectory` + `Confluent.Kafka.Library.Load`. |
| `KafkaLauncher/LauncherSettings.cs` | Loads/saves XML settings in `%LocalAppData%\KafkaLauncher\settings.xml` (`KafkaRoot`, `CustomJavaHome`, `RunMode`, config paths, Heap, Bootstrap Server). |
| `KafkaLauncher/KafkaProcessManager.cs` | Manages Zookeeper/Kafka `.bat` processes, `taskkill /T /F` process tree shutdown, KRaft storage format (`kafka-storage.bat`), and Java environment detection (`DetectJavaAsync`). |
| `KafkaLauncher/KafkaExplorerService.cs` | Native `Confluent.Kafka` Admin/Consumer/Producer operations, `KafkaLiveConsumer`, JSON Pretty-Print, and Hex Dump helpers. |
| `KafkaLauncher/MainWindow.xaml` & `.cs` | Dark-theme WPF UI with two main tabs (`런처 & 로그` and `Offset Explorer`). |

## Modification Checklist

1. **Adding a new C# file or resource**
   - Register it in `KafkaLauncher/KafkaLauncher.csproj` (`<Compile Include="..." />` or `<Resource Include="..." />`).
2. **Adding persistent settings**
   - Add a public get/set property with a safe default in `LauncherSettings.cs`.
   - Bind read/write in `MainWindow.ApplySettingsToUi` and `MainWindow.CaptureSettingsFromUi`.
3. **Editing `MainWindow.xaml`**
   - Never put `--` inside XML comments (`<!-- ... -->`).
   - Reuse existing dark styles (`DarkTextBox`, `DarkComboBox`, `DarkDataGrid`, `CardStyle`, `BaseButton`, `AccentButton`, `DangerButton`, `PillRadio`).
   - Ensure any new action buttons are toggled in `SetExplorerButtonsEnabled` if they run async Kafka calls.
4. **Editing process / background thread code**
   - Route UI updates through `SafeUiInvoke(...)`.
   - Ensure any long-running background task or consumer is cleanly stopped in `Window_Closing` and `BtnStopAll_Click`.
5. **Verification**
   - After edits, follow the `build-single-exe` skill to rebuild and confirm 0 errors and 0 warnings.
