---
name: build-single-exe
description: >-
  Use this skill when building, restoring NuGet packages, or verifying the single-EXE
  Costura.Fody + librdkafka output of KafkaLauncher on Windows using MSBuild.
---

# Build & Verify Single-EXE (`KafkaLauncher`)

Use this runbook to restore NuGet packages, rebuild `KafkaLauncher` into a single standalone `.exe`, and verify that both managed assemblies (`Costura.Fody`) and native `librdkafka` binaries load cleanly without loose DLLs.

## Prerequisites

- Visual Studio 2022 MSBuild:
  `C:\Program Files\Microsoft Visual Studio\2022\Professional\MSBuild\Current\Bin\MSBuild.exe`
  (or locate via `vswhere -latest -requires Microsoft.Component.MSBuild -find MSBuild\**\Bin\MSBuild.exe`).

## Steps

1. **Check for locked executable**
   - Check if `KafkaLauncher.exe` is currently running before rebuilding `Debug`:
     ```powershell
     Get-Process -Name KafkaLauncher -ErrorAction SilentlyContinue
     ```
   - Do not kill a user's running instance unless asked; if `Debug` is locked, verify compilation against `Release` and inform the user.

2. **Restore NuGet packages** (required after changing `PackageReference` in `KafkaLauncher.csproj`)
   ```powershell
   & "$msbuild" "KafkaLauncher.sln" /t:Restore /v:minimal
   ```

3. **Rebuild Release & Debug**
   ```powershell
   & "$msbuild" "KafkaLauncher.sln" /t:Rebuild /p:Configuration=Release /v:minimal
   & "$msbuild" "KafkaLauncher.sln" /t:Rebuild /p:Configuration=Debug /v:minimal
   ```

4. **Verify Single-EXE Output Hygiene**
   - Confirm `KafkaLauncher\bin\Release\` contains `KafkaLauncher.exe` and **zero** `.dll` files and **no** `librdkafka\` folder:
     ```powershell
     Get-ChildItem "KafkaLauncher\bin\Release" -Recurse | Select-Object FullName, Length
     ```

5. **Standalone Smoke Test (after packaging or dependency changes)**
   - Copy only `KafkaLauncher.exe` to an empty temporary folder, load the assembly in a fresh PowerShell process, invoke `KafkaLauncher.NativeKafkaLoader.EnsureLoaded()`, and confirm:
     - `KafkaLauncher.KafkaExplorerService.FormatJsonPretty('{"ok":true}')` succeeds (`Newtonsoft.Json` resolved via Costura)
     - `[Confluent.Kafka.Library]::IsLoaded` is `True` and `[Confluent.Kafka.Library]::VersionString` matches the NuGet package version
   - Remove the temporary directory after the test process exits.
