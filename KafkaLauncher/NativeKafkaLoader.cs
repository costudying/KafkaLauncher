using System;
using System.IO;
using System.IO.Compression;
using System.Reflection;
using System.Runtime.InteropServices;

namespace KafkaLauncher
{
    internal static class NativeKafkaLoader
    {
        private const string ResourceName = "KafkaLauncher.librdkafka_native.zip";
        private static readonly object _initLock = new object();
        private static bool _initialized = false;

        [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
        private static extern bool SetDllDirectory(string lpPathName);

        public static void EnsureLoaded()
        {
            if (_initialized) return;

            lock (_initLock)
            {
                if (_initialized) return;

                string arch = Environment.Is64BitProcess ? "x64" : "x86";
                var asm = Assembly.GetExecutingAssembly();

                using (var stream = asm.GetManifestResourceStream(ResourceName))
                {
                    if (stream == null)
                    {
                        // 개발 환경 등에서 내장 리소스가 없는 경우 기본 경로 로드 허용
                        _initialized = true;
                        return;
                    }

                    // 리소스 길이 기반 버전 디렉토리 (패키지 갱신 시 자동 재추출)
                    string cacheKey = $"2.6.1-{stream.Length}-{arch}";
                    string targetDir = Path.Combine(
                        Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
                        "KafkaLauncher",
                        "native",
                        cacheKey);

                    string markerPath = Path.Combine(targetDir, ".ready");
                    string librdkafkaPath = Path.Combine(targetDir, "librdkafka.dll");

                    if (!File.Exists(markerPath) || !File.Exists(librdkafkaPath))
                    {
                        Directory.CreateDirectory(targetDir);
                        string prefix = arch + "/";

                        using (var archive = new ZipArchive(stream, ZipArchiveMode.Read))
                        {
                            foreach (var entry in archive.Entries)
                            {
                                string normalized = entry.FullName.Replace('\\', '/');
                                if (!normalized.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
                                    continue;
                                if (string.IsNullOrEmpty(entry.Name))
                                    continue;

                                string destFile = Path.Combine(targetDir, entry.Name);
                                if (!File.Exists(destFile) || new FileInfo(destFile).Length != entry.Length)
                                {
                                    entry.ExtractToFile(destFile, overwrite: true);
                                }
                            }
                        }

                        File.WriteAllText(markerPath, DateTime.UtcNow.ToString("O"));
                    }

                    // librdkafka.dll의 종속 네이티브 DLL(libcrypto, libssl, libcurl, zstd 등)을 같은 폴더에서 찾도록 설정
                    SetDllDirectory(targetDir);
                    Confluent.Kafka.Library.Load(librdkafkaPath);
                    _initialized = true;
                }
            }
        }
    }
}
