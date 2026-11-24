using System;
using System.IO;
using System.Xml.Serialization;

namespace KafkaLauncher
{
    public enum KafkaRunMode
    {
        Zookeeper,
        KRaft
    }

    public class LauncherSettings
    {
        public string KafkaRoot { get; set; } = string.Empty;
        public string CustomJavaHome { get; set; } = string.Empty;
        public KafkaRunMode RunMode { get; set; } = KafkaRunMode.Zookeeper;
        public string ZkConfigPath { get; set; } = string.Empty;
        public string KafkaConfigPath { get; set; } = string.Empty;
        public string KRaftConfigPath { get; set; } = string.Empty;
        public string HeapXms { get; set; } = "512M";
        public string HeapXmx { get; set; } = "1G";
        public bool OpenInNewWindow { get; set; } = false;
        public bool AutoScrollLog { get; set; } = true;
        public string BootstrapServer { get; set; } = "localhost:9092";
        public string LastClusterId { get; set; } = string.Empty;

        public static string SettingsFilePath =>
            Path.Combine(
                Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
                "KafkaLauncher",
                "settings.xml");

        public static LauncherSettings Load()
        {
            LauncherSettings settings = null;
            try
            {
                string path = SettingsFilePath;
                if (File.Exists(path))
                {
                    var serializer = new XmlSerializer(typeof(LauncherSettings));
                    using (var stream = File.OpenRead(path))
                    {
                        settings = serializer.Deserialize(stream) as LauncherSettings;
                    }
                }
            }
            catch
            {
                // 설정 파일이 손상된 경우 기본값으로 복구
                settings = null;
            }

            if (settings == null)
            {
                settings = new LauncherSettings();
            }

            settings.ApplyDefaultsForMissingPaths();
            return settings;
        }

        public void Save()
        {
            try
            {
                string path = SettingsFilePath;
                string dir = Path.GetDirectoryName(path);
                if (!string.IsNullOrEmpty(dir) && !Directory.Exists(dir))
                {
                    Directory.CreateDirectory(dir);
                }

                string tempPath = path + ".tmp";
                var serializer = new XmlSerializer(typeof(LauncherSettings));
                using (var stream = File.Create(tempPath))
                {
                    serializer.Serialize(stream, this);
                }

                if (File.Exists(path))
                {
                    File.Delete(path);
                }
                File.Move(tempPath, path);
            }
            catch
            {
                // 설정 저장 실패 시 앱 동작을 중단하지 않음
            }
        }

        public void ApplyDefaultsForMissingPaths()
        {
            if (string.IsNullOrWhiteSpace(HeapXms)) HeapXms = "512M";
            if (string.IsNullOrWhiteSpace(HeapXmx)) HeapXmx = "1G";
            if (string.IsNullOrWhiteSpace(BootstrapServer)) BootstrapServer = "localhost:9092";

            if (!string.IsNullOrWhiteSpace(KafkaRoot))
            {
                if (string.IsNullOrWhiteSpace(ZkConfigPath))
                    ZkConfigPath = Path.Combine(KafkaRoot, "config", "zookeeper.properties");

                if (string.IsNullOrWhiteSpace(KafkaConfigPath))
                    KafkaConfigPath = Path.Combine(KafkaRoot, "config", "server.properties");

                if (string.IsNullOrWhiteSpace(KRaftConfigPath))
                    KRaftConfigPath = ResolveDefaultKRaftConfigPath(KafkaRoot);
            }
        }

        public static string ResolveDefaultKRaftConfigPath(string kafkaRoot)
        {
            if (string.IsNullOrWhiteSpace(kafkaRoot))
                return string.Empty;

            // Kafka 3.x 기본 KRaft 설정 경로 우선 확인
            string kraft3Path = Path.Combine(kafkaRoot, "config", "kraft", "server.properties");
            if (File.Exists(kraft3Path))
                return kraft3Path;

            // Kafka 4.x 기본 경로 (KRaft 단일화) 또는 fallback
            string serverPath = Path.Combine(kafkaRoot, "config", "server.properties");
            if (File.Exists(serverPath))
                return serverPath;

            return kraft3Path;
        }
    }
}
