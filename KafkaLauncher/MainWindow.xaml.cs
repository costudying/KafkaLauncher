using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Threading;
using Microsoft.Win32;
using WinForms = System.Windows.Forms;

namespace KafkaLauncher
{
    /// <summary>
    /// MainWindow.xaml에 대한 상호 작용 논리
    /// </summary>
    public partial class MainWindow : Window
    {
        private const int MaxLogLines = 2000;
        private const int LogTrimThreshold = 2400;
        private const int MaxLiveMessages = 1000;

        private readonly KafkaProcessManager _manager = new KafkaProcessManager();
        private readonly KafkaExplorerService _explorer = new KafkaExplorerService();
        private readonly KafkaLiveConsumer _liveConsumer = new KafkaLiveConsumer();

        private readonly Queue<string> _logBuffer = new Queue<string>(LogTrimThreshold + 16);
        private readonly List<string> _pendingLogLines = new List<string>(256);
        private readonly object _logLock = new object();
        private bool _logNeedsFullRefresh = false;

        private readonly Queue<KafkaMessageRecord> _pendingLiveMessages = new Queue<KafkaMessageRecord>(256);
        private readonly object _liveLock = new object();
        private readonly ObservableCollection<KafkaMessageRecord> _liveMessages = new ObservableCollection<KafkaMessageRecord>();
        private readonly DispatcherTimer _uiFlushTimer;

        private LauncherSettings _settings;
        private List<KafkaTopicSummary> _allTopics = new List<KafkaTopicSummary>();
        private List<KafkaMessageRecord> _fetchedMessages = new List<KafkaMessageRecord>();
        private List<KafkaPartitionInfo> _currentPartitions = new List<KafkaPartitionInfo>();
        private int _liveMessageTotalCount = 0;
        private bool _isInitializing = true;
        private bool _isClosing = false;

        private static readonly SolidColorBrush RunningDotBrush = new SolidColorBrush(Color.FromRgb(0x34, 0xC3, 0x8F));
        private static readonly SolidColorBrush IdleDotBrush = new SolidColorBrush(Color.FromRgb(0x5A, 0x64, 0x78));
        private static readonly SolidColorBrush ErrorDotBrush = new SolidColorBrush(Color.FromRgb(0xE0, 0x5A, 0x5A));
        private static readonly SolidColorBrush RunningTextBrush = new SolidColorBrush(Color.FromRgb(0xE8, 0xEC, 0xF3));
        private static readonly SolidColorBrush IdleTextBrush = new SolidColorBrush(Color.FromRgb(0x88, 0x92, 0xA6));

        public MainWindow()
        {
            InitializeComponent();

            _settings = LauncherSettings.Load();
            ApplySettingsToUi(_settings);
            _isInitializing = false;

            ApplyRunModeUi(GetSelectedRunMode());
            UpdateProcessStateUi(zkRunning: false, kafkaRunning: false);
            ResetPartitionDropdowns(0);

            DgLiveMessages.ItemsSource = _liveMessages;

            _manager.OutputReceived += (s, line) => AppendLog(line);
            _manager.ErrorReceived += (s, line) => AppendLog("[ERR] " + line);
            _manager.StateChanged += (s, state) => SafeUiInvoke(() => TxtStatus.Text = state);
            _manager.ZookeeperStateChanged += (s, running) => SafeUiInvoke(() => UpdateZookeeperUi(running));
            _manager.KafkaStateChanged += (s, running) => SafeUiInvoke(() => UpdateKafkaUi(running));

            _liveConsumer.MessageReceived += (s, record) => EnqueueLiveMessage(record);
            _liveConsumer.ErrorReceived += (s, err) => AppendLog("[LiveConsumer] " + err);
            _liveConsumer.StateChanged += (s, running) => SafeUiInvoke(() => UpdateConsumerUi(running));

            // 대량 로그/메시지 폭주 시 UI 스레드 멈춤(프리징)을 막기 위한 100ms 배치 플러시 타이머
            _uiFlushTimer = new DispatcherTimer(DispatcherPriority.Background, Dispatcher)
            {
                Interval = TimeSpan.FromMilliseconds(100)
            };
            _uiFlushTimer.Tick += UiFlushTimer_Tick;
            _uiFlushTimer.Start();

            Loaded += async (s, e) => await CheckJavaEnvironmentAsync(logResult: true);
        }

        // ======================================
        // 설정 로드 / 저장 / UI 반영
        // ======================================
        private void ApplySettingsToUi(LauncherSettings s)
        {
            TxtKafkaRoot.Text = s.KafkaRoot ?? string.Empty;
            TxtJavaHome.Text = s.CustomJavaHome ?? string.Empty;
            TxtZkConfig.Text = s.ZkConfigPath ?? string.Empty;
            TxtKafkaConfig.Text = s.KafkaConfigPath ?? string.Empty;
            TxtKRaftConfig.Text = s.KRaftConfigPath ?? string.Empty;
            TxtClusterId.Text = s.LastClusterId ?? string.Empty;
            TxtXms.Text = s.HeapXms ?? "512M";
            TxtXmx.Text = s.HeapXmx ?? "1G";
            ChkNewWindow.IsChecked = s.OpenInNewWindow;
            ChkAutoScroll.IsChecked = s.AutoScrollLog;
            TxtBootstrapServer.Text = s.BootstrapServer ?? "localhost:9092";

            if (s.RunMode == KafkaRunMode.KRaft)
                RbModeKRaft.IsChecked = true;
            else
                RbModeZk.IsChecked = true;
        }

        private void CaptureSettingsFromUi()
        {
            _settings.KafkaRoot = TxtKafkaRoot.Text?.Trim() ?? string.Empty;
            _settings.CustomJavaHome = KafkaProcessManager.NormalizeJavaHome(TxtJavaHome.Text);
            _settings.RunMode = GetSelectedRunMode();
            _settings.ZkConfigPath = TxtZkConfig.Text?.Trim() ?? string.Empty;
            _settings.KafkaConfigPath = TxtKafkaConfig.Text?.Trim() ?? string.Empty;
            _settings.KRaftConfigPath = TxtKRaftConfig.Text?.Trim() ?? string.Empty;
            _settings.LastClusterId = TxtClusterId.Text?.Trim() ?? string.Empty;
            _settings.HeapXms = TxtXms.Text?.Trim() ?? "512M";
            _settings.HeapXmx = TxtXmx.Text?.Trim() ?? "1G";
            _settings.OpenInNewWindow = ChkNewWindow.IsChecked == true;
            _settings.AutoScrollLog = ChkAutoScroll.IsChecked == true;
            _settings.BootstrapServer = TxtBootstrapServer.Text?.Trim() ?? "localhost:9092";
        }

        private KafkaRunMode GetSelectedRunMode()
        {
            return RbModeKRaft != null && RbModeKRaft.IsChecked == true
                ? KafkaRunMode.KRaft
                : KafkaRunMode.Zookeeper;
        }

        private void RunMode_Changed(object sender, RoutedEventArgs e)
        {
            if (_isInitializing) return;
            ApplyRunModeUi(GetSelectedRunMode());
        }

        private void ApplyRunModeUi(KafkaRunMode mode)
        {
            bool isKraft = mode == KafkaRunMode.KRaft;

            TxtModeBadge.Text = isKraft ? "KRaft" : "Zookeeper";
            BadgeMode.Background = isKraft
                ? new SolidColorBrush(Color.FromRgb(0xF5, 0xA6, 0x23))
                : new SolidColorBrush(Color.FromRgb(0x4F, 0x8E, 0xF7));

            Visibility zkVis = isKraft ? Visibility.Collapsed : Visibility.Visible;
            Visibility kraftVis = isKraft ? Visibility.Visible : Visibility.Collapsed;

            LblZkConfig.Visibility = zkVis;
            TxtZkConfig.Visibility = zkVis;
            BtnBrowseZkConfig.Visibility = zkVis;
            LblKafkaConfig.Visibility = zkVis;
            TxtKafkaConfig.Visibility = zkVis;
            BtnBrowseKafkaConfig.Visibility = zkVis;

            LblKRaftConfig.Visibility = kraftVis;
            TxtKRaftConfig.Visibility = kraftVis;
            BtnBrowseKRaftConfig.Visibility = kraftVis;
            LblClusterId.Visibility = kraftVis;
            TxtClusterId.Visibility = kraftVis;

            PillZk.Visibility = zkVis;
            BtnStartZk.Visibility = zkVis;
            BtnFormatKRaft.Visibility = kraftVis;

            BtnStartKafka.Content = isKraft ? "Kafka (KRaft) 시작" : "Kafka 시작";
            BtnStartKafka.Tag = isKraft ? "Kafka (KRaft) 중지" : "Kafka 중지";
        }

        private void BtnSaveSettings_Click(object sender, RoutedEventArgs e)
        {
            CaptureSettingsFromUi();
            _settings.Save();
            AppendLog($"[설정] 저장 완료: {LauncherSettings.SettingsFilePath}");
            TxtStatus.Text = "설정 저장됨";
        }

        // ======================================
        // 경로 찾아보기
        // ======================================
        private void BtnBrowseRoot_Click(object sender, RoutedEventArgs e)
        {
            using (var dlg = new WinForms.FolderBrowserDialog())
            {
                dlg.Description = "Kafka Root 폴더 선택 (예: .../kafka)";
                if (Directory.Exists(TxtKafkaRoot.Text))
                {
                    dlg.SelectedPath = TxtKafkaRoot.Text;
                }

                if (dlg.ShowDialog() == WinForms.DialogResult.OK)
                {
                    string selected = dlg.SelectedPath;
                    TxtKafkaRoot.Text = selected;
                    TxtZkConfig.Text = Path.Combine(selected, "config", "zookeeper.properties");
                    TxtKafkaConfig.Text = Path.Combine(selected, "config", "server.properties");
                    TxtKRaftConfig.Text = LauncherSettings.ResolveDefaultKRaftConfigPath(selected);

                    CaptureSettingsFromUi();
                    _settings.Save();
                    AppendLog($"[설정] Kafka Root 변경됨: {selected}");
                }
            }
        }

        private async void BtnBrowseJavaHome_Click(object sender, RoutedEventArgs e)
        {
            using (var dlg = new WinForms.FolderBrowserDialog())
            {
                dlg.Description = "Portable JDK / JAVA_HOME 폴더 선택 (예: .../jdk-17)";
                string current = KafkaProcessManager.NormalizeJavaHome(TxtJavaHome.Text);
                if (!string.IsNullOrEmpty(current) && Directory.Exists(current))
                {
                    dlg.SelectedPath = current;
                }

                if (dlg.ShowDialog() == WinForms.DialogResult.OK)
                {
                    string normalized = KafkaProcessManager.NormalizeJavaHome(dlg.SelectedPath);
                    TxtJavaHome.Text = normalized;
                    CaptureSettingsFromUi();
                    _settings.Save();
                    await CheckJavaEnvironmentAsync(logResult: true);
                }
            }
        }

        private async void BtnResetJavaHome_Click(object sender, RoutedEventArgs e)
        {
            TxtJavaHome.Text = string.Empty;
            CaptureSettingsFromUi();
            _settings.Save();
            await CheckJavaEnvironmentAsync(logResult: true);
        }

        private async void BtnCheckJava_Click(object sender, RoutedEventArgs e)
        {
            string normalized = KafkaProcessManager.NormalizeJavaHome(TxtJavaHome.Text);
            if (!string.IsNullOrEmpty(TxtJavaHome.Text))
                TxtJavaHome.Text = normalized;

            var res = await CheckJavaEnvironmentAsync(logResult: true);
            MessageBox.Show(
                res.Message,
                res.IsAvailable ? "Java 환경 정상" : "Java 환경 진단",
                MessageBoxButton.OK,
                res.IsAvailable ? MessageBoxImage.Information : MessageBoxImage.Warning);
        }

        private async Task<JavaCheckResult> CheckJavaEnvironmentAsync(bool logResult)
        {
            string customJavaHome = TxtJavaHome.Text?.Trim();
            TxtJavaBadge.Text = "Java: 확인 중...";
            DotJava.Fill = IdleDotBrush;

            var res = await KafkaProcessManager.DetectJavaAsync(customJavaHome);
            if (res.IsAvailable)
            {
                DotJava.Fill = RunningDotBrush;
                TxtJavaBadge.Text = $"Java {res.Version}";
                TxtJavaBadge.Foreground = RunningTextBrush;
                PillJava.ToolTip = res.Message;

                TxtJavaDetail.Text = $"{res.Source}: Java {res.Version}";
                TxtJavaDetail.Foreground = RunningDotBrush;
            }
            else
            {
                DotJava.Fill = ErrorDotBrush;
                TxtJavaBadge.Text = "Java 미설치";
                TxtJavaBadge.Foreground = ErrorDotBrush;
                PillJava.ToolTip = res.Message;

                TxtJavaDetail.Text = "Java 미감지 (로컬 서버 실행 시 Portable JDK 지정 필요 · Offset Explorer는 사용 가능)";
                TxtJavaDetail.Foreground = new SolidColorBrush(Color.FromRgb(0xF5, 0xA6, 0x23));
            }

            if (logResult)
            {
                AppendLog("[Java] " + res.Message);
            }

            return res;
        }

        private async Task<bool> EnsureJavaReadyForLocalServerAsync()
        {
            var res = await CheckJavaEnvironmentAsync(logResult: false);
            if (res.IsAvailable) return true;

            AppendLog("[Java Error] " + res.Message);
            MessageBox.Show(
                res.Message + "\n\n" +
                "해결 방법:\n" +
                "1) PC에 JDK(Java 11/17 등)를 설치하거나,\n" +
                "2) 압축만 푼 Portable JDK(예: Eclipse Temurin) 폴더를 '실행 설정 → JDK 경로 (선택)'에 지정해 주세요.\n\n" +
                "※ 원격/외부 Kafka 서버 접속(Offset Explorer 탭)은 Java 없이도 바로 사용할 수 있습니다.",
                "Java(JDK) 필요",
                MessageBoxButton.OK,
                MessageBoxImage.Warning);
            return false;
        }

        private void BtnBrowseZkConfig_Click(object sender, RoutedEventArgs e) =>
            BrowsePropertiesFile("Zookeeper 설정 파일 선택", TxtZkConfig);

        private void BtnBrowseKafkaConfig_Click(object sender, RoutedEventArgs e) =>
            BrowsePropertiesFile("Kafka 설정 파일 선택", TxtKafkaConfig);

        private void BtnBrowseKRaftConfig_Click(object sender, RoutedEventArgs e) =>
            BrowsePropertiesFile("KRaft 설정 파일 선택", TxtKRaftConfig);

        private void BrowsePropertiesFile(string title, TextBox targetBox)
        {
            var dlg = new OpenFileDialog
            {
                Title = title,
                Filter = "Properties Files (*.properties)|*.properties|All Files (*.*)|*.*"
            };

            string current = targetBox.Text;
            if (!string.IsNullOrWhiteSpace(current))
            {
                string dir = Path.GetDirectoryName(current);
                if (!string.IsNullOrEmpty(dir) && Directory.Exists(dir))
                    dlg.InitialDirectory = dir;
            }

            if (dlg.ShowDialog() == true)
            {
                targetBox.Text = dlg.FileName;
            }
        }

        // ======================================
        // 런처 프로세스 제어
        // ======================================
        private async void BtnStartZk_Click(object sender, RoutedEventArgs e)
        {
            BtnStartZk.IsEnabled = false;
            try
            {
                if (_manager.IsZookeeperRunning)
                {
                    await _manager.StopZookeeperAsync();
                }
                else
                {
                    if (!await EnsureJavaReadyForLocalServerAsync()) return;

                    string root = TxtKafkaRoot.Text.Trim();
                    string zkBat = Path.Combine(root, "bin", "windows", "zookeeper-server-start.bat");
                    string zkCfg = TxtZkConfig.Text.Trim();

                    ValidateFile(zkBat, "zookeeper-server-start.bat");
                    ValidateFile(zkCfg, "zookeeper.properties");

                    await _manager.StartZookeeperAsync(zkBat, zkCfg, ChkNewWindow.IsChecked == true, TxtJavaHome.Text.Trim());
                }
            }
            catch (Exception ex)
            {
                AppendLog("[Start/Stop ZK Error] " + ex.Message);
            }
            finally
            {
                UpdateZookeeperUi(_manager.IsZookeeperRunning);
                BtnStartZk.IsEnabled = true;
            }
        }

        private async void BtnStartKafka_Click(object sender, RoutedEventArgs e)
        {
            BtnStartKafka.IsEnabled = false;
            try
            {
                if (_manager.IsKafkaRunning)
                {
                    await _manager.StopKafkaAsync();
                }
                else
                {
                    if (!await EnsureJavaReadyForLocalServerAsync()) return;

                    KafkaRunMode mode = GetSelectedRunMode();
                    string root = TxtKafkaRoot.Text.Trim();
                    string kafkaBat = Path.Combine(root, "bin", "windows", "kafka-server-start.bat");
                    string kafkaCfg = mode == KafkaRunMode.KRaft
                        ? TxtKRaftConfig.Text.Trim()
                        : TxtKafkaConfig.Text.Trim();

                    ValidateFile(kafkaBat, "kafka-server-start.bat");
                    ValidateFile(kafkaCfg, mode == KafkaRunMode.KRaft ? "KRaft server.properties" : "server.properties");

                    string xms = string.IsNullOrWhiteSpace(TxtXms.Text) ? "512M" : TxtXms.Text.Trim();
                    string xmx = string.IsNullOrWhiteSpace(TxtXmx.Text) ? "1G" : TxtXmx.Text.Trim();
                    string heap = $"-Xms{xms} -Xmx{xmx}";

                    await _manager.StartKafkaAsync(kafkaBat, kafkaCfg, heap, ChkNewWindow.IsChecked == true, mode, TxtJavaHome.Text.Trim());
                }
            }
            catch (Exception ex)
            {
                AppendLog("[Start/Stop Kafka Error] " + ex.Message);
            }
            finally
            {
                UpdateKafkaUi(_manager.IsKafkaRunning);
                BtnStartKafka.IsEnabled = true;
            }
        }

        private async void BtnFormatKRaft_Click(object sender, RoutedEventArgs e)
        {
            if (_manager.IsKafkaRunning)
            {
                MessageBox.Show("Kafka가 실행 중입니다. 먼저 Kafka를 중지한 후 포맷하세요.", "안내", MessageBoxButton.OK, MessageBoxImage.Warning);
                return;
            }

            BtnFormatKRaft.IsEnabled = false;
            try
            {
                if (!await EnsureJavaReadyForLocalServerAsync()) return;

                var result = await _manager.FormatKRaftStorageAsync(
                    TxtKafkaRoot.Text.Trim(),
                    TxtKRaftConfig.Text.Trim(),
                    TxtClusterId.Text.Trim(),
                    ignoreFormatted: true,
                    customJavaHome: TxtJavaHome.Text.Trim());

                if (result.Success)
                {
                    if (!string.IsNullOrWhiteSpace(result.ClusterId))
                    {
                        TxtClusterId.Text = result.ClusterId;
                        CaptureSettingsFromUi();
                        _settings.Save();
                    }
                    MessageBox.Show(result.Message, "KRaft 스토리지 포맷 완료", MessageBoxButton.OK, MessageBoxImage.Information);
                }
                else
                {
                    MessageBox.Show(result.Message, "KRaft 스토리지 포맷 실패", MessageBoxButton.OK, MessageBoxImage.Error);
                }
            }
            catch (Exception ex)
            {
                AppendLog("[KRaft Format Error] " + ex.Message);
                MessageBox.Show(ex.Message, "KRaft 스토리지 포맷 오류", MessageBoxButton.OK, MessageBoxImage.Error);
            }
            finally
            {
                BtnFormatKRaft.IsEnabled = true;
            }
        }

        private async void BtnStopAll_Click(object sender, RoutedEventArgs e)
        {
            BtnStopAll.IsEnabled = false;
            BtnStartZk.IsEnabled = false;
            BtnStartKafka.IsEnabled = false;
            try
            {
                await _liveConsumer.StopAsync();
                await _manager.StopAllAsync();
            }
            catch (Exception ex)
            {
                AppendLog("[StopAll Error] " + ex.Message);
            }
            finally
            {
                UpdateProcessStateUi(_manager.IsZookeeperRunning, _manager.IsKafkaRunning);
                UpdateConsumerUi(_liveConsumer.IsRunning);
                BtnStopAll.IsEnabled = true;
                BtnStartZk.IsEnabled = true;
                BtnStartKafka.IsEnabled = true;
            }
        }

        private async void BtnCleanData_Click(object sender, RoutedEventArgs e)
        {
            if (_manager.IsZookeeperRunning || _manager.IsKafkaRunning)
            {
                MessageBox.Show(
                    "실행 중인 Zookeeper 또는 Kafka 프로세스가 있습니다.\n먼저 '모두 종료'를 눌러 프로세스를 종료한 뒤 데이터 초기화를 진행해 주세요.",
                    "프로세스 실행 중",
                    MessageBoxButton.OK,
                    MessageBoxImage.Warning);
                return;
            }

            KafkaRunMode mode = GetSelectedRunMode();
            var targets = new List<string>();

            if (mode == KafkaRunMode.KRaft)
            {
                targets.AddRange(KafkaProcessManager.ParsePropertyDirectories(TxtKRaftConfig.Text.Trim(), "log.dirs"));
            }
            else
            {
                targets.AddRange(KafkaProcessManager.ParsePropertyDirectories(TxtKafkaConfig.Text.Trim(), "log.dirs"));
                targets.AddRange(KafkaProcessManager.ParsePropertyDirectories(TxtZkConfig.Text.Trim(), "dataDir"));
            }

            targets = targets.Distinct(StringComparer.OrdinalIgnoreCase).ToList();
            if (targets.Count == 0)
            {
                MessageBox.Show(
                    "현재 설정 파일(.properties)에서 log.dirs 또는 dataDir 경로를 찾지 못했습니다.",
                    "초기화 대상 없음",
                    MessageBoxButton.OK,
                    MessageBoxImage.Information);
                return;
            }

            string listText = string.Join("\n", targets.Select(d => " • " + d));
            string extraNote = mode == KafkaRunMode.KRaft
                ? "\n\n※ KRaft 모드에서는 초기화 후 'KRaft 스토리지 포맷'을 다시 1회 실행해야 합니다."
                : string.Empty;

            var confirm = MessageBox.Show(
                $"아래 로컬 데이터 디렉토리의 모든 토픽·메시지·스냅샷 파일을 삭제합니다.\n계속하시겠습니까?\n\n{listText}{extraNote}",
                "로컬 데이터 초기화 확인",
                MessageBoxButton.YesNo,
                MessageBoxImage.Warning);

            if (confirm != MessageBoxResult.Yes) return;

            BtnCleanData.IsEnabled = false;
            try
            {
                string result = await KafkaProcessManager.CleanDataDirectoriesAsync(targets);
                AppendLog("[Clean Reset]\n" + result);
                if (mode == KafkaRunMode.KRaft)
                {
                    TxtClusterId.Clear();
                }
                MessageBox.Show(result, "데이터 초기화 결과", MessageBoxButton.OK, MessageBoxImage.Information);
            }
            catch (Exception ex)
            {
                AppendLog("[Clean Reset Error] " + ex.Message);
                MessageBox.Show(ex.Message, "데이터 초기화 오류", MessageBoxButton.OK, MessageBoxImage.Error);
            }
            finally
            {
                BtnCleanData.IsEnabled = true;
            }
        }

        private void UpdateProcessStateUi(bool zkRunning, bool kafkaRunning)
        {
            UpdateZookeeperUi(zkRunning);
            UpdateKafkaUi(kafkaRunning);
        }

        private void UpdateZookeeperUi(bool running)
        {
            BtnStartZk.IsChecked = running;
            DotZk.Fill = running ? RunningDotBrush : IdleDotBrush;
            TxtZkState.Text = running ? "ZK: 실행 중" : "ZK: 대기";
            TxtZkState.Foreground = running ? RunningTextBrush : IdleTextBrush;
        }

        private void UpdateKafkaUi(bool running)
        {
            BtnStartKafka.IsChecked = running;
            DotKafka.Fill = running ? RunningDotBrush : IdleDotBrush;
            TxtKafkaState.Text = running ? "Kafka: 실행 중" : "Kafka: 대기";
            TxtKafkaState.Foreground = running ? RunningTextBrush : IdleTextBrush;
        }

        // ======================================
        // 런처 실행 로그 (100ms 배치 플러시로 UI 프리징 방지)
        // ======================================
        private void AppendLog(string line)
        {
            if (_isClosing) return;

            string stamped = $"[{DateTime.Now:HH:mm:ss}] {line}";
            lock (_logLock)
            {
                _logBuffer.Enqueue(stamped);
                if (_logBuffer.Count > LogTrimThreshold)
                {
                    while (_logBuffer.Count > MaxLogLines)
                        _logBuffer.Dequeue();
                    _pendingLogLines.Clear();
                    _logNeedsFullRefresh = true;
                }
                else if (!_logNeedsFullRefresh)
                {
                    _pendingLogLines.Add(stamped);
                }
            }
        }

        private void UiFlushTimer_Tick(object sender, EventArgs e)
        {
            if (_isClosing) return;
            FlushPendingLogs();
            FlushPendingLiveMessages();
        }

        private void FlushPendingLogs()
        {
            string batchText = null;
            bool fullRefresh = false;
            int count = 0;

            lock (_logLock)
            {
                if (_logNeedsFullRefresh)
                {
                    fullRefresh = true;
                    _logNeedsFullRefresh = false;
                    _pendingLogLines.Clear();
                    batchText = _logBuffer.Count > 0
                        ? string.Join(Environment.NewLine, _logBuffer) + Environment.NewLine
                        : string.Empty;
                    count = _logBuffer.Count;
                }
                else if (_pendingLogLines.Count > 0)
                {
                    batchText = string.Join(Environment.NewLine, _pendingLogLines) + Environment.NewLine;
                    _pendingLogLines.Clear();
                    count = _logBuffer.Count;
                }
            }

            if (batchText == null) return;

            if (fullRefresh)
                TxtLog.Text = batchText;
            else
                TxtLog.AppendText(batchText);

            TxtLogCount.Text = $"({count} lines)";

            if (ChkAutoScroll.IsChecked == true)
            {
                TxtLog.ScrollToEnd();
            }
        }

        private void BtnClearLog_Click(object sender, RoutedEventArgs e)
        {
            lock (_logLock)
            {
                _logBuffer.Clear();
                _pendingLogLines.Clear();
                _logNeedsFullRefresh = false;
            }
            TxtLog.Clear();
            TxtLogCount.Text = "(0 lines)";
        }

        private void SafeUiInvoke(Action action)
        {
            if (_isClosing || Dispatcher.HasShutdownStarted || Dispatcher.HasShutdownFinished)
                return;

            Dispatcher.BeginInvoke(action);
        }

        // ======================================
        // Offset Explorer: 클러스터 연결 & 토픽 목록
        // ======================================
        private KafkaTopicSummary SelectedTopic => LstTopics.SelectedItem as KafkaTopicSummary;

        private async void BtnConnectCluster_Click(object sender, RoutedEventArgs e)
        {
            await RefreshClusterAsync();
        }

        private async Task RefreshClusterAsync(string reselectTopicName = null)
        {
            SetExplorerButtonsEnabled(false);
            TxtClusterStatus.Text = "클러스터 연결 중...";
            try
            {
                string bootstrap = TxtBootstrapServer.Text.Trim();
                var meta = await _explorer.GetClusterMetadataAsync(bootstrap);
                _allTopics = meta.Topics;

                string targetSelection = reselectTopicName ?? SelectedTopic?.Name;
                ApplyTopicFilter(targetSelection);

                DotCluster.Fill = RunningDotBrush;
                TxtClusterStatus.Foreground = RunningTextBrush;
                TxtClusterStatus.Text = $"연결됨 · Broker {meta.BrokerCount}대 ({meta.BrokerSummary}) · 토픽 {_allTopics.Count}개";
                AppendLog($"[Explorer] 클러스터 연결 성공 ({meta.BrokerSummary}, 토픽 {_allTopics.Count}개)");
            }
            catch (Exception ex)
            {
                DotCluster.Fill = ErrorDotBrush;
                TxtClusterStatus.Foreground = IdleTextBrush;
                TxtClusterStatus.Text = "연결 실패";
                AppendLog("[Explorer Connect Error] " + ex.Message);
                MessageBox.Show(ex.Message, "클러스터 연결 실패", MessageBoxButton.OK, MessageBoxImage.Error);
            }
            finally
            {
                SetExplorerButtonsEnabled(true);
            }
        }

        private void ChkHideInternalTopics_Changed(object sender, RoutedEventArgs e)
        {
            if (_isInitializing) return;
            ApplyTopicFilter(SelectedTopic?.Name);
        }

        private void TxtTopicFilter_TextChanged(object sender, TextChangedEventArgs e)
        {
            ApplyTopicFilter(SelectedTopic?.Name);
        }

        private void ApplyTopicFilter(string preserveTopicName = null)
        {
            string keyword = TxtTopicFilter?.Text?.Trim() ?? string.Empty;
            bool hideInternal = ChkHideInternalTopics?.IsChecked == true;

            IEnumerable<KafkaTopicSummary> query = _allTopics;
            if (hideInternal)
                query = query.Where(t => !t.IsInternal);
            if (!string.IsNullOrEmpty(keyword))
                query = query.Where(t => t.Name.IndexOf(keyword, StringComparison.OrdinalIgnoreCase) >= 0);

            var list = query.ToList();
            LstTopics.ItemsSource = list;
            TxtTopicCount.Text = $"{list.Count}개";

            if (!string.IsNullOrEmpty(preserveTopicName))
            {
                var match = list.FirstOrDefault(t => string.Equals(t.Name, preserveTopicName, StringComparison.Ordinal));
                if (match != null)
                {
                    LstTopics.SelectedItem = match;
                }
            }
        }

        private async void LstTopics_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            var topic = SelectedTopic;
            if (topic == null)
            {
                ResetPartitionDropdowns(0);
                return;
            }

            ResetPartitionDropdowns(topic.PartitionCount);
            await RefreshPartitionsForSelectedTopicAsync(showErrorDialog: false);
        }

        private void ResetPartitionDropdowns(int partitionCount)
        {
            CboPartition.Items.Clear();
            CboPartition.Items.Add("전체");
            for (int i = 0; i < partitionCount; i++)
                CboPartition.Items.Add($"P{i}");
            CboPartition.SelectedIndex = 0;

            CboProducePartition.Items.Clear();
            CboProducePartition.Items.Add("자동");
            for (int i = 0; i < partitionCount; i++)
                CboProducePartition.Items.Add($"P{i}");
            CboProducePartition.SelectedIndex = 0;
        }

        private async void BtnCreateTopic_Click(object sender, RoutedEventArgs e)
        {
            string topicName = TxtNewTopicName.Text.Trim();
            if (!int.TryParse(TxtNewTopicPartitions.Text.Trim(), out int partitions) || partitions < 1)
            {
                MessageBox.Show("파티션 수는 1 이상의 정수로 입력하세요.", "입력 오류", MessageBoxButton.OK, MessageBoxImage.Warning);
                return;
            }
            if (!int.TryParse(TxtNewTopicReplication.Text.Trim(), out int replication) || replication < 1)
            {
                MessageBox.Show("복제 팩터는 1 이상의 정수로 입력하세요.", "입력 오류", MessageBoxButton.OK, MessageBoxImage.Warning);
                return;
            }

            SetExplorerButtonsEnabled(false);
            try
            {
                await _explorer.CreateTopicAsync(TxtBootstrapServer.Text.Trim(), topicName, partitions, replication);
                AppendLog($"[Explorer] 토픽 '{topicName}' 생성 완료 (P={partitions}, R={replication})");
                TxtNewTopicName.Clear();
                await RefreshClusterAsync(reselectTopicName: topicName);
            }
            catch (Exception ex)
            {
                AppendLog("[Topic Create Error] " + ex.Message);
                MessageBox.Show(ex.Message, "토픽 생성 실패", MessageBoxButton.OK, MessageBoxImage.Error);
            }
            finally
            {
                SetExplorerButtonsEnabled(true);
            }
        }

        private async void BtnDeleteTopic_Click(object sender, RoutedEventArgs e)
        {
            var topic = SelectedTopic;
            if (topic == null)
            {
                MessageBox.Show("삭제할 토픽을 목록에서 선택하세요.", "안내", MessageBoxButton.OK, MessageBoxImage.Information);
                return;
            }

            var confirm = MessageBox.Show(
                $"토픽 '{topic.Name}' 을(를) 정말 삭제하시겠습니까?",
                "토픽 삭제 확인",
                MessageBoxButton.YesNo,
                MessageBoxImage.Warning);

            if (confirm != MessageBoxResult.Yes) return;

            SetExplorerButtonsEnabled(false);
            try
            {
                await _explorer.DeleteTopicAsync(TxtBootstrapServer.Text.Trim(), topic.Name);
                AppendLog($"[Explorer] 토픽 '{topic.Name}' 삭제 요청 완료");
                _fetchedMessages.Clear();
                DgMessages.ItemsSource = null;
                DgPartitions.ItemsSource = null;
                await Task.Delay(300);
                await RefreshClusterAsync();
            }
            catch (Exception ex)
            {
                AppendLog("[Topic Delete Error] " + ex.Message);
                MessageBox.Show(ex.Message, "토픽 삭제 실패", MessageBoxButton.OK, MessageBoxImage.Error);
            }
            finally
            {
                SetExplorerButtonsEnabled(true);
            }
        }

        // ======================================
        // 서브탭 1: 메시지 탐색 (Offset Explorer)
        // ======================================
        private void CboFetchMode_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            if (PnlSpecificOffset == null) return;
            PnlSpecificOffset.Visibility = CboFetchMode.SelectedIndex == 2 ? Visibility.Visible : Visibility.Collapsed;
        }

        private int GetSelectedMaxMessages()
        {
            string text = (CboMaxMessages.SelectedItem as ComboBoxItem)?.Content?.ToString() ?? "50";
            return int.TryParse(text, out int n) ? n : 50;
        }

        private int? GetSelectedPartitionFilter()
        {
            if (CboPartition.SelectedIndex <= 0) return null;
            return CboPartition.SelectedIndex - 1;
        }

        private MessageFetchMode GetSelectedFetchMode()
        {
            switch (CboFetchMode.SelectedIndex)
            {
                case 1: return MessageFetchMode.Earliest;
                case 2: return MessageFetchMode.SpecificOffset;
                default: return MessageFetchMode.Latest;
            }
        }

        private async void BtnOffsetPrev_Click(object sender, RoutedEventArgs e)
        {
            if (!long.TryParse(TxtSpecificOffset.Text.Trim(), out long current)) current = 0;
            long next = Math.Max(0, current - GetSelectedMaxMessages());
            TxtSpecificOffset.Text = next.ToString();
            await FetchMessagesAsync();
        }

        private async void BtnOffsetNext_Click(object sender, RoutedEventArgs e)
        {
            if (!long.TryParse(TxtSpecificOffset.Text.Trim(), out long current)) current = 0;
            long next = current + GetSelectedMaxMessages();
            TxtSpecificOffset.Text = next.ToString();
            await FetchMessagesAsync();
        }

        private async void BtnFetchMessages_Click(object sender, RoutedEventArgs e)
        {
            await FetchMessagesAsync();
        }

        private async Task FetchMessagesAsync()
        {
            var topic = SelectedTopic;
            if (topic == null)
            {
                MessageBox.Show("조회할 토픽을 좌측 목록에서 먼저 선택하세요.", "안내", MessageBoxButton.OK, MessageBoxImage.Information);
                return;
            }

            MessageFetchMode mode = GetSelectedFetchMode();
            long specificOffset = 0;
            if (mode == MessageFetchMode.SpecificOffset)
            {
                if (!long.TryParse(TxtSpecificOffset.Text.Trim(), out specificOffset) || specificOffset < 0)
                {
                    MessageBox.Show("시작 오프셋은 0 이상의 정수로 입력하세요.", "입력 오류", MessageBoxButton.OK, MessageBoxImage.Warning);
                    return;
                }
            }

            int maxMessages = GetSelectedMaxMessages();
            int? partitionFilter = GetSelectedPartitionFilter();

            SetExplorerButtonsEnabled(false);
            TxtExplorerSummary.Text = $"[{topic.Name}] 메시지 조회 중...";
            try
            {
                var res = await _explorer.FetchMessagesAsync(
                    TxtBootstrapServer.Text.Trim(),
                    topic.Name,
                    partitionFilter,
                    mode,
                    specificOffset,
                    maxMessages);

                _fetchedMessages = res.Messages;
                ApplyMessageKeywordFilter();

                TxtExplorerSummary.Text = $"[{topic.Name}] {res.Summary}";
                if (DgMessages.Items.Count > 0)
                {
                    DgMessages.SelectedIndex = 0;
                }
                else
                {
                    RenderMessageDetail(null);
                }
            }
            catch (Exception ex)
            {
                TxtExplorerSummary.Text = "[조회 실패] " + ex.Message;
                AppendLog("[Message Fetch Error] " + ex.Message);
                MessageBox.Show(ex.Message, "메시지 조회 실패", MessageBoxButton.OK, MessageBoxImage.Error);
            }
            finally
            {
                SetExplorerButtonsEnabled(true);
            }
        }

        private void TxtMessageKeyword_TextChanged(object sender, TextChangedEventArgs e)
        {
            ApplyMessageKeywordFilter();
        }

        private void ApplyMessageKeywordFilter()
        {
            string kw = TxtMessageKeyword?.Text?.Trim() ?? string.Empty;
            if (string.IsNullOrEmpty(kw))
            {
                DgMessages.ItemsSource = _fetchedMessages;
            }
            else
            {
                DgMessages.ItemsSource = _fetchedMessages
                    .Where(m =>
                        (m.KeyText != null && m.KeyText.IndexOf(kw, StringComparison.OrdinalIgnoreCase) >= 0) ||
                        (m.ValueText != null && m.ValueText.IndexOf(kw, StringComparison.OrdinalIgnoreCase) >= 0))
                    .ToList();
            }
        }

        private void DgMessages_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            RenderMessageDetail(DgMessages.SelectedItem as KafkaMessageRecord);
        }

        private void ExplorerViewMode_Changed(object sender, RoutedEventArgs e)
        {
            if (_isInitializing) return;
            RenderMessageDetail(DgMessages.SelectedItem as KafkaMessageRecord);
        }

        private void RenderMessageDetail(KafkaMessageRecord msg)
        {
            if (TxtDetailValue == null) return;

            if (msg == null)
            {
                TxtSelectedMsgMeta.Text = "메시지를 선택하면 상세 내용(JSON Pretty / Raw / Hex / Headers)이 표시됩니다.";
                TxtDetailKey.Text = string.Empty;
                TxtDetailValue.Text = string.Empty;
                return;
            }

            TxtSelectedMsgMeta.Text = $"Partition {msg.Partition}  |  Offset {msg.Offset}  |  {msg.TimestampText} ({msg.TimestampType})  |  {msg.SizeBytes} B  |  Headers: {msg.HeaderCount}";
            TxtDetailKey.Text = msg.KeyText;
            TxtDetailValue.Text = FormatMessageForView(
                msg,
                RbViewJson.IsChecked == true,
                RbViewHex.IsChecked == true,
                RbViewHeaders.IsChecked == true);
        }

        private static string FormatMessageForView(KafkaMessageRecord msg, bool json, bool hex, bool headers)
        {
            if (headers)
            {
                if (msg.Headers == null || msg.Headers.Count == 0)
                    return "(헤더 없음)";

                var sb = new StringBuilder();
                foreach (var h in msg.Headers)
                {
                    sb.AppendLine($"{h.Key} = {h.ValueText}");
                    if (!string.IsNullOrEmpty(h.ValueHex))
                        sb.AppendLine($"   HEX: {h.ValueHex}");
                }
                return sb.ToString().TrimEnd();
            }

            if (hex)
            {
                return KafkaExplorerService.FormatHexDump(msg.ValueBytes);
            }

            if (json)
            {
                return KafkaExplorerService.FormatJsonPretty(msg.ValueText);
            }

            return msg.ValueText ?? string.Empty;
        }

        private void BtnCopyValue_Click(object sender, RoutedEventArgs e)
        {
            if (!string.IsNullOrEmpty(TxtDetailValue.Text))
            {
                Clipboard.SetText(TxtDetailValue.Text);
                TxtExplorerSummary.Text = "선택한 메시지 내용을 클립보드에 복사했습니다.";
            }
        }

        // ======================================
        // 서브탭 2: 실시간 스트림 (Native Live Consumer)
        // ======================================
        private async void BtnToggleConsumer_Click(object sender, RoutedEventArgs e)
        {
            BtnToggleConsumer.IsEnabled = false;
            try
            {
                if (_liveConsumer.IsRunning)
                {
                    await _liveConsumer.StopAsync();
                    AppendLog("[LiveConsumer] 구독 중지");
                }
                else
                {
                    var topic = SelectedTopic;
                    if (topic == null)
                    {
                        MessageBox.Show("구독할 토픽을 좌측 목록에서 먼저 선택하세요.", "안내", MessageBoxButton.OK, MessageBoxImage.Information);
                        return;
                    }

                    bool fromBeginning = ChkFromBeginning.IsChecked == true;
                    _liveConsumer.Start(TxtBootstrapServer.Text.Trim(), topic.Name, fromBeginning);
                    AppendLog($"[LiveConsumer] '{topic.Name}' 실시간 구독 시작 (fromBeginning={fromBeginning})");
                }
            }
            catch (Exception ex)
            {
                AppendLog("[LiveConsumer Error] " + ex.Message);
                MessageBox.Show(ex.Message, "실시간 구독 오류", MessageBoxButton.OK, MessageBoxImage.Error);
            }
            finally
            {
                UpdateConsumerUi(_liveConsumer.IsRunning);
                BtnToggleConsumer.IsEnabled = true;
            }
        }

        private void EnqueueLiveMessage(KafkaMessageRecord record)
        {
            if (_isClosing || record == null) return;
            lock (_liveLock)
            {
                _pendingLiveMessages.Enqueue(record);
                while (_pendingLiveMessages.Count > MaxLiveMessages)
                {
                    _pendingLiveMessages.Dequeue();
                }
            }
        }

        private void FlushPendingLiveMessages()
        {
            List<KafkaMessageRecord> batch = null;
            lock (_liveLock)
            {
                if (_pendingLiveMessages.Count == 0) return;
                batch = new List<KafkaMessageRecord>(_pendingLiveMessages.Count);
                while (_pendingLiveMessages.Count > 0)
                {
                    batch.Add(_pendingLiveMessages.Dequeue());
                }
            }

            foreach (var record in batch)
            {
                _liveMessageTotalCount++;
                _liveMessages.Add(record);
            }

            while (_liveMessages.Count > MaxLiveMessages)
            {
                _liveMessages.RemoveAt(0);
            }

            TxtConsumerCount.Text = $"({_liveMessageTotalCount} msgs)";

            if (ChkConsumerAutoScroll.IsChecked == true && _liveMessages.Count > 0)
            {
                DgLiveMessages.ScrollIntoView(_liveMessages[_liveMessages.Count - 1]);
            }
        }

        private void UpdateConsumerUi(bool running)
        {
            BtnToggleConsumer.IsChecked = running;
            ChkFromBeginning.IsEnabled = !running;

            if (running && !string.IsNullOrEmpty(_liveConsumer.CurrentTopic))
            {
                TxtConsumerStatus.Text = $"구독 중: {_liveConsumer.CurrentTopic}";
                TxtConsumerStatus.Foreground = RunningDotBrush;
            }
            else
            {
                TxtConsumerStatus.Text = "대기";
                TxtConsumerStatus.Foreground = IdleTextBrush;
            }
        }

        private void BtnClearConsumerLog_Click(object sender, RoutedEventArgs e)
        {
            lock (_liveLock)
            {
                _pendingLiveMessages.Clear();
            }
            _liveMessages.Clear();
            _liveMessageTotalCount = 0;
            TxtConsumerCount.Text = "(0 msgs)";
            RenderLiveMessageDetail(null);
        }

        private void DgLiveMessages_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            RenderLiveMessageDetail(DgLiveMessages.SelectedItem as KafkaMessageRecord);
        }

        private void LiveViewMode_Changed(object sender, RoutedEventArgs e)
        {
            if (_isInitializing) return;
            RenderLiveMessageDetail(DgLiveMessages.SelectedItem as KafkaMessageRecord);
        }

        private void RenderLiveMessageDetail(KafkaMessageRecord msg)
        {
            if (TxtLiveDetailValue == null) return;
            if (msg == null)
            {
                TxtLiveSelectedMeta.Text = "실시간 수신 행을 클릭하면 JSON Pretty / Raw / Hex 상세 내용을 볼 수 있습니다.";
                TxtLiveDetailValue.Text = string.Empty;
                return;
            }

            TxtLiveSelectedMeta.Text = $"Partition {msg.Partition}  |  Offset {msg.Offset}  |  Key: {msg.KeyText}  |  {msg.TimestampText}";
            TxtLiveDetailValue.Text = FormatMessageForView(
                msg,
                RbLiveJson.IsChecked == true,
                RbLiveHex.IsChecked == true,
                headers: false);
        }

        private void BtnCopyLiveValue_Click(object sender, RoutedEventArgs e)
        {
            if (!string.IsNullOrEmpty(TxtLiveDetailValue.Text))
            {
                Clipboard.SetText(TxtLiveDetailValue.Text);
            }
        }

        // ======================================
        // 서브탭 3: 파티션 정보 & 메시지 발행
        // ======================================
        private async void BtnRefreshPartitions_Click(object sender, RoutedEventArgs e)
        {
            await RefreshPartitionsForSelectedTopicAsync(showErrorDialog: true);
        }

        private async Task RefreshPartitionsForSelectedTopicAsync(bool showErrorDialog)
        {
            var topic = SelectedTopic;
            if (topic == null) return;

            try
            {
                _currentPartitions = await _explorer.GetTopicPartitionsAsync(TxtBootstrapServer.Text.Trim(), topic.Name);
                DgPartitions.ItemsSource = _currentPartitions;

                long totalMsgs = _currentPartitions.Sum(p => p.MessageCount);
                TxtExplorerSummary.Text = $"[{topic.Name}] 파티션 {_currentPartitions.Count}개 · 총 메시지 {totalMsgs:N0}건";
            }
            catch (Exception ex)
            {
                AppendLog("[Partition Info Error] " + ex.Message);
                if (showErrorDialog)
                {
                    MessageBox.Show(ex.Message, "파티션 조회 실패", MessageBoxButton.OK, MessageBoxImage.Error);
                }
            }
        }

        private async void DgPartitions_MouseDoubleClick(object sender, MouseButtonEventArgs e)
        {
            if (DgPartitions.SelectedItem is KafkaPartitionInfo part)
            {
                int targetIndex = part.PartitionId + 1;
                if (targetIndex < CboPartition.Items.Count)
                {
                    CboPartition.SelectedIndex = targetIndex;
                }
                CboFetchMode.SelectedIndex = 0; // 최신 N건
                ExplorerTabs.SelectedIndex = 0; // 메시지 탐색 탭으로 이동
                await FetchMessagesAsync();
            }
        }

        private void BtnFormatProduceJson_Click(object sender, RoutedEventArgs e)
        {
            TxtProduceMessage.Text = KafkaExplorerService.FormatJsonPretty(TxtProduceMessage.Text);
        }

        private async void BtnProduceMessage_Click(object sender, RoutedEventArgs e)
        {
            var topic = SelectedTopic;
            if (topic == null)
            {
                MessageBox.Show("메시지를 발행할 토픽을 좌측 목록에서 선택하세요.", "안내", MessageBoxButton.OK, MessageBoxImage.Information);
                return;
            }

            int? targetPartition = CboProducePartition.SelectedIndex > 0
                ? (int?)(CboProducePartition.SelectedIndex - 1)
                : null;

            BtnProduceMessage.IsEnabled = false;
            TxtProduceStatus.Text = "발행 중...";
            try
            {
                string result = await _explorer.ProduceMessageAsync(
                    TxtBootstrapServer.Text.Trim(),
                    topic.Name,
                    targetPartition,
                    TxtProduceKey.Text,
                    TxtProduceMessage.Text,
                    TxtProduceHeaders.Text);

                TxtProduceStatus.Text = result;
                AppendLog("[Producer] " + result);
                await RefreshPartitionsForSelectedTopicAsync(showErrorDialog: false);
            }
            catch (Exception ex)
            {
                TxtProduceStatus.Text = "발행 실패";
                AppendLog("[Producer Error] " + ex.Message);
                MessageBox.Show(ex.Message, "메시지 발행 실패", MessageBoxButton.OK, MessageBoxImage.Error);
            }
            finally
            {
                BtnProduceMessage.IsEnabled = true;
            }
        }

        // ======================================
        // 서브탭 4: Consumer Groups & Lag
        // ======================================
        private async void BtnRefreshGroups_Click(object sender, RoutedEventArgs e)
        {
            BtnRefreshGroups.IsEnabled = false;
            TxtGroupStatus.Text = "Consumer Group 조회 중...";
            try
            {
                string prevGroupId = (CboConsumerGroups.SelectedItem as KafkaConsumerGroupSummary)?.GroupId;
                var groups = await _explorer.ListConsumerGroupsAsync(TxtBootstrapServer.Text.Trim());
                CboConsumerGroups.ItemsSource = groups;

                if (groups.Count == 0)
                {
                    DgConsumerGroupOffsets.ItemsSource = null;
                    TxtGroupStatus.Text = "등록된 Consumer Group이 없습니다.";
                }
                else
                {
                    TxtGroupStatus.Text = $"총 {groups.Count}개 그룹 조회 완료";
                    var match = !string.IsNullOrEmpty(prevGroupId)
                        ? groups.FirstOrDefault(g => g.GroupId == prevGroupId)
                        : null;
                    CboConsumerGroups.SelectedItem = match ?? groups[0];
                }
            }
            catch (Exception ex)
            {
                TxtGroupStatus.Text = "조회 실패";
                AppendLog("[ConsumerGroups Error] " + ex.Message);
                MessageBox.Show(ex.Message, "Consumer Group 조회 실패", MessageBoxButton.OK, MessageBoxImage.Error);
            }
            finally
            {
                BtnRefreshGroups.IsEnabled = true;
            }
        }

        private async void CboConsumerGroups_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            if (!(CboConsumerGroups.SelectedItem is KafkaConsumerGroupSummary group))
                return;

            TxtGroupStatus.Text = $"'{group.GroupId}' 오프셋 / Lag 조회 중...";
            try
            {
                var rows = await _explorer.GetConsumerGroupOffsetsAsync(TxtBootstrapServer.Text.Trim(), group.GroupId);
                DgConsumerGroupOffsets.ItemsSource = rows;
                long totalLag = rows.Sum(r => r.Lag);
                TxtGroupStatus.Text = $"[{group.GroupId}] 파티션 {rows.Count}개 · 총 Lag: {totalLag:N0}";
            }
            catch (Exception ex)
            {
                TxtGroupStatus.Text = "오프셋 조회 실패";
                AppendLog("[Group Offsets Error] " + ex.Message);
            }
        }

        private void SetExplorerButtonsEnabled(bool enabled)
        {
            BtnConnectCluster.IsEnabled = enabled;
            BtnCreateTopic.IsEnabled = enabled;
            BtnDeleteTopic.IsEnabled = enabled;
            BtnFetchMessages.IsEnabled = enabled;
            BtnRefreshPartitions.IsEnabled = enabled;
        }

        // ======================================
        // 창 종료 시 설정 저장 및 리소스 정리
        // ======================================
        private void Window_Closing(object sender, CancelEventArgs e)
        {
            CaptureSettingsFromUi();
            _settings.Save();

            if (_manager.IsZookeeperRunning || _manager.IsKafkaRunning)
            {
                var answer = MessageBox.Show(
                    "실행 중인 Kafka / Zookeeper 프로세스가 있습니다.\n\n" +
                    "• 예(Y): 실행 중인 프로세스를 모두 종료하고 창 닫기\n" +
                    "• 아니오(N): 프로세스를 유지한 채 창만 닫기\n" +
                    "• 취소(C): 닫기 취소",
                    "실행 중인 프로세스 확인",
                    MessageBoxButton.YesNoCancel,
                    MessageBoxImage.Question);

                if (answer == MessageBoxResult.Cancel)
                {
                    e.Cancel = true;
                    return;
                }

                _isClosing = true;
                _uiFlushTimer.Stop();
                _liveConsumer.StopSync();

                if (answer == MessageBoxResult.Yes)
                {
                    _manager.StopAllSync();
                }
            }
            else
            {
                _isClosing = true;
                _uiFlushTimer.Stop();
                _liveConsumer.StopSync();
            }
        }

        private static void ValidateFile(string path, string displayName)
        {
            if (string.IsNullOrWhiteSpace(path) || !File.Exists(path))
                throw new FileNotFoundException($"{displayName} 경로를 찾을 수 없습니다.", path);
        }
    }
}
