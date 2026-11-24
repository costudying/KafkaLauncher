using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace KafkaLauncher
{
    public enum MessageFetchMode
    {
        Latest,
        Earliest,
        SpecificOffset
    }

    public class ClusterMetadataSummary
    {
        public int BrokerCount { get; set; }
        public string BrokerSummary { get; set; }
        public List<KafkaTopicSummary> Topics { get; set; } = new List<KafkaTopicSummary>();
    }

    public class KafkaTopicSummary
    {
        public string Name { get; set; }
        public int PartitionCount { get; set; }
        public int ReplicationFactor { get; set; }
        public bool IsInternal => !string.IsNullOrEmpty(Name) && Name.StartsWith("__", StringComparison.Ordinal);
        public string DisplayBadge => $"P:{PartitionCount}";
    }

    public class KafkaPartitionInfo
    {
        public int PartitionId { get; set; }
        public int Leader { get; set; }
        public string Replicas { get; set; }
        public string InSyncReplicas { get; set; }
        public long EarliestOffset { get; set; }
        public long LatestOffset { get; set; }
        public long MessageCount => Math.Max(0, LatestOffset - EarliestOffset);
    }

    public class KafkaHeaderItem
    {
        public string Key { get; set; }
        public string ValueText { get; set; }
        public string ValueHex { get; set; }
    }

    public class KafkaMessageRecord
    {
        public int Partition { get; set; }
        public long Offset { get; set; }
        public DateTime LocalTimestamp { get; set; }
        public string TimestampText { get; set; }
        public string TimestampType { get; set; }
        public byte[] KeyBytes { get; set; }
        public byte[] ValueBytes { get; set; }
        public string KeyText { get; set; }
        public string ValueText { get; set; }
        public string ValuePreview { get; set; }
        public int SizeBytes { get; set; }
        public List<KafkaHeaderItem> Headers { get; set; } = new List<KafkaHeaderItem>();
        public int HeaderCount => Headers?.Count ?? 0;
    }

    public class MessageFetchResult
    {
        public List<KafkaMessageRecord> Messages { get; set; } = new List<KafkaMessageRecord>();
        public long ElapsedMs { get; set; }
        public string Summary { get; set; }
    }

    public class KafkaConsumerGroupSummary
    {
        public string GroupId { get; set; }
        public string State { get; set; }
        public string Display => string.IsNullOrEmpty(State) ? GroupId : $"{GroupId}  [{State}]";
    }

    public class KafkaConsumerGroupOffsetRow
    {
        public string Topic { get; set; }
        public int Partition { get; set; }
        public long CurrentOffset { get; set; }
        public long LogEndOffset { get; set; }
        public long Lag => Math.Max(0, LogEndOffset - CurrentOffset);
        public bool HasLag => Lag > 0;
    }

    public class KafkaLiveConsumer
    {
        private readonly object _syncLock = new object();
        private CancellationTokenSource _cts;
        private Task _workerTask;

        public event EventHandler<KafkaMessageRecord> MessageReceived;
        public event EventHandler<string> ErrorReceived;
        public event EventHandler<bool> StateChanged;

        public string CurrentTopic { get; private set; }

        public bool IsRunning
        {
            get
            {
                lock (_syncLock)
                {
                    return _workerTask != null && !_workerTask.IsCompleted;
                }
            }
        }

        public void Start(string bootstrapServer, string topicName, bool fromBeginning)
        {
            KafkaExplorerService.ValidateBootstrapServer(bootstrapServer);
            KafkaExplorerService.ValidateTopicName(topicName);

            StopSync();

            var cts = new CancellationTokenSource();
            string cleanBootstrap = bootstrapServer.Trim();
            string cleanTopic = topicName.Trim();

            lock (_syncLock)
            {
                _cts = cts;
                CurrentTopic = cleanTopic;
                _workerTask = Task.Run(() => RunConsumeLoop(cleanBootstrap, cleanTopic, fromBeginning, cts.Token), cts.Token);
            }

            StateChanged?.Invoke(this, true);
        }

        public async Task StopAsync()
        {
            Task worker;
            lock (_syncLock)
            {
                if (_cts != null)
                {
                    try { _cts.Cancel(); } catch { }
                }
                worker = _workerTask;
            }

            if (worker != null)
            {
                try
                {
                    await Task.WhenAny(worker, Task.Delay(3000));
                }
                catch { }
            }

            CleanupState();
        }

        public void StopSync()
        {
            Task worker;
            lock (_syncLock)
            {
                if (_cts != null)
                {
                    try { _cts.Cancel(); } catch { }
                }
                worker = _workerTask;
            }

            if (worker != null)
            {
                try { worker.Wait(2000); } catch { }
            }

            CleanupState();
        }

        private void CleanupState()
        {
            bool changed = false;
            lock (_syncLock)
            {
                if (_cts != null || _workerTask != null || CurrentTopic != null)
                {
                    try { _cts?.Dispose(); } catch { }
                    _cts = null;
                    _workerTask = null;
                    CurrentTopic = null;
                    changed = true;
                }
            }

            if (changed)
            {
                StateChanged?.Invoke(this, false);
            }
        }

        private void RunConsumeLoop(string bootstrapServer, string topicName, bool fromBeginning, CancellationToken ct)
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = bootstrapServer,
                GroupId = "kafka-launcher-live-" + Guid.NewGuid().ToString("N"),
                EnableAutoCommit = false,
                AutoOffsetReset = fromBeginning ? AutoOffsetReset.Earliest : AutoOffsetReset.Latest,
                SessionTimeoutMs = 10000
            };

            try
            {
                using (var consumer = new ConsumerBuilder<byte[], byte[]>(config)
                    .SetErrorHandler((_, err) =>
                    {
                        if (!ct.IsCancellationRequested)
                            ErrorReceived?.Invoke(this, $"[Kafka Error] {err.Reason}");
                    })
                    .Build())
                {
                    // 파티션 목록 조회 후 수동 Assign → 그룹 리밸런싱 지연(3초+) 없이 수십 ms 내 즉시 구독 시작!
                    using (var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServer }).Build())
                    {
                        var meta = admin.GetMetadata(topicName, TimeSpan.FromSeconds(5));
                        var topicMeta = meta.Topics.FirstOrDefault(t => t.Topic == topicName);
                        if (topicMeta == null || topicMeta.Partitions.Count == 0)
                        {
                            throw new InvalidOperationException($"토픽 '{topicName}' 메타데이터를 찾을 수 없습니다.");
                        }

                        var assignments = new List<TopicPartitionOffset>();
                        foreach (var p in topicMeta.Partitions)
                        {
                            var tp = new TopicPartition(topicName, new Partition(p.PartitionId));
                            Offset startOff = fromBeginning ? Offset.Beginning : Offset.End;
                            assignments.Add(new TopicPartitionOffset(tp, startOff));
                        }
                        consumer.Assign(assignments);
                    }

                    while (!ct.IsCancellationRequested)
                    {
                        try
                        {
                            var cr = consumer.Consume(TimeSpan.FromMilliseconds(200));
                            if (cr != null && !cr.IsPartitionEOF && cr.Message != null)
                            {
                                var record = KafkaExplorerService.ToMessageRecord(cr);
                                MessageReceived?.Invoke(this, record);
                            }
                        }
                        catch (ConsumeException cex)
                        {
                            if (!ct.IsCancellationRequested)
                                ErrorReceived?.Invoke(this, cex.Error.Reason);
                        }
                        catch (OperationCanceledException)
                        {
                            break;
                        }
                    }

                    try { consumer.Close(); } catch { }
                }
            }
            catch (Exception ex)
            {
                if (!ct.IsCancellationRequested)
                    ErrorReceived?.Invoke(this, ex.Message);
            }
            finally
            {
                CleanupState();
            }
        }
    }

    public class KafkaExplorerService
    {
        private static readonly Regex ValidTopicNameRegex = new Regex(@"^[a-zA-Z0-9._\-]{1,249}$", RegexOptions.Compiled);

        internal static void ValidateBootstrapServer(string bootstrapServer)
        {
            if (string.IsNullOrWhiteSpace(bootstrapServer))
                throw new ArgumentException("Bootstrap Server 주소를 입력하세요. (예: localhost:9092)");
        }

        internal static void ValidateTopicName(string topicName)
        {
            if (string.IsNullOrWhiteSpace(topicName))
                throw new ArgumentException("토픽 이름을 입력하세요.");

            if (topicName == "." || topicName == ".." || !ValidTopicNameRegex.IsMatch(topicName))
                throw new ArgumentException("토픽 이름은 영문, 숫자, '.', '_', '-' 만 사용할 수 있습니다.");
        }

        private static AdminClientConfig CreateAdminConfig(string bootstrapServer) => new AdminClientConfig
        {
            BootstrapServers = bootstrapServer.Trim(),
            SocketTimeoutMs = 5000
        };

        public Task<ClusterMetadataSummary> GetClusterMetadataAsync(string bootstrapServer)
        {
            ValidateBootstrapServer(bootstrapServer);
            return Task.Run(() =>
            {
                using (var admin = new AdminClientBuilder(CreateAdminConfig(bootstrapServer)).Build())
                {
                    var meta = admin.GetMetadata(TimeSpan.FromSeconds(5));
                    var topics = meta.Topics
                        .Where(t => !t.Error.IsError)
                        .Select(t => new KafkaTopicSummary
                        {
                            Name = t.Topic,
                            PartitionCount = t.Partitions.Count,
                            ReplicationFactor = t.Partitions.Count > 0 ? t.Partitions[0].Replicas.Length : 0
                        })
                        .OrderBy(t => t.Name, StringComparer.OrdinalIgnoreCase)
                        .ToList();

                    string brokers = string.Join(", ", meta.Brokers.Select(b => $"{b.Host}:{b.Port}"));
                    return new ClusterMetadataSummary
                    {
                        BrokerCount = meta.Brokers.Count,
                        BrokerSummary = brokers,
                        Topics = topics
                    };
                }
            });
        }

        public async Task CreateTopicAsync(string bootstrapServer, string topicName, int partitions, int replicationFactor)
        {
            ValidateBootstrapServer(bootstrapServer);
            ValidateTopicName(topicName);

            if (partitions < 1) throw new ArgumentException("파티션 수는 1 이상이어야 합니다.");
            if (replicationFactor < 1) throw new ArgumentException("복제 팩터는 1 이상이어야 합니다.");

            using (var admin = new AdminClientBuilder(CreateAdminConfig(bootstrapServer)).Build())
            {
                await admin.CreateTopicsAsync(new[]
                {
                    new TopicSpecification
                    {
                        Name = topicName.Trim(),
                        NumPartitions = partitions,
                        ReplicationFactor = (short)replicationFactor
                    }
                }, new CreateTopicsOptions { RequestTimeout = TimeSpan.FromSeconds(10) });
            }
        }

        public async Task DeleteTopicAsync(string bootstrapServer, string topicName)
        {
            ValidateBootstrapServer(bootstrapServer);
            ValidateTopicName(topicName);

            using (var admin = new AdminClientBuilder(CreateAdminConfig(bootstrapServer)).Build())
            {
                await admin.DeleteTopicsAsync(
                    new[] { topicName.Trim() },
                    new DeleteTopicsOptions { RequestTimeout = TimeSpan.FromSeconds(10) });
            }
        }

        public Task<List<KafkaPartitionInfo>> GetTopicPartitionsAsync(string bootstrapServer, string topicName)
        {
            ValidateBootstrapServer(bootstrapServer);
            ValidateTopicName(topicName);

            return Task.Run(() =>
            {
                var result = new List<KafkaPartitionInfo>();
                string cleanTopic = topicName.Trim();

                using (var admin = new AdminClientBuilder(CreateAdminConfig(bootstrapServer)).Build())
                using (var consumer = new ConsumerBuilder<byte[], byte[]>(new ConsumerConfig
                {
                    BootstrapServers = bootstrapServer.Trim(),
                    GroupId = "kafka-launcher-wm-" + Guid.NewGuid().ToString("N"),
                    EnableAutoCommit = false
                }).Build())
                {
                    var meta = admin.GetMetadata(cleanTopic, TimeSpan.FromSeconds(5));
                    var topicMeta = meta.Topics.FirstOrDefault(t => t.Topic == cleanTopic);
                    if (topicMeta == null) return result;

                    foreach (var p in topicMeta.Partitions.OrderBy(x => x.PartitionId))
                    {
                        var tp = new TopicPartition(cleanTopic, new Partition(p.PartitionId));
                        var wm = consumer.QueryWatermarkOffsets(tp, TimeSpan.FromSeconds(5));
                        result.Add(new KafkaPartitionInfo
                        {
                            PartitionId = p.PartitionId,
                            Leader = p.Leader,
                            Replicas = string.Join(", ", p.Replicas),
                            InSyncReplicas = string.Join(", ", p.InSyncReplicas),
                            EarliestOffset = wm.Low.Value,
                            LatestOffset = wm.High.Value
                        });
                    }
                }

                return result;
            });
        }

        public Task<MessageFetchResult> FetchMessagesAsync(
            string bootstrapServer,
            string topicName,
            int? partitionFilter,
            MessageFetchMode mode,
            long specificOffset,
            int maxMessages,
            CancellationToken ct = default(CancellationToken))
        {
            ValidateBootstrapServer(bootstrapServer);
            ValidateTopicName(topicName);

            if (maxMessages < 1 || maxMessages > 2000)
                throw new ArgumentException("조회 개수는 1 ~ 2000 사이로 입력하세요.");
            if (mode == MessageFetchMode.SpecificOffset && specificOffset < 0)
                throw new ArgumentException("시작 오프셋은 0 이상이어야 합니다.");

            return Task.Run(() =>
            {
                var sw = System.Diagnostics.Stopwatch.StartNew();
                string cleanTopic = topicName.Trim();
                var records = new List<KafkaMessageRecord>();

                var consumerConfig = new ConsumerConfig
                {
                    BootstrapServers = bootstrapServer.Trim(),
                    GroupId = "kafka-launcher-explorer-" + Guid.NewGuid().ToString("N"),
                    EnableAutoCommit = false,
                    EnablePartitionEof = true,
                    AutoOffsetReset = AutoOffsetReset.Earliest,
                    FetchWaitMaxMs = 100
                };

                using (var admin = new AdminClientBuilder(CreateAdminConfig(bootstrapServer)).Build())
                using (var consumer = new ConsumerBuilder<byte[], byte[]>(consumerConfig).Build())
                {
                    var meta = admin.GetMetadata(cleanTopic, TimeSpan.FromSeconds(5));
                    var topicMeta = meta.Topics.FirstOrDefault(t => t.Topic == cleanTopic);
                    if (topicMeta == null || topicMeta.Partitions.Count == 0)
                    {
                        throw new InvalidOperationException($"토픽 '{cleanTopic}' 정보를 찾을 수 없습니다.");
                    }

                    IEnumerable<int> targetPartitions = partitionFilter.HasValue
                        ? topicMeta.Partitions.Where(p => p.PartitionId == partitionFilter.Value).Select(p => p.PartitionId)
                        : topicMeta.Partitions.Select(p => p.PartitionId).OrderBy(id => id);

                    var partitionIds = targetPartitions.ToList();
                    if (partitionIds.Count == 0)
                    {
                        throw new InvalidOperationException($"파티션 {partitionFilter} 이(가) 존재하지 않습니다.");
                    }

                    var assignments = new List<TopicPartitionOffset>();
                    var endOffsets = new Dictionary<int, long>();
                    var remainingPerPartition = new Dictionary<int, int>();

                    foreach (int pid in partitionIds)
                    {
                        var tp = new TopicPartition(cleanTopic, new Partition(pid));
                        var wm = consumer.QueryWatermarkOffsets(tp, TimeSpan.FromSeconds(5));
                        long low = wm.Low.Value;
                        long high = wm.High.Value;
                        if (high <= low) continue; // 비어있는 파티션

                        long startOff;
                        switch (mode)
                        {
                            case MessageFetchMode.Latest:
                                startOff = Math.Max(low, high - maxMessages);
                                break;
                            case MessageFetchMode.Earliest:
                                startOff = low;
                                break;
                            case MessageFetchMode.SpecificOffset:
                                if (specificOffset >= high) continue;
                                startOff = Math.Max(low, specificOffset);
                                break;
                            default:
                                startOff = low;
                                break;
                        }

                        long available = high - startOff;
                        if (available <= 0) continue;

                        assignments.Add(new TopicPartitionOffset(tp, new Offset(startOff)));
                        endOffsets[pid] = high - 1;
                        remainingPerPartition[pid] = (int)Math.Min(available, maxMessages);
                    }

                    if (assignments.Count == 0)
                    {
                        sw.Stop();
                        return new MessageFetchResult
                        {
                            Messages = records,
                            ElapsedMs = sw.ElapsedMilliseconds,
                            Summary = $"조회된 메시지가 없습니다. ({sw.ElapsedMilliseconds}ms)"
                        };
                    }

                    consumer.Assign(assignments);
                    var activePartitions = new HashSet<int>(assignments.Select(a => a.Partition.Value));
                    int totalCap = mode == MessageFetchMode.Latest
                        ? maxMessages * Math.Min(assignments.Count, 8)
                        : maxMessages;

                    int consecutiveEmptyPolls = 0;
                    while (activePartitions.Count > 0 && records.Count < totalCap && !ct.IsCancellationRequested)
                    {
                        var cr = consumer.Consume(TimeSpan.FromMilliseconds(250));
                        if (cr == null)
                        {
                            consecutiveEmptyPolls++;
                            if (consecutiveEmptyPolls >= 8) break; // 최대 ~2초 대기 후 종료
                            continue;
                        }

                        consecutiveEmptyPolls = 0;
                        int pid = cr.Partition.Value;

                        if (cr.IsPartitionEOF)
                        {
                            activePartitions.Remove(pid);
                            continue;
                        }

                        if (cr.Message != null && activePartitions.Contains(pid))
                        {
                            records.Add(ToMessageRecord(cr));

                            if (remainingPerPartition.ContainsKey(pid))
                            {
                                remainingPerPartition[pid]--;
                                if (remainingPerPartition[pid] <= 0 || cr.Offset.Value >= endOffsets[pid])
                                {
                                    activePartitions.Remove(pid);
                                }
                            }
                        }
                    }

                    try { consumer.Close(); } catch { }
                }

                // 정렬: 최신 조회는 최신순(내림차순), 그 외는 파티션/오프셋 오름차순
                if (mode == MessageFetchMode.Latest)
                {
                    records = records
                        .OrderByDescending(r => r.LocalTimestamp)
                        .ThenByDescending(r => r.Offset)
                        .Take(maxMessages)
                        .ToList();
                }
                else
                {
                    records = records
                        .OrderBy(r => r.Partition)
                        .ThenBy(r => r.Offset)
                        .Take(maxMessages)
                        .ToList();
                }

                sw.Stop();
                string partLabel = partitionFilter.HasValue ? $"P{partitionFilter.Value}" : "전체 파티션";
                return new MessageFetchResult
                {
                    Messages = records,
                    ElapsedMs = sw.ElapsedMilliseconds,
                    Summary = $"{partLabel} · {records.Count}건 조회 완료 ({sw.ElapsedMilliseconds}ms)"
                };
            }, ct);
        }

        public async Task<string> ProduceMessageAsync(
            string bootstrapServer,
            string topicName,
            int? targetPartition,
            string key,
            string value,
            string rawHeaders)
        {
            ValidateBootstrapServer(bootstrapServer);
            ValidateTopicName(topicName);

            var config = new ProducerConfig
            {
                BootstrapServers = bootstrapServer.Trim(),
                MessageTimeoutMs = 10000
            };

            var msg = new Message<byte[], byte[]>
            {
                Key = string.IsNullOrEmpty(key) ? null : Encoding.UTF8.GetBytes(key),
                Value = value == null ? null : Encoding.UTF8.GetBytes(value)
            };

            if (!string.IsNullOrWhiteSpace(rawHeaders))
            {
                var headers = new Headers();
                var lines = rawHeaders.Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);
                foreach (string line in lines)
                {
                    int sep = line.IndexOf('=');
                    if (sep <= 0) sep = line.IndexOf(':');
                    if (sep > 0)
                    {
                        string hk = line.Substring(0, sep).Trim();
                        string hv = line.Substring(sep + 1).Trim();
                        if (!string.IsNullOrEmpty(hk))
                        {
                            headers.Add(hk, Encoding.UTF8.GetBytes(hv));
                        }
                    }
                }
                if (headers.Count > 0) msg.Headers = headers;
            }

            using (var producer = new ProducerBuilder<byte[], byte[]>(config).Build())
            {
                DeliveryResult<byte[], byte[]> res = targetPartition.HasValue
                    ? await producer.ProduceAsync(new TopicPartition(topicName.Trim(), new Partition(targetPartition.Value)), msg)
                    : await producer.ProduceAsync(topicName.Trim(), msg);

                return $"발행 완료 → Topic: {res.Topic}, Partition: {res.Partition.Value}, Offset: {res.Offset.Value}";
            }
        }

        public async Task<List<KafkaConsumerGroupSummary>> ListConsumerGroupsAsync(string bootstrapServer)
        {
            ValidateBootstrapServer(bootstrapServer);
            using (var admin = new AdminClientBuilder(CreateAdminConfig(bootstrapServer)).Build())
            {
                var result = await admin.ListConsumerGroupsAsync(new ListConsumerGroupsOptions
                {
                    RequestTimeout = TimeSpan.FromSeconds(8)
                });

                return result.Valid
                    .Select(g => new KafkaConsumerGroupSummary
                    {
                        GroupId = g.GroupId,
                        State = g.State.ToString()
                    })
                    .OrderBy(g => g.GroupId, StringComparer.OrdinalIgnoreCase)
                    .ToList();
            }
        }

        public async Task<List<KafkaConsumerGroupOffsetRow>> GetConsumerGroupOffsetsAsync(string bootstrapServer, string groupId)
        {
            ValidateBootstrapServer(bootstrapServer);
            if (string.IsNullOrWhiteSpace(groupId))
                throw new ArgumentException("Consumer Group을 선택하세요.");

            using (var admin = new AdminClientBuilder(CreateAdminConfig(bootstrapServer)).Build())
            {
                var offsetsResult = await admin.ListConsumerGroupOffsetsAsync(
                    new[] { new ConsumerGroupTopicPartitions(groupId.Trim(), null) },
                    new ListConsumerGroupOffsetsOptions { RequestTimeout = TimeSpan.FromSeconds(8) });

                var rows = new List<KafkaConsumerGroupOffsetRow>();
                var groupRes = offsetsResult.FirstOrDefault();
                if (groupRes == null || groupRes.Partitions == null) return rows;

                using (var consumer = new ConsumerBuilder<byte[], byte[]>(new ConsumerConfig
                {
                    BootstrapServers = bootstrapServer.Trim(),
                    GroupId = "kafka-launcher-lag-" + Guid.NewGuid().ToString("N"),
                    EnableAutoCommit = false
                }).Build())
                {
                    foreach (var p in groupRes.Partitions.OrderBy(x => x.Topic).ThenBy(x => x.Partition.Value))
                    {
                        if (p.Error.IsError) continue;
                        long current = (p.Offset == Offset.Unset || p.Offset.Value < 0) ? 0 : p.Offset.Value;
                        long logEnd = current;
                        try
                        {
                            var wm = consumer.QueryWatermarkOffsets(new TopicPartition(p.Topic, p.Partition), TimeSpan.FromSeconds(4));
                            logEnd = wm.High.Value;
                        }
                        catch { }

                        rows.Add(new KafkaConsumerGroupOffsetRow
                        {
                            Topic = p.Topic,
                            Partition = p.Partition.Value,
                            CurrentOffset = current,
                            LogEndOffset = logEnd
                        });
                    }
                }

                return rows;
            }
        }

        internal static KafkaMessageRecord ToMessageRecord(ConsumeResult<byte[], byte[]> cr)
        {
            byte[] keyBytes = cr.Message.Key;
            byte[] valBytes = cr.Message.Value;

            string keyText = keyBytes == null ? "<null>" : DecodeUtf8Safe(keyBytes);
            string valText = valBytes == null ? "<null>" : DecodeUtf8Safe(valBytes);

            DateTime localTs = cr.Message.Timestamp.Type != TimestampType.NotAvailable
                ? cr.Message.Timestamp.UtcDateTime.ToLocalTime()
                : DateTime.MinValue;

            string tsText = localTs != DateTime.MinValue
                ? localTs.ToString("yyyy-MM-dd HH:mm:ss.fff")
                : "-";

            var headerList = new List<KafkaHeaderItem>();
            if (cr.Message.Headers != null)
            {
                foreach (var h in cr.Message.Headers)
                {
                    byte[] hBytes = h.GetValueBytes();
                    headerList.Add(new KafkaHeaderItem
                    {
                        Key = h.Key,
                        ValueText = hBytes == null ? "<null>" : DecodeUtf8Safe(hBytes),
                        ValueHex = hBytes == null ? string.Empty : BitConverter.ToString(hBytes).Replace("-", " ")
                    });
                }
            }

            string preview = valText.Replace("\r", " ").Replace("\n", " ").Trim();
            if (preview.Length > 180)
                preview = preview.Substring(0, 180) + "...";

            return new KafkaMessageRecord
            {
                Partition = cr.Partition.Value,
                Offset = cr.Offset.Value,
                LocalTimestamp = localTs,
                TimestampText = tsText,
                TimestampType = cr.Message.Timestamp.Type.ToString(),
                KeyBytes = keyBytes,
                ValueBytes = valBytes,
                KeyText = keyText,
                ValueText = valText,
                ValuePreview = preview,
                SizeBytes = (keyBytes?.Length ?? 0) + (valBytes?.Length ?? 0),
                Headers = headerList
            };
        }

        public static string FormatJsonPretty(string text)
        {
            if (string.IsNullOrWhiteSpace(text) || text == "<null>")
                return text ?? string.Empty;

            string trimmed = text.Trim();
            if ((trimmed.StartsWith("{") && trimmed.EndsWith("}")) ||
                (trimmed.StartsWith("[") && trimmed.EndsWith("]")))
            {
                try
                {
                    return JToken.Parse(trimmed).ToString(Formatting.Indented);
                }
                catch
                {
                    // 유효한 JSON이 아니면 원문 그대로 반환
                }
            }

            return text;
        }

        public static string FormatHexDump(byte[] bytes, int maxBytes = 4096)
        {
            if (bytes == null || bytes.Length == 0)
                return "(비어있음 / null)";

            int length = Math.Min(bytes.Length, maxBytes);
            var sb = new StringBuilder();
            for (int i = 0; i < length; i += 16)
            {
                sb.AppendFormat("{0:X8}  ", i);
                for (int j = 0; j < 16; j++)
                {
                    if (i + j < length)
                        sb.AppendFormat("{0:X2} ", bytes[i + j]);
                    else
                        sb.Append("   ");
                    if (j == 7) sb.Append(" ");
                }
                sb.Append(" |");
                for (int j = 0; j < 16 && i + j < length; j++)
                {
                    byte b = bytes[i + j];
                    sb.Append(b >= 32 && b <= 126 ? (char)b : '.');
                }
                sb.AppendLine("|");
            }

            if (bytes.Length > maxBytes)
            {
                sb.AppendLine($"... ({bytes.Length - maxBytes} bytes 생략됨)");
            }

            return sb.ToString();
        }

        private static string DecodeUtf8Safe(byte[] bytes)
        {
            try
            {
                return Encoding.UTF8.GetString(bytes);
            }
            catch
            {
                return BitConverter.ToString(bytes);
            }
        }
    }
}
