using System.Collections.Concurrent;
using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using OpcMqTdengine.Config;

namespace OpcMqTdengine.Writer;

public class TdengineWriter : IDisposable
{
    private readonly HttpClient _httpClient;
    private readonly IotOptions _options;
    private readonly ILogger<TdengineWriter> _logger;
    private readonly ConcurrentQueue<MetricData> _bufferQueue = new();
    private readonly Timer _timer;
    private readonly string _authHeaderValue;
    private readonly object _flushLock = new();
    private int _consecutiveFailures;
    private DateTime _nextRetryUtc = DateTime.MinValue;

    public TdengineWriter(HttpClient httpClient, IOptions<IotOptions> options, ILogger<TdengineWriter> logger)
    {
        _httpClient = httpClient;
        _options = options.Value;
        _logger = logger;

        var authString = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{_options.Tdengine.Username}:{_options.Tdengine.Password}"));
        _authHeaderValue = $"Basic {authString}";

        InitDatabase();

        _timer = new Timer(
            callback: _ => Flush(),
            state: null,
            dueTime: _options.Tdengine.FlushIntervalMs,
            period: _options.Tdengine.FlushIntervalMs
        );

        _logger.LogInformation(
            "TDengine writer initialized. FlushIntervalMs={Interval}, BatchSize={BatchSize}, MaxQueueSize={MaxQueueSize}",
            _options.Tdengine.FlushIntervalMs,
            _options.Tdengine.BatchSize,
            _options.Tdengine.MaxQueueSize);
    }

    public class MetricData
    {
        public long Timestamp { get; set; }
        public double Value { get; set; }
        public string DeviceName { get; set; } = string.Empty;
        public string MetricName { get; set; } = string.Empty;
    }

    private void InitDatabase()
    {
        _logger.LogInformation("Initializing TDengine database and stable...");
        try
        {
            ExecuteSqlAsync("CREATE DATABASE IF NOT EXISTS iot_data KEEP 3650").GetAwaiter().GetResult();
            ExecuteSqlAsync("CREATE STABLE IF NOT EXISTS iot_data.meters (ts TIMESTAMP, val DOUBLE) TAGS (device_name VARCHAR(50), metric_name VARCHAR(50))").GetAwaiter().GetResult();
            _logger.LogInformation("TDengine database and stable are ready.");
        }
        catch (Exception ex)
        {
            RegisterWriteFailure(ex, "TDengine initialization failed. The writer will retry when data is flushed.");
        }
    }

    public void Write(long timestamp, double value, string deviceName, string metricName)
    {
        EnforceQueueLimit();

        _bufferQueue.Enqueue(new MetricData
        {
            Timestamp = timestamp,
            Value = value,
            DeviceName = deviceName,
            MetricName = metricName
        });

        if (_bufferQueue.Count >= _options.Tdengine.BatchSize)
        {
            Task.Run(() => Flush());
        }
    }

    public void Flush()
    {
        Flush(force: false);
    }

    private void Flush(bool force)
    {
        if (_bufferQueue.IsEmpty) return;

        lock (_flushLock)
        {
            if (_bufferQueue.IsEmpty) return;

            var now = DateTime.UtcNow;
            if (!force && now < _nextRetryUtc)
            {
                _logger.LogDebug(
                    "Skipping TDengine flush during retry backoff. QueueSize={QueueSize}, NextRetryUtc={NextRetryUtc:o}",
                    _bufferQueue.Count,
                    _nextRetryUtc);
                return;
            }

            var drainList = new List<MetricData>();
            while (drainList.Count < _options.Tdengine.BatchSize && _bufferQueue.TryDequeue(out var data))
            {
                drainList.Add(data);
            }

            if (drainList.Count == 0) return;

            var startTime = DateTime.UtcNow;
            var sql = BuildInsertSql(drainList);

            try
            {
                ExecuteSqlAsync(sql).GetAwaiter().GetResult();
                _consecutiveFailures = 0;
                _nextRetryUtc = DateTime.MinValue;

                _logger.LogDebug(
                    "Wrote {Count} rows to TDengine in {Elapsed} ms. QueueSize={QueueSize}",
                    drainList.Count,
                    (DateTime.UtcNow - startTime).TotalMilliseconds,
                    _bufferQueue.Count);
            }
            catch (Exception ex)
            {
                foreach (var data in drainList)
                {
                    _bufferQueue.Enqueue(data);
                }

                TrimQueueToLimit();
                RegisterWriteFailure(ex, $"Failed to write {drainList.Count} rows to TDengine. Rows were re-queued.");
            }
        }
    }

    public long? GetLatestTimestamp(string deviceName, string metricName)
    {
        var tableName = BuildTableName(deviceName, metricName);
        var sql = $"SELECT MAX(ts) AS max_ts FROM `iot_data`.`{tableName}`";

        try
        {
            var json = ExecuteSqlAsync(sql).GetAwaiter().GetResult();
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;
            if (root.TryGetProperty("code", out var codeProp) && codeProp.GetInt32() == 0)
            {
                if (root.TryGetProperty("data", out var dataProp) &&
                    dataProp.ValueKind == JsonValueKind.Array &&
                    dataProp.GetArrayLength() > 0)
                {
                    var firstRow = dataProp[0];
                    if (firstRow.ValueKind == JsonValueKind.Array && firstRow.GetArrayLength() > 0)
                    {
                        var cell = firstRow[0];
                        if (cell.ValueKind == JsonValueKind.Number)
                        {
                            return cell.GetInt64();
                        }

                        if (cell.ValueKind == JsonValueKind.String)
                        {
                            var dateStr = cell.GetString();
                            if (!string.IsNullOrEmpty(dateStr) && DateTime.TryParse(dateStr, out var dt))
                            {
                                return new DateTimeOffset(dt).ToUnixTimeMilliseconds();
                            }
                        }
                    }
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning("Failed to query latest TDengine timestamp. The table may not exist yet: {Message}", ex.Message);
        }

        return null;
    }

    private string BuildInsertSql(IEnumerable<MetricData> rows)
    {
        var sqlBuilder = new StringBuilder("INSERT INTO ");
        foreach (var data in rows)
        {
            var tableName = BuildTableName(data.DeviceName, data.MetricName);
            var value = data.Value.ToString(CultureInfo.InvariantCulture);

            sqlBuilder.Append($"`iot_data`.`{tableName}` ")
                .Append($"USING `iot_data`.`meters` TAGS ('{EscapeSqlString(data.DeviceName)}', '{EscapeSqlString(data.MetricName)}') ")
                .Append($"VALUES ({data.Timestamp}, {value}) ");
        }

        return sqlBuilder.ToString();
    }

    private static string BuildTableName(string deviceName, string metricName)
    {
        var cleanDevice = Regex.Replace(deviceName.ToLowerInvariant(), "[^a-z0-9_]", "_");
        var cleanMetric = Regex.Replace(metricName.ToLowerInvariant(), "[^a-z0-9_]", "_");
        return $"d_{cleanDevice}_{cleanMetric}";
    }

    private static string EscapeSqlString(string value)
    {
        return value.Replace("'", "''");
    }

    private void EnforceQueueLimit()
    {
        var maxQueueSize = _options.Tdengine.MaxQueueSize;
        if (maxQueueSize <= 0) return;

        while (_bufferQueue.Count >= maxQueueSize && _bufferQueue.TryDequeue(out _))
        {
            _logger.LogWarning("TDengine buffer queue is full. Dropped oldest row. MaxQueueSize={MaxQueueSize}", maxQueueSize);
        }
    }

    private void TrimQueueToLimit()
    {
        var maxQueueSize = _options.Tdengine.MaxQueueSize;
        if (maxQueueSize <= 0) return;

        var dropped = 0;
        while (_bufferQueue.Count > maxQueueSize && _bufferQueue.TryDequeue(out _))
        {
            dropped++;
        }

        if (dropped > 0)
        {
            _logger.LogWarning(
                "TDengine buffer queue exceeded MaxQueueSize after requeue. Dropped {Dropped} oldest rows. MaxQueueSize={MaxQueueSize}",
                dropped,
                maxQueueSize);
        }
    }

    private void RegisterWriteFailure(Exception ex, string message)
    {
        _consecutiveFailures++;

        var baseDelayMs = Math.Max(1, _options.Tdengine.RetryBaseDelayMs);
        var maxDelayMs = Math.Max(baseDelayMs, _options.Tdengine.RetryMaxDelayMs);
        var exponentialDelay = baseDelayMs * Math.Pow(2, Math.Min(_consecutiveFailures - 1, 10));
        var delayMs = (int)Math.Min(maxDelayMs, exponentialDelay);
        _nextRetryUtc = DateTime.UtcNow.AddMilliseconds(delayMs);

        _logger.LogError(
            ex,
            "{Message} RetryDelayMs={RetryDelayMs}, ConsecutiveFailures={Failures}, QueueSize={QueueSize}",
            message,
            delayMs,
            _consecutiveFailures,
            _bufferQueue.Count);
    }

    private async Task<string> ExecuteSqlAsync(string sql)
    {
        using var request = new HttpRequestMessage(HttpMethod.Post, _options.Tdengine.Url);
        request.Headers.Add("Authorization", _authHeaderValue);
        request.Content = new StringContent(sql, Encoding.UTF8, "text/plain");

        using var response = await _httpClient.SendAsync(request);
        var content = await response.Content.ReadAsStringAsync();

        if (!response.IsSuccessStatusCode)
        {
            throw new HttpRequestException($"TDengine REST API returned {response.StatusCode}: {content}");
        }

        return content;
    }

    public void Dispose()
    {
        _timer.Dispose();
        Flush(force: true);
    }
}
