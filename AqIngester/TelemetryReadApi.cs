using System.Globalization;
using System.Net;
using System.Text.Json;
using Azure.Storage.Blobs;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;

namespace AqIngester;

public class TelemetryReadApi
{
    private readonly ILogger<TelemetryReadApi> _logger;
    private readonly BlobContainerClient _container;

    public TelemetryReadApi(ILogger<TelemetryReadApi> logger)
    {
        _logger = logger;

        var storageConn = Environment.GetEnvironmentVariable("AzureWebJobsStorage")
            ?? throw new InvalidOperationException("Missing AzureWebJobsStorage");
        var containerName = Environment.GetEnvironmentVariable("BLOB_CONTAINER") ?? "telemetry-raw";

        _container = new BlobContainerClient(storageConn, containerName);
    }

    [Function("GetLatestTelemetry")]
    public async Task<HttpResponseData> GetLatest(
        [HttpTrigger(AuthorizationLevel.Function, "get", Route = "latest")] HttpRequestData req)
    {
        var deviceId = GetQuery(req, "deviceId") ?? "aq-tracker-01";
        var hours = ParseInt(GetQuery(req, "hours"), 48, 1, 168);

        var toUtc = DateTimeOffset.UtcNow;
        var fromUtc = toUtc.AddHours(-hours);

        var points = await ReadTelemetryWindow(deviceId, fromUtc, toUtc);
        var latest = points.OrderByDescending(p => p.IngestTsUtc).FirstOrDefault();

        if (latest is null)
        {
            var notFound = req.CreateResponse(HttpStatusCode.NotFound);
            await notFound.WriteStringAsync("No telemetry found");
            return notFound;
        }

        var response = req.CreateResponse(HttpStatusCode.OK);
        await response.WriteAsJsonAsync(new
        {
            deviceId = latest.DeviceId,
            ingestTsUtc = latest.IngestTsUtc,
            pm1_0 = latest.Pm1_0,
            pm2_5 = latest.Pm2_5,
            pm10 = latest.Pm10,
            wifiRssi = latest.WifiRssi,
            fw = latest.Fw
        });

        return response;
    }

    [Function("GetTelemetryTrend")]
    public async Task<HttpResponseData> GetTrend(
        [HttpTrigger(AuthorizationLevel.Function, "get", Route = "trend")] HttpRequestData req)
    {
        var deviceId = GetQuery(req, "deviceId") ?? "aq-tracker-01";
        var hours = ParseInt(GetQuery(req, "hours"), 24, 1, 168);
        var bucketMinutes = ParseInt(GetQuery(req, "bucketMinutes"), 15, 1, 120);

        var toUtc = DateTimeOffset.UtcNow;
        var fromUtc = toUtc.AddHours(-hours);

        var points = await ReadTelemetryWindow(deviceId, fromUtc, toUtc);

        var buckets = points
            .GroupBy(p => TruncateToBucket(p.IngestTsUtc, bucketMinutes))
            .OrderBy(g => g.Key)
            .Select(g => new
            {
                tsUtc = g.Key,
                count = g.Count(),
                pm1_0_avg = Math.Round(g.Average(x => x.Pm1_0), 2),
                pm2_5_avg = Math.Round(g.Average(x => x.Pm2_5), 2),
                pm10_avg = Math.Round(g.Average(x => x.Pm10), 2),
                pm1_0_min = g.Min(x => x.Pm1_0),
                pm1_0_max = g.Max(x => x.Pm1_0),
                pm2_5_min = g.Min(x => x.Pm2_5),
                pm2_5_max = g.Max(x => x.Pm2_5),
                pm10_min = g.Min(x => x.Pm10),
                pm10_max = g.Max(x => x.Pm10)
            });

        var response = req.CreateResponse(HttpStatusCode.OK);
        await response.WriteAsJsonAsync(new
        {
            deviceId,
            fromUtc,
            toUtc,
            bucketMinutes,
            points = buckets
        });

        return response;
    }

    private async Task<List<TelemetryPoint>> ReadTelemetryWindow(
        string deviceId,
        DateTimeOffset fromUtc,
        DateTimeOffset toUtc)
    {
        var results = new List<TelemetryPoint>();

        for (var day = fromUtc.Date; day <= toUtc.Date; day = day.AddDays(1))
        {
            var prefix = $"raw/{day:yyyy/MM/dd}/{deviceId}/";

            await foreach (var blobItem in _container.GetBlobsAsync(prefix: prefix))
            {
                try
                {
                    var blob = _container.GetBlobClient(blobItem.Name);
                    var download = await blob.DownloadContentAsync();
                    var json = download.Value.Content.ToString();

                    var parsed = ParseTelemetry(json);
                    if (parsed is null) continue;
                    if (parsed.IngestTsUtc < fromUtc || parsed.IngestTsUtc > toUtc) continue;

                    results.Add(parsed);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Skipping unreadable blob {BlobName}", blobItem.Name);
                }
            }
        }

        return results;
    }

    private static TelemetryPoint? ParseTelemetry(string json)
    {
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        if (!root.TryGetProperty("ingestTsUtc", out var ingestTsProp)) return null;
        if (!DateTimeOffset.TryParse(ingestTsProp.GetString(), out var ingestTsUtc)) return null;

        if (!root.TryGetProperty("payload", out var payload)) return null;
        if (!payload.TryGetProperty("deviceId", out var deviceIdProp)) return null;
        if (!payload.TryGetProperty("pm", out var pm)) return null;

        var deviceId = deviceIdProp.GetString() ?? "unknown";
        var pm1 = ReadNumber(pm, "pm1_0");
        var pm25 = ReadNumber(pm, "pm2_5");
        var pm10 = ReadNumber(pm, "pm10");

        int? wifiRssi = null;
        string? fw = null;
        if (payload.TryGetProperty("status", out var status))
        {
            if (status.TryGetProperty("wifiRssi", out var rssiEl) && rssiEl.TryGetInt32(out var rssi))
                wifiRssi = rssi;
            if (status.TryGetProperty("fw", out var fwEl))
                fw = fwEl.GetString();
        }

        return new TelemetryPoint
        {
            DeviceId = deviceId,
            IngestTsUtc = ingestTsUtc,
            Pm1_0 = pm1,
            Pm2_5 = pm25,
            Pm10 = pm10,
            WifiRssi = wifiRssi,
            Fw = fw
        };
    }

    private static double ReadNumber(JsonElement obj, string key)
    {
        if (!obj.TryGetProperty(key, out var el)) return 0;
        return el.ValueKind switch
        {
            JsonValueKind.Number => el.GetDouble(),
            JsonValueKind.String when double.TryParse(el.GetString(), NumberStyles.Any, CultureInfo.InvariantCulture, out var v) => v,
            _ => 0
        };
    }

    private static DateTimeOffset TruncateToBucket(DateTimeOffset ts, int bucketMinutes)
    {
        var minute = (ts.Minute / bucketMinutes) * bucketMinutes;
        return new DateTimeOffset(ts.Year, ts.Month, ts.Day, ts.Hour, minute, 0, TimeSpan.Zero);
    }

    private static string? GetQuery(HttpRequestData req, string key)
    {
        var query = req.Url.Query;
        if (string.IsNullOrWhiteSpace(query)) return null;
        if (query.StartsWith("?")) query = query[1..];

        foreach (var pair in query.Split('&', StringSplitOptions.RemoveEmptyEntries))
        {
            var kv = pair.Split('=', 2);
            if (kv.Length != 2) continue;
            if (!string.Equals(Uri.UnescapeDataString(kv[0]), key, StringComparison.OrdinalIgnoreCase)) continue;
            return Uri.UnescapeDataString(kv[1]);
        }
        return null;
    }

    private static int ParseInt(string? value, int defaultValue, int min, int max)
    {
        if (!int.TryParse(value, out var parsed)) return defaultValue;
        return Math.Clamp(parsed, min, max);
    }

    private class TelemetryPoint
    {
        public string DeviceId { get; set; } = "";
        public DateTimeOffset IngestTsUtc { get; set; }
        public double Pm1_0 { get; set; }
        public double Pm2_5 { get; set; }
        public double Pm10 { get; set; }
        public int? WifiRssi { get; set; }
        public string? Fw { get; set; }
    }
}