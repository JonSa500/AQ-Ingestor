using System.Net;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;

namespace AqIngester;

public class IngestFunction
{
    private readonly ILogger<IngestFunction> _logger;

    public IngestFunction(ILogger<IngestFunction> logger)
    {
        _logger = logger;
    }

    [Function("IngestTelemetry")]
    public async Task<IngestOutputs> Run(
    [HttpTrigger(AuthorizationLevel.Function, "post", Route = "ingest")] HttpRequestData req)
    {
        string body = await new StreamReader(req.Body).ReadToEndAsync();

        TelemetryPayload? payload;
        try
        {
            payload = JsonSerializer.Deserialize<TelemetryPayload>(body, new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true
            });
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Invalid JSON");
            var bad = req.CreateResponse(HttpStatusCode.BadRequest);
            await bad.WriteStringAsync("Invalid JSON");
            return new IngestOutputs { HttpResponse = bad };
        }

        if (payload is null ||
        payload.SchemaVersion <= 0 ||
        string.IsNullOrWhiteSpace(payload.DeviceId) ||
        payload.Pm is null)
        {
            var bad = req.CreateResponse(HttpStatusCode.BadRequest);
            await bad.WriteStringAsync("Missing required fields: schemaVersion, deviceId, pm");
            return new IngestOutputs { HttpResponse = bad };
        }

        var queued = new QueueEnvelope
        {
            IngestTsUtc = DateTimeOffset.UtcNow,
            Payload = payload
        };

        string queueMessage = JsonSerializer.Serialize(queued);

        var ok = req.CreateResponse(HttpStatusCode.Accepted);
        await ok.WriteStringAsync("Accepted");

        return new IngestOutputs
        {
            HttpResponse = ok,
            QueueMessage = queueMessage
        };
    }
}

public class IngestOutputs
{
    public HttpResponseData HttpResponse { get; set; } = default!;

    [QueueOutput("%QUEUE_NAME%", Connection = "AzureWebJobsStorage")]
    public string? QueueMessage { get; set; }
}

public class QueueEnvelope
{
    [JsonPropertyName("ingestTsUtc")]
    public DateTimeOffset IngestTsUtc { get; set; }

    [JsonPropertyName("payload")]
    public TelemetryPayload Payload { get; set; } = default!;
}

public class TelemetryPayload
{
    [JsonPropertyName("schemaVersion")]
    public int SchemaVersion { get; set; }

    [JsonPropertyName("deviceId")]
    public string DeviceId { get; set; } = string.Empty;

    [JsonPropertyName("seq")]
    public int Seq { get; set; }

    [JsonPropertyName("tsMs")]
    public long TsMs { get; set; }

    [JsonPropertyName("pm")]
    public PmBlock? Pm { get; set; }

    [JsonPropertyName("scd41")]
    public Scd41Block? Scd41 { get; set; }

    [JsonPropertyName("status")]
    public StatusBlock? Status { get; set; }
}

public class PmBlock
{
    [JsonPropertyName("pm1_0")]
    public double? Pm1_0 { get; set; }

    [JsonPropertyName("pm2_5")]
    public double? Pm2_5 { get; set; }

    [JsonPropertyName("pm10")]
    public double? Pm10 { get; set; }
}

public class Scd41Block
{
    [JsonPropertyName("co2")]
    public double? Co2 { get; set; }

    [JsonPropertyName("tempC")]
    public double? TempC { get; set; }

    [JsonPropertyName("rhPct")]
    public double? RhPct { get; set; }
}

public class StatusBlock
{
    [JsonPropertyName("wifiRssi")]
    public int? WifiRssi { get; set; }

    [JsonPropertyName("fw")]
    public string? Fw { get; set; }
}