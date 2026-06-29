using Azure.Storage.Blobs;
using Azure.Storage.Queues.Models;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using System;
using System.Text;
using System.Text.Json;

namespace AqIngester;

public class QueueToBlobArchiveFunction
{
    private readonly ILogger<QueueToBlobArchiveFunction> _logger;

    public QueueToBlobArchiveFunction(ILogger<QueueToBlobArchiveFunction> logger)
    {
        _logger = logger;
    }

    [Function("QueueToBlobArchive")]
    public async Task Run(
        [QueueTrigger("%QUEUE_NAME%", Connection = "AzureWebJobsStorage")] string queueMessage)
    {
        // Required app settings
        var storageConn = Environment.GetEnvironmentVariable("AzureWebJobsStorage");
        var containerName = Environment.GetEnvironmentVariable("BLOB_CONTAINER") ?? "telemetry-raw";

        if (string.IsNullOrWhiteSpace(storageConn))
        {
            throw new InvalidOperationException("AzureWebJobsStorage is not configured.");
        }

        // Parse message safely and derive deterministic blob path
        using var doc = JsonDocument.Parse(queueMessage);
        var root = doc.RootElement;

        string ingestTs = root.TryGetProperty("ingestTsUtc", out var ingestTsProp)
            ? ingestTsProp.GetString() ?? DateTimeOffset.UtcNow.ToString("o")
            : DateTimeOffset.UtcNow.ToString("o");

        string deviceId = "unknown-device";
        int seq = -1;

        if (root.TryGetProperty("payload", out var payload))
        {
            if (payload.TryGetProperty("deviceId", out var deviceProp))
            {
                deviceId = SanitizePathPart(deviceProp.GetString() ?? deviceId);
            }

            if (payload.TryGetProperty("seq", out var seqProp) && seqProp.TryGetInt32(out var seqVal))
            {
                seq = seqVal;
            }
        }

        var ts = DateTimeOffset.TryParse(ingestTs, out var parsedTs) ? parsedTs : DateTimeOffset.UtcNow;
        var folder = $"raw/{ts:yyyy/MM/dd}";
        var file = $"{deviceId}/{seq}-{ts:yyyyMMddTHHmmssfffZ}.json";
        var blobPath = $"{folder}/{file}";

        var blobService = new BlobServiceClient(storageConn);
        var container = blobService.GetBlobContainerClient(containerName);
        await container.CreateIfNotExistsAsync();

        var blob = container.GetBlobClient(blobPath);
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(queueMessage));
        await blob.UploadAsync(stream, overwrite: false);

        _logger.LogInformation("Archived queue message to blob path: {BlobPath}", blobPath);
    }

    private static string SanitizePathPart(string value)
    {
        var invalid = Path.GetInvalidFileNameChars();
        var cleaned = new string(value.Select(c => invalid.Contains(c) ? '-' : c).ToArray());
        return string.IsNullOrWhiteSpace(cleaned) ? "unknown-device" : cleaned;
    }
}