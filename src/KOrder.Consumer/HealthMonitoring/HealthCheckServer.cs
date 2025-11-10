using System.Net;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Logging;

namespace KThread.Consumer.HealthMonitoring;

/// <summary>
/// Lightweight HTTP server for K8s health checks (liveness and readiness probes).
/// The kubelet makes direct HTTP calls to the container port - NO ingress required.
/// Probes are cluster-internal only and not exposed externally.
/// </summary>
public class HealthCheckServer : IDisposable
{
    private readonly HttpListener _listener;
    private readonly ConsumerHealthMonitor _healthMonitor;
    private readonly ILogger<HealthCheckServer> _logger;
    private readonly int _port;
    private readonly CancellationTokenSource _cts = new();
    private Task? _serverTask;

    public HealthCheckServer(ConsumerHealthMonitor healthMonitor, ILogger<HealthCheckServer> logger, int port = 8080)
    {
        _healthMonitor = healthMonitor;
        _logger = logger;
        _port = port;
        _listener = new HttpListener();
        _listener.Prefixes.Add($"http://+:{_port}/");
    }

    public void Start()
    {
        _listener.Start();
        _serverTask = Task.Run(() => HandleRequestsAsync(_cts.Token));
        _logger.LogInformation("Health check server started on port {Port}", _port);
        _logger.LogInformation("Liveness probe: http://localhost:{Port}/health/live", _port);
        _logger.LogInformation("Readiness probe: http://localhost:{Port}/health/ready", _port);
    }

    private async Task HandleRequestsAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var context = await _listener.GetContextAsync();
                _ = Task.Run(() => ProcessRequest(context), cancellationToken);
            }
            catch (HttpListenerException) when (cancellationToken.IsCancellationRequested)
            {
                // Expected when stopping
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Health check server error");
            }
        }
    }

    private void ProcessRequest(HttpListenerContext context)
    {
        try
        {
            var request = context.Request;
            var response = context.Response;

            // Route requests
            switch (request.Url?.AbsolutePath)
            {
                case "/health/live":
                    HandleLivenessProbe(response);
                    break;

                case "/health/ready":
                    HandleReadinessProbe(response);
                    break;

                case "/metrics":
                    HandleMetrics(response);
                    break;

                default:
                    response.StatusCode = 404;
                    byte[] notFoundBytes = Encoding.UTF8.GetBytes("Not Found");
                    response.OutputStream.Write(notFoundBytes, 0, notFoundBytes.Length);
                    break;
            }

            response.Close();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Request processing error");
        }
    }

    /// <summary>
    /// Liveness probe - K8s will restart pod if this fails
    /// Should only fail if the process is deadlocked or unrecoverable
    /// </summary>
    private void HandleLivenessProbe(HttpListenerResponse response)
    {
        // Simple check: is the process alive?
        // In production, you might check for deadlocks, etc.
        response.StatusCode = 200;
        byte[] responseBytes = Encoding.UTF8.GetBytes("OK");
        response.OutputStream.Write(responseBytes, 0, responseBytes.Length);
    }

    /// <summary>
    /// Readiness probe - K8s will stop sending traffic if this fails
    /// Should fail if consumer is stuck, lagging severely, or can't process messages
    /// </summary>
    private void HandleReadinessProbe(HttpListenerResponse response)
    {
        var healthStatus = _healthMonitor.CheckHealth();

        if (healthStatus.IsHealthy)
        {
            // Consumer is healthy - ready to receive traffic
            response.StatusCode = 200;
            var responseBytes = Encoding.UTF8.GetBytes("Ready");
            response.OutputStream.Write(responseBytes, 0, responseBytes.Length);
        }
        else
        {
            // Consumer is unhealthy - K8s should restart this pod
            response.StatusCode = 503; // Service Unavailable
            var responseBody = new
            {
                status = "unhealthy",
                reason = healthStatus.Reason,
                partitionLags = healthStatus.PartitionLags,
                timestamp = healthStatus.LastCheckTime
            };
            var jsonBytes = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(responseBody));
            response.ContentType = "application/json";
            response.OutputStream.Write(jsonBytes, 0, jsonBytes.Length);

            _logger.LogWarning("Readiness check FAILED: {Reason}", healthStatus.Reason);
        }
    }

    /// <summary>
    /// Metrics endpoint for Prometheus scraping (optional)
    /// </summary>
    private void HandleMetrics(HttpListenerResponse response)
    {
        HealthStatus healthStatus = _healthMonitor.CheckHealth();
        long totalLag = _healthMonitor.GetTotalLag();

        // Prometheus format
        StringBuilder metrics = new StringBuilder();
        metrics.AppendLine("# HELP kafka_consumer_lag Current lag per partition");
        metrics.AppendLine("# TYPE kafka_consumer_lag gauge");

        foreach (var kvp in healthStatus.PartitionLags)
        {
            metrics.AppendLine($"kafka_consumer_lag{{partition=\"{kvp.Key}\"}} {kvp.Value}");
        }

        metrics.AppendLine("# HELP kafka_consumer_total_lag Total lag across all partitions");
        metrics.AppendLine("# TYPE kafka_consumer_total_lag gauge");
        metrics.AppendLine($"kafka_consumer_total_lag {totalLag}");

        metrics.AppendLine("# HELP kafka_consumer_healthy Consumer health status (1=healthy, 0=unhealthy)");
        metrics.AppendLine("# TYPE kafka_consumer_healthy gauge");
        metrics.AppendLine($"kafka_consumer_healthy {(healthStatus.IsHealthy ? 1 : 0)}");

        response.StatusCode = 200;
        response.ContentType = "text/plain; version=0.0.4";
        byte[] metricsBytes = Encoding.UTF8.GetBytes(metrics.ToString());
        response.OutputStream.Write(metricsBytes, 0, metricsBytes.Length);
    }

    public void Dispose()
    {
        _cts.Cancel();
        _listener.Stop();
        _listener.Close();
        _serverTask?.Wait(TimeSpan.FromSeconds(5));
        _cts.Dispose();
        _logger.LogInformation("Health check server stopped");
    }
}
