using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.Logging;
using NifiKit.Models;
using Polly;
using Polly.Registry;

namespace NifiKit.Services;

public class NifiTransportService : INifiTransportService {
  private readonly IFlowFileService flow_file_service_;
  private readonly HttpClient http_client_;
  private readonly ILogger<NifiTransportService>? logger_;
  private readonly ResiliencePipelineProvider<string>? pipeline_provider_;

  public NifiTransportService(HttpClient http_client,
                              IFlowFileService flow_file_service,
                              ILogger<NifiTransportService>? logger = null,
                              ResiliencePipelineProvider<string>?
                                pipeline_provider = null) {
    http_client_ = http_client;
    flow_file_service_ = flow_file_service;
    logger_ = logger;
    pipeline_provider_ = pipeline_provider;
  }

  #region Transport

  public async Task<bool> PostToNiFiAsync(string url, Stream flow_file_stream,
                                          string content_type =
                                            "application/flowfile-v3",
                                          CancellationToken ct = default) {
    logger_?.LogDebug(
      "Posting FlowFile to {Url} with content type {ContentType}",
      url,
      content_type
    );

    StreamContent content = new(flow_file_stream);
    content.Headers.ContentType = new MediaTypeHeaderValue(content_type);

    HttpResponseMessage response =
      await http_client_.PostAsync(url, content, ct);

    if (!response.IsSuccessStatusCode) {
      logger_?.LogWarning(
        "PostToNiFi failed with status code {StatusCode}",
        response.StatusCode
      );
    }

    return response.IsSuccessStatusCode;
  }

  public async Task<bool> SendViaS2SAsync(string nifi_base_url,
                                          string input_port_name,
                                          IEnumerable<NifiPackage> packages,
                                          CancellationToken ct = default) {
    if (pipeline_provider_ != null &&
        pipeline_provider_.TryGetPipeline(
          "NifiS2S",
          out ResiliencePipeline? pipeline
        )) {
      return await pipeline.ExecuteAsync(
               async token => await SendViaS2SInternalAsync(
                                nifi_base_url,
                                input_port_name,
                                packages,
                                token
                              ),
               ct
             );
    }

    return await SendViaS2SInternalAsync(
             nifi_base_url,
             input_port_name,
             packages,
             ct
           );
  }

  private async Task<bool> SendViaS2SInternalAsync(
    string nifi_base_url, string input_port_name,
    IEnumerable<NifiPackage> packages, CancellationToken ct) {
    nifi_base_url = nifi_base_url.TrimEnd('/');
    logger_?.LogInformation(
      "Starting Site-to-Site transfer to {BaseUrl}, Port: {PortName}",
      nifi_base_url,
      input_port_name
    );

    // 1. Discover Port ID
    NifiSiteToSiteDto? s2_s_info =
      await http_client_.GetFromJsonAsync<NifiSiteToSiteDto>(
        $"{nifi_base_url}/nifi-api/site-to-site",
        ct
      );
    NifiPortDto? port = s2_s_info?.controller?.input_ports?.FirstOrDefault(
      p => p.name != null &&
           p.name.Equals(input_port_name, StringComparison.OrdinalIgnoreCase)
    );

    if (port == null || string.IsNullOrEmpty(port.id)) {
      logger_?.LogError(
        "Could not find input port '{PortName}' at {BaseUrl}",
        input_port_name,
        nifi_base_url
      );
      return false;
    }

    // 2. Create Transaction
    HttpResponseMessage transaction_response =
      await http_client_.PostAsJsonAsync(
        $"{nifi_base_url}/nifi-api/site-to-site/input-ports/{port.id}/transactions",
        new {
        },
        ct
      );
    if (!transaction_response.IsSuccessStatusCode) {
      logger_?.LogWarning(
        "Failed to create S2S transaction for port {PortId}. Status: {StatusCode}",
        port.id,
        transaction_response.StatusCode
      );
      return false;
    }

    NifiTransactionDto? transaction =
      await transaction_response.Content
                                .ReadFromJsonAsync<NifiTransactionDto>(ct);
    if (transaction == null || string.IsNullOrEmpty(transaction.id)) {
      logger_?.LogError(
        "S2S transaction response was empty for port {PortId}",
        port.id
      );
      return false;
    }

    logger_?.LogDebug(
      "Created S2S transaction {TransactionId}",
      transaction.id
    );

    try {
      // 3. Send Data (FlowFile V3 Stream)
      using MemoryStream ms = new();
      await flow_file_service_.WriteFlowFilesV3Async(packages, ms);
      ms.Position = 0;

      StreamContent content = new(ms);
      content.Headers.ContentType =
        new MediaTypeHeaderValue("application/flowfile-v3");

      string transfer_url =
        $"{nifi_base_url}/nifi-api/site-to-site/input-ports/{port.id}/transactions/{transaction.id}/flow-files";
      HttpResponseMessage transfer_response =
        await http_client_.PostAsync(transfer_url, content, ct);

      if (!transfer_response.IsSuccessStatusCode) {
        logger_?.LogWarning(
          "Failed to transfer flow files in transaction {TransactionId}. Status: {StatusCode}",
          transaction.id,
          transfer_response.StatusCode
        );
        return false;
      }

      // 4. Commit Transaction
      string confirm_url =
        $"{nifi_base_url}/nifi-api/site-to-site/input-ports/{port.id}/transactions/{transaction.id}?state=TRANSACTION_CONFIRMED";
      HttpResponseMessage confirm_response =
        await http_client_.PutAsync(confirm_url, null, ct);

      if (confirm_response.IsSuccessStatusCode) {
        logger_?.LogInformation(
          "Successfully committed S2S transaction {TransactionId}",
          transaction.id
        );
      } else {
        logger_?.LogWarning(
          "Failed to commit S2S transaction {TransactionId}. Status: {StatusCode}",
          transaction.id,
          confirm_response.StatusCode
        );
      }

      return confirm_response.IsSuccessStatusCode;
    } catch (Exception ex) {
      logger_?.LogError(
        ex,
        "Error during S2S transfer in transaction {TransactionId}",
        transaction.id
      );
      return false;
    }
  }

  #endregion

  #region DTOs for NiFi S2S API

  private class NifiSiteToSiteDto {
    [JsonPropertyName("controller")]
    public NifiControllerDto? controller { get; set; }
  }

  private class NifiControllerDto {
    [JsonPropertyName("inputPorts")]
    public List<NifiPortDto>? input_ports { get; set; }
  }

  private class NifiPortDto {
    [JsonPropertyName("id")]
    public string? id { get; set; }

    [JsonPropertyName("name")]
    public string? name { get; set; }
  }

  private class NifiTransactionDto {
    [JsonPropertyName("id")]
    public string? id { get; init; }
  }

  #endregion
}