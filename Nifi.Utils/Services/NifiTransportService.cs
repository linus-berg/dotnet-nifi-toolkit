using System.Net.Http.Headers;
using System.Net.Http.Json;
using Nifi.Utils.Models;

namespace Nifi.Utils.Services;

public class NifiTransportService : INifiTransportService {
  private readonly IFlowFileService flow_file_service_;
  private readonly HttpClient http_client_;

  public NifiTransportService(HttpClient http_client,
                              IFlowFileService flow_file_service) {
    http_client_ = http_client;
    flow_file_service_ = flow_file_service;
  }

  #region Transport

  public async Task<bool> PostToNiFiAsync(string url, Stream flow_file_stream,
                                          string content_type =
                                            "application/flowfile-v3") {
    StreamContent content = new(flow_file_stream);
    content.Headers.ContentType = new MediaTypeHeaderValue(content_type);

    HttpResponseMessage response = await http_client_.PostAsync(url, content);
    return response.IsSuccessStatusCode;
  }

  public async Task<bool> SendViaS2SAsync(string nifi_base_url,
                                          string input_port_name,
                                          IEnumerable<NifiPackage> packages) {
    nifi_base_url = nifi_base_url.TrimEnd('/');

    // 1. Discover Port ID
    NifiSiteToSiteDto? s2_s_info =
      await http_client_.GetFromJsonAsync<NifiSiteToSiteDto>(
        $"{nifi_base_url}/nifi-api/site-to-site"
      );
    NifiPortDto? port = s2_s_info?.controller?.input_ports?.FirstOrDefault(
      p => p.name != null &&
           p.name.Equals(input_port_name, StringComparison.OrdinalIgnoreCase)
    );
    if (port == null || string.IsNullOrEmpty(port.id)) {
      return false;
    }

    // 2. Create Transaction
    HttpResponseMessage transaction_response =
      await http_client_.PostAsJsonAsync(
        $"{nifi_base_url}/nifi-api/site-to-site/input-ports/{port.id}/transactions",
        new {
        }
      );
    if (!transaction_response.IsSuccessStatusCode) {
      return false;
    }

    NifiTransactionDto? transaction =
      await transaction_response.Content
                                .ReadFromJsonAsync<NifiTransactionDto>();
    if (transaction == null || string.IsNullOrEmpty(transaction.id)) {
      return false;
    }

    try {
      // 3. Send Data (FlowFile V3 Stream)
      using MemoryStream ms = new();
      await flow_file_service_.WriteFlowFilesV3Async(packages, ms);
      ms.Position = 0;

      StreamContent content = new(ms);
      content.Headers.ContentType =
        new MediaTypeHeaderValue("application/flowfile-v3");

      // NiFi S2S Transaction flow-files endpoint
      string transfer_url =
        $"{nifi_base_url}/nifi-api/site-to-site/input-ports/{port.id}/transactions/{transaction.id}/flow-files";
      HttpResponseMessage transfer_response =
        await http_client_.PostAsync(transfer_url, content);
      if (!transfer_response.IsSuccessStatusCode) {
        return false;
      }

      // 4. Commit Transaction
      // First: Confirm (set state to TRANSACTION_CONFIRMED)
      string confirm_url =
        $"{nifi_base_url}/nifi-api/site-to-site/input-ports/{port.id}/transactions/{transaction.id}?state=TRANSACTION_CONFIRMED";
      HttpResponseMessage confirm_response =
        await http_client_.PutAsync(confirm_url, null);

      return confirm_response.IsSuccessStatusCode;
    } catch {
      // Optional: Try to cancel the transaction if possible
      return false;
    }
  }

  #endregion

  #region DTOs for NiFi S2S API

  private class NifiSiteToSiteDto {
    public NifiControllerDto? controller { get; set; }
  }

  private class NifiControllerDto {
    public List<NifiPortDto>? input_ports { get; set; }
  }

  private class NifiPortDto {
    public string? id { get; set; }
    public string? name { get; set; }
  }

  private class NifiTransactionDto {
    public string? id { get; set; }
  }

  #endregion
}