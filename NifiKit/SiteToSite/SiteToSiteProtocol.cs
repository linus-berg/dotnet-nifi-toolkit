using System.Text.Json.Serialization;

namespace NifiKit.SiteToSite;

/// <summary>
///   HTTP Site-to-Site constants (org.apache.nifi.remote.protocol.http.HttpHeaders
///   and DataTransferResource).
/// </summary>
internal static class SiteToSiteProtocol {
  // The only HTTP transport protocol version NiFi supports
  public const string PROTOCOL_VERSION = "1";

  public const string PROTOCOL_VERSION_HEADER =
    "x-nifi-site-to-site-protocol-version";

  public const string LOCATION_URI_INTENT_HEADER = "x-location-uri-intent";
  public const string LOCATION_URI_INTENT_VALUE = "transaction-url";

  // Relative to the API base URL (".../nifi-api/")
  public const string SITE_TO_SITE_PATH = "site-to-site";

  public const string INPUT_PORT_TRANSACTIONS_PATH =
    "data-transfer/input-ports/{0}/transactions";

  public const string OCTET_STREAM = "application/octet-stream";

  // StandardFlowFileCodec.MAX_NUM_ATTRIBUTES
  public const int MAX_NUM_ATTRIBUTES = 25_000;
}

/// <summary>
///   GET /nifi-api/site-to-site (ControllerEntity).
/// </summary>
internal sealed class SiteToSiteDetailsDto {
  [JsonPropertyName("controller")]
  public ControllerDto? controller { get; set; }
}

internal sealed class ControllerDto {
  [JsonPropertyName("inputPorts")]
  public List<PortDto>? input_ports { get; set; }

  [JsonPropertyName("siteToSiteSecure")]
  public bool? site_to_site_secure { get; set; }
}

internal sealed class PortDto {
  [JsonPropertyName("id")]
  public string? id { get; set; }

  [JsonPropertyName("name")]
  public string? name { get; set; }

  // RUNNING, STOPPED or DISABLED
  [JsonPropertyName("state")]
  public string? state { get; set; }
}

/// <summary>
///   TransactionResultEntity returned by the data-transfer endpoints.
/// </summary>
internal sealed class TransactionResultDto {
  [JsonPropertyName("flowFileSent")]
  public int flow_file_sent { get; set; }

  [JsonPropertyName("responseCode")]
  public int response_code { get; set; }

  [JsonPropertyName("message")]
  public string? message { get; set; }
}
