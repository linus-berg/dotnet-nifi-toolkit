using System.Globalization;
using System.Net;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using NifiKit.Models;
using NifiKit.SiteToSite;
using Polly;
using Polly.Registry;

namespace NifiKit.Services;

/// <summary>
///   Transports FlowFiles to NiFi via ListenHTTP and HTTP(S) Site-to-Site.
/// </summary>
/// <remarks>
///   Site-to-Site follows NiFi's HTTP transport protocol version 1
///   (DataTransferResource / SiteToSiteRestApiClient):
///   <list type="number">
///     <item>GET <c>/nifi-api/site-to-site</c> resolves the input port id.</item>
///     <item>POST <c>/nifi-api/data-transfer/input-ports/{id}/transactions</c>
///       creates a transaction; its URL is returned in the Location header.</item>
///     <item>POST <c>{transaction}/flow-files</c> streams the FlowFiles encoded
///       with StandardFlowFileCodec; NiFi answers with its CRC32 checksum.</item>
///     <item>DELETE <c>{transaction}?responseCode=12</c> confirms (commits) the
///       transaction when the checksums match; 19 (bad checksum) or 15 (cancel)
///       roll it back.</item>
///   </list>
///   Delivery is at-least-once: if the commit response is lost, a retry can
///   deliver the same FlowFiles again.
/// </remarks>
public class NifiTransportService : INifiTransportService {
  /// <summary>
  ///   Name of the optional resilience pipeline wrapping each S2S transaction.
  /// </summary>
  public const string S2S_PIPELINE_NAME = "NifiS2S";

  private const string FLOWFILE_V3_CONTENT_TYPE = "application/flowfile-v3";
  private const int MAX_ERROR_BODY_LENGTH = 1000;
  private const string RUNNING_STATE = "RUNNING";

  // Upper bound for best-effort transaction cancellation after a failure
  private static readonly TimeSpan S_CANCEL_TIMEOUT_ = TimeSpan.FromSeconds(10);

  private readonly IFlowFileService flow_file_service_;
  private readonly HttpClient http_client_;
  private readonly ILogger<NifiTransportService>? logger_;
  private readonly ResiliencePipelineProvider<string>? pipeline_provider_;
  private readonly SiteToSitePortCache port_cache_;

  public NifiTransportService(HttpClient http_client,
                              IFlowFileService flow_file_service,
                              ILogger<NifiTransportService>? logger = null,
                              ResiliencePipelineProvider<string>?
                                pipeline_provider = null,
                              SiteToSitePortCache? port_cache = null) {
    ArgumentNullException.ThrowIfNull(http_client);
    ArgumentNullException.ThrowIfNull(flow_file_service);
    http_client_ = http_client;
    flow_file_service_ = flow_file_service;
    logger_ = logger;
    pipeline_provider_ = pipeline_provider;
    port_cache_ = port_cache ?? new SiteToSitePortCache();
  }

  #region Transport: ListenHTTP

  /// <inheritdoc />
  public async Task<bool> PostToNiFiAsync(string url, Stream flow_file_stream,
                                          string content_type =
                                            FLOWFILE_V3_CONTENT_TYPE,
                                          CancellationToken ct = default) {
    logger_?.LogDebug(
      "Posting FlowFile to {Url} with content type {ContentType}",
      url,
      content_type
    );

    // Not disposed on purpose: disposing StreamContent would close the
    // caller's stream.
    StreamContent content = new(flow_file_stream);
    content.Headers.ContentType = new MediaTypeHeaderValue(content_type);

    using HttpResponseMessage response =
      await http_client_.PostAsync(url, content, ct);

    if (!response.IsSuccessStatusCode) {
      logger_?.LogWarning(
        "PostToNiFi failed with status code {StatusCode}",
        response.StatusCode
      );
    }

    return response.IsSuccessStatusCode;
  }

  /// <summary>
  ///   Packs the packages as FlowFile V3 and posts them to a ListenHTTP
  ///   processor in a single request.
  /// </summary>
  public async Task<bool> PostToNiFiAsync(string url,
                                          IEnumerable<NifiPackage> packages,
                                          CancellationToken ct = default) {
    using MemoryStream ms = new();
    await flow_file_service_.WriteFlowFilesV3Async(packages, ms, ct);
    ms.Position = 0;
    return await PostToNiFiAsync(url, ms, FLOWFILE_V3_CONTENT_TYPE, ct);
  }

  #endregion

  #region Transport: Site-to-Site

  /// <inheritdoc />
  public async Task<SiteToSiteTransferResult> SendViaS2SAsync(
    string nifi_base_url,
    string input_port_name,
    IEnumerable<NifiPackage> packages,
    CancellationToken ct = default) {
    ArgumentException.ThrowIfNullOrEmpty(nifi_base_url);
    ArgumentException.ThrowIfNullOrEmpty(input_port_name);
    ArgumentNullException.ThrowIfNull(packages);

    Uri api_base = ResolveApiBaseUrl(nifi_base_url);

    // Encode once, outside the retry loop: every attempt sends exactly the
    // same bytes, and non-seekable content is only read once.
    List<SiteToSitePacket> packets = new();
    try {
      foreach (NifiPackage package in packages) {
        packets.Add(await SiteToSiteCodec.CreatePacketAsync(package, ct));
      }

      if (packets.Count == 0) {
        logger_?.LogDebug("No FlowFiles to send to port {PortName}",
                          input_port_name);
        return new SiteToSiteTransferResult();
      }

      if (pipeline_provider_ != null &&
          pipeline_provider_.TryGetPipeline(
            S2S_PIPELINE_NAME,
            out ResiliencePipeline? pipeline
          )) {
        return await pipeline.ExecuteAsync(
                 async token => await SendTransactionAsync(
                                  api_base,
                                  input_port_name,
                                  packets,
                                  token
                                ),
                 ct
               );
      }

      return await SendTransactionAsync(
               api_base,
               input_port_name,
               packets,
               ct
             );
    } finally {
      foreach (SiteToSitePacket packet in packets) {
        await packet.DisposeAsync();
      }
    }
  }

  private async Task<SiteToSiteTransferResult> SendTransactionAsync(
    Uri api_base, string port_name, IReadOnlyList<SiteToSitePacket> packets,
    CancellationToken ct) {
    try {
      return await SendTransactionCoreAsync(api_base, port_name, packets, ct);
    } catch (OperationCanceledException ex) when (!ct.IsCancellationRequested) {
      // Not the caller's token: HttpClient.Timeout elapsed
      throw new NifiSiteToSiteException(
        $"Site-to-Site transfer to {api_base} timed out.",
        true,
        inner_exception: ex
      );
    }
  }

  private async Task<SiteToSiteTransferResult> SendTransactionCoreAsync(
    Uri api_base, string port_name, IReadOnlyList<SiteToSitePacket> packets,
    CancellationToken ct) {
    logger_?.LogInformation(
      "Starting Site-to-Site transfer of {Count} FlowFiles to {BaseUrl}, Port: {PortName}",
      packets.Count,
      api_base,
      port_name
    );

    // 1. Discover the port id
    (string port_id, bool cached) =
      await ResolveInputPortIdAsync(api_base, port_name, ct);

    // 2. Create a transaction
    Uri transaction_url =
      await CreateTransactionAsync(api_base, port_name, port_id, cached, ct);
    logger_?.LogDebug("Created S2S transaction {TransactionUrl}",
                      transaction_url);

    // Once NiFi has resolved the transaction (committed or rolled back) it
    // must not be cancelled; before that, any failure cancels it.
    bool resolved = false;
    try {
      // 3. Send the FlowFiles; NiFi replies with the CRC32 of what it read
      (string server_checksum, string local_checksum, long bytes_sent) =
        await SendFlowFilesAsync(transaction_url, packets, ct);

      if (!string.Equals(server_checksum, local_checksum,
                         StringComparison.Ordinal)) {
        logger_?.LogWarning(
          "S2S checksum mismatch in {TransactionUrl}: sent {LocalChecksum}, NiFi received {ServerChecksum}",
          transaction_url,
          local_checksum,
          server_checksum
        );
        await FinishTransactionAsync(
          transaction_url,
          SiteToSiteResponseCode.BAD_CHECKSUM,
          ct
        );
        resolved = true;
        throw new NifiSiteToSiteException(
          $"CRC32 mismatch for transaction {transaction_url}: sent " +
          $"{local_checksum}, NiFi received {server_checksum}. NiFi rolled " +
          "the transaction back.",
          true,
          response_code: SiteToSiteResponseCode.BAD_CHECKSUM
        );
      }

      // 4. Confirm (commit) the transaction
      TransactionResultDto result = await FinishTransactionAsync(
                                      transaction_url,
                                      SiteToSiteResponseCode
                                        .CONFIRM_TRANSACTION,
                                      ct
                                    );
      resolved = true;

      SiteToSiteResponseCode code =
        (SiteToSiteResponseCode)result.response_code;
      if (code != SiteToSiteResponseCode.TRANSACTION_FINISHED &&
          code != SiteToSiteResponseCode
                  .TRANSACTION_FINISHED_BUT_DESTINATION_FULL) {
        throw new NifiSiteToSiteException(
          $"NiFi did not finish transaction {transaction_url}: {code} " +
          $"({result.response_code}) {result.message}",
          false,
          response_code: code
        );
      }

      if (result.flow_file_sent != packets.Count) {
        logger_?.LogWarning(
          "NiFi confirmed {Confirmed} FlowFiles in {TransactionUrl}, but {Sent} were sent",
          result.flow_file_sent,
          transaction_url,
          packets.Count
        );
      }

      bool destination_full =
        code == SiteToSiteResponseCode.TRANSACTION_FINISHED_BUT_DESTINATION_FULL;
      logger_?.LogInformation(
        "Committed S2S transaction {TransactionUrl}: {Count} FlowFiles, {Bytes} bytes, destination full: {DestinationFull}",
        transaction_url,
        result.flow_file_sent,
        bytes_sent,
        destination_full
      );

      return new SiteToSiteTransferResult {
        transaction_url = transaction_url,
        flow_files_sent = result.flow_file_sent,
        bytes_sent = bytes_sent,
        destination_full = destination_full,
        checksum = local_checksum
      };
    } catch (Exception) when (!resolved) {
      await CancelTransactionQuietlyAsync(transaction_url);
      throw;
    }
  }

  /// <summary>
  ///   Resolves an input port name to its id via GET /nifi-api/site-to-site.
  ///   NiFi only lists ports the client identity may send data to.
  /// </summary>
  private async Task<(string port_id, bool cached)> ResolveInputPortIdAsync(
    Uri api_base, string port_name, CancellationToken ct) {
    if (port_cache_.TryGet(api_base, port_name, out string cached_id)) {
      return (cached_id, true);
    }

    Uri details_url = new(api_base, SiteToSiteProtocol.SITE_TO_SITE_PATH);
    using HttpRequestMessage request =
      CreateRequest(HttpMethod.Get, details_url, "application/json");
    using HttpResponseMessage response =
      await http_client_.SendAsync(request, ct);
    if (!response.IsSuccessStatusCode) {
      throw await CreateHttpExceptionAsync(
              "Retrieving Site-to-Site details",
              response,
              null,
              "the client identity needs the 'retrieve site-to-site details' " +
              "policy",
              ct
            );
    }

    SiteToSiteDetailsDto? details =
      await response.Content.ReadFromJsonAsync<SiteToSiteDetailsDto>(ct);
    List<PortDto> ports = details?.controller?.input_ports ?? new List<PortDto>();

    // Exact match, like NiFi's own client
    PortDto? port = ports.FirstOrDefault(
      p => string.Equals(p.name, port_name, StringComparison.Ordinal)
    );
    if (port == null || string.IsNullOrEmpty(port.id)) {
      string available = string.Join(
        ", ",
        ports.Select(p => $"'{p.name}'")
      );
      throw new NifiSiteToSiteException(
        $"Input port '{port_name}' was not found at {api_base}. NiFi only " +
        "lists input ports that allow remote access and that the client " +
        "identity has the 'receive data via site-to-site' policy for. " +
        $"Available: [{available}]",
        false,
        response_code: SiteToSiteResponseCode.UNKNOWN_PORT
      );
    }

    if (port.state != null &&
        !string.Equals(port.state, RUNNING_STATE, StringComparison.Ordinal)) {
      throw new NifiSiteToSiteException(
        $"Input port '{port_name}' is {port.state}; it must be running to " +
        "receive data.",
        true,
        response_code: SiteToSiteResponseCode.PORT_NOT_IN_VALID_STATE
      );
    }

    port_cache_.Set(api_base, port_name, port.id);
    return (port.id, false);
  }

  private async Task<Uri> CreateTransactionAsync(Uri api_base,
                                                 string port_name,
                                                 string port_id, bool cached,
                                                 CancellationToken ct) {
    Uri transactions_url = new(
      api_base,
      string.Format(
        CultureInfo.InvariantCulture,
        SiteToSiteProtocol.INPUT_PORT_TRANSACTIONS_PATH,
        Uri.EscapeDataString(port_id)
      )
    );
    using HttpRequestMessage request =
      CreateRequest(HttpMethod.Post, transactions_url, "application/json");
    using HttpResponseMessage response =
      await http_client_.SendAsync(request, ct);

    if (!response.IsSuccessStatusCode) {
      // The port may have been deleted or re-created: look it up again next
      // time. A stale cached id is worth a retry.
      port_cache_.Remove(api_base, port_name);
      bool? transient = response.StatusCode == HttpStatusCode.NotFound
                          ? cached
                          : null;
      throw await CreateHttpExceptionAsync(
              $"Creating a transaction on input port '{port_name}'",
              response,
              transient,
              "the client identity needs the 'receive data via site-to-site' " +
              "policy on the input port",
              ct
            );
    }

    Uri? location = response.Headers.Location;
    if (location == null) {
      throw new NifiSiteToSiteException(
        "NiFi created a Site-to-Site transaction but returned no Location " +
        "header with the transaction URL.",
        false,
        response.StatusCode
      );
    }

    return location.IsAbsoluteUri ? location : new Uri(transactions_url, location);
  }

  private async Task<(string server_checksum, string local_checksum, long
      bytes_sent)>
    SendFlowFilesAsync(Uri transaction_url,
                       IReadOnlyList<SiteToSitePacket> packets,
                       CancellationToken ct) {
    Uri flow_files_url = new(transaction_url.AbsoluteUri.TrimEnd('/') +
                             "/flow-files");
    SiteToSiteContent content = new(packets);
    using HttpRequestMessage request =
      CreateRequest(HttpMethod.Post, flow_files_url, "text/plain");
    request.Content = content;
    using HttpResponseMessage response =
      await http_client_.SendAsync(request, ct);

    if (!response.IsSuccessStatusCode) {
      // 404 here means the transaction expired; a new one may succeed
      bool? transient = response.StatusCode == HttpStatusCode.NotFound
                          ? true
                          : null;
      throw await CreateHttpExceptionAsync(
              "Sending FlowFiles",
              response,
              transient,
              null,
              ct
            );
    }

    string server_checksum =
      (await response.Content.ReadAsStringAsync(ct)).Trim();
    return (server_checksum, content.checksum, content.length);
  }

  /// <summary>
  ///   Confirms, rejects (bad checksum) or cancels a transaction.
  /// </summary>
  private async Task<TransactionResultDto> FinishTransactionAsync(
    Uri transaction_url, SiteToSiteResponseCode response_code,
    CancellationToken ct) {
    Uri url = new(
      transaction_url.AbsoluteUri +
      $"?responseCode={(int)response_code}"
    );
    using HttpRequestMessage request =
      CreateRequest(HttpMethod.Delete, url, "application/json");
    using HttpResponseMessage response =
      await http_client_.SendAsync(request, ct);
    if (!response.IsSuccessStatusCode) {
      throw await CreateHttpExceptionAsync(
              $"Finishing the transaction with {response_code}",
              response,
              null,
              null,
              ct
            );
    }

    return await response.Content.ReadFromJsonAsync<TransactionResultDto>(ct)
           ?? throw new NifiSiteToSiteException(
             "NiFi returned an empty response when finishing the transaction.",
             false,
             response.StatusCode
           );
  }

  /// <summary>
  ///   Best-effort cancellation so NiFi releases the transaction immediately
  ///   instead of waiting for it to expire.
  /// </summary>
  private async Task CancelTransactionQuietlyAsync(Uri transaction_url) {
    try {
      using CancellationTokenSource cts = new(S_CANCEL_TIMEOUT_);
      await FinishTransactionAsync(
        transaction_url,
        SiteToSiteResponseCode.CANCEL_TRANSACTION,
        cts.Token
      );
      logger_?.LogDebug("Cancelled S2S transaction {TransactionUrl}",
                        transaction_url);
    } catch (Exception ex) {
      logger_?.LogDebug(
        ex,
        "Could not cancel S2S transaction {TransactionUrl}",
        transaction_url
      );
    }
  }

  #endregion

  #region Helpers

  /// <summary>
  ///   Resolves the NiFi REST API base URL the same way NiFi's own
  ///   Site-to-Site client does (ClusterUrlParser): <c>https://host:8443</c>,
  ///   <c>.../nifi</c> and <c>.../nifi-api</c> all map to <c>.../nifi-api/</c>;
  ///   any other path (e.g. a proxy prefix) gets <c>/nifi-api</c> appended.
  /// </summary>
  internal static Uri ResolveApiBaseUrl(string nifi_base_url) {
    if (!Uri.TryCreate(nifi_base_url.Trim(), UriKind.Absolute, out Uri? uri) ||
        (uri.Scheme != Uri.UriSchemeHttp && uri.Scheme != Uri.UriSchemeHttps)) {
      throw new ArgumentException(
        $"'{nifi_base_url}' is not an absolute http or https URL.",
        nameof(nifi_base_url)
      );
    }

    string path = uri.AbsolutePath.TrimEnd('/');
    if (path.Length == 0) {
      path = "/nifi-api";
    } else if (path.EndsWith("/nifi", StringComparison.Ordinal)) {
      path += "-api";
    } else if (!path.EndsWith("/nifi-api", StringComparison.Ordinal)) {
      path += "/nifi-api";
    }

    return new UriBuilder(uri.Scheme, uri.Host, uri.Port, path + "/").Uri;
  }

  private static HttpRequestMessage CreateRequest(HttpMethod method, Uri url,
                                                  string accept) {
    HttpRequestMessage request = new(method, url);
    request.Headers.TryAddWithoutValidation(
      SiteToSiteProtocol.PROTOCOL_VERSION_HEADER,
      SiteToSiteProtocol.PROTOCOL_VERSION
    );
    request.Headers.Accept.ParseAdd(accept);
    return request;
  }

  /// <summary>
  ///   Builds an exception from an error response. NiFi usually returns a
  ///   TransactionResultEntity (JSON) with a response code and message.
  /// </summary>
  private static async Task<NifiSiteToSiteException> CreateHttpExceptionAsync(
    string action, HttpResponseMessage response, bool? is_transient,
    string? authorization_hint, CancellationToken ct) {
    string body = await ReadBodySafelyAsync(response, ct);
    SiteToSiteResponseCode? response_code = null;
    string detail = body;
    try {
      TransactionResultDto? result =
        JsonSerializer.Deserialize<TransactionResultDto>(body);
      if (result != null && result.response_code != 0) {
        response_code = (SiteToSiteResponseCode)result.response_code;
        detail = $"{response_code}: {result.message}";
      }
    } catch (JsonException) {
      // Plain-text error body
    }

    HttpStatusCode status = response.StatusCode;
    string message =
      $"{action} failed with HTTP {(int)status} ({status}): {detail}";
    if (authorization_hint != null &&
        status is HttpStatusCode.Unauthorized or HttpStatusCode.Forbidden) {
      message += $" Hint: {authorization_hint}.";
    }

    return new NifiSiteToSiteException(
      message,
      is_transient ?? IsTransientStatus(status),
      status,
      response_code
    );
  }

  private static bool IsTransientStatus(HttpStatusCode status) {
    return status is HttpStatusCode.RequestTimeout
                     or HttpStatusCode.TooManyRequests
                     or HttpStatusCode.InternalServerError
                     or HttpStatusCode.BadGateway
                     or HttpStatusCode.ServiceUnavailable
                     or HttpStatusCode.GatewayTimeout;
  }

  private static async Task<string> ReadBodySafelyAsync(
    HttpResponseMessage response, CancellationToken ct) {
    try {
      string body = await response.Content.ReadAsStringAsync(ct);
      return body.Length > MAX_ERROR_BODY_LENGTH
               ? body.Substring(0, MAX_ERROR_BODY_LENGTH) + "..."
               : body;
    } catch (Exception ex) when (ex is not OperationCanceledException) {
      return string.Empty;
    }
  }

  #endregion
}
