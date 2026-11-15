using NifiKit.Models;
using NifiKit.SiteToSite;

namespace NifiKit.Services;

/// <summary>
///   Service for transporting NiFi FlowFiles using ListenHTTP and Site-to-Site (S2S).
///   TLS and mutual TLS are configured on the HttpClient handler, see
///   <see cref="NifiKit.Security.NifiHttpHandlerFactory" />.
/// </summary>
public interface INifiTransportService {
  // --- Transport: ListenHTTP ---
  Task<bool> PostToNiFiAsync(string url, Stream flow_file_stream,
                             string content_type = "application/flowfile-v3",
                             CancellationToken ct = default);

  // --- Transport: Site-to-Site (S2S) ---

  /// <summary>
  ///   Sends the packages to a NiFi input port over HTTP(S) Site-to-Site in a
  ///   single transaction, verifies NiFi's CRC32 checksum and commits.
  /// </summary>
  /// <param name="nifi_base_url">
  ///   NiFi's base URL, e.g. <c>https://nifi.example.com:8443</c> (the part
  ///   before <c>/nifi-api</c>).
  /// </param>
  /// <param name="input_port_name">Exact name of the remote input port.</param>
  /// <param name="packages">The FlowFiles to send in this transaction.</param>
  /// <param name="ct">Cancellation token.</param>
  /// <returns>
  ///   The committed transaction. When <paramref name="packages" /> is empty
  ///   nothing is sent and <see cref="SiteToSiteTransferResult.flow_files_sent" />
  ///   is 0.
  /// </returns>
  /// <exception cref="NifiSiteToSiteException">The transfer failed.</exception>
  /// <exception cref="HttpRequestException">A network error occurred.</exception>
  Task<SiteToSiteTransferResult> SendViaS2SAsync(
    string nifi_base_url, string input_port_name,
    IEnumerable<NifiPackage> packages,
    CancellationToken ct = default);
}
