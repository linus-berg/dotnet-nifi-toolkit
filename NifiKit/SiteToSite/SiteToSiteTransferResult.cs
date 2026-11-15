namespace NifiKit.SiteToSite;

/// <summary>
///   Outcome of a committed Site-to-Site transaction.
/// </summary>
public sealed class SiteToSiteTransferResult {
  /// <summary>
  ///   The transaction URL NiFi assigned (null when there was nothing to send).
  /// </summary>
  public Uri? transaction_url { get; init; }

  /// <summary>
  ///   Number of FlowFiles NiFi confirmed for the transaction.
  /// </summary>
  public int flow_files_sent { get; init; }

  /// <summary>
  ///   Number of bytes sent in the request body (encoded attributes + content).
  /// </summary>
  public long bytes_sent { get; init; }

  /// <summary>
  ///   The data was committed, but NiFi reported that the port's destination
  ///   is full (back-pressure). Callers should back off before sending more.
  /// </summary>
  public bool destination_full { get; init; }

  /// <summary>
  ///   CRC32 checksum of the sent data, as verified by both client and NiFi.
  /// </summary>
  public string? checksum { get; init; }
}
