using System.Net;

namespace NifiKit.SiteToSite;

/// <summary>
///   Thrown when a Site-to-Site transfer fails. Network failures surface as
///   <see cref="HttpRequestException" /> and caller cancellation as
///   <see cref="OperationCanceledException" />.
/// </summary>
/// <remarks>
///   Use <see cref="is_transient" /> in a Polly <c>ShouldHandle</c> predicate
///   to retry only failures that can succeed on another attempt.
/// </remarks>
public class NifiSiteToSiteException : Exception {
  public NifiSiteToSiteException(string message,
                                 bool is_transient,
                                 HttpStatusCode? status_code = null,
                                 SiteToSiteResponseCode? response_code = null,
                                 Exception? inner_exception = null)
    : base(message, inner_exception) {
    this.is_transient = is_transient;
    this.status_code = status_code;
    this.response_code = response_code;
  }

  /// <summary>
  ///   True when retrying the transfer may succeed (e.g. NiFi unavailable, port
  ///   stopped or full, checksum mismatch, timeout). False for configuration
  ///   and authorization problems that a retry will not fix.
  /// </summary>
  public bool is_transient { get; }

  /// <summary>
  ///   HTTP status returned by NiFi, if the failure came from an HTTP response.
  /// </summary>
  public HttpStatusCode? status_code { get; }

  /// <summary>
  ///   Site-to-Site response code reported by NiFi, if any.
  /// </summary>
  public SiteToSiteResponseCode? response_code { get; }
}
