using Microsoft.Extensions.DependencyInjection.Extensions;
using NifiKit.Security;
using NifiKit.Services;
using NifiKit.SiteToSite;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>
///   Options for <see cref="NifiKitServiceCollectionExtensions.AddNifiKit" />.
/// </summary>
public sealed class NifiKitOptions {
  /// <summary>
  ///   TLS / mutual TLS settings for the transport HttpClient.
  /// </summary>
  public NifiTlsOptions tls { get; } = new();

  /// <summary>
  ///   Limits used by <see cref="IFlowFileService" /> when unpacking.
  /// </summary>
  public FlowFileServiceOptions flow_files { get; } = new();

  /// <summary>
  ///   HttpClient timeout per request. A Site-to-Site upload is a single
  ///   request, so raise this for large transactions. Defaults to 100 seconds.
  /// </summary>
  public TimeSpan http_timeout { get; set; } = TimeSpan.FromSeconds(100);
}

public static class NifiKitServiceCollectionExtensions {
  /// <summary>
  ///   Registers <see cref="IFlowFileService" /> and
  ///   <see cref="INifiTransportService" />, the latter as a typed HttpClient
  ///   whose primary handler applies <see cref="NifiKitOptions.tls" />
  ///   (client certificate, trusted CAs).
  /// </summary>
  /// <returns>
  ///   The HttpClient builder, for adding further handlers. Avoid generic
  ///   HTTP retry handlers here: they retry individual Site-to-Site requests,
  ///   which are not idempotent. Register a resilience pipeline named
  ///   <see cref="NifiTransportService.S2S_PIPELINE_NAME" /> instead; it
  ///   retries the whole transaction.
  /// </returns>
  public static IHttpClientBuilder AddNifiKit(
    this IServiceCollection services,
    Action<NifiKitOptions>? configure = null) {
    ArgumentNullException.ThrowIfNull(services);

    NifiKitOptions options = new();
    configure?.Invoke(options);

    services.TryAddSingleton(options.flow_files);
    services.TryAddSingleton<IFlowFileService, FlowFileService>();
    // Shared across the transient typed-client instances
    services.TryAddSingleton<SiteToSitePortCache>();

    return services
           .AddHttpClient<INifiTransportService, NifiTransportService>(
             client => client.Timeout = options.http_timeout
           )
           .ConfigurePrimaryHttpMessageHandler(
             () => NifiHttpHandlerFactory.Create(options.tls)
           );
  }
}
