using System.Net.Security;
using System.Security.Cryptography.X509Certificates;

namespace NifiKit.Security;

/// <summary>
///   Creates HTTP handlers configured for NiFi, including mutual TLS.
/// </summary>
/// <example>
///   <code>
///   NifiTlsOptions tls = new() {
///     client_certificate = NifiCertificateLoader.LoadPkcs12("client.p12", "secret"),
///     trusted_root_certificates = NifiCertificateLoader.LoadCertificates("ca.crt")
///   };
///   HttpClient http_client = new(NifiHttpHandlerFactory.Create(tls));
///   </code>
/// </example>
public static class NifiHttpHandlerFactory {
  /// <summary>
  ///   Creates a <see cref="SocketsHttpHandler" /> with the given TLS settings.
  /// </summary>
  public static SocketsHttpHandler Create(NifiTlsOptions? tls_options = null) {
    SocketsHttpHandler handler = new() {
      // Re-resolve DNS periodically for long-lived clients
      PooledConnectionLifetime = TimeSpan.FromMinutes(5)
    };
    if (tls_options != null) {
      ConfigureTls(handler.SslOptions, tls_options);
    }

    return handler;
  }

  /// <summary>
  ///   Applies the TLS settings to existing SSL options, e.g. those of a
  ///   handler you build yourself.
  /// </summary>
  public static void ConfigureTls(SslClientAuthenticationOptions ssl_options,
                                  NifiTlsOptions tls_options) {
    ArgumentNullException.ThrowIfNull(ssl_options);
    ArgumentNullException.ThrowIfNull(tls_options);

    X509Certificate2? client_certificate = tls_options.client_certificate;
    if (client_certificate != null) {
      if (!client_certificate.HasPrivateKey) {
        throw new ArgumentException(
          $"The client certificate '{client_certificate.Subject}' has no " +
          "private key, so it cannot be used for mutual TLS.",
          nameof(tls_options)
        );
      }

      // A certificate context always presents this certificate (with its
      // chain) when NiFi asks for one, regardless of the CA names NiFi lists.
      ssl_options.ClientCertificateContext = SslStreamCertificateContext.Create(
        client_certificate,
        tls_options.client_certificate_chain,
        true
      );
    }

    if (tls_options.server_certificate_validation != null) {
      ssl_options.RemoteCertificateValidationCallback =
        tls_options.server_certificate_validation;
    } else if (tls_options.trusted_root_certificates is { Count: > 0 }) {
      X509ChainPolicy policy = new() {
        TrustMode = X509ChainTrustMode.CustomRootTrust,
        RevocationMode = tls_options.revocation_mode
      };
      policy.CustomTrustStore.AddRange(tls_options.trusted_root_certificates);
      ssl_options.CertificateChainPolicy = policy;
    }
  }
}
