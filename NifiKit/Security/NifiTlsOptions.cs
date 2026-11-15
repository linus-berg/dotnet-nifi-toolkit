using System.Net.Security;
using System.Security.Cryptography.X509Certificates;

namespace NifiKit.Security;

/// <summary>
///   TLS settings for talking to a secured NiFi, including the client
///   certificate used for mutual TLS (two-way SSL) authentication.
/// </summary>
/// <remarks>
///   With mutual TLS, NiFi identifies the client by the certificate's subject
///   DN (e.g. <c>CN=my-client</c>), after any <c>nifi.security.identity.mapping</c>
///   rules. For Site-to-Site that identity needs the policies "retrieve
///   site-to-site details" and "receive data via site-to-site" on the input port.
/// </remarks>
public sealed class NifiTlsOptions {
  /// <summary>
  ///   Client certificate, including its private key, presented to NiFi.
  ///   Load one with <see cref="NifiCertificateLoader" />.
  /// </summary>
  public X509Certificate2? client_certificate { get; set; }

  /// <summary>
  ///   Intermediate CA certificates sent along with the client certificate
  ///   when NiFi's truststore only contains the root CA.
  /// </summary>
  public X509Certificate2Collection? client_certificate_chain { get; set; }

  /// <summary>
  ///   CA certificates (or self-signed NiFi certificates) trusted for NiFi's
  ///   server certificate instead of the operating system trust store. Set this
  ///   when NiFi uses a private CA. The host name is still validated.
  /// </summary>
  public X509Certificate2Collection? trusted_root_certificates { get; set; }

  /// <summary>
  ///   Revocation checking for NiFi's certificate chain when
  ///   <see cref="trusted_root_certificates" /> is used. Defaults to
  ///   <see cref="X509RevocationMode.NoCheck" />, matching HttpClient's
  ///   default, because private CAs rarely publish revocation lists.
  /// </summary>
  public X509RevocationMode revocation_mode { get; set; } =
    X509RevocationMode.NoCheck;

  /// <summary>
  ///   Optional custom server certificate validation. When set it replaces the
  ///   built-in validation entirely (including <see cref="trusted_root_certificates" />).
  /// </summary>
  public RemoteCertificateValidationCallback? server_certificate_validation {
    get;
    set;
  }
}
