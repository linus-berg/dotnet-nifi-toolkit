using System.Security.Cryptography.X509Certificates;

namespace NifiKit.Security;

/// <summary>
///   Loads certificates for mutual TLS with NiFi from the formats NiFi
///   deployments commonly use (PKCS#12 keystores, PEM files).
/// </summary>
public static class NifiCertificateLoader {
  /// <summary>
  ///   Loads a client certificate and private key from a PKCS#12 (.p12/.pfx)
  ///   file, such as one produced by the NiFi Toolkit or openssl.
  /// </summary>
  public static X509Certificate2 LoadPkcs12(string path, string? password) {
    ArgumentException.ThrowIfNullOrEmpty(path);
#if NET9_0_OR_GREATER
    return X509CertificateLoader.LoadPkcs12FromFile(path, password);
#else
    return new X509Certificate2(path, password);
#endif
  }

  /// <summary>
  ///   Loads a client certificate from a PEM certificate file and a PEM
  ///   private key file (PKCS#1, PKCS#8 or encrypted PKCS#8).
  /// </summary>
  public static X509Certificate2 LoadPem(string certificate_path,
                                         string private_key_path,
                                         string? key_password = null) {
    ArgumentException.ThrowIfNullOrEmpty(certificate_path);
    ArgumentException.ThrowIfNullOrEmpty(private_key_path);

    X509Certificate2 certificate = string.IsNullOrEmpty(key_password)
                                     ? X509Certificate2.CreateFromPemFile(
                                       certificate_path,
                                       private_key_path
                                     )
                                     : X509Certificate2.CreateFromEncryptedPemFile(
                                       certificate_path,
                                       key_password,
                                       private_key_path
                                     );
    if (!OperatingSystem.IsWindows()) {
      return certificate;
    }

    // Windows' TLS stack (SChannel) cannot use the ephemeral key of a
    // PEM-loaded certificate; round-trip through PKCS#12 to persist it.
    using (certificate) {
      byte[] pkcs12 = certificate.Export(X509ContentType.Pkcs12);
#if NET9_0_OR_GREATER
      return X509CertificateLoader.LoadPkcs12(pkcs12, null);
#else
      return new X509Certificate2(pkcs12);
#endif
    }
  }

  /// <summary>
  ///   Loads CA certificates, e.g. for <see cref="NifiTlsOptions.trusted_root_certificates" />.
  ///   Accepts a PEM bundle, a single DER certificate, or a PKCS#12
  ///   truststore (such as NiFi's truststore.p12).
  /// </summary>
  public static X509Certificate2Collection LoadCertificates(string path,
    string? password = null) {
    ArgumentException.ThrowIfNullOrEmpty(path);

    byte[] data = File.ReadAllBytes(path);
    X509Certificate2Collection collection = new();
    if (IsPem(data)) {
      collection.ImportFromPemFile(path);
    } else if (IsPkcs12(data)) {
#if NET9_0_OR_GREATER
      collection.AddRange(
        X509CertificateLoader.LoadPkcs12Collection(data, password)
      );
#else
      collection.Import(data, password, X509KeyStorageFlags.DefaultKeySet);
#endif
    } else {
#if NET9_0_OR_GREATER
      collection.Add(X509CertificateLoader.LoadCertificate(data));
#else
      collection.Add(new X509Certificate2(data));
#endif
    }

    if (collection.Count == 0) {
      throw new InvalidDataException($"No certificates found in '{path}'.");
    }

    return collection;
  }

  private static bool IsPem(byte[] data) {
    ReadOnlySpan<byte> marker = "-----BEGIN"u8;
    return data.AsSpan().IndexOf(marker) >= 0;
  }

  private static bool IsPkcs12(byte[] data) {
    return X509Certificate2.GetCertContentType(data) == X509ContentType.Pkcs12;
  }
}
