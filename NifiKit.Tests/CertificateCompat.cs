using System.Security.Cryptography.X509Certificates;

namespace NifiKit.Tests;

/// <summary>
///   X509CertificateLoader only exists from .NET 9; the tests also run on .NET 8.
/// </summary>
internal static class CertificateCompat {
  public static X509Certificate2 LoadCertificate(byte[] data) {
#if NET9_0_OR_GREATER
    return X509CertificateLoader.LoadCertificate(data);
#else
    return new X509Certificate2(data);
#endif
  }

  public static X509Certificate2 LoadPkcs12(byte[] data, string? password) {
#if NET9_0_OR_GREATER
    return X509CertificateLoader.LoadPkcs12(data, password);
#else
    return new X509Certificate2(data, password);
#endif
  }
}
