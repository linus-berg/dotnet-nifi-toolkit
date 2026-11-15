using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;

namespace NifiKit.Tests;

/// <summary>
///   Throw-away PKI: a CA, a server certificate for localhost and a client
///   certificate, like a secured NiFi deployment.
/// </summary>
internal sealed class TestPki : IDisposable {
  public TestPki() {
    ca = CreateCa("CN=NifiKit Test CA");
    server = Issue(ca, "CN=localhost", true, "localhost");
    wrong_name_server = Issue(ca, "CN=elsewhere", true, "elsewhere.example");
    client = Issue(ca, "CN=nifikit-test-client", false, null);
    other_ca = CreateCa("CN=Untrusted CA");
    untrusted_client = Issue(other_ca, "CN=intruder", false, null);
  }

  public X509Certificate2 ca { get; }
  public X509Certificate2 server { get; }
  public X509Certificate2 wrong_name_server { get; }
  public X509Certificate2 client { get; }
  public X509Certificate2 other_ca { get; }
  public X509Certificate2 untrusted_client { get; }

  public void Dispose() {
    ca.Dispose();
    server.Dispose();
    wrong_name_server.Dispose();
    client.Dispose();
    other_ca.Dispose();
    untrusted_client.Dispose();
  }

  private static X509Certificate2 CreateCa(string subject) {
    using RSA key = RSA.Create(2048);
    CertificateRequest request = new(subject, key, HashAlgorithmName.SHA256,
                                     RSASignaturePadding.Pkcs1);
    request.CertificateExtensions.Add(
      new X509BasicConstraintsExtension(true, false, 0, true)
    );
    request.CertificateExtensions.Add(
      new X509KeyUsageExtension(
        X509KeyUsageFlags.KeyCertSign | X509KeyUsageFlags.CrlSign,
        true
      )
    );
    request.CertificateExtensions.Add(
      new X509SubjectKeyIdentifierExtension(request.PublicKey, false)
    );
    using X509Certificate2 ca = request.CreateSelfSigned(
      DateTimeOffset.UtcNow.AddDays(-1),
      DateTimeOffset.UtcNow.AddDays(30)
    );
    return Reload(ca);
  }

  private static X509Certificate2 Issue(X509Certificate2 issuer,
                                        string subject, bool server_cert,
                                        string? dns_name) {
    using RSA key = RSA.Create(2048);
    CertificateRequest request = new(subject, key, HashAlgorithmName.SHA256,
                                     RSASignaturePadding.Pkcs1);
    request.CertificateExtensions.Add(
      new X509BasicConstraintsExtension(false, false, 0, false)
    );
    request.CertificateExtensions.Add(
      new X509EnhancedKeyUsageExtension(
        new OidCollection {
          server_cert
            ? new Oid("1.3.6.1.5.5.7.3.1") // serverAuth
            : new Oid("1.3.6.1.5.5.7.3.2") // clientAuth
        },
        false
      )
    );
    if (dns_name != null) {
      SubjectAlternativeNameBuilder san = new();
      san.AddDnsName(dns_name);
      if (dns_name == "localhost") {
        san.AddIpAddress(IPAddress.Loopback);
      }

      request.CertificateExtensions.Add(san.Build());
    }

    byte[] serial = RandomNumberGenerator.GetBytes(16);
    using X509Certificate2 certificate = request.Create(
      issuer,
      DateTimeOffset.UtcNow.AddDays(-1),
      DateTimeOffset.UtcNow.AddDays(29),
      serial
    );
    using X509Certificate2 with_key = certificate.CopyWithPrivateKey(key);
    return Reload(with_key);
  }

  // Round-trip through PKCS#12 so the private key is usable by every TLS stack
  private static X509Certificate2 Reload(X509Certificate2 certificate) {
    return CertificateCompat.LoadPkcs12(
      certificate.Export(X509ContentType.Pkcs12),
      null
    );
  }
}

/// <summary>
///   Minimal HTTPS server that requires a client certificate (like NiFi with
///   two-way TLS) and answers every request with a small JSON body.
/// </summary>
internal sealed class TlsTestServer : IAsyncDisposable {
  private readonly X509Certificate2 trusted_client_ca_;
  private readonly TcpListener listener_;
  private readonly CancellationTokenSource stop_ = new();
  private readonly Task accept_loop_;

  public TlsTestServer(X509Certificate2 server_certificate,
                       X509Certificate2 trusted_client_ca) {
    trusted_client_ca_ = trusted_client_ca;
    listener_ = new TcpListener(IPAddress.Loopback, 0);
    listener_.Start();
    port = ((IPEndPoint)listener_.LocalEndpoint).Port;
    accept_loop_ = AcceptLoopAsync(server_certificate);
  }

  public int port { get; }

  /// <summary>
  ///   Subjects of the client certificates presented in successful handshakes.
  /// </summary>
  public List<string> client_subjects { get; } = new();

  public async ValueTask DisposeAsync() {
    stop_.Cancel();
    listener_.Stop();
    try {
      await accept_loop_;
    } catch (Exception) {
      // Listener stopped
    }

    stop_.Dispose();
  }

  private async Task AcceptLoopAsync(X509Certificate2 server_certificate) {
    while (!stop_.IsCancellationRequested) {
      TcpClient tcp = await listener_.AcceptTcpClientAsync(stop_.Token);
      _ = HandleAsync(tcp, server_certificate);
    }
  }

  private async Task HandleAsync(TcpClient tcp,
                                 X509Certificate2 server_certificate) {
    using (tcp) {
      await using SslStream ssl = new(tcp.GetStream());
      X509Certificate2? presented = null;
      try {
        await ssl.AuthenticateAsServerAsync(
          new SslServerAuthenticationOptions {
            ServerCertificate = server_certificate,
            ClientCertificateRequired = true,
            RemoteCertificateValidationCallback = (_, certificate, _, _) => {
              if (certificate == null) {
                return false;
              }

              presented = new X509Certificate2(certificate);
              return IsIssuedByTrustedCa(presented);
            }
          },
          stop_.Token
        );
      } catch (Exception) {
        return; // handshake rejected
      }

      lock (client_subjects) {
        client_subjects.Add(presented!.Subject);
      }

      // Read the complete request (head and body) before answering. Closing
      // while the client is still uploading would break its pipe.
      if (!await ReadRequestAsync(ssl)) {
        return;
      }

      string body = "{\"identity\":\"" + presented!.Subject + "\"}";
      string response = "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n" +
                        $"Content-Length: {Encoding.UTF8.GetByteCount(body)}\r\n" +
                        "Connection: close\r\n\r\n" + body;
      await ssl.WriteAsync(Encoding.UTF8.GetBytes(response), stop_.Token);
      await ssl.FlushAsync(stop_.Token);
    }
  }

  /// <summary>
  ///   Consumes one HTTP/1.1 request; the body is read according to
  ///   Content-Length or chunked transfer encoding and discarded.
  /// </summary>
  private async Task<bool> ReadRequestAsync(Stream stream) {
    byte[] buffer = new byte[64 * 1024];
    MemoryStream received = new();
    int head_end;
    while ((head_end = received.GetBuffer()
                               .AsSpan(0, (int)received.Length)
                               .IndexOf("\r\n\r\n"u8)) < 0) {
      int read = await stream.ReadAsync(buffer, stop_.Token);
      if (read == 0) {
        return false;
      }

      received.Write(buffer, 0, read);
    }

    string head = Encoding.ASCII.GetString(received.GetBuffer(), 0, head_end);
    string[] header_lines = head.Split("\r\n");
    bool chunked = header_lines.Any(
      l => l.StartsWith("Transfer-Encoding:", StringComparison.OrdinalIgnoreCase) &&
           l.Contains("chunked", StringComparison.OrdinalIgnoreCase)
    );
    string? content_length = header_lines
                             .Where(l => l.StartsWith("Content-Length:",
                                           StringComparison.OrdinalIgnoreCase))
                             .Select(l => l.Substring("Content-Length:".Length)
                                           .Trim())
                             .FirstOrDefault();

    // Bytes after the head that already arrived with it
    byte[] tail = received.GetBuffer()
                          .AsSpan(head_end + 4,
                                  (int)received.Length - head_end - 4)
                          .ToArray();
    if (chunked) {
      // Read until the terminating zero-length chunk
      List<byte> body = new(tail);
      while (!EndsWith(body, "0\r\n\r\n"u8)) {
        int read = await stream.ReadAsync(buffer, stop_.Token);
        if (read == 0) {
          return false;
        }

        body.AddRange(buffer.AsSpan(0, read).ToArray());
      }
    } else if (content_length != null) {
      long remaining = long.Parse(content_length) - tail.Length;
      while (remaining > 0) {
        int read = await stream.ReadAsync(
                     buffer.AsMemory(0, (int)Math.Min(buffer.Length, remaining)),
                     stop_.Token
                   );
        if (read == 0) {
          return false;
        }

        remaining -= read;
      }
    }

    return true;
  }

  private static bool EndsWith(List<byte> data, ReadOnlySpan<byte> suffix) {
    if (data.Count < suffix.Length) {
      return false;
    }

    for (int i = 0; i < suffix.Length; i++) {
      if (data[data.Count - suffix.Length + i] != suffix[i]) {
        return false;
      }
    }

    return true;
  }

  private bool IsIssuedByTrustedCa(X509Certificate2 certificate) {
    using X509Chain chain = new();
    chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
    chain.ChainPolicy.CustomTrustStore.Add(trusted_client_ca_);
    chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
    return chain.Build(certificate);
  }
}
