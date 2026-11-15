using System.Net.Http.Json;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Microsoft.Extensions.DependencyInjection;
using NifiKit.Security;
using NifiKit.Services;

namespace NifiKit.Tests;

public sealed class MutualTlsTests : IDisposable {
  private readonly TestPki pki_ = new();
  private readonly string temp_dir_ =
    Directory.CreateTempSubdirectory("nifikit-tls-").FullName;

  public void Dispose() {
    pki_.Dispose();
    Directory.Delete(temp_dir_, true);
  }

  [Fact]
  public async Task Presents_Client_Certificate_And_Trusts_Private_Ca() {
    await using TlsTestServer server = new(pki_.server, pki_.ca);
    using HttpClient http_client = new(NifiHttpHandlerFactory.Create(
                                         new NifiTlsOptions {
                                           client_certificate = pki_.client,
                                           trusted_root_certificates = new() {
                                             pki_.ca
                                           }
                                         }
                                       ));

    Identity? identity = await http_client.GetFromJsonAsync<Identity>(
                           $"https://localhost:{server.port}/nifi-api/flow/current-user"
                         );

    Assert.Equal("CN=nifikit-test-client", identity!.identity);
    Assert.Equal(new[] { "CN=nifikit-test-client" }, server.client_subjects);
  }

  [Fact]
  public async Task Server_Rejects_Missing_Client_Certificate() {
    await using TlsTestServer server = new(pki_.server, pki_.ca);
    using HttpClient http_client = new(NifiHttpHandlerFactory.Create(
                                         new NifiTlsOptions {
                                           trusted_root_certificates = new() {
                                             pki_.ca
                                           }
                                         }
                                       ));

    await Assert.ThrowsAsync<HttpRequestException>(
      () => http_client.GetAsync($"https://localhost:{server.port}/")
    );
    Assert.Empty(server.client_subjects);
  }

  [Fact]
  public async Task Server_Rejects_Client_Certificate_From_Another_Ca() {
    await using TlsTestServer server = new(pki_.server, pki_.ca);
    using HttpClient http_client = new(NifiHttpHandlerFactory.Create(
                                         new NifiTlsOptions {
                                           client_certificate =
                                             pki_.untrusted_client,
                                           trusted_root_certificates = new() {
                                             pki_.ca
                                           }
                                         }
                                       ));

    await Assert.ThrowsAsync<HttpRequestException>(
      () => http_client.GetAsync($"https://localhost:{server.port}/")
    );
    Assert.Empty(server.client_subjects);
  }

  [Fact]
  public async Task Untrusted_Server_Certificate_Is_Rejected() {
    await using TlsTestServer server = new(pki_.server, pki_.ca);
    using HttpClient http_client = new(NifiHttpHandlerFactory.Create(
                                         new NifiTlsOptions {
                                           client_certificate = pki_.client,
                                           trusted_root_certificates = new() {
                                             pki_.other_ca
                                           }
                                         }
                                       ));

    await Assert.ThrowsAsync<HttpRequestException>(
      () => http_client.GetAsync($"https://localhost:{server.port}/")
    );
  }

  [Fact]
  public async Task Host_Name_Is_Still_Validated_With_A_Private_Ca() {
    // Issued by the trusted CA, but for another host than localhost
    await using TlsTestServer server = new(pki_.wrong_name_server, pki_.ca);
    using HttpClient http_client = new(NifiHttpHandlerFactory.Create(
                                         new NifiTlsOptions {
                                           client_certificate = pki_.client,
                                           trusted_root_certificates = new() {
                                             pki_.ca
                                           }
                                         }
                                       ));

    await Assert.ThrowsAsync<HttpRequestException>(
      () => http_client.GetAsync($"https://localhost:{server.port}/")
    );
  }

  [Fact]
  public void Certificate_Without_Private_Key_Is_Rejected() {
    using X509Certificate2 public_only =
      CertificateCompat.LoadCertificate(pki_.client.RawData);

    Assert.Throws<ArgumentException>(
      () => NifiHttpHandlerFactory.Create(
        new NifiTlsOptions { client_certificate = public_only }
      )
    );
  }

  [Fact]
  public async Task Loads_Pkcs12_And_Pem_Client_Certificates() {
    string p12 = Path.Combine(temp_dir_, "client.p12");
    File.WriteAllBytes(p12, pki_.client.Export(X509ContentType.Pkcs12, "pw"));
    string crt = Path.Combine(temp_dir_, "client.crt");
    File.WriteAllText(crt, pki_.client.ExportCertificatePem());
    string key = Path.Combine(temp_dir_, "client.key");
    File.WriteAllText(key,
                      pki_.client.GetRSAPrivateKey()!.ExportPkcs8PrivateKeyPem());
    string encrypted_key = Path.Combine(temp_dir_, "client-encrypted.key");
    File.WriteAllText(
      encrypted_key,
      pki_.client.GetRSAPrivateKey()!.ExportEncryptedPkcs8PrivateKeyPem(
        "key-pw",
        new PbeParameters(PbeEncryptionAlgorithm.Aes256Cbc,
                          HashAlgorithmName.SHA256, 100_000)
      )
    );
    string ca_pem = Path.Combine(temp_dir_, "ca.crt");
    File.WriteAllText(ca_pem, pki_.ca.ExportCertificatePem());

    X509Certificate2[] loaded = {
      NifiCertificateLoader.LoadPkcs12(p12, "pw"),
      NifiCertificateLoader.LoadPem(crt, key),
      NifiCertificateLoader.LoadPem(crt, encrypted_key, "key-pw")
    };
    X509Certificate2Collection trusted =
      NifiCertificateLoader.LoadCertificates(ca_pem);

    await using TlsTestServer server = new(pki_.server, pki_.ca);
    foreach (X509Certificate2 certificate in loaded) {
      Assert.True(certificate.HasPrivateKey);
      using HttpClient http_client = new(NifiHttpHandlerFactory.Create(
                                           new NifiTlsOptions {
                                             client_certificate = certificate,
                                             trusted_root_certificates = trusted
                                           }
                                         ));
      HttpResponseMessage response =
        await http_client.GetAsync($"https://localhost:{server.port}/");
      Assert.True(response.IsSuccessStatusCode);
    }

    Assert.Equal(3, server.client_subjects.Count);
  }

  [Fact]
  public void Loads_Truststore_Formats() {
    string der = Path.Combine(temp_dir_, "ca.der");
    File.WriteAllBytes(der, pki_.ca.RawData);
    string bundle = Path.Combine(temp_dir_, "bundle.pem");
    File.WriteAllText(bundle,
                      pki_.ca.ExportCertificatePem() + "\n" +
                      pki_.other_ca.ExportCertificatePem());
    string p12 = Path.Combine(temp_dir_, "truststore.p12");
    X509Certificate2Collection store = new() {
      CertificateCompat.LoadCertificate(pki_.ca.RawData)
    };
    File.WriteAllBytes(p12, store.Export(X509ContentType.Pkcs12, "pw")!);

    Assert.Single(NifiCertificateLoader.LoadCertificates(der));
    Assert.Equal(2, NifiCertificateLoader.LoadCertificates(bundle).Count);
    Assert.Equal(pki_.ca.Thumbprint,
                 NifiCertificateLoader.LoadCertificates(p12, "pw")[0].Thumbprint);
  }

  [Fact]
  public async Task AddNifiKit_Configures_The_Typed_Client_For_Mutual_Tls() {
    await using TlsTestServer server = new(pki_.server, pki_.ca);
    ServiceCollection services = new();
    services.AddLogging();
    services.AddNifiKit(options => {
      options.tls.client_certificate = pki_.client;
      options.tls.trusted_root_certificates = new X509Certificate2Collection {
        pki_.ca
      };
      options.http_timeout = TimeSpan.FromSeconds(30);
    });
    await using ServiceProvider provider = services.BuildServiceProvider();

    INifiTransportService transport =
      provider.GetRequiredService<INifiTransportService>();
    Assert.NotNull(provider.GetRequiredService<IFlowFileService>());

    // The test server answers anything with 200, which is enough to prove
    // the transport's HttpClient completed a mutual TLS handshake
    bool ok = await transport.PostToNiFiAsync(
                $"https://localhost:{server.port}/contentListener",
                new MemoryStream(TestData.Fixture("nifi_v3_two_flowfiles.bin"))
              );
    Assert.True(ok);
    Assert.Equal(new[] { "CN=nifikit-test-client" }, server.client_subjects);
  }

  private sealed record Identity(string identity);
}
