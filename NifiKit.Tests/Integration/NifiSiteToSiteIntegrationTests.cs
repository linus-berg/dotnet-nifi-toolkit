using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Microsoft.Extensions.DependencyInjection;
using NifiKit.Models;
using NifiKit.Security;
using NifiKit.Services;
using NifiKit.SiteToSite;

namespace NifiKit.Tests.Integration;

/// <summary>
///   Runs only when NIFIKIT_IT_URL is set, e.g. against docker-compose.yml:
///   <code>
///   ./scripts/generate-test-certs.sh &amp;&amp; docker compose up -d
///   NIFIKIT_IT_URL=https://localhost:8443 dotnet test
///   </code>
/// </summary>
public sealed class NifiIntegrationFactAttribute : FactAttribute {
  public NifiIntegrationFactAttribute() {
    if (string.IsNullOrEmpty(NifiIntegrationSettings.url)) {
      Skip = "Set NIFIKIT_IT_URL to run against a live, mutual-TLS NiFi.";
    }
  }
}

internal static class NifiIntegrationSettings {
  public static string? url => Environment.GetEnvironmentVariable("NIFIKIT_IT_URL");

  // Defaults match scripts/generate-test-certs.sh
  public static string client_certificate =>
    Environment.GetEnvironmentVariable("NIFIKIT_IT_CLIENT_CERT") ??
    Path.Combine(FindCertsDirectory(), "client.p12");

  public static string client_certificate_password =>
    Environment.GetEnvironmentVariable("NIFIKIT_IT_CLIENT_CERT_PASSWORD") ??
    "nifikit-test-password";

  public static string ca_certificate =>
    Environment.GetEnvironmentVariable("NIFIKIT_IT_CA_CERT") ??
    Path.Combine(FindCertsDirectory(), "ca.crt");

  private static string FindCertsDirectory() {
    DirectoryInfo? directory = new(AppContext.BaseDirectory);
    while (directory != null &&
           !File.Exists(Path.Combine(directory.FullName, "certs", "client.p12"))) {
      directory = directory.Parent;
    }

    return directory == null
             ? "certs"
             : Path.Combine(directory.FullName, "certs");
  }
}

public sealed class NifiSiteToSiteIntegrationTests {
  [NifiIntegrationFact]
  public async Task Sends_FlowFiles_To_A_Live_NiFi_Over_Mutual_Tls() {
    string base_url = NifiIntegrationSettings.url!;
    using X509Certificate2 client_certificate =
      NifiCertificateLoader.LoadPkcs12(
        NifiIntegrationSettings.client_certificate,
        NifiIntegrationSettings.client_certificate_password
      );
    NifiTlsOptions tls = new() {
      client_certificate = client_certificate,
      trusted_root_certificates =
        NifiCertificateLoader.LoadCertificates(
          NifiIntegrationSettings.ca_certificate
        )
    };

    // Admin REST client authenticated with the same client certificate
    NifiAdminClient admin = new(
      new HttpClient(NifiHttpHandlerFactory.Create(tls)),
      NifiTransportService.ResolveApiBaseUrl(base_url)
    );
    string identity = await admin.GetIdentityAsync();
    Assert.Equal(client_certificate.SubjectName.Name, identity);

    string root = await admin.GetRootGroupIdAsync();
    string user = await admin.GetUserIdAsync(identity);
    await admin.GrantAsync("read", $"/process-groups/{root}", user);
    await admin.GrantAsync("write", $"/process-groups/{root}", user);
    await admin.GrantAsync("read", $"/data/process-groups/{root}", user);
    await admin.GrantAsync("write", $"/data/process-groups/{root}", user);
    await admin.GrantAsync("read", "/site-to-site", user);

    string port_name = $"nifikit-it-{Guid.NewGuid():N}";
    string? port_id = null;
    string? funnel_id = null;
    string? connection_id = null;
    try {
      port_id = await admin.CreateInputPortAsync(root, port_name);
      funnel_id = await admin.CreateFunnelAsync(root);
      connection_id =
        await admin.CreateConnectionAsync(root, port_id, funnel_id);
      await admin.GrantAsync("write", $"/data-transfer/input-ports/{port_id}",
                             user);
      await admin.SetPortStateAsync(port_id, "RUNNING");

      // The transport as an application would register it
      ServiceCollection services = new();
      services.AddLogging();
      services.AddNifiKit(options => {
        options.tls.client_certificate = client_certificate;
        options.tls.trusted_root_certificates = tls.trusted_root_certificates;
      });
      await using ServiceProvider provider = services.BuildServiceProvider();
      INifiTransportService transport =
        provider.GetRequiredService<INifiTransportService>();

      byte[] large = RandomNumberGenerator.GetBytes(1024 * 1024);
      NifiPackage streamed = new NifiPackage()
                             .AddAttribute("filename", "large.bin")
                             .AddAttribute("sha256",
                                           Convert.ToHexString(SHA256.HashData(large)))
                             .SetContent(new NonSeekableStream(large));

      SiteToSiteTransferResult result = await transport.SendViaS2SAsync(
                                          base_url,
                                          port_name,
                                          new[] {
                                            TestData.PackageA(),
                                            TestData.PackageB(),
                                            streamed
                                          }
                                        );

      Assert.Equal(3, result.flow_files_sent);
      Assert.False(result.destination_full);

      List<QueuedFlowFile> queued =
        await admin.GetQueuedFlowFilesAsync(connection_id);
      Assert.Equal(3, queued.Count);
      QueuedFlowFile a = queued.Single(f => f.attributes["filename"] == "a.txt");
      Assert.Equal("åäö ✓ 😀", a.attributes["unicode"]);
      Assert.Equal("", a.attributes["empty"]);
      Assert.Equal(70_000, a.attributes["big"].Length);
      Assert.Equal(TestData.Utf8("hello world"), a.content);
      QueuedFlowFile b = queued.Single(f => f.attributes["filename"] == "b.bin");
      Assert.Equal("application/octet-stream", b.attributes["mime.type"]);
      Assert.Equal(TestData.ContentB(), b.content);
      QueuedFlowFile c = queued.Single(f => f.attributes["filename"] == "large.bin");
      Assert.Equal(large, c.content);

      // A port name NiFi does not know is a permanent failure
      NifiSiteToSiteException missing =
        await Assert.ThrowsAsync<NifiSiteToSiteException>(
          () => transport.SendViaS2SAsync(
            base_url,
            port_name + "-missing",
            new[] { TestData.PackageA() }
          )
        );
      Assert.False(missing.is_transient);
      Assert.Equal(SiteToSiteResponseCode.UNKNOWN_PORT, missing.response_code);
    } finally {
      await admin.DeleteFlowAsync(port_id, connection_id, funnel_id);
    }
  }

  [NifiIntegrationFact]
  public async Task NiFi_Rejects_A_Client_Without_A_Certificate() {
    NifiTlsOptions tls = new() {
      trusted_root_certificates =
        NifiCertificateLoader.LoadCertificates(
          NifiIntegrationSettings.ca_certificate
        )
    };
    NifiTransportService transport = new(
      new HttpClient(NifiHttpHandlerFactory.Create(tls)),
      new FlowFileService()
    );

    // NiFi only lets anonymous users in over plain HTTP, so this is either a
    // failed handshake or a 401/403 from NiFi, never a successful transfer.
    Exception ex = await Record.ExceptionAsync(
                     () => transport.SendViaS2SAsync(
                       NifiIntegrationSettings.url!,
                       "any-port",
                       new[] { TestData.PackageA() }
                     )
                   );

    Assert.True(ex is HttpRequestException ||
                ex is NifiSiteToSiteException { is_transient: false },
                $"Unexpected: {ex}");
  }
}
