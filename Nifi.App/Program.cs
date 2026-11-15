// Sends FlowFiles to a NiFi input port over Site-to-Site with mutual TLS.
//
// Usage: dotnet run --project Nifi.App [-- <nifi-url> <port-name> <count>]
// Defaults: https://localhost:8443/nifi, port "test", 5 FlowFiles, using the
// client certificate and CA from ./certs (scripts/generate-test-certs.sh).

using Bogus;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Nifi.App;
using NifiKit.Models;
using NifiKit.Security;
using NifiKit.Services;
using NifiKit.SiteToSite;

string nifi_url = args.Length > 0 ? args[0] : "https://localhost:8443/nifi";
string port_name = args.Length > 1 ? args[1] : "test";
int count = args.Length > 2 ? int.Parse(args[2]) : 5;

string certs_dir = Path.Join(SolutionHelper.GetSolutionDirectory(), "certs");
string client_p12 = Path.Join(certs_dir, "client.p12");
string ca_crt = Path.Join(certs_dir, "ca.crt");
const string CERT_PASSWORD = "nifikit-test-password";

ServiceCollection services = new();
services.AddLogging(
  builder => builder.AddSimpleConsole(o => o.SingleLine = true)
                    .SetMinimumLevel(LogLevel.Information)
                    // Per-request HttpClient logs; set to Information to see them
                    .AddFilter("System.Net.Http.HttpClient", LogLevel.Warning)
);
services.AddNifiKit(options => {
  // Mutual TLS: NiFi authenticates us by this certificate (CN=nifikit-client)
  options.tls.client_certificate =
    NifiCertificateLoader.LoadPkcs12(client_p12, CERT_PASSWORD);
  // NiFi's certificate is issued by the test CA, not a public one
  options.tls.trusted_root_certificates =
    NifiCertificateLoader.LoadCertificates(ca_crt);
});

await using ServiceProvider provider = services.BuildServiceProvider();
INifiTransportService transport =
  provider.GetRequiredService<INifiTransportService>();

Faker faker = new();
List<NifiPackage> packages = new();
for (int i = 0; i < count; i++) {
  packages.Add(
    new NifiPackage()
      .AddAttribute("filename", faker.System.FileName("txt"))
      .AddAttribute("mime.type", "text/plain")
      .AddAttribute("author", faker.Name.FullName())
      .AddAttribute("a-weird-value", "åäö")
      .AddAttribute("nifikit.sequence", i.ToString())
      .SetContent(faker.Lorem.Paragraphs(3))
  );
}

Console.WriteLine(
  $"Sending {count} FlowFiles to input port '{port_name}' at {nifi_url} ..."
);
try {
  SiteToSiteTransferResult result =
    await transport.SendViaS2SAsync(nifi_url, port_name, packages);

  Console.WriteLine();
  Console.WriteLine($"Committed:        {result.flow_files_sent} FlowFiles, " +
                    $"{result.bytes_sent} bytes");
  Console.WriteLine($"Transaction:      {result.transaction_url}");
  Console.WriteLine($"CRC32 (verified): {result.checksum}");
  Console.WriteLine($"Destination full: {result.destination_full}");
  return 0;
} catch (NifiSiteToSiteException ex) {
  Console.Error.WriteLine();
  Console.Error.WriteLine($"Site-to-Site failed (transient: {ex.is_transient}): " +
                          ex.Message);
  return 1;
} catch (HttpRequestException ex) {
  Console.Error.WriteLine();
  Console.Error.WriteLine($"Could not reach NiFi at {nifi_url}: {ex.Message}");
  return 1;
} finally {
  foreach (NifiPackage package in packages) {
    package.Dispose();
  }
}
