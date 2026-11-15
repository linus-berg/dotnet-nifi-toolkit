using System.Net;
using Microsoft.Extensions.DependencyInjection;
using NifiKit.Models;
using NifiKit.Services;
using NifiKit.SiteToSite;
using Polly;
using Polly.Registry;
using Polly.Retry;

namespace NifiKit.Tests;

public class SiteToSiteTransportTests {
  private readonly FakeNifi nifi_ = new();

  private NifiTransportService CreateService(
    ResiliencePipelineProvider<string>? pipelines = null) {
    return new NifiTransportService(
      new HttpClient(nifi_),
      new FlowFileService(),
      null,
      pipelines
    );
  }

  [Fact]
  public async Task Sends_Transaction_Exactly_Like_NiFis_Http_Protocol() {
    NifiTransportService service = CreateService();

    SiteToSiteTransferResult result = await service.SendViaS2SAsync(
                                        "https://nifi.test:8443/nifi",
                                        FakeNifi.PORT_NAME,
                                        new[] {
                                          TestData.PackageA(),
                                          TestData.PackageB()
                                        }
                                      );

    string tx = $"/nifi-api/data-transfer/input-ports/{FakeNifi.PORT_ID}" +
                "/transactions/tx-1";
    Assert.Collection(
      nifi_.requests,
      r => Assert.Equal((HttpMethod.Get, "/nifi-api/site-to-site"),
                        (r.method, r.path)),
      r => {
        Assert.Equal(
          (HttpMethod.Post,
           $"/nifi-api/data-transfer/input-ports/{FakeNifi.PORT_ID}/transactions"),
          (r.method, r.path)
        );
        Assert.Null(r.body);
      },
      r => {
        Assert.Equal((HttpMethod.Post, tx + "/flow-files"), (r.method, r.path));
        Assert.Equal("application/octet-stream", r.content_type);
        Assert.Equal(TestData.Fixture("nifi_s2s_two_packets.bin"), r.body);
      },
      r => Assert.Equal((HttpMethod.Delete, tx + "?responseCode=12"),
                        (r.method, r.path))
    );
    Assert.All(nifi_.requests, r => Assert.True(r.has_protocol_version));

    Assert.Equal(2, result.flow_files_sent);
    Assert.Equal(TestData.S2S_FIXTURE_CRC32, result.checksum);
    Assert.Equal(new Uri("https://nifi.test:8443" + tx), result.transaction_url);
    Assert.False(result.destination_full);
  }

  [Theory]
  [InlineData("https://nifi.test:8443", "https://nifi.test:8443/nifi-api/")]
  [InlineData("https://nifi.test:8443/", "https://nifi.test:8443/nifi-api/")]
  [InlineData("https://nifi.test:8443/nifi", "https://nifi.test:8443/nifi-api/")]
  [InlineData("https://nifi.test:8443/nifi/", "https://nifi.test:8443/nifi-api/")]
  [InlineData("https://nifi.test:8443/nifi-api", "https://nifi.test:8443/nifi-api/")]
  [InlineData("https://proxy.test/team-a", "https://proxy.test/team-a/nifi-api/")]
  [InlineData("http://nifi.test:8080/nifi", "http://nifi.test:8080/nifi-api/")]
  public void Resolves_Api_Base_Url_Like_NiFis_Client(string input,
                                                      string expected) {
    Assert.Equal(new Uri(expected), NifiTransportService.ResolveApiBaseUrl(input));
  }

  [Fact]
  public async Task Checksum_Mismatch_Rejects_The_Transaction() {
    nifi_.corrupt_checksum = true;

    NifiSiteToSiteException ex = await Assert.ThrowsAsync<NifiSiteToSiteException>(
                                   () => CreateService().SendViaS2SAsync(
                                     FakeNifi.API,
                                     FakeNifi.PORT_NAME,
                                     new[] { TestData.PackageA() }
                                   )
                                 );

    Assert.Equal(SiteToSiteResponseCode.BAD_CHECKSUM, ex.response_code);
    Assert.True(ex.is_transient);
    Assert.Equal(new[] { 19 }, nifi_.finish_codes);
  }

  [Fact]
  public async Task Failed_Transfer_Cancels_The_Transaction() {
    nifi_.fail_flow_files_times = 1;
    nifi_.fail_flow_files_status = HttpStatusCode.InternalServerError;

    NifiSiteToSiteException ex = await Assert.ThrowsAsync<NifiSiteToSiteException>(
                                   () => CreateService().SendViaS2SAsync(
                                     FakeNifi.API,
                                     FakeNifi.PORT_NAME,
                                     new[] { TestData.PackageA() }
                                   )
                                 );

    Assert.Equal(HttpStatusCode.InternalServerError, ex.status_code);
    Assert.Equal(SiteToSiteResponseCode.ABORT, ex.response_code);
    Assert.True(ex.is_transient);
    Assert.Equal(new[] { 15 }, nifi_.finish_codes);
  }

  [Fact]
  public async Task Reports_Destination_Full() {
    nifi_.destination_full = true;

    SiteToSiteTransferResult result = await CreateService().SendViaS2SAsync(
                                        FakeNifi.API,
                                        FakeNifi.PORT_NAME,
                                        new[] { TestData.PackageA() }
                                      );

    Assert.True(result.destination_full);
    Assert.Equal(1, result.flow_files_sent);
  }

  [Fact]
  public async Task Unknown_Port_Is_Not_Transient_And_Lists_Available_Ports() {
    NifiSiteToSiteException ex = await Assert.ThrowsAsync<NifiSiteToSiteException>(
                                   () => CreateService().SendViaS2SAsync(
                                     FakeNifi.API,
                                     "from .net", // case differs: no match
                                     new[] { TestData.PackageA() }
                                   )
                                 );

    Assert.False(ex.is_transient);
    Assert.Equal(SiteToSiteResponseCode.UNKNOWN_PORT, ex.response_code);
    Assert.Contains($"'{FakeNifi.PORT_NAME}'", ex.Message);
  }

  [Fact]
  public async Task Stopped_Port_Is_Transient() {
    nifi_.port_state = "STOPPED";

    NifiSiteToSiteException ex = await Assert.ThrowsAsync<NifiSiteToSiteException>(
                                   () => CreateService().SendViaS2SAsync(
                                     FakeNifi.API,
                                     FakeNifi.PORT_NAME,
                                     new[] { TestData.PackageA() }
                                   )
                                 );

    Assert.True(ex.is_transient);
    Assert.Equal(SiteToSiteResponseCode.PORT_NOT_IN_VALID_STATE,
                 ex.response_code);
  }

  [Fact]
  public async Task Forbidden_Explains_The_Missing_Policy() {
    nifi_.site_to_site_status = HttpStatusCode.Forbidden;

    NifiSiteToSiteException ex = await Assert.ThrowsAsync<NifiSiteToSiteException>(
                                   () => CreateService().SendViaS2SAsync(
                                     FakeNifi.API,
                                     FakeNifi.PORT_NAME,
                                     new[] { TestData.PackageA() }
                                   )
                                 );

    Assert.False(ex.is_transient);
    Assert.Equal(HttpStatusCode.Forbidden, ex.status_code);
    Assert.Contains("retrieve site-to-site details", ex.Message);
  }

  [Fact]
  public async Task Port_Unavailable_On_Create_Is_Transient() {
    nifi_.create_transaction_status = HttpStatusCode.ServiceUnavailable;
    nifi_.create_transaction_error_code = 201;

    NifiSiteToSiteException ex = await Assert.ThrowsAsync<NifiSiteToSiteException>(
                                   () => CreateService().SendViaS2SAsync(
                                     FakeNifi.API,
                                     FakeNifi.PORT_NAME,
                                     new[] { TestData.PackageA() }
                                   )
                                 );

    Assert.True(ex.is_transient);
    Assert.Equal(SiteToSiteResponseCode.PORT_NOT_IN_VALID_STATE,
                 ex.response_code);
    Assert.Empty(nifi_.finish_codes); // no transaction was created
  }

  [Fact]
  public async Task Retry_Pipeline_Resends_Identical_Bytes() {
    nifi_.fail_flow_files_times = 1;
    ResiliencePipelineRegistry<string> registry = new();
    registry.TryAddBuilder(
      NifiTransportService.S2S_PIPELINE_NAME,
      (builder, _) => builder.AddRetry(
        new RetryStrategyOptions {
          ShouldHandle = new PredicateBuilder()
                         .Handle<NifiSiteToSiteException>(e => e.is_transient)
                         .Handle<HttpRequestException>(),
          MaxRetryAttempts = 3,
          Delay = TimeSpan.Zero
        }
      )
    );
    NifiPackage non_seekable = new NifiPackage()
                               .AddAttribute("filename", "stream")
                               .SetContent(
                                 new NonSeekableStream(TestData.Utf8("read once"))
                               );

    SiteToSiteTransferResult result = await CreateService(registry)
                                        .SendViaS2SAsync(
                                          FakeNifi.API,
                                          FakeNifi.PORT_NAME,
                                          new[] {
                                            non_seekable,
                                            TestData.PackageB()
                                          }
                                        );

    RecordedRequest[] attempts = nifi_.requests
                                      .Where(r => r.path.EndsWith("flow-files"))
                                      .ToArray();
    Assert.Equal(2, attempts.Length);
    Assert.Equal(attempts[0].body, attempts[1].body);
    Assert.Equal(new[] { 15, 12 }, nifi_.finish_codes); // cancel, then commit
    Assert.Equal(2, result.flow_files_sent);
  }

  [Fact]
  public async Task Caller_Cancellation_Propagates_And_Cancels_The_Transaction() {
    nifi_.hang_on_flow_files = true;
    using CancellationTokenSource cts = new(TimeSpan.FromMilliseconds(300));

    await Assert.ThrowsAnyAsync<OperationCanceledException>(
      () => CreateService().SendViaS2SAsync(
        FakeNifi.API,
        FakeNifi.PORT_NAME,
        new[] { TestData.PackageA() },
        cts.Token
      )
    );

    Assert.Equal(new[] { 15 }, nifi_.finish_codes);
  }

  [Fact]
  public async Task HttpClient_Timeout_Becomes_A_Transient_Failure() {
    nifi_.hang_on_flow_files = true;
    NifiTransportService service = new(
      new HttpClient(nifi_) { Timeout = TimeSpan.FromMilliseconds(300) },
      new FlowFileService()
    );

    NifiSiteToSiteException ex = await Assert.ThrowsAsync<NifiSiteToSiteException>(
                                   () => service.SendViaS2SAsync(
                                     FakeNifi.API,
                                     FakeNifi.PORT_NAME,
                                     new[] { TestData.PackageA() }
                                   )
                                 );

    Assert.True(ex.is_transient);
  }

  [Fact]
  public async Task Empty_Batch_Sends_Nothing() {
    SiteToSiteTransferResult result = await CreateService().SendViaS2SAsync(
                                        FakeNifi.API,
                                        FakeNifi.PORT_NAME,
                                        Array.Empty<NifiPackage>()
                                      );

    Assert.Equal(0, result.flow_files_sent);
    Assert.Empty(nifi_.requests);
  }

  [Fact]
  public async Task Port_Id_Is_Cached_And_Refreshed_When_Stale() {
    NifiTransportService service = CreateService();
    await service.SendViaS2SAsync(FakeNifi.API, FakeNifi.PORT_NAME,
                                  new[] { TestData.PackageA() });
    await service.SendViaS2SAsync(FakeNifi.API, FakeNifi.PORT_NAME,
                                  new[] { TestData.PackageA() });
    Assert.Single(nifi_.requests, r => r.path == "/nifi-api/site-to-site");

    // The port was deleted: the cached id now yields 404 -> transient, and
    // the next attempt looks the port up again
    nifi_.create_transaction_status = HttpStatusCode.NotFound;
    nifi_.create_transaction_error_code = 200;
    NifiSiteToSiteException ex = await Assert.ThrowsAsync<NifiSiteToSiteException>(
                                   () => service.SendViaS2SAsync(
                                     FakeNifi.API,
                                     FakeNifi.PORT_NAME,
                                     new[] { TestData.PackageA() }
                                   )
                                 );
    Assert.True(ex.is_transient);

    nifi_.create_transaction_status = null;
    await service.SendViaS2SAsync(FakeNifi.API, FakeNifi.PORT_NAME,
                                  new[] { TestData.PackageA() });
    Assert.Equal(2, nifi_.requests.Count(r => r.path == "/nifi-api/site-to-site"));
  }

  [Fact]
  public async Task Port_Cache_Is_Shared_Between_Di_Resolved_Instances() {
    ServiceCollection services = new();
    services.AddNifiKit().ConfigurePrimaryHttpMessageHandler(() => nifi_);
    await using ServiceProvider provider = services.BuildServiceProvider();

    // Typed HttpClients are transient: two resolutions, two instances
    INifiTransportService first =
      provider.GetRequiredService<INifiTransportService>();
    INifiTransportService second =
      provider.GetRequiredService<INifiTransportService>();
    Assert.NotSame(first, second);

    await first.SendViaS2SAsync(FakeNifi.API, FakeNifi.PORT_NAME,
                                new[] { TestData.PackageA() });
    await second.SendViaS2SAsync(FakeNifi.API, FakeNifi.PORT_NAME,
                                 new[] { TestData.PackageA() });

    Assert.Single(nifi_.requests, r => r.path == "/nifi-api/site-to-site");
  }

  [Fact]
  public async Task Relative_Location_Header_Is_Resolved() {
    nifi_.relative_location = true;

    SiteToSiteTransferResult result = await CreateService().SendViaS2SAsync(
                                        FakeNifi.API,
                                        FakeNifi.PORT_NAME,
                                        new[] { TestData.PackageA() }
                                      );

    Assert.Equal("nifi.test", result.transaction_url!.Host);
    Assert.Equal(1, result.flow_files_sent);
  }

  [Fact]
  public async Task Posts_Packages_To_ListenHttp_As_FlowFile_V3() {
    ListenHttpHandler listen = new();
    NifiTransportService service = new(new HttpClient(listen),
                                       new FlowFileService());

    bool ok = await service.PostToNiFiAsync(
                "https://nifi.test:9999/contentListener",
                new[] { TestData.PackageA(), TestData.PackageB() }
              );

    Assert.True(ok);
    Assert.Equal("application/flowfile-v3", listen.content_type);
    Assert.Equal(TestData.Fixture("nifi_v3_two_flowfiles.bin"), listen.body);
  }

  private sealed class ListenHttpHandler : HttpMessageHandler {
    public string? content_type { get; private set; }
    public byte[]? body { get; private set; }

    protected override async Task<HttpResponseMessage> SendAsync(
      HttpRequestMessage request, CancellationToken ct) {
      content_type = request.Content!.Headers.ContentType!.ToString();
      body = await request.Content.ReadAsByteArrayAsync(ct);
      return new HttpResponseMessage(HttpStatusCode.OK);
    }
  }
}
