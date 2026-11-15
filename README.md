# NifiKit

A lightweight .NET library for producing, receiving, and transporting Apache NiFi FlowFiles. It supports FlowFile V3 (binary stream), FlowFile V1 (tar archive), posting to `ListenHTTP`, and sending to NiFi input ports over HTTP(S) Site-to-Site (S2S), including mutual TLS (client certificate) authentication.

## Features

- **FlowFile V3**: Pack and unpack binary FlowFiles, byte-for-byte compatible with NiFi's `FlowFilePackagerV3`/`FlowFileUnpackagerV3`.
- **System.IO.Pipelines**: Low-allocation V3 packing and unpacking on `PipeReader`/`PipeWriter` as well as `Stream`.
- **FlowFile V1**: Pack the legacy tar format (attributes as a Java XML properties document, as NiFi expects). NiFi 2.x deprecates V1; prefer V3.
- **Transport**:
  - **ListenHTTP**: Post FlowFiles to NiFi's `ListenHTTP` processor.
  - **Site-to-Site (S2S)**: Send FlowFiles to NiFi input ports using NiFi's HTTP Site-to-Site protocol, with CRC32 verification and two-phase commit.
- **Mutual TLS**: Authenticate to secured NiFi instances with a client certificate (PKCS#12 or PEM) and trust private CAs.
- **Resilience**: Typed exceptions with an `is_transient` flag, plus an optional Polly pipeline that retries whole S2S transactions safely.
- **Structured Logging**: Integrated with `Microsoft.Extensions.Logging`.
- **Multi-targeting**: .NET 8.0, 9.0 and 10.0.

## Architecture

- **`IFlowFileService` / `FlowFileService`**: Serialization and deserialization of FlowFile V1 and V3 (`Stream` and `System.IO.Pipelines`).
- **`INifiTransportService` / `NifiTransportService`**: Network transport (ListenHTTP and Site-to-Site) on top of an `HttpClient`.
- **`NifiKit.Security`**: `NifiTlsOptions`, `NifiHttpHandlerFactory` and `NifiCertificateLoader` for TLS and mutual TLS.

## Project Structure

- `NifiKit`: The core library.
  - `Models/`: The `NifiPackage` data model.
  - `Services/`: FlowFile and transport services.
  - `SiteToSite/`: Site-to-Site protocol types (results, exceptions, response codes).
  - `Security/`: TLS / client certificate support.
- `NifiKit.Tests`: Unit tests (checked against fixtures produced by NiFi's own Java code) and opt-in integration tests against a live NiFi.
- `Nifi.App`: A console application (placeholder for usage and testing).

## Getting Started

### Core Model

`NifiPackage` represents a single FlowFile:

```csharp
using NifiKit.Models;

var package = new NifiPackage()
    .AddAttribute("filename", "test.txt")
    .AddAttribute("mime.type", "text/plain")
    .SetContent("Hello NiFi");
```

Content semantics: a **seekable** content stream is always treated as the whole stream (from position 0), so a package can be serialized several times, for example when a transaction is retried. A **non-seekable** stream is read once and buffered where the format needs its length up front.

### Registering the services (dependency injection)

```csharp
using Microsoft.Extensions.DependencyInjection;
using NifiKit.Security;
using NifiKit.Services;

services.AddLogging();
services.AddNifiKit(options =>
{
    // Mutual TLS: the certificate NiFi authenticates this client with
    options.tls.client_certificate =
        NifiCertificateLoader.LoadPkcs12("client.p12", "password");
    // Trust NiFi's (private) CA instead of the OS trust store
    options.tls.trusted_root_certificates =
        NifiCertificateLoader.LoadCertificates("ca.crt");
    // One S2S transaction is one HTTP request; allow time for large batches
    options.http_timeout = TimeSpan.FromMinutes(5);
});
```

`AddNifiKit` registers `IFlowFileService` and `INifiTransportService` (a typed `HttpClient`) and returns the `IHttpClientBuilder`. Leave out `NifiTlsOptions` for plain HTTP or publicly trusted HTTPS.

Without DI:

```csharp
var tls = new NifiTlsOptions
{
    client_certificate = NifiCertificateLoader.LoadPem("client.crt", "client.key"),
    trusted_root_certificates = NifiCertificateLoader.LoadCertificates("ca.crt")
};
var transport = new NifiTransportService(
    new HttpClient(NifiHttpHandlerFactory.Create(tls)),
    new FlowFileService());
```

`NifiCertificateLoader` reads PKCS#12 keystores (`.p12`/`.pfx`), PEM certificate + key files (optionally encrypted), and PEM, DER or PKCS#12 truststores (such as NiFi's `truststore.p12`).

### Sending to NiFi via Site-to-Site (S2S)

```csharp
using NifiKit.SiteToSite;

SiteToSiteTransferResult result = await transport.SendViaS2SAsync(
    "https://nifi.example.com:8443/nifi",   // .../nifi, .../nifi-api or the host root
    "From .NET",                            // exact input port name
    packages);

if (result.destination_full)
{
    // Committed, but NiFi reports back-pressure: slow down
}
```

All packages are sent in one transaction. NiFi computes a CRC32 of what it received; NifiKit compares it with its own before confirming, so the transaction commits only if the data arrived intact. On any failure the transaction is cancelled and NiFi rolls it back.

Failures throw `NifiSiteToSiteException` (with `status_code`, `response_code` and `is_transient`). Network errors throw `HttpRequestException`, and cancellation throws `OperationCanceledException`.

#### NiFi requirements

- HTTP Site-to-Site enabled (`nifi.remote.input.http.enabled=true`, the default).
- A **public** input port that is **running**: a port on the root process group, or one with "Receive from remote connections" enabled.
- When NiFi is secured, the client identity (the certificate subject DN, e.g. `CN=my-client`, after any `nifi.security.identity.mapping` rules) needs:
  - the global policy **retrieve site-to-site details**, and
  - **receive data via site-to-site** on the input port.

  NiFi only lists ports the identity may send to. A missing policy therefore shows up as "port not found" or HTTP 403, and the exception message says which policy to add.

#### Retries

Register a Polly pipeline named `NifiS2S` (`NifiTransportService.S2S_PIPELINE_NAME`). It wraps the **whole transaction**. Packages are encoded once, so every attempt sends identical bytes, even for non-seekable content.

```csharp
using Polly;
using Polly.Retry;

services.AddResiliencePipeline(NifiTransportService.S2S_PIPELINE_NAME, builder =>
{
    builder.AddRetry(new RetryStrategyOptions
    {
        ShouldHandle = new PredicateBuilder()
            .Handle<NifiSiteToSiteException>(e => e.is_transient)
            .Handle<HttpRequestException>(),
        MaxRetryAttempts = 3,
        Delay = TimeSpan.FromSeconds(2),
        BackoffType = DelayBackoffType.Exponential
    });
});
```

(`AddResiliencePipeline` comes from the `Polly.Extensions` package.) Don't add generic HTTP retry handlers such as `AddStandardResilienceHandler()` to the NifiKit client. They retry individual S2S requests, which are not idempotent, and their default 10-second attempt timeout cuts off large uploads.

Delivery is at-least-once: if the commit response is lost on the network, a retry can deliver the same FlowFiles again.

### Posting to ListenHTTP

```csharp
// Packs the packages as FlowFile V3 (application/flowfile-v3)
bool ok = await transport.PostToNiFiAsync("https://nifi:9443/contentListener", packages);
```

The same TLS settings apply, so `ListenHTTP` with an SSL Context Service that requires client authentication works too.

### FlowFile V3 with streams and pipelines

```csharp
byte[] flowFile = await flowFileService.CreateFlowFileV3Async(package);

await foreach (var received in flowFileService.UnpackFlowFilesV3Async(inputStream))
{
    Console.WriteLine($"Received file: {received.GetAttribute("filename")}");
    // received.content holds the content
}

// System.IO.Pipelines
await flowFileService.WriteFlowFileV3Async(package, pipeWriter);
await foreach (var received in flowFileService.UnpackFlowFilesV3Async(pipeReader)) { }
```

NiFi rejects V3 FlowFiles without attributes, so packing a package with no attributes throws `ArgumentException`. When unpacking untrusted input, `FlowFileServiceOptions` limits the attribute count, attribute size and content size (content is buffered in memory).

## Implementation Details

The formats and protocols are implemented from the Apache NiFi source and tested against bytes produced by NiFi's own classes (see `NifiKit.Tests/Fixtures`).

### FlowFile V3
[FlowFilePackagerV3.java](https://github.com/apache/nifi/blob/main/nifi-commons/nifi-flowfile-packager/src/main/java/org/apache/nifi/util/FlowFilePackagerV3.java) / [FlowFileUnpackagerV3.java](https://github.com/apache/nifi/blob/main/nifi-commons/nifi-flowfile-packager/src/main/java/org/apache/nifi/util/FlowFileUnpackagerV3.java)

1. **Magic Header**: `NiFiFF3`
2. **Attribute Count**: 2-byte field length (or `0xFFFF` + 4 bytes for values of 65535 and above).
3. **Attributes**: Key and value as length-prefixed UTF-8 strings (same length encoding).
4. **Content Length**: 8-byte big-endian long.
5. **Content**: Raw bytes.

### FlowFile V1
[FlowFilePackagerV1.java](https://github.com/apache/nifi/blob/main/nifi-commons/nifi-flowfile-packager/src/main/java/org/apache/nifi/util/FlowFilePackagerV1.java): a tar archive with two entries:
- `flowfile.attributes`: a Java XML properties document (`Properties.loadFromXML`).
- `flowfile.content`: the payload.

### Site-to-Site (HTTP transport, protocol version 1)
[DataTransferResource.java](https://github.com/apache/nifi/blob/main/nifi-framework-bundle/nifi-framework/nifi-web/nifi-web-api/src/main/java/org/apache/nifi/web/api/DataTransferResource.java) / [SiteToSiteRestApiClient.java](https://github.com/apache/nifi/blob/main/nifi-commons/nifi-site-to-site-client/src/main/java/org/apache/nifi/remote/util/SiteToSiteRestApiClient.java)

Every request carries `x-nifi-site-to-site-protocol-version: 1`.

1. `GET /nifi-api/site-to-site`: resolves the input port id from its name.
2. `POST /nifi-api/data-transfer/input-ports/{portId}/transactions`: NiFi returns `201` with the transaction URL in the `Location` header.
3. `POST {transactionUrl}/flow-files` (`application/octet-stream`): FlowFiles encoded with [StandardFlowFileCodec](https://github.com/apache/nifi/blob/main/nifi-commons/nifi-site-to-site-client/src/main/java/org/apache/nifi/remote/codec/StandardFlowFileCodec.java) (per FlowFile: `int32` attribute count, `int32`-length-prefixed UTF-8 keys/values, `int64` content length, content). NiFi returns `202` with its CRC32.
4. `DELETE {transactionUrl}?responseCode=12` to confirm (commit), `19` for a bad checksum, or `15` to cancel. NiFi answers with response code `13` (finished) or `14` (finished, destination full).

## Development

### Tests

```bash
dotnet test
```

The unit tests need nothing external. The integration tests in `NifiKit.Tests/Integration` run against a live NiFi with mutual TLS and are skipped unless `NIFIKIT_IT_URL` is set:

```bash
./scripts/generate-test-certs.sh   # test CA, NiFi keystore/truststore, client certificate in ./certs
docker compose up -d               # NiFi 2.x at https://localhost:8443/nifi (takes a minute to start)
NIFIKIT_IT_URL=https://localhost:8443 dotnet test
```

The tests authenticate as `CN=nifikit-client` (the initial admin), grant themselves the Site-to-Site policies, create a temporary input port, send FlowFiles, check what arrived in NiFi's queue, and clean up afterwards. Override the certificate locations with `NIFIKIT_IT_CLIENT_CERT`, `NIFIKIT_IT_CLIENT_CERT_PASSWORD` and `NIFIKIT_IT_CA_CERT`.

## Publishing to NuGet

This project includes a GitHub Action that runs the tests and publishes the library to NuGet when a new version tag is pushed.

### Setup
1. Create a [NuGet API Key](https://www.nuget.org/account/apikeys).
2. Add the key as a secret in your GitHub repository:
   - Name: `NUGET_API_KEY`
   - Value: (Your API Key)

### Triggering a Release
To trigger a new release, create and push a tag:
```bash
git tag v1.0.0
git push origin v1.0.0
```
