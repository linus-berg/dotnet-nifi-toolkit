# .NET NiFi Utilities

A lightweight .NET 10 library for producing, receiving, and transporting Apache NiFi FlowFiles. This utility provides support for FlowFile V1 (Tar archive), FlowFile V3 (Binary stream), and Site-to-Site (S2S) protocol communication.

## Features

- **FlowFile V3 Support**: Create and unpack binary-encoded FlowFiles (V3) with full attribute and content preservation.
- **FlowFile V1 Support**: Create FlowFiles using the Tar-based V1 format.
- **Transport Mechanisms**:
  - **ListenHTTP**: Post FlowFiles directly to NiFi's `ListenHTTP` processor.
  - **Site-to-Site (S2S)**: Push data to NiFi Input Ports using the native S2S protocol.
- **Asynchronous Design**: Fully async/await compliant for high-performance streaming.

## Architecture

The library is divided into two primary services to separate concerns between data formatting and network transport:

- **`IFlowFileService` / `FlowFileService`**: Responsible for the low-level serialization and deserialization of NiFi FlowFile formats (V1 and V3).
- **`INifiTransportService` / `NifiTransportService`**: Responsible for high-level communication protocols, utilizing the `IFlowFileService` for payload preparation.

## Project Structure

- `Nifi.Utils`: The core library.
  - `Services/`: Contains the interface and implementation for FlowFile and Transport services.
  - `Models/`: Contains the `NifiPackage` data model.
- `Nifi.App`: A console application (placeholder for usage and testing).

## Getting Started

### Prerequisites

- .NET 10.0 SDK

### Core Models

#### `NifiPackage`
Represents a single NiFi FlowFile. It provides fluent helper methods for setting attributes and content.

```csharp
using Nifi.Utils.Models;

var package = new NifiPackage()
    .AddAttribute("filename", "test.txt")
    .AddAttribute("mime.type", "text/plain")
    .SetContent(Encoding.UTF8.GetBytes("Hello NiFi"));
```

### Usage Examples

#### Initializing Services (Dependency Injection)

```csharp
using Nifi.Utils.Services;
using Microsoft.Extensions.DependencyInjection;

// Registration example
var services = new ServiceCollection();
services.AddHttpClient();
services.AddSingleton<IFlowFileService, FlowFileService>();
services.AddSingleton<INifiTransportService, NifiTransportService>();

var provider = services.BuildServiceProvider();
var transportService = provider.GetRequiredService<INifiTransportService>();
var flowFileService = provider.GetRequiredService<IFlowFileService>();
```

#### Creating a FlowFile V3 Stream

```csharp
var attributes = new Dictionary<string, string> { { "source", "dotnet-app" } };
byte[] content = Encoding.UTF8.GetBytes("Data for NiFi");

// Create a single V3 FlowFile as a byte array using IFlowFileService
var package = flowFileService.CreatePackage(attributes, content);
byte[] v3Data = await flowFileService.CreateFlowFileV3Async(package);
```

#### Sending to NiFi via Site-to-Site (S2S)
The S2S protocol allows for reliable, high-volume data transfer to NiFi Input Ports.

```csharp
var packages = new List<NifiPackage> { ... };
bool success = await transportService.SendViaS2SAsync(
    "http://nifi-server:8080", 
    "MyInputPort", 
    packages
);
```

#### Posting to ListenHTTP
Use this for simple HTTP-based ingestion.

```csharp
using (var ms = new MemoryStream(v3Data))
{
    bool success = await transportService.PostToNiFiAsync(
        "http://nifi-server:8000/contentListener", 
        ms
    );
}
```

#### Unpacking FlowFiles (V3)
If you are receiving V3 streams from NiFi (e.g., via a webhook), you can unpack them easily.

```csharp
await foreach (var package in flowFileService.UnpackFlowFilesV3Async(inputStream))
{
    Console.WriteLine($"Received file: {package.attributes["filename"]}");
    // Process package.content...
}
```

## Implementation Details

The implementation of FlowFile formats and protocols in this library is based on the official Apache NiFi source code.

### FlowFile V3 Format
The library implements the NiFi FlowFile V3 specification as defined in the NiFi source:
- [FlowFilePackagerV3.java](https://github.com/apache/nifi/blob/main/nifi-commons/nifi-flowfile-packager/src/main/java/org/apache/nifi/util/FlowFilePackagerV3.java)
- [FlowFileUnpackagerV3.java](https://github.com/apache/nifi/blob/main/nifi-commons/nifi-flowfile-packager/src/main/java/org/apache/nifi/util/FlowFileUnpackagerV3.java)

1. **Magic Header**: `NiFiFF3`
2. **Attributes Count**: 2-byte or 6-byte encoded length.
3. **Attributes**: Key-value pairs with length-prefixed strings.
4. **Content Length**: 8-byte long.
5. **Content**: Raw binary data.

### FlowFile V1 Format
FlowFile V1 uses the standard Tar archive format containing two files:
- `flowfile.attributes`: A key-value properties file.
- `flowfile.content`: The actual payload.

## Publishing to NuGet

This project includes a GitHub Action to automatically publish the library to NuGet when a new version tag is pushed.

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
