# .NET NiFi Utilities

A lightweight .NET 10 library for producing, receiving, and transporting Apache NiFi FlowFiles. This utility provides support for FlowFile V1 (Tar archive), FlowFile V3 (Binary stream), and Site-to-Site (S2S) protocol communication.

## Features

- **FlowFile V3 Support**: Create and unpack binary-encoded FlowFiles (V3) with full attribute and content preservation.
- **FlowFile V1 Support**: Create FlowFiles using the Tar-based V1 format.
- **Transport Mechanisms**:
  - **ListenHTTP**: Post FlowFiles directly to NiFi's `ListenHTTP` processor.
  - **Site-to-Site (S2S)**: Push data to NiFi Input Ports using the native S2S protocol (REST API based implementation).
- **Asynchronous Design**: Fully async/await compliant for high-performance streaming.

## Project Structure

- `Nifi.Utils`: The core library containing the service and models.
- `Nifi.App`: A console application (placeholder for usage and testing).

## Getting Started

### Prerequisites

- .NET 10.0 SDK

### Core Models

#### `NifiPackage`
Represents a single NiFi FlowFile, containing a dictionary of attributes and a stream for the content.

```csharp
using Nifi.Utils.Models;

var package = new NifiPackage
{
    attributes = new Dictionary<string, string> { { "filename", "test.txt" } },
    content = new MemoryStream(Encoding.UTF8.GetBytes("Hello NiFi"))
};
```

### Usage Examples

#### Creating a FlowFile V3 Stream
FlowFile V3 is a binary format that prepends metadata to the content.

```csharp
using Nifi.Utils;

var nifiService = new NifiService(new HttpClient());
var attributes = new Dictionary<string, string> { { "source", "dotnet-app" } };
byte[] content = Encoding.UTF8.GetBytes("Data for NiFi");

// Create a single V3 FlowFile as a byte array
byte[] v3Data = await nifiService.CreateFlowFileV3Async(attributes, content);
```

#### Sending to NiFi via Site-to-Site (S2S)
The S2S protocol allows for reliable, high-volume data transfer to NiFi Input Ports.

```csharp
var packages = new List<NifiPackage> { ... };
bool success = await nifiService.SendViaS2SAsync(
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
    bool success = await nifiService.PostToNiFiAsync(
        "http://nifi-server:8000/contentListener", 
        ms
    );
}
```

#### Unpacking FlowFiles (V3)
If you are receiving V3 streams from NiFi (e.g., via a webhook), you can unpack them easily.

```csharp
await foreach (var package in nifiService.UnpackFlowFilesV3Async(inputStream))
{
    Console.WriteLine($"Received file: {package.attributes["filename"]}");
    // Process package.content...
}
```

## Implementation Details

### FlowFile V3 Format
The library implements the NiFi FlowFile V3 specification:
1. **Magic Header**: `NiFiFF3`
2. **Attributes Count**: 2-byte or 6-byte encoded length.
3. **Attributes**: Key-value pairs with length-prefixed strings.
4. **Content Length**: 8-byte long.
5. **Content**: Raw binary data.

### FlowFile V1 Format
FlowFile V1 uses the standard Tar archive format containing two files:
- `flowfile.attributes`: A key-value properties file.
- `flowfile.content`: The actual payload.
