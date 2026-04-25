using System.Runtime.CompilerServices;
using Nifi.Utils.Models;

namespace Nifi.Utils.Services;

/// <summary>
/// Service for producing and receiving NiFi FlowFiles using V1 and V3 formats.
/// </summary>
public interface IFlowFileService
{
    // --- Package Helpers ---
    NifiPackage CreatePackage(IDictionary<string, string> attributes, byte[] content);

    // --- V3 (Binary Stream) ---
    Task<byte[]> CreateFlowFileV3Async(NifiPackage package);
    Task WriteFlowFileV3Async(NifiPackage package, Stream output_stream);
    Task WriteFlowFilesV3Async(IEnumerable<NifiPackage> packages, Stream output_stream);
    IAsyncEnumerable<NifiPackage> UnpackFlowFilesV3Async(Stream input_stream, CancellationToken ct = default);

    // --- V1 (Tar Archive) ---
    Task<byte[]> CreateFlowFileV1Async(NifiPackage package);
    Task WriteFlowFileV1Async(NifiPackage package, Stream output_stream);
}
