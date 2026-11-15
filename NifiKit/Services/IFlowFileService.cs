using System.IO.Pipelines;
using NifiKit.Models;

namespace NifiKit.Services;

/// <summary>
///   Service for producing and receiving NiFi FlowFiles using V1 and V3 formats.
/// </summary>
public interface IFlowFileService {
  // --- Package Helpers ---
  NifiPackage CreatePackage(IDictionary<string, string> attributes,
                            byte[] content);

  // --- V3 (Binary Stream) ---

  /// <summary>
  ///   Packs a FlowFile in V3 format (<c>application/flowfile-v3</c>).
  /// </summary>
  /// <exception cref="ArgumentException">
  ///   The package has no attributes. NiFi rejects V3 FlowFiles without
  ///   attributes.
  /// </exception>
  Task<byte[]> CreateFlowFileV3Async(NifiPackage package,
                                     CancellationToken ct = default);

  Task WriteFlowFileV3Async(NifiPackage package, Stream output_stream,
                            CancellationToken ct = default);

  Task WriteFlowFilesV3Async(IEnumerable<NifiPackage> packages,
                             Stream output_stream,
                             CancellationToken ct = default);

  IAsyncEnumerable<NifiPackage> UnpackFlowFilesV3Async(
    Stream input_stream, CancellationToken ct = default);

  // --- V3 (Pipelines) ---
  Task WriteFlowFileV3Async(NifiPackage package, PipeWriter writer,
                            CancellationToken ct = default);

  IAsyncEnumerable<NifiPackage> UnpackFlowFilesV3Async(
    PipeReader reader, CancellationToken ct = default);

  // --- V1 (Tar Archive) ---

  /// <summary>
  ///   Packs a FlowFile in the legacy V1 (tar) format. NiFi 2.x deprecates
  ///   this format; prefer V3.
  /// </summary>
  Task<byte[]> CreateFlowFileV1Async(NifiPackage package,
                                     CancellationToken ct = default);

  Task WriteFlowFileV1Async(NifiPackage package, Stream output_stream,
                            CancellationToken ct = default);
}
