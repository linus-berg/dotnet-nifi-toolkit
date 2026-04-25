using System.Runtime.CompilerServices;
using Nifi.Utils.Models;

namespace Nifi.Utils;

/// <summary>
/// Service for producing, receiving, and transporting NiFi FlowFiles using V1, V3, and S2S.
/// </summary>
public interface INifiService
{
  // --- V3 (Binary Stream) ---
  Task<byte[]> CreateFlowFileV3Async(IDictionary<string, string> attributes, byte[] content);
  Task WriteFlowFileV3Async(IDictionary<string, string> attributes, Stream content, Stream output_stream);
  Task WriteFlowFilesV3Async(IEnumerable<NifiPackage> packages, Stream output_stream);
  IAsyncEnumerable<NifiPackage> UnpackFlowFilesV3Async(Stream input_stream, [EnumeratorCancellation] CancellationToken ct = default);

  // --- V1 (Tar Archive) ---
  Task<byte[]> CreateFlowFileV1Async(IDictionary<string, string> attributes, byte[] content);
  Task WriteFlowFileV1Async(IDictionary<string, string> attributes, Stream content, Stream output_stream);

  // --- Transport: ListenHTTP ---
  Task<bool> PostToNiFiAsync(string url, Stream flow_file_stream, string content_type = "application/flowfile-v3");

  // --- Transport: Site-to-Site (S2S) ---
  Task<bool> SendViaS2SAsync(string nifi_base_url, string input_port_name, IEnumerable<NifiPackage> packages);
}