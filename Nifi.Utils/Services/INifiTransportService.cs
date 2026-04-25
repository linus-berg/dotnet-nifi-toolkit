using Nifi.Utils.Models;

namespace Nifi.Utils.Services;

/// <summary>
///   Service for transporting NiFi FlowFiles using ListenHTTP and Site-to-Site (S2S).
/// </summary>
public interface INifiTransportService {
  // --- Transport: ListenHTTP ---
  Task<bool> PostToNiFiAsync(string url, Stream flow_file_stream,
                             string content_type = "application/flowfile-v3");

  // --- Transport: Site-to-Site (S2S) ---
  Task<bool> SendViaS2SAsync(string nifi_base_url, string input_port_name,
                             IEnumerable<NifiPackage> packages);
}