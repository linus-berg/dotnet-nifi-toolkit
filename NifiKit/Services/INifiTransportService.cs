using NifiKit.Models;

namespace NifiKit.Services;

/// <summary>
/// Service for transporting NiFi FlowFiles using ListenHTTP and Site-to-Site (S2S).
/// </summary>
public interface INifiTransportService {
  // --- Transport: ListenHTTP ---
  Task<bool> PostToNiFiAsync(string url, Stream flow_file_stream,
                             string content_type = "application/flowfile-v3",
                             CancellationToken ct = default);

  // --- Transport: Site-to-Site (S2S) ---
  Task<bool> SendViaS2SAsync(string nifi_base_url, string input_port_name,
                             IEnumerable<NifiPackage> packages,
                             CancellationToken ct = default);
}