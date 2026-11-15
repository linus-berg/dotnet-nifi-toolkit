using System.Collections.Concurrent;

namespace NifiKit.SiteToSite;

/// <summary>
///   Caches input port name to id lookups so each transaction does not need a
///   GET /nifi-api/site-to-site round trip. Entries are dropped when NiFi
///   rejects a transaction for the port.
/// </summary>
/// <remarks>
///   <c>AddNifiKit</c> registers one shared instance, because the transport
///   service itself is a transient typed HttpClient.
/// </remarks>
public sealed class SiteToSitePortCache {
  private readonly ConcurrentDictionary<string, string> port_ids_ = new();

  internal bool TryGet(Uri api_base, string port_name, out string port_id) {
    bool found = port_ids_.TryGetValue(Key(api_base, port_name),
                                       out string? id);
    port_id = id ?? string.Empty;
    return found;
  }

  internal void Set(Uri api_base, string port_name, string port_id) {
    port_ids_[Key(api_base, port_name)] = port_id;
  }

  internal void Remove(Uri api_base, string port_name) {
    port_ids_.TryRemove(Key(api_base, port_name), out _);
  }

  private static string Key(Uri api_base, string port_name) {
    return api_base.AbsoluteUri + "\n" + port_name;
  }
}
