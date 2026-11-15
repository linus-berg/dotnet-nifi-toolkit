namespace NifiKit.Services;

/// <summary>
///   Limits applied when unpacking FlowFiles. They protect a receiver from
///   malformed or hostile input that would otherwise make it allocate huge
///   amounts of memory.
/// </summary>
public sealed class FlowFileServiceOptions {
  /// <summary>
  ///   Maximum number of attributes per FlowFile. Defaults to 25,000, the same
  ///   limit NiFi applies to Site-to-Site data packets.
  /// </summary>
  public int max_attribute_count { get; set; } = 25_000;

  /// <summary>
  ///   Maximum size in bytes (UTF-8) of a single attribute key or value.
  ///   Defaults to 64 MiB.
  /// </summary>
  public int max_attribute_length { get; set; } = 64 * 1024 * 1024;

  /// <summary>
  ///   Maximum content size in bytes per FlowFile. Unpacked content is buffered
  ///   in memory, so this defaults to the largest possible
  ///   <see cref="MemoryStream" />.
  /// </summary>
  public long max_content_length { get; set; } = Array.MaxLength;
}
