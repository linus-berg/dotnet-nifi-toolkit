namespace NifiKit.Models;

/// <summary>
///   A package's content stream, positioned at its start, with a known length.
/// </summary>
internal readonly struct PackageContent : IAsyncDisposable {
  private readonly bool owns_stream_;

  private PackageContent(Stream stream, long length, bool owns_stream) {
    this.stream = stream;
    this.length = length;
    owns_stream_ = owns_stream;
  }

  public Stream stream { get; }
  public long length { get; }

  public ValueTask DisposeAsync() {
    return owns_stream_ ? stream.DisposeAsync() : ValueTask.CompletedTask;
  }

  /// <summary>
  ///   Opens the content of a package for serialization. Seekable streams are
  ///   rewound and used as-is (the whole stream is the content); non-seekable
  ///   streams are buffered in memory because every NiFi format needs the
  ///   length before the data.
  /// </summary>
  public static async ValueTask<PackageContent> OpenAsync(
    Stream content, CancellationToken ct) {
    if (content.CanSeek) {
      content.Position = 0;
      return new PackageContent(content, content.Length, false);
    }

    MemoryStream buffer = new();
    await content.CopyToAsync(buffer, ct);
    buffer.Position = 0;
    return new PackageContent(buffer, buffer.Length, true);
  }

  /// <summary>
  ///   Rewinds the content to its start (used before re-sending it).
  /// </summary>
  public void Rewind() {
    stream.Position = 0;
  }
}
