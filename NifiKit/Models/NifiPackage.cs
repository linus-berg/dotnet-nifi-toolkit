using System.Text;

namespace NifiKit.Models;

/// <summary>
///   Represents a single NiFi FlowFile (attributes + content).
/// </summary>
/// <remarks>
///   <para>
///     Content semantics: a <b>seekable</b> content stream is always treated as
///     the whole stream, from position 0 to its length. Serializers rewind it
///     before reading, so the same package can be written more than once (for
///     example when a Site-to-Site transaction is retried).
///   </para>
///   <para>
///     A <b>non-seekable</b> content stream can only be read once. Serializers
///     buffer it in memory where the format requires the length up front.
///   </para>
/// </remarks>
public class NifiPackage : IDisposable {
  /// <summary>
  ///   FlowFile attributes.
  /// </summary>
  public IDictionary<string, string> attributes { get; } =
    new Dictionary<string, string>();

  /// <summary>
  ///   FlowFile content stream. See the class remarks for how seekable and
  ///   non-seekable streams are read.
  /// </summary>
  public Stream content { get; set; } = Stream.Null;

  public void Dispose() {
    content.Dispose();
  }

  #region Attribute Helpers

  /// <summary>
  ///   Adds or updates a single attribute.
  /// </summary>
  public NifiPackage AddAttribute(string key, string value) {
    attributes[key] = value;
    return this;
  }

  /// <summary>
  ///   Adds or updates multiple attributes.
  /// </summary>
  public NifiPackage AddAttributes(IDictionary<string, string> new_attributes) {
    foreach (KeyValuePair<string, string> kvp in new_attributes) {
      attributes[kvp.Key] = kvp.Value;
    }

    return this;
  }

  /// <summary>
  ///   Removes a single attribute by key.
  /// </summary>
  public NifiPackage RemoveAttribute(string key) {
    attributes.Remove(key);
    return this;
  }

  /// <summary>
  ///   Removes multiple attributes by their keys.
  /// </summary>
  public NifiPackage RemoveAttributes(params string[] keys) {
    foreach (string key in keys) {
      attributes.Remove(key);
    }

    return this;
  }

  /// <summary>
  ///   Clears all attributes.
  /// </summary>
  public NifiPackage ClearAttributes() {
    attributes.Clear();
    return this;
  }

  /// <summary>
  ///   Gets an attribute value or a default value if not found.
  /// </summary>
  public string? GetAttribute(string key, string? default_value = null) {
    return attributes.TryGetValue(key, out string? value)
             ? value
             : default_value;
  }

  /// <summary>
  ///   Checks if an attribute exists.
  /// </summary>
  public bool HasAttribute(string key) {
    return attributes.ContainsKey(key);
  }

  #endregion

  #region Content Helpers

  /// <summary>
  ///   Sets the content from a byte array.
  /// </summary>
  public NifiPackage SetContent(byte[] data) {
    content.Dispose();
    content = new MemoryStream(data);
    return this;
  }

  /// <summary>
  ///   Sets the content from a string using the specified encoding (defaults to UTF-8).
  /// </summary>
  public NifiPackage SetContent(string text, Encoding? encoding = null) {
    return SetContent((encoding ?? Encoding.UTF8).GetBytes(text));
  }

  /// <summary>
  ///   Sets the content from a stream. The package takes ownership of the
  ///   stream and disposes it together with the package.
  /// </summary>
  public NifiPackage SetContent(Stream stream) {
    if (content != stream) {
      content.Dispose();
      content = stream;
    }

    return this;
  }

  /// <summary>
  ///   Reads the entire content as a byte array.
  ///   Note: This will consume the stream if it is not seekable.
  /// </summary>
  public async Task<byte[]> GetContentAsBytesAsync(
    CancellationToken ct = default) {
    if (content == Stream.Null) {
      return Array.Empty<byte>();
    }

    if (content is MemoryStream ms) {
      return ms.ToArray();
    }

    using MemoryStream memory_stream = new();
    if (content.CanSeek) {
      long original_position = content.Position;
      content.Position = 0;
      await content.CopyToAsync(memory_stream, ct);
      content.Position = original_position;
    } else {
      await content.CopyToAsync(memory_stream, ct);
    }

    return memory_stream.ToArray();
  }

  /// <summary>
  ///   Reads the entire content as a string.
  ///   Note: This will consume the stream if it is not seekable.
  /// </summary>
  public async Task<string> GetContentAsStringAsync(Encoding? encoding = null,
                                                   CancellationToken ct =
                                                     default) {
    byte[] bytes = await GetContentAsBytesAsync(ct);
    return (encoding ?? Encoding.UTF8).GetString(bytes);
  }

  #endregion
}
