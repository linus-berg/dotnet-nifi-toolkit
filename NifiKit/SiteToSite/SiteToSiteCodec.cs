using System.Buffers.Binary;
using System.Text;
using NifiKit.Models;

namespace NifiKit.SiteToSite;

/// <summary>
///   A FlowFile encoded for Site-to-Site: the codec header plus its content.
/// </summary>
internal sealed class SiteToSitePacket : IAsyncDisposable {
  public SiteToSitePacket(byte[] header, PackageContent content) {
    this.header = header;
    this.content = content;
  }

  /// <summary>
  ///   Attribute count, attributes and content length.
  /// </summary>
  public byte[] header { get; }

  public PackageContent content { get; }

  public long encoded_length => header.Length + content.length;

  public ValueTask DisposeAsync() {
    return content.DisposeAsync();
  }
}

/// <summary>
///   Encoder for org.apache.nifi.remote.codec.StandardFlowFileCodec (version 1),
///   the wire format of HTTP Site-to-Site. Per FlowFile:
///   int32 attribute count, then for each key and value an int32 UTF-8 byte
///   length and the bytes, then an int64 content length and the content.
///   Packets are concatenated without any separator; NiFi detects the end of
///   the transaction from the end of the request body.
/// </summary>
internal static class SiteToSiteCodec {
  public static async ValueTask<SiteToSitePacket> CreatePacketAsync(
    NifiPackage package, CancellationToken ct) {
    if (package.attributes.Count > SiteToSiteProtocol.MAX_NUM_ATTRIBUTES) {
      throw new ArgumentException(
        $"A FlowFile can have at most {SiteToSiteProtocol.MAX_NUM_ATTRIBUTES} " +
        $"attributes over Site-to-Site; found {package.attributes.Count}.",
        nameof(package)
      );
    }

    PackageContent content =
      await PackageContent.OpenAsync(package.content, ct);
    return new SiteToSitePacket(
      EncodeHeader(package.attributes, content.length),
      content
    );
  }

  public static byte[] EncodeHeader(IDictionary<string, string> attributes,
                                    long content_length) {
    long size = sizeof(int) + sizeof(long);
    foreach (KeyValuePair<string, string> attribute in attributes) {
      size += sizeof(int) + Encoding.UTF8.GetByteCount(attribute.Key);
      size += sizeof(int) +
              Encoding.UTF8.GetByteCount(attribute.Value ?? string.Empty);
    }

    if (size > Array.MaxLength) {
      throw new ArgumentException(
        "The FlowFile attributes are too large to encode."
      );
    }

    byte[] header = new byte[size];
    Span<byte> span = header;
    BinaryPrimitives.WriteInt32BigEndian(span, attributes.Count);
    int offset = sizeof(int);
    foreach (KeyValuePair<string, string> attribute in attributes) {
      offset += WriteString(span.Slice(offset), attribute.Key);
      offset += WriteString(span.Slice(offset), attribute.Value ?? string.Empty);
    }

    BinaryPrimitives.WriteInt64BigEndian(span.Slice(offset), content_length);
    return header;
  }

  private static int WriteString(Span<byte> destination, string value) {
    int length = Encoding.UTF8.GetBytes(value, destination.Slice(sizeof(int)));
    BinaryPrimitives.WriteInt32BigEndian(destination, length);
    return sizeof(int) + length;
  }
}
