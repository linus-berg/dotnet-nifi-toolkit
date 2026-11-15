using System.Buffers;
using System.Globalization;
using System.IO.Hashing;
using System.Net;
using System.Net.Http.Headers;

namespace NifiKit.SiteToSite;

/// <summary>
///   Streams encoded Site-to-Site packets as a request body and computes the
///   CRC32 checksum of exactly the bytes sent, which NiFi verifies against
///   its own (java.util.zip.CRC32 uses the same IEEE polynomial). Content is
///   rewound on every serialization, so the body can be re-sent on retry.
/// </summary>
internal sealed class SiteToSiteContent : HttpContent {
  private const int COPY_BUFFER_SIZE = 81920;

  private readonly IReadOnlyList<SiteToSitePacket> packets_;
  private readonly long length_;
  private string? checksum_;

  public SiteToSiteContent(IReadOnlyList<SiteToSitePacket> packets) {
    packets_ = packets;
    foreach (SiteToSitePacket packet in packets) {
      length_ += packet.encoded_length;
    }

    Headers.ContentType =
      new MediaTypeHeaderValue(SiteToSiteProtocol.OCTET_STREAM);
  }

  /// <summary>
  ///   Decimal CRC32 of the last complete serialization, formatted like
  ///   Java's <c>String.valueOf(crc.getValue())</c>.
  /// </summary>
  public string checksum =>
    checksum_ ??
    throw new InvalidOperationException("The content has not been sent yet.");

  public long length => length_;

  protected override Task SerializeToStreamAsync(Stream stream,
                                                 TransportContext? context) {
    return SerializeToStreamAsync(stream, context, CancellationToken.None);
  }

  protected override async Task SerializeToStreamAsync(Stream stream,
    TransportContext? context, CancellationToken ct) {
    checksum_ = null;
    Crc32 crc = new();
    byte[] buffer = ArrayPool<byte>.Shared.Rent(COPY_BUFFER_SIZE);
    try {
      foreach (SiteToSitePacket packet in packets_) {
        crc.Append(packet.header);
        await stream.WriteAsync(packet.header, ct);

        packet.content.Rewind();
        long remaining = packet.content.length;
        while (remaining > 0) {
          int to_read = (int)Math.Min(buffer.Length, remaining);
          int read =
            await packet.content.stream.ReadAsync(buffer.AsMemory(0, to_read),
                                                  ct);
          if (read == 0) {
            throw new EndOfStreamException(
              $"FlowFile content ended {remaining} bytes before its declared " +
              "length; was the content stream modified during the transfer?"
            );
          }

          crc.Append(buffer.AsSpan(0, read));
          await stream.WriteAsync(buffer.AsMemory(0, read), ct);
          remaining -= read;
        }
      }
    } finally {
      ArrayPool<byte>.Shared.Return(buffer);
    }

    checksum_ =
      crc.GetCurrentHashAsUInt32().ToString(CultureInfo.InvariantCulture);
  }

  protected override bool TryComputeLength(out long length) {
    length = length_;
    return true;
  }
}
