using System.Buffers;
using System.Buffers.Binary;
using System.Formats.Tar;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Text;
using Microsoft.Extensions.Logging;
using NifiKit.Models;

namespace NifiKit.Services;

/// <summary>
///   Service for producing and receiving NiFi FlowFiles using V1 (Tar) and V3 (Binary) formats.
///   This implementation uses System.IO.Pipelines for high-performance, low-allocation I/O.
/// </summary>
/// <remarks>
///   The formats follow NiFi's FlowFilePackagerV3 / FlowFileUnpackagerV3 and
///   FlowFilePackagerV1 (nifi-commons/nifi-flowfile-packager).
/// </remarks>
public class FlowFileService : IFlowFileService {
  // Magic header for NiFi FlowFile V3
  private static readonly byte[] S_MAGIC_HEADER_V3_ = "NiFiFF3"u8.ToArray();

  // Field lengths below this value use 2 bytes, otherwise 0xFFFF + 4 bytes
  private const int MAX_VALUE_2_BYTES = 0xFFFF;

  // Size hint for content chunks copied into a PipeWriter
  private const int CONTENT_CHUNK_SIZE = 64 * 1024;

  // Initial buffer for unpacked content; it grows as data actually arrives
  private const int MAX_INITIAL_CONTENT_CAPACITY = 4 * 1024 * 1024;

  private const string V1_ATTRIBUTES_ENTRY = "flowfile.attributes";
  private const string V1_CONTENT_ENTRY = "flowfile.content";

  private readonly ILogger<FlowFileService>? logger_;
  private readonly FlowFileServiceOptions options_;

  public FlowFileService(ILogger<FlowFileService>? logger = null,
                         FlowFileServiceOptions? options = null) {
    logger_ = logger;
    options_ = options ?? new FlowFileServiceOptions();
  }

  #region Package Helpers

  /// <inheritdoc />
  public NifiPackage CreatePackage(IDictionary<string, string> attributes,
                                   byte[] content) {
    return new NifiPackage()
           .AddAttributes(attributes)
           .SetContent(content);
  }

  #endregion

  #region FlowFile V3 (Binary)

  /// <inheritdoc />
  public async Task<byte[]> CreateFlowFileV3Async(NifiPackage package,
                                                  CancellationToken ct =
                                                    default) {
    using MemoryStream ms = new();
    await WriteFlowFileV3Async(package, ms, ct);
    return ms.ToArray();
  }

  /// <inheritdoc />
  public Task WriteFlowFileV3Async(NifiPackage package,
                                   Stream output_stream,
                                   CancellationToken ct = default) {
    ArgumentNullException.ThrowIfNull(package);
    return WriteFlowFilesV3Async(new[] { package }, output_stream, ct);
  }

  /// <inheritdoc />
  public async Task WriteFlowFilesV3Async(IEnumerable<NifiPackage> packages,
                                          Stream output_stream,
                                          CancellationToken ct = default) {
    ArgumentNullException.ThrowIfNull(packages);
    ArgumentNullException.ThrowIfNull(output_stream);

    // leaveOpen: the caller owns the stream; completing the writer returns
    // its pooled buffers without closing the stream.
    PipeWriter writer = PipeWriter.Create(
      output_stream,
      new StreamPipeWriterOptions(leaveOpen: true)
    );
    try {
      foreach (NifiPackage package in packages) {
        await WriteFlowFileV3Async(package, writer, ct);
      }

      await writer.FlushAsync(ct);
    } catch (Exception ex) {
      await writer.CompleteAsync(ex);
      throw;
    }

    await writer.CompleteAsync();
  }

  /// <inheritdoc />
  public async Task WriteFlowFileV3Async(NifiPackage package,
                                         PipeWriter writer,
                                         CancellationToken ct = default) {
    ArgumentNullException.ThrowIfNull(package);
    ArgumentNullException.ThrowIfNull(writer);
    if (package.attributes.Count == 0) {
      // FlowFileUnpackagerV3: "FlowFiles cannot have zero attributes"
      throw new ArgumentException(
        "A FlowFile V3 package must have at least one attribute; NiFi " +
        "rejects V3 FlowFiles without attributes (add e.g. 'filename').",
        nameof(package)
      );
    }

    logger_?.LogDebug(
      "Packing FlowFile V3 with {AttributeCount} attributes",
      package.attributes.Count
    );

    await using PackageContent content =
      await PackageContent.OpenAsync(package.content, ct);

    // 1. Magic header, 2. attribute count, 3. attributes, 4. content length
    writer.Write(S_MAGIC_HEADER_V3_);
    WriteFieldLength(writer, package.attributes.Count);
    foreach (KeyValuePair<string, string> attribute in package.attributes) {
      // Each string is written as a length-prefixed UTF-8 sequence
      WriteStringV3(writer, attribute.Key);
      WriteStringV3(writer, attribute.Value ?? string.Empty);
    }

    WriteInt64(writer, content.length);

    // 5. Content, copied straight into the writer's buffers. Exactly
    // content.length bytes are written so the header always matches the data.
    await CopyContentAsync(content.stream, content.length, writer, ct);
    await FlushOrThrowAsync(writer, ct);
  }

  /// <inheritdoc />
  public async IAsyncEnumerable<NifiPackage> UnpackFlowFilesV3Async(
    Stream input_stream,
    [EnumeratorCancellation] CancellationToken ct = default) {
    ArgumentNullException.ThrowIfNull(input_stream);

    // Wrap the input stream in a PipeReader for efficient asynchronous reading
    PipeReader reader = PipeReader.Create(
      input_stream,
      new StreamPipeReaderOptions(leaveOpen: true)
    );
    try {
      await foreach (NifiPackage package in
                     UnpackFlowFilesV3Async(reader, ct)) {
        yield return package;
      }
    } finally {
      await reader.CompleteAsync();
    }
  }

  /// <inheritdoc />
  public async IAsyncEnumerable<NifiPackage> UnpackFlowFilesV3Async(
    PipeReader reader,
    [EnumeratorCancellation] CancellationToken ct = default) {
    ArgumentNullException.ThrowIfNull(reader);

    while (true) {
      ct.ThrowIfCancellationRequested();

      // Try to read the header to determine if a new FlowFile is starting
      ReadResult result = await reader.ReadAtLeastAsync(
                            S_MAGIC_HEADER_V3_.Length,
                            ct
                          );
      ThrowIfCanceled(result, ct);
      ReadOnlySequence<byte> buffer = result.Buffer;

      // Empty and completed: we've reached the end of the stream
      if (buffer.IsEmpty && result.IsCompleted) {
        reader.AdvanceTo(buffer.End);
        yield break;
      }

      if (buffer.Length < S_MAGIC_HEADER_V3_.Length) {
        reader.AdvanceTo(buffer.Start, buffer.End);
        throw new EndOfStreamException(
          "Unexpected end of FlowFile V3 stream while reading the magic header."
        );
      }

      // Verify the Magic Header (NiFiFF3)
      if (!StartsWith(buffer, S_MAGIC_HEADER_V3_)) {
        reader.AdvanceTo(buffer.Start, buffer.End);
        logger_?.LogError("Invalid NiFi FlowFile V3 Magic Header");
        throw new InvalidDataException("Invalid NiFi FlowFile V3 Magic Header");
      }

      // Consume the header bytes
      reader.AdvanceTo(buffer.GetPosition(S_MAGIC_HEADER_V3_.Length));

      // 1. Read the number of attributes
      int attr_count = await ReadFieldLengthAsync(reader, ct);
      if (attr_count > options_.max_attribute_count) {
        throw new InvalidDataException(
          $"FlowFile has {attr_count} attributes, more than the configured " +
          $"maximum of {options_.max_attribute_count}."
        );
      }

      // 2. Read each attribute key-value pair
      NifiPackage package = new();
      for (int i = 0; i < attr_count; i++) {
        string key = await ReadStringV3Async(reader, ct);
        string value = await ReadStringV3Async(reader, ct);
        package.attributes[key] = value;
      }

      // 3. Read the content length
      long content_length = await ReadInt64Async(reader, ct);
      if (content_length < 0 || content_length > options_.max_content_length) {
        throw new InvalidDataException(
          $"FlowFile content length {content_length} is negative or larger " +
          $"than the configured maximum of {options_.max_content_length}."
        );
      }

      // 4. Read the raw content bytes
      package.content = await ReadContentAsync(reader, content_length, ct);

      logger_?.LogTrace(
        "Unpacked FlowFile V3 with {AttributeCount} attributes and {ContentLength} bytes",
        attr_count,
        content_length
      );
      yield return package;
    }
  }

  #endregion

  #region FlowFile V1 (Tar)

  /// <inheritdoc />
  public async Task<byte[]> CreateFlowFileV1Async(NifiPackage package,
                                                  CancellationToken ct =
                                                    default) {
    using MemoryStream ms = new();
    await WriteFlowFileV1Async(package, ms, ct);
    return ms.ToArray();
  }

  /// <inheritdoc />
  public async Task WriteFlowFileV1Async(NifiPackage package,
                                         Stream output_stream,
                                         CancellationToken ct = default) {
    ArgumentNullException.ThrowIfNull(package);
    ArgumentNullException.ThrowIfNull(output_stream);
    logger_?.LogDebug("Packing FlowFile V1 (Tar)");

    // Serialize first so invalid attributes fail before anything is written
    byte[] attr_bytes =
      Encoding.UTF8.GetBytes(SerializeAttributesV1(package.attributes));

    // The tar header needs the size up front; non-seekable content is buffered
    await using PackageContent content =
      await PackageContent.OpenAsync(package.content, ct);

    // FlowFile V1 is a Tar archive containing 'flowfile.attributes' and 'flowfile.content'
    await using TarWriter writer = new(
      output_stream,
      TarEntryFormat.Ustar,
      true
    );

    // 1. Attributes, as a Java XML properties document
    using MemoryStream attr_stream = new(attr_bytes);
    UstarTarEntry attr_entry = new(
      TarEntryType.RegularFile,
      V1_ATTRIBUTES_ENTRY
    ) {
      DataStream = attr_stream
    };
    await writer.WriteEntryAsync(attr_entry, ct);

    // 2. Content
    UstarTarEntry content_entry =
      new(TarEntryType.RegularFile, V1_CONTENT_ENTRY) {
        DataStream = content.stream
      };
    await writer.WriteEntryAsync(content_entry, ct);
  }

  #endregion

  #region Binary Helpers (Pipelines)

  /// <summary>
  ///   Writes a length-prefixed UTF-8 string without intermediate allocations.
  /// </summary>
  private static void WriteStringV3(IBufferWriter<byte> writer, string value) {
    WriteFieldLength(writer, Encoding.UTF8.GetByteCount(value));
    Encoding.UTF8.GetBytes(value.AsSpan(), writer);
  }

  /// <summary>
  ///   Writes a field length using NiFi's variable-length encoding (2 or 6 bytes).
  /// </summary>
  private static void WriteFieldLength(IBufferWriter<byte> writer,
                                       int length) {
    if (length < MAX_VALUE_2_BYTES) {
      // Small length: write as 2 bytes
      Span<byte> span = writer.GetSpan(2);
      BinaryPrimitives.WriteUInt16BigEndian(span, (ushort)length);
      writer.Advance(2);
    } else {
      // Large length: write 0xFFFF indicator followed by 4 bytes
      Span<byte> span = writer.GetSpan(6);
      BinaryPrimitives.WriteUInt16BigEndian(span, MAX_VALUE_2_BYTES);
      BinaryPrimitives.WriteInt32BigEndian(span.Slice(2), length);
      writer.Advance(6);
    }
  }

  /// <summary>
  ///   Writes a 64-bit integer in Big Endian format.
  /// </summary>
  private static void WriteInt64(IBufferWriter<byte> writer, long value) {
    Span<byte> span = writer.GetSpan(8);
    BinaryPrimitives.WriteInt64BigEndian(span, value);
    writer.Advance(8);
  }

  /// <summary>
  ///   Copies exactly <paramref name="length" /> bytes of content into the
  ///   writer, flushing per chunk so a Pipe applies back-pressure.
  /// </summary>
  private static async Task CopyContentAsync(Stream source, long length,
                                             PipeWriter writer,
                                             CancellationToken ct) {
    long remaining = length;
    while (remaining > 0) {
      Memory<byte> memory = writer.GetMemory(CONTENT_CHUNK_SIZE);
      if (memory.Length > remaining) {
        memory = memory.Slice(0, (int)remaining);
      }

      int read = await source.ReadAsync(memory, ct);
      if (read == 0) {
        throw new EndOfStreamException(
          $"FlowFile content ended {remaining} bytes before its declared " +
          "length; was the content stream modified while it was written?"
        );
      }

      writer.Advance(read);
      remaining -= read;
      await FlushOrThrowAsync(writer, ct);
    }
  }

  private static async ValueTask FlushOrThrowAsync(PipeWriter writer,
                                                   CancellationToken ct) {
    FlushResult result = await writer.FlushAsync(ct);
    if (result.IsCanceled) {
      throw new OperationCanceledException("The pipe flush was canceled.", ct);
    }

    if (result.IsCompleted) {
      throw new InvalidOperationException(
        "The pipe reader completed before the FlowFile was fully written."
      );
    }
  }

  /// <summary>
  ///   Reads a length-prefixed UTF-8 string from the PipeReader.
  /// </summary>
  private async ValueTask<string> ReadStringV3Async(PipeReader reader,
                                                    CancellationToken ct) {
    int length = await ReadFieldLengthAsync(reader, ct);
    if (length > options_.max_attribute_length) {
      throw new InvalidDataException(
        $"FlowFile attribute of {length} bytes exceeds the configured " +
        $"maximum of {options_.max_attribute_length}."
      );
    }

    if (length == 0) {
      return string.Empty;
    }

    ReadOnlySequence<byte> buffer =
      await ReadExactlyAsync(reader, length, "an attribute", ct);
    ReadOnlySequence<byte> value_bytes = buffer.Slice(0, length);
    string value = Encoding.UTF8.GetString(value_bytes);
    reader.AdvanceTo(value_bytes.End);
    return value;
  }

  /// <summary>
  ///   Reads a field length using NiFi's variable-length encoding.
  /// </summary>
  private static async ValueTask<int> ReadFieldLengthAsync(PipeReader reader,
                                                           CancellationToken
                                                             ct) {
    ReadOnlySequence<byte> buffer =
      await ReadExactlyAsync(reader, 2, "a field length", ct);
    int length = ReadUInt16BigEndian(buffer);
    reader.AdvanceTo(buffer.GetPosition(2));
    if (length < MAX_VALUE_2_BYTES) {
      return length;
    }

    // If length is 0xFFFF, the actual length follows in the next 4 bytes
    buffer = await ReadExactlyAsync(reader, 4, "a field length", ct);
    int full_length = ReadInt32BigEndian(buffer);
    reader.AdvanceTo(buffer.GetPosition(4));
    if (full_length < 0) {
      throw new InvalidDataException(
        $"Invalid FlowFile V3 field length {full_length}."
      );
    }

    return full_length;
  }

  /// <summary>
  ///   Reads a 64-bit integer from the PipeReader in Big Endian format.
  /// </summary>
  private static async ValueTask<long> ReadInt64Async(PipeReader reader,
                                                      CancellationToken ct) {
    ReadOnlySequence<byte> buffer =
      await ReadExactlyAsync(reader, 8, "the content length", ct);
    long value = ReadInt64BigEndian(buffer);
    reader.AdvanceTo(buffer.GetPosition(8));
    return value;
  }

  /// <summary>
  ///   Reads <paramref name="length" /> content bytes into a MemoryStream.
  /// </summary>
  private static async ValueTask<MemoryStream> ReadContentAsync(
    PipeReader reader, long length, CancellationToken ct) {
    MemoryStream ms = new((int)Math.Min(length, MAX_INITIAL_CONTENT_CAPACITY));
    long remaining = length;
    while (remaining > 0) {
      ReadResult result = await reader.ReadAsync(ct);
      ThrowIfCanceled(result, ct);
      ReadOnlySequence<byte> buffer = result.Buffer;
      long to_copy = Math.Min(buffer.Length, remaining);

      foreach (ReadOnlyMemory<byte> segment in buffer.Slice(0, to_copy)) {
        ms.Write(segment.Span);
      }

      reader.AdvanceTo(buffer.GetPosition(to_copy));
      remaining -= to_copy;

      // If the stream ended before we got all the content, the data is corrupted
      if (remaining > 0 && result.IsCompleted) {
        throw new EndOfStreamException(
          $"Unexpected end of FlowFile V3 stream: {remaining} content bytes " +
          "missing."
        );
      }
    }

    ms.Position = 0;
    return ms;
  }

  /// <summary>
  ///   Waits until at least <paramref name="count" /> bytes are buffered. The
  ///   caller must call AdvanceTo on the reader after using the buffer.
  /// </summary>
  private static async ValueTask<ReadOnlySequence<byte>> ReadExactlyAsync(
    PipeReader reader, int count, string what, CancellationToken ct) {
    ReadResult result = await reader.ReadAtLeastAsync(count, ct);
    ThrowIfCanceled(result, ct);
    if (result.Buffer.Length < count) {
      reader.AdvanceTo(result.Buffer.Start, result.Buffer.End);
      throw new EndOfStreamException(
        $"Unexpected end of FlowFile V3 stream while reading {what}."
      );
    }

    return result.Buffer;
  }

  private static void ThrowIfCanceled(ReadResult result,
                                      CancellationToken ct) {
    if (result.IsCanceled) {
      throw new OperationCanceledException("The pipe read was canceled.", ct);
    }
  }

  private static ushort ReadUInt16BigEndian(ReadOnlySequence<byte> buffer) {
    Span<byte> bytes = stackalloc byte[2];
    buffer.Slice(0, 2).CopyTo(bytes);
    return BinaryPrimitives.ReadUInt16BigEndian(bytes);
  }

  private static int ReadInt32BigEndian(ReadOnlySequence<byte> buffer) {
    Span<byte> bytes = stackalloc byte[4];
    buffer.Slice(0, 4).CopyTo(bytes);
    return BinaryPrimitives.ReadInt32BigEndian(bytes);
  }

  private static long ReadInt64BigEndian(ReadOnlySequence<byte> buffer) {
    Span<byte> bytes = stackalloc byte[8];
    buffer.Slice(0, 8).CopyTo(bytes);
    return BinaryPrimitives.ReadInt64BigEndian(bytes);
  }

  /// <summary>
  ///   Checks whether a ReadOnlySequence starts with the given bytes.
  /// </summary>
  private static bool StartsWith(ReadOnlySequence<byte> sequence,
                                 byte[] prefix) {
    Span<byte> head = stackalloc byte[prefix.Length];
    sequence.Slice(0, prefix.Length).CopyTo(head);
    return head.SequenceEqual(prefix);
  }

  #endregion

  #region V1 Attribute Helpers

  /// <summary>
  ///   Serializes attributes as a Java XML properties document, exactly like
  ///   NiFi's FlowFilePackagerV1 (read back with Properties.loadFromXML).
  /// </summary>
  internal static string SerializeAttributesV1(
    IDictionary<string, string> attributes) {
    StringBuilder sb = new();
    sb.Append(
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?><!DOCTYPE properties\n" +
      "  SYSTEM \"http://java.sun.com/dtd/properties.dtd\">\n"
    );
    sb.Append("<properties>");
    foreach (KeyValuePair<string, string> attr in attributes) {
      sb.Append("\n  <entry key=\"");
      AppendXmlEscaped(sb, attr.Key, true);
      sb.Append("\">");
      AppendXmlEscaped(sb, attr.Value ?? string.Empty, false);
      sb.Append("</entry>");
    }

    sb.Append("</properties>");
    return sb.ToString();
  }

  /// <summary>
  ///   XML-escapes a value. Line breaks and tabs inside the key attribute, and
  ///   carriage returns anywhere, are written as character references because
  ///   an XML parser would otherwise normalize them away.
  /// </summary>
  private static void AppendXmlEscaped(StringBuilder sb, string value,
                                       bool in_attribute) {
    for (int i = 0; i < value.Length; i++) {
      char c = value[i];
      switch (c) {
        case '&':
          sb.Append("&amp;");
          break;
        case '<':
          sb.Append("&lt;");
          break;
        case '>':
          sb.Append("&gt;");
          break;
        case '"':
          sb.Append("&quot;");
          break;
        case '\'':
          sb.Append("&apos;");
          break;
        case '\r':
          sb.Append("&#13;");
          break;
        case '\n' when in_attribute:
          sb.Append("&#10;");
          break;
        case '\t' when in_attribute:
          sb.Append("&#9;");
          break;
        default:
          if (char.IsHighSurrogate(c) && i + 1 < value.Length &&
              char.IsLowSurrogate(value[i + 1])) {
            sb.Append(c).Append(value[++i]);
          } else if (IsXml10Char(c)) {
            sb.Append(c);
          } else {
            throw new ArgumentException(
              $"FlowFile V1 attributes are stored as XML 1.0, which cannot " +
              $"represent the character U+{(int)c:X4}. Use FlowFile V3 instead."
            );
          }

          break;
      }
    }
  }

  private static bool IsXml10Char(char c) {
    return c is '\t' or '\n' or '\r' or
             (>= '\u0020' and <= '\uD7FF') or
             (>= '\uE000' and <= '\uFFFD');
  }

  #endregion
}
