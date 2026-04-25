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
public class FlowFileService : IFlowFileService {
  // Magic header for NiFi FlowFile V3
  private static readonly byte[] S_MAGIC_HEADER_V3_ =
    Encoding.ASCII.GetBytes("NiFiFF3");

  private readonly ILogger<FlowFileService>? logger_;

  public FlowFileService(ILogger<FlowFileService>? logger = null) {
    logger_ = logger;
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
  public async Task<byte[]> CreateFlowFileV3Async(NifiPackage package) {
    using MemoryStream ms = new();
    await WriteFlowFileV3Async(package, ms);
    return ms.ToArray();
  }

  /// <inheritdoc />
  public async Task WriteFlowFileV3Async(NifiPackage package,
                                         Stream output_stream) {
    // Create a PipeWriter to wrap the output stream for efficient writing
    PipeWriter writer = PipeWriter.Create(output_stream);
    await WriteFlowFileV3Async(package, writer);
    await writer.FlushAsync();
  }

  /// <inheritdoc />
  public async Task WriteFlowFileV3Async(NifiPackage package,
                                         PipeWriter writer,
                                         CancellationToken ct = default) {
    logger_?.LogDebug(
      "Packing FlowFile V3 with {AttributeCount} attributes",
      package.attributes.Count
    );

    // 1. Write the Magic Header (NiFiFF3)
    // Get memory from the PipeWriter to avoid extra allocations
    Memory<byte> header_memory = writer.GetMemory(S_MAGIC_HEADER_V3_.Length);
    S_MAGIC_HEADER_V3_.CopyTo(header_memory);
    writer.Advance(S_MAGIC_HEADER_V3_.Length);

    // 2. Write the count of attributes
    // This is encoded as either a 2-byte or 6-byte field
    WriteFieldLength(writer, package.attributes.Count);

    // 3. Write each attribute key and value
    foreach (KeyValuePair<string, string> attribute in package.attributes) {
      // Each string is written as a length-prefixed UTF-8 sequence
      WriteStringV3(writer, attribute.Key);
      WriteStringV3(writer, attribute.Value ?? string.Empty);
    }

    // 4. Write the content length and data
    if (package.content.CanSeek) {
      // If the stream is seekable, we know the length upfront
      long length = package.content.Length;
      WriteInt64(writer, length);
      await writer.FlushAsync(ct);
      // Stream the content directly to the writer's underlying stream
      await package.content.CopyToAsync(writer.AsStream(), ct);
    } else {
      // If the stream isn't seekable (like a NetworkStream), we must buffer
      // it to determine the total length required by the V3 format
      using MemoryStream buffer = new();
      await package.content.CopyToAsync(buffer, ct);
      WriteInt64(writer, buffer.Length);
      await writer.FlushAsync(ct);
      buffer.Position = 0;
      await buffer.CopyToAsync(writer.AsStream(), ct);
    }

    // Ensure all data is flushed to the underlying transport
    await writer.FlushAsync(ct);
  }

  /// <inheritdoc />
  public async Task WriteFlowFilesV3Async(IEnumerable<NifiPackage> packages,
                                          Stream output_stream) {
    PipeWriter writer = PipeWriter.Create(output_stream);
    foreach (NifiPackage package in packages) {
      await WriteFlowFileV3Async(package, writer);
    }

    await writer.FlushAsync();
  }

  /// <inheritdoc />
  public async IAsyncEnumerable<NifiPackage> UnpackFlowFilesV3Async(
    Stream input_stream,
    [EnumeratorCancellation] CancellationToken ct = default) {
    // Wrap the input stream in a PipeReader for efficient asynchronous reading
    PipeReader reader = PipeReader.Create(input_stream);
    await foreach (NifiPackage package in
                   UnpackFlowFilesV3Async(reader, ct)) {
      yield return package;
    }
  }

  /// <inheritdoc />
  public async IAsyncEnumerable<NifiPackage> UnpackFlowFilesV3Async(
    PipeReader reader,
    [EnumeratorCancellation] CancellationToken ct = default) {
    while (!ct.IsCancellationRequested) {
      // Try to read the header to determine if a new FlowFile is starting
      ReadResult result = await reader.ReadAtLeastAsync(
                            S_MAGIC_HEADER_V3_.Length,
                            ct
                          );
      ReadOnlySequence<byte> buffer = result.Buffer;

      // If the buffer is empty and the reader is done, we've reached the end of the stream
      if (buffer.IsEmpty && result.IsCompleted) {
        yield break;
      }

      // Ensure we have enough data for the magic header
      if (buffer.Length < S_MAGIC_HEADER_V3_.Length) {
        if (result.IsCompleted) {
          throw new EndOfStreamException();
        }

        reader.AdvanceTo(buffer.Start, buffer.End);
        continue;
      }

      // Verify the Magic Header (NiFiFF3)
      ReadOnlySequence<byte> header_buffer =
        buffer.Slice(0, S_MAGIC_HEADER_V3_.Length);
      if (!SequenceEqual(header_buffer, S_MAGIC_HEADER_V3_)) {
        logger_?.LogError("Invalid NiFi FlowFile V3 Magic Header");
        throw new InvalidDataException("Invalid NiFi FlowFile V3 Magic Header");
      }

      // Consume the header bytes
      reader.AdvanceTo(buffer.GetPosition(S_MAGIC_HEADER_V3_.Length));

      NifiPackage package = new();

      // 1. Read the number of attributes
      int attr_count = await ReadFieldLengthAsync(reader, ct);

      // 2. Read each attribute key-value pair
      for (int i = 0; i < attr_count; i++) {
        string key = await ReadStringV3Async(reader, ct);
        string value = await ReadStringV3Async(reader, ct);
        package.AddAttribute(key, value);
      }

      // 3. Read the content length
      long content_length = await ReadInt64Async(reader, ct);

      // 4. Read the raw content bytes
      MemoryStream ms = new();
      long remaining = content_length;
      while (remaining > 0) {
        ReadResult read_result = await reader.ReadAsync(ct);
        ReadOnlySequence<byte> read_buffer = read_result.Buffer;
        long to_copy = Math.Min(read_buffer.Length, remaining);

        // Copy segments of the buffer to the package content stream
        ReadOnlySequence<byte> slice = read_buffer.Slice(0, to_copy);
        foreach (ReadOnlyMemory<byte> segment in slice) {
          await ms.WriteAsync(segment, ct);
        }

        // Advance the reader by the amount of data processed
        SequencePosition consumed = read_buffer.GetPosition(to_copy);
        reader.AdvanceTo(consumed);
        remaining -= to_copy;

        // If the stream ended before we got all the content, the data is corrupted
        if (read_result.IsCompleted && remaining > 0) {
          throw new EndOfStreamException();
        }
      }

      ms.Position = 0;
      package.content = ms;

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
  public async Task<byte[]> CreateFlowFileV1Async(NifiPackage package) {
    using MemoryStream ms = new();
    await WriteFlowFileV1Async(package, ms);
    return ms.ToArray();
  }

  /// <inheritdoc />
  public async Task WriteFlowFileV1Async(NifiPackage package,
                                         Stream output_stream) {
    logger_?.LogDebug("Packing FlowFile V1 (Tar)");

    // FlowFile V1 is a Tar archive containing 'flowfile.attributes' and 'flowfile.content'
    await using TarWriter writer = new(
      output_stream,
      TarEntryFormat.Ustar,
      true
    );

    // 1. Serialize and write attributes file
    string attr_content = SerializeAttributesV1(package.attributes);
    byte[] attr_bytes = Encoding.UTF8.GetBytes(attr_content);
    UstarTarEntry attr_entry = new(
      TarEntryType.RegularFile,
      "flowfile.attributes"
    ) {
      DataStream = new MemoryStream(attr_bytes)
    };
    await writer.WriteEntryAsync(attr_entry);

    // 2. Write content file
    UstarTarEntry content_entry =
      new(TarEntryType.RegularFile, "flowfile.content") {
        DataStream = package.content
      };
    await writer.WriteEntryAsync(content_entry);
  }

  #endregion

  #region Binary Helpers (Pipelines)

  /// <summary>
  ///   Writes a length-prefixed UTF-8 string to the PipeWriter.
  /// </summary>
  private void WriteStringV3(PipeWriter writer, string value) {
    byte[] bytes = Encoding.UTF8.GetBytes(value);
    WriteFieldLength(writer, bytes.Length);
    writer.Write(bytes);
  }

  /// <summary>
  ///   Reads a length-prefixed UTF-8 string from the PipeReader.
  /// </summary>
  private async Task<string> ReadStringV3Async(PipeReader reader,
                                               CancellationToken ct) {
    int length = await ReadFieldLengthAsync(reader, ct);
    ReadResult result = await reader.ReadAtLeastAsync(length, ct);
    ReadOnlySequence<byte> buffer = result.Buffer.Slice(0, length);
    string value = Encoding.UTF8.GetString(buffer);
    reader.AdvanceTo(buffer.End);
    return value;
  }

  /// <summary>
  ///   Writes a field length to the PipeWriter using NiFi's variable-length encoding (2 or 6 bytes).
  /// </summary>
  private void WriteFieldLength(PipeWriter writer, int length) {
    if (length < 0xFFFF) {
      // Small length: write as 2 bytes
      Span<byte> span = writer.GetSpan(2);
      BinaryPrimitives.WriteUInt16BigEndian(span, (ushort)length);
      writer.Advance(2);
    } else {
      // Large length: write 0xFFFF indicator followed by 4 bytes
      Span<byte> span = writer.GetSpan(6);
      BinaryPrimitives.WriteUInt16BigEndian(span, 0xFFFF);
      BinaryPrimitives.WriteInt32BigEndian(span.Slice(2), length);
      writer.Advance(6);
    }
  }

  /// <summary>
  ///   Reads a field length from the PipeReader using NiFi's variable-length encoding.
  /// </summary>
  private async Task<int> ReadFieldLengthAsync(PipeReader reader,
                                               CancellationToken ct) {
    ReadResult result = await reader.ReadAtLeastAsync(2, ct);
    ReadOnlySequence<byte> buffer = result.Buffer;
    ushort length =
      BinaryPrimitives.ReadUInt16BigEndian(buffer.Slice(0, 2).ToArray());

    if (length < 0xFFFF) {
      reader.AdvanceTo(buffer.GetPosition(2));
      return length;
    }

    // If length is 0xFFFF, the actual length follows in the next 4 bytes
    reader.AdvanceTo(buffer.GetPosition(2));
    result = await reader.ReadAtLeastAsync(4, ct);
    buffer = result.Buffer;
    int full_length =
      BinaryPrimitives.ReadInt32BigEndian(buffer.Slice(0, 4).ToArray());
    reader.AdvanceTo(buffer.GetPosition(4));
    return full_length;
  }

  /// <summary>
  ///   Writes a 64-bit integer to the PipeWriter in Big Endian format.
  /// </summary>
  private void WriteInt64(PipeWriter writer, long value) {
    Span<byte> span = writer.GetSpan(8);
    BinaryPrimitives.WriteInt64BigEndian(span, value);
    writer.Advance(8);
  }

  /// <summary>
  ///   Reads a 64-bit integer from the PipeReader in Big Endian format.
  /// </summary>
  private async Task<long> ReadInt64Async(PipeReader reader,
                                          CancellationToken ct) {
    ReadResult result = await reader.ReadAtLeastAsync(8, ct);
    ReadOnlySequence<byte> buffer = result.Buffer.Slice(0, 8);
    long value = BinaryPrimitives.ReadInt64BigEndian(buffer.ToArray());
    reader.AdvanceTo(buffer.End);
    return value;
  }

  /// <summary>
  ///   Helper to compare a ReadOnlySequence with a byte array.
  /// </summary>
  private bool SequenceEqual(ReadOnlySequence<byte> sequence, byte[] match) {
    if (sequence.Length != match.Length) {
      return false;
    }

    int index = 0;
    foreach (ReadOnlyMemory<byte> segment in sequence) {
      for (int i = 0; i < segment.Length; i++) {
        if (segment.Span[i] != match[index++]) {
          return false;
        }
      }
    }

    return true;
  }

  #endregion

  #region Other Helpers

  /// <summary>
  ///   Serializes attributes to the NiFi V1 property format (key=value\n).
  /// </summary>
  private string
    SerializeAttributesV1(IDictionary<string, string> attributes) {
    StringBuilder sb = new();
    foreach (KeyValuePair<string, string> attr in attributes) {
      sb.AppendLine(
        $"{EscapeProperty(attr.Key)}={EscapeProperty(attr.Value ?? string.Empty)}"
      );
    }

    return sb.ToString();
  }

  /// <summary>
  ///   Escapes special characters for the NiFi V1 properties format.
  /// </summary>
  private string EscapeProperty(string value) {
    return value.Replace("\\", "\\\\")
                .Replace("=", "\\=")
                .Replace(":", "\\:")
                .Replace("\n", "\\n")
                .Replace("\r", "\\r")
                .Replace("\t", "\\t");
  }

  #endregion
}