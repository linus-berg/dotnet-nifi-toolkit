using System.Buffers;
using System.Buffers.Binary;
using System.Formats.Tar;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Text;
using Microsoft.Extensions.Logging;
using NifiKit.Models;

namespace NifiKit.Services {
  public class FlowFileService : IFlowFileService {
    private static readonly byte[] S_MAGIC_HEADER_V3_ =
      Encoding.ASCII.GetBytes("NiFiFF3");

    private readonly ILogger<FlowFileService>? logger_;

    public FlowFileService(ILogger<FlowFileService>? logger = null) {
      logger_ = logger;
    }

    #region Package Helpers

    public NifiPackage CreatePackage(IDictionary<string, string> attributes,
                                     byte[] content) {
      return new NifiPackage()
             .AddAttributes(attributes)
             .SetContent(content);
    }

    #endregion

    #region FlowFile V3 (Binary)

    public async Task<byte[]> CreateFlowFileV3Async(NifiPackage package) {
      using MemoryStream ms = new MemoryStream();
      await WriteFlowFileV3Async(package, ms);
      return ms.ToArray();
    }

    public async Task WriteFlowFileV3Async(NifiPackage package,
                                           Stream output_stream) {
      PipeWriter writer = PipeWriter.Create(output_stream);
      await WriteFlowFileV3Async(package, writer);
      await writer.FlushAsync();
    }

    public async Task WriteFlowFileV3Async(NifiPackage package,
                                           PipeWriter writer,
                                           CancellationToken ct = default) {
      logger_?.LogDebug(
        "Packing FlowFile V3 with {AttributeCount} attributes",
        package.attributes.Count
      );

      // 1. Magic Header
      Memory<byte> header_memory = writer.GetMemory(S_MAGIC_HEADER_V3_.Length);
      S_MAGIC_HEADER_V3_.CopyTo(header_memory);
      writer.Advance(S_MAGIC_HEADER_V3_.Length);

      // 2. Attributes Count
      WriteFieldLength(writer, package.attributes.Count);

      // 3. Attributes
      foreach (KeyValuePair<string, string> attribute in package.attributes) {
        WriteStringV3(writer, attribute.Key);
        WriteStringV3(writer, attribute.Value ?? string.Empty);
      }

      // 4. Content
      if (package.content.CanSeek) {
        long length = package.content.Length;
        WriteInt64(writer, length);
        await writer.FlushAsync(ct);
        await package.content.CopyToAsync(writer.AsStream(), ct);
      } else {
        // Must buffer to get length if stream is not seekable
        using MemoryStream buffer = new MemoryStream();
        await package.content.CopyToAsync(buffer, ct);
        WriteInt64(writer, buffer.Length);
        await writer.FlushAsync(ct);
        buffer.Position = 0;
        await buffer.CopyToAsync(writer.AsStream(), ct);
      }

      await writer.FlushAsync(ct);
    }

    public async Task WriteFlowFilesV3Async(IEnumerable<NifiPackage> packages,
                                            Stream output_stream) {
      PipeWriter writer = PipeWriter.Create(output_stream);
      foreach (NifiPackage package in packages) {
        await WriteFlowFileV3Async(package, writer);
      }

      await writer.FlushAsync();
    }

    public async IAsyncEnumerable<NifiPackage> UnpackFlowFilesV3Async(
      Stream input_stream,
      [EnumeratorCancellation] CancellationToken ct = default) {
      PipeReader reader = PipeReader.Create(input_stream);
      await foreach (NifiPackage package in
                     UnpackFlowFilesV3Async(reader, ct)) {
        yield return package;
      }
    }

    public async IAsyncEnumerable<NifiPackage> UnpackFlowFilesV3Async(
      PipeReader reader,
      [EnumeratorCancellation] CancellationToken ct = default) {
      while (!ct.IsCancellationRequested) {
        ReadResult result = await reader.ReadAtLeastAsync(
                              S_MAGIC_HEADER_V3_.Length,
                              ct
                            );
        ReadOnlySequence<byte> buffer = result.Buffer;

        if (buffer.IsEmpty && result.IsCompleted)
          yield break;

        if (buffer.Length < S_MAGIC_HEADER_V3_.Length) {
          if (result.IsCompleted)
            throw new EndOfStreamException();
          reader.AdvanceTo(buffer.Start, buffer.End);
          continue;
        }

        // Verify Magic Header
        ReadOnlySequence<byte> header_buffer =
          buffer.Slice(0, S_MAGIC_HEADER_V3_.Length);
        if (!SequenceEqual(header_buffer, S_MAGIC_HEADER_V3_)) {
          logger_?.LogError("Invalid NiFi FlowFile V3 Magic Header");
          throw new InvalidDataException(
            "Invalid NiFi FlowFile V3 Magic Header"
          );
        }

        reader.AdvanceTo(buffer.GetPosition(S_MAGIC_HEADER_V3_.Length));

        NifiPackage package = new NifiPackage();

        // Read Attribute Count
        int attr_count = await ReadFieldLengthAsync(reader, ct);

        for (int i = 0; i < attr_count; i++) {
          string key = await ReadStringV3Async(reader, ct);
          string value = await ReadStringV3Async(reader, ct);
          package.AddAttribute(key, value);
        }

        // Read Content Length
        long content_length = await ReadInt64Async(reader, ct);

        // Read Content
        MemoryStream ms = new MemoryStream();
        long remaining = content_length;
        while (remaining > 0) {
          ReadResult read_result = await reader.ReadAsync(ct);
          ReadOnlySequence<byte> read_buffer = read_result.Buffer;
          long to_copy = Math.Min(read_buffer.Length, remaining);

          ReadOnlySequence<byte> slice = read_buffer.Slice(0, to_copy);
          foreach (ReadOnlyMemory<byte> segment in slice) {
            await ms.WriteAsync(segment, ct);
          }

          SequencePosition consumed = read_buffer.GetPosition(to_copy);
          reader.AdvanceTo(consumed);
          remaining -= to_copy;

          if (read_result.IsCompleted && remaining > 0)
            throw new EndOfStreamException();
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

    public async Task<byte[]> CreateFlowFileV1Async(NifiPackage package) {
      using MemoryStream ms = new MemoryStream();
      await WriteFlowFileV1Async(package, ms);
      return ms.ToArray();
    }

    public async Task WriteFlowFileV1Async(NifiPackage package,
                                           Stream output_stream) {
      logger_?.LogDebug("Packing FlowFile V1 (Tar)");
      await using TarWriter writer = new TarWriter(
        output_stream,
        TarEntryFormat.Ustar,
        leaveOpen: true
      );

      string attr_content = SerializeAttributesV1(package.attributes);
      byte[] attr_bytes = Encoding.UTF8.GetBytes(attr_content);
      UstarTarEntry attr_entry = new UstarTarEntry(
        TarEntryType.RegularFile,
        "flowfile.attributes"
      ) {
        DataStream = new MemoryStream(attr_bytes)
      };
      await writer.WriteEntryAsync(attr_entry);

      UstarTarEntry content_entry =
        new UstarTarEntry(TarEntryType.RegularFile, "flowfile.content") {
          DataStream = package.content
        };
      await writer.WriteEntryAsync(content_entry);
    }

    #endregion

    #region Binary Helpers (Pipelines)

    private void WriteStringV3(PipeWriter writer, string value) {
      byte[] bytes = Encoding.UTF8.GetBytes(value);
      WriteFieldLength(writer, bytes.Length);
      writer.Write(bytes);
    }

    private async Task<string> ReadStringV3Async(
      PipeReader reader, CancellationToken ct) {
      int length = await ReadFieldLengthAsync(reader, ct);
      ReadResult result = await reader.ReadAtLeastAsync(length, ct);
      ReadOnlySequence<byte> buffer = result.Buffer.Slice(0, length);
      string value = Encoding.UTF8.GetString(buffer);
      reader.AdvanceTo(buffer.End);
      return value;
    }

    private void WriteFieldLength(PipeWriter writer, int length) {
      if (length < 0xFFFF) {
        Span<byte> span = writer.GetSpan(2);
        BinaryPrimitives.WriteUInt16BigEndian(span, (ushort)length);
        writer.Advance(2);
      } else {
        Span<byte> span = writer.GetSpan(6);
        BinaryPrimitives.WriteUInt16BigEndian(span, 0xFFFF);
        BinaryPrimitives.WriteInt32BigEndian(span.Slice(2), length);
        writer.Advance(6);
      }
    }

    private async Task<int> ReadFieldLengthAsync(
      PipeReader reader, CancellationToken ct) {
      ReadResult result = await reader.ReadAtLeastAsync(2, ct);
      ReadOnlySequence<byte> buffer = result.Buffer;
      ushort length =
        BinaryPrimitives.ReadUInt16BigEndian(buffer.Slice(0, 2).ToArray());

      if (length < 0xFFFF) {
        reader.AdvanceTo(buffer.GetPosition(2));
        return length;
      }

      reader.AdvanceTo(buffer.GetPosition(2));
      result = await reader.ReadAtLeastAsync(4, ct);
      buffer = result.Buffer;
      int full_length =
        BinaryPrimitives.ReadInt32BigEndian(buffer.Slice(0, 4).ToArray());
      reader.AdvanceTo(buffer.GetPosition(4));
      return full_length;
    }

    private void WriteInt64(PipeWriter writer, long value) {
      Span<byte> span = writer.GetSpan(8);
      BinaryPrimitives.WriteInt64BigEndian(span, value);
      writer.Advance(8);
    }

    private async Task<long> ReadInt64Async(PipeReader reader,
                                            CancellationToken ct) {
      ReadResult result = await reader.ReadAtLeastAsync(8, ct);
      ReadOnlySequence<byte> buffer = result.Buffer.Slice(0, 8);
      long value = BinaryPrimitives.ReadInt64BigEndian(buffer.ToArray());
      reader.AdvanceTo(buffer.End);
      return value;
    }

    private bool SequenceEqual(ReadOnlySequence<byte> sequence, byte[] match) {
      if (sequence.Length != match.Length)
        return false;
      int index = 0;
      foreach (ReadOnlyMemory<byte> segment in sequence) {
        for (int i = 0; i < segment.Length; i++) {
          if (segment.Span[i] != match[index++])
            return false;
        }
      }

      return true;
    }

    #endregion

    #region Other Helpers

    private string
      SerializeAttributesV1(IDictionary<string, string> attributes) {
      StringBuilder sb = new StringBuilder();
      foreach (KeyValuePair<string, string> attr in attributes) {
        sb.AppendLine(
          $"{EscapeProperty(attr.Key)}={EscapeProperty(attr.Value ?? string.Empty)}"
        );
      }

      return sb.ToString();
    }

    private string EscapeProperty(string value) =>
      value.Replace("\\", "\\\\")
           .Replace("=", "\\=")
           .Replace(":", "\\:")
           .Replace("\n", "\\n")
           .Replace("\r", "\\r")
           .Replace("\t", "\\t");

    #endregion
  }
}