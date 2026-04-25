using System.Buffers.Binary;
using System.Formats.Tar;
using System.Runtime.CompilerServices;
using System.Text;
using Nifi.Utils.Models;

namespace Nifi.Utils.Services {
  public class FlowFileService : IFlowFileService {
    private static readonly byte[] S_MAGIC_HEADER_V3_ =
      Encoding.ASCII.GetBytes("NiFiFF3");

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
      await output_stream.WriteAsync(
        S_MAGIC_HEADER_V3_,
        0,
        S_MAGIC_HEADER_V3_.Length
      );
      await WriteFieldLengthAsync(output_stream, package.attributes.Count);

      foreach (KeyValuePair<string, string> attribute in package.attributes) {
        await WriteStringV3Async(output_stream, attribute.Key);
        await WriteStringV3Async(
          output_stream,
          attribute.Value ?? string.Empty
        );
      }

      long content_length = 0;
      if (package.content.CanSeek) {
        content_length = package.content.Length;
        await WriteInt64Async(output_stream, content_length);
        await package.content.CopyToAsync(output_stream);
      } else {
        using MemoryStream buffer = new MemoryStream();
        await package.content.CopyToAsync(buffer);
        content_length = buffer.Length;
        await WriteInt64Async(output_stream, content_length);
        buffer.Position = 0;
        await buffer.CopyToAsync(output_stream);
      }
    }

    public async Task WriteFlowFilesV3Async(IEnumerable<NifiPackage> packages,
                                            Stream output_stream) {
      foreach (NifiPackage package in packages) {
        await WriteFlowFileV3Async(package, output_stream);
      }
    }

    public async IAsyncEnumerable<NifiPackage> UnpackFlowFilesV3Async(
      Stream input_stream,
      [EnumeratorCancellation] CancellationToken ct = default) {
      while (!ct.IsCancellationRequested) {
        byte[] header = new byte[S_MAGIC_HEADER_V3_.Length];
        int read = await input_stream.ReadAsync(header, 0, header.Length, ct);
        if (read == 0)
          yield break;
        if (read < header.Length || !header.SequenceEqual(S_MAGIC_HEADER_V3_))
          throw new InvalidDataException(
            "Invalid NiFi FlowFile V3 Magic Header"
          );

        NifiPackage package = new NifiPackage();
        int attr_count = await ReadFieldLengthAsync(input_stream, ct);

        for (int i = 0; i < attr_count; i++) {
          string key = await ReadStringV3Async(input_stream, ct);
          string value = await ReadStringV3Async(input_stream, ct);
          package.attributes[key] = value;
        }

        long content_length = await ReadInt64Async(input_stream, ct);
        MemoryStream ms = new MemoryStream();
        byte[] buffer = new byte[8192];
        long remaining = content_length;
        while (remaining > 0) {
          int to_read = (int)Math.Min(buffer.Length, remaining);
          int bytes_read = await input_stream.ReadAsync(buffer, 0, to_read, ct);
          if (bytes_read == 0)
            throw new EndOfStreamException();
          await ms.WriteAsync(buffer, 0, bytes_read, ct);
          remaining -= bytes_read;
        }

        ms.Position = 0;
        package.content = ms;

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
      using TarWriter writer = new TarWriter(
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

    #region Helpers

    private async Task WriteStringV3Async(Stream stream, string value) {
      byte[] bytes = Encoding.UTF8.GetBytes(value);
      await WriteFieldLengthAsync(stream, bytes.Length);
      await stream.WriteAsync(bytes, 0, bytes.Length);
    }

    private async Task<string> ReadStringV3Async(
      Stream stream, CancellationToken ct) {
      int length = await ReadFieldLengthAsync(stream, ct);
      byte[] bytes = new byte[length];
      int read = 0;
      while (read < length) {
        int r = await stream.ReadAsync(bytes, read, length - read, ct);
        if (r == 0)
          throw new EndOfStreamException();
        read += r;
      }

      return Encoding.UTF8.GetString(bytes);
    }

    private async Task WriteFieldLengthAsync(Stream stream, int length) {
      if (length < 0xFFFF) {
        byte[] bytes = new byte[2];
        BinaryPrimitives.WriteUInt16BigEndian(bytes, (ushort)length);
        await stream.WriteAsync(bytes, 0, bytes.Length);
      } else {
        byte[] bytes = new byte[6];
        BinaryPrimitives.WriteUInt16BigEndian(bytes, 0xFFFF);
        BinaryPrimitives.WriteInt32BigEndian(bytes.AsSpan(2), length);
        await stream.WriteAsync(bytes, 0, bytes.Length);
      }
    }

    private async Task<int> ReadFieldLengthAsync(
      Stream stream, CancellationToken ct) {
      byte[] bytes = new byte[2];
      if (await stream.ReadAsync(bytes, 0, 2, ct) < 2)
        throw new EndOfStreamException();
      ushort length = BinaryPrimitives.ReadUInt16BigEndian(bytes);
      if (length < 0xFFFF)
        return length;
      byte[] full_bytes = new byte[4];
      if (await stream.ReadAsync(full_bytes, 0, 4, ct) < 4)
        throw new EndOfStreamException();
      return BinaryPrimitives.ReadInt32BigEndian(full_bytes);
    }

    private async Task WriteInt64Async(Stream stream, long value) {
      byte[] bytes = new byte[8];
      BinaryPrimitives.WriteInt64BigEndian(bytes, value);
      await stream.WriteAsync(bytes, 0, bytes.Length);
    }

    private async Task<long>
      ReadInt64Async(Stream stream, CancellationToken ct) {
      byte[] bytes = new byte[8];
      if (await stream.ReadAsync(bytes, 0, 8, ct) < 8)
        throw new EndOfStreamException();
      return BinaryPrimitives.ReadInt64BigEndian(bytes);
    }

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