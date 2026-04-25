using System.Buffers.Binary;
using System.Formats.Tar;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Runtime.CompilerServices;
using System.Text;
using Nifi.Utils.Models;

namespace Nifi.Utils
{
    public class NifiService : INifiService
    {
        private static readonly byte[] S_MAGIC_HEADER_V3_ = Encoding.ASCII.GetBytes("NiFiFF3");
        private readonly HttpClient http_client_;

        public NifiService(HttpClient http_client)
        {
            http_client_ = http_client;
        }

        #region FlowFile V3 (Binary)

        public async Task<byte[]> CreateFlowFileV3Async(IDictionary<string, string> attributes, byte[] content)
        {
            using MemoryStream ms = new MemoryStream();
            using MemoryStream content_stream = new MemoryStream(content);
            await WriteFlowFileV3Async(attributes, content_stream, ms);
            return ms.ToArray();
        }

        public async Task WriteFlowFileV3Async(IDictionary<string, string> attributes, Stream content, Stream output_stream)
        {
            await output_stream.WriteAsync(S_MAGIC_HEADER_V3_, 0, S_MAGIC_HEADER_V3_.Length);
            await WriteFieldLengthAsync(output_stream, attributes.Count);

            foreach (KeyValuePair<string, string> attribute in attributes)
            {
                await WriteStringV3Async(output_stream, attribute.Key);
                await WriteStringV3Async(output_stream, attribute.Value ?? string.Empty);
            }

            long content_length = 0;
            if (content.CanSeek)
            {
                content_length = content.Length;
                await WriteInt64Async(output_stream, content_length);
                await content.CopyToAsync(output_stream);
            }
            else
            {
                using MemoryStream buffer = new MemoryStream();
                await content.CopyToAsync(buffer);
                content_length = buffer.Length;
                await WriteInt64Async(output_stream, content_length);
                buffer.Position = 0;
                await buffer.CopyToAsync(output_stream);
            }
        }

        public async Task WriteFlowFilesV3Async(IEnumerable<NifiPackage> packages, Stream output_stream)
        {
            foreach (NifiPackage package in packages)
            {
                await WriteFlowFileV3Async(package.attributes, package.content, output_stream);
            }
        }

        public async IAsyncEnumerable<NifiPackage> UnpackFlowFilesV3Async(Stream input_stream, [EnumeratorCancellation] CancellationToken ct = default)
        {
            while (!ct.IsCancellationRequested)
            {
                byte[] header = new byte[S_MAGIC_HEADER_V3_.Length];
                int read = await input_stream.ReadAsync(header, 0, header.Length, ct);
                if (read == 0) yield break;
                if (read < header.Length || !header.SequenceEqual(S_MAGIC_HEADER_V3_))
                    throw new InvalidDataException("Invalid NiFi FlowFile V3 Magic Header");

                NifiPackage package = new NifiPackage();
                int attr_count = await ReadFieldLengthAsync(input_stream, ct);

                for (int i = 0; i < attr_count; i++)
                {
                    string key = await ReadStringV3Async(input_stream, ct);
                    string value = await ReadStringV3Async(input_stream, ct);
                    package.attributes[key] = value;
                }

                long content_length = await ReadInt64Async(input_stream, ct);
                MemoryStream ms = new MemoryStream();
                byte[] buffer = new byte[8192];
                long remaining = content_length;
                while (remaining > 0)
                {
                    int to_read = (int)Math.Min(buffer.Length, remaining);
                    int bytes_read = await input_stream.ReadAsync(buffer, 0, to_read, ct);
                    if (bytes_read == 0) throw new EndOfStreamException();
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

        public async Task<byte[]> CreateFlowFileV1Async(IDictionary<string, string> attributes, byte[] content)
        {
            using MemoryStream ms = new MemoryStream();
            using MemoryStream content_stream = new MemoryStream(content);
            await WriteFlowFileV1Async(attributes, content_stream, ms);
            return ms.ToArray();
        }

        public async Task WriteFlowFileV1Async(IDictionary<string, string> attributes, Stream content, Stream output_stream)
        {
            using TarWriter writer = new TarWriter(output_stream, TarEntryFormat.Ustar, leaveOpen: true);

            string attr_content = SerializeAttributesV1(attributes);
            byte[] attr_bytes = Encoding.UTF8.GetBytes(attr_content);
            UstarTarEntry attr_entry = new UstarTarEntry(TarEntryType.RegularFile, "flowfile.attributes")
            {
                DataStream = new MemoryStream(attr_bytes)
            };
            await writer.WriteEntryAsync(attr_entry);

            UstarTarEntry content_entry = new UstarTarEntry(TarEntryType.RegularFile, "flowfile.content")
            {
                DataStream = content
            };
            await writer.WriteEntryAsync(content_entry);
        }

        #endregion

        #region Transport

        public async Task<bool> PostToNiFiAsync(string url, Stream flow_file_stream, string content_type = "application/flowfile-v3")
        {
            StreamContent content = new StreamContent(flow_file_stream);
            content.Headers.ContentType = new MediaTypeHeaderValue(content_type);

            HttpResponseMessage response = await http_client_.PostAsync(url, content);
            return response.IsSuccessStatusCode;
        }

        public async Task<bool> SendViaS2SAsync(string nifi_base_url, string input_port_name, IEnumerable<NifiPackage> packages)
        {
            nifi_base_url = nifi_base_url.TrimEnd('/');

            // 1. Discover Port ID
            NifiSiteToSiteDto? s2_s_info = await http_client_.GetFromJsonAsync<NifiSiteToSiteDto>($"{nifi_base_url}/nifi-api/site-to-site");
            NifiPortDto? port = s2_s_info?.controller?.input_ports?.FirstOrDefault(p => p.name.Equals(input_port_name, StringComparison.OrdinalIgnoreCase));
            if (port == null || string.IsNullOrEmpty(port.id)) return false;

            // 2. Create Transaction
            HttpResponseMessage transaction_response = await http_client_.PostAsJsonAsync($"{nifi_base_url}/nifi-api/site-to-site/input-ports/{port.id}/transactions", new { });
            if (!transaction_response.IsSuccessStatusCode) return false;

            NifiTransactionDto? transaction = await transaction_response.Content.ReadFromJsonAsync<NifiTransactionDto>();
            if (transaction == null || string.IsNullOrEmpty(transaction.id)) return false;

            try
            {
                // 3. Send Data (FlowFile V3 Stream)
                using MemoryStream ms = new MemoryStream();
                await WriteFlowFilesV3Async(packages, ms);
                ms.Position = 0;

                StreamContent content = new StreamContent(ms);
                content.Headers.ContentType = new MediaTypeHeaderValue("application/flowfile-v3");
                
                // NiFi S2S Transaction flow-files endpoint
                string transfer_url = $"{nifi_base_url}/nifi-api/site-to-site/input-ports/{port.id}/transactions/{transaction.id}/flow-files";
                HttpResponseMessage transfer_response = await http_client_.PostAsync(transfer_url, content);
                if (!transfer_response.IsSuccessStatusCode) return false;

                // 4. Commit Transaction
                // First: Confirm (set state to TRANSACTION_CONFIRMED)
                string confirm_url = $"{nifi_base_url}/nifi-api/site-to-site/input-ports/{port.id}/transactions/{transaction.id}?state=TRANSACTION_CONFIRMED";
                HttpResponseMessage confirm_response = await http_client_.PutAsync(confirm_url, null);
                
                return confirm_response.IsSuccessStatusCode;
            }
            catch
            {
                // Optional: Try to cancel the transaction if possible
                return false;
            }
        }

        #endregion

        #region Helpers & Models

        private async Task WriteStringV3Async(Stream stream, string value)
        {
            byte[] bytes = Encoding.UTF8.GetBytes(value);
            await WriteFieldLengthAsync(stream, bytes.Length);
            await stream.WriteAsync(bytes, 0, bytes.Length);
        }

        private async Task<string> ReadStringV3Async(Stream stream, CancellationToken ct)
        {
            int length = await ReadFieldLengthAsync(stream, ct);
            byte[] bytes = new byte[length];
            int read = 0;
            while (read < length)
            {
                int r = await stream.ReadAsync(bytes, read, length - read, ct);
                if (r == 0) throw new EndOfStreamException();
                read += r;
            }
            return Encoding.UTF8.GetString(bytes);
        }

        private async Task WriteFieldLengthAsync(Stream stream, int length)
        {
            if (length < 0xFFFF)
            {
                byte[] bytes = new byte[2];
                BinaryPrimitives.WriteUInt16BigEndian(bytes, (ushort)length);
                await stream.WriteAsync(bytes, 0, bytes.Length);
            }
            else
            {
                byte[] bytes = new byte[6];
                BinaryPrimitives.WriteUInt16BigEndian(bytes, 0xFFFF);
                BinaryPrimitives.WriteInt32BigEndian(bytes.AsSpan(2), length);
                await stream.WriteAsync(bytes, 0, bytes.Length);
            }
        }

        private async Task<int> ReadFieldLengthAsync(Stream stream, CancellationToken ct)
        {
            byte[] bytes = new byte[2];
            if (await stream.ReadAsync(bytes, 0, 2, ct) < 2) throw new EndOfStreamException();
            ushort length = BinaryPrimitives.ReadUInt16BigEndian(bytes);
            if (length < 0xFFFF) return length;
            byte[] full_bytes = new byte[4];
            if (await stream.ReadAsync(full_bytes, 0, 4, ct) < 4) throw new EndOfStreamException();
            return BinaryPrimitives.ReadInt32BigEndian(full_bytes);
        }

        private async Task WriteInt64Async(Stream stream, long value)
        {
            byte[] bytes = new byte[8];
            BinaryPrimitives.WriteInt64BigEndian(bytes, value);
            await stream.WriteAsync(bytes, 0, bytes.Length);
        }

        private async Task<long> ReadInt64Async(Stream stream, CancellationToken ct)
        {
            byte[] bytes = new byte[8];
            if (await stream.ReadAsync(bytes, 0, 8, ct) < 8) throw new EndOfStreamException();
            return BinaryPrimitives.ReadInt64BigEndian(bytes);
        }

        private string SerializeAttributesV1(IDictionary<string, string> attributes)
        {
            StringBuilder sb = new StringBuilder();
            foreach (KeyValuePair<string, string> attr in attributes)
            {
                sb.AppendLine($"{EscapeProperty(attr.Key)}={EscapeProperty(attr.Value ?? string.Empty)}");
            }
            return sb.ToString();
        }

        private string EscapeProperty(string value) => value.Replace("\\", "\\\\").Replace("=", "\\=").Replace(":", "\\:").Replace("\n", "\\n").Replace("\r", "\\r").Replace("\t", "\\t");

        // DTOs for NiFi S2S API
        private class NifiSiteToSiteDto
        {
            public NifiControllerDto? controller { get; set; }
        }

        private class NifiControllerDto
        {
            public List<NifiPortDto>? input_ports { get; set; }
        }

        private class NifiPortDto
        {
            public string? id { get; set; }
            public string? name { get; set; }
        }

        private class NifiTransactionDto
        {
            public string? id { get; set; }
        }

        #endregion
    }
}
