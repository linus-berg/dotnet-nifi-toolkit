using System.Globalization;
using System.IO.Hashing;
using System.Net;
using System.Text;
using System.Text.Json;

namespace NifiKit.Tests;

/// <summary>
///   In-memory NiFi that enforces the rules of DataTransferResource and
///   SiteToSiteResource: paths, methods, the protocol version header, the
///   flow-files content type, CRC32 checksums and the commit response codes.
/// </summary>
internal sealed class FakeNifi : HttpMessageHandler {
  public const string PORT_ID = "11111111-2222-3333-4444-555555555555";
  public const string PORT_NAME = "From .NET";
  public const string API = "https://nifi.test:8443/nifi-api";

  private int transaction_counter_;

  public List<RecordedRequest> requests { get; } = new();
  public List<byte[]> flow_file_bodies { get; } = new();
  public List<int> finish_codes { get; } = new();

  public string port_state { get; set; } = "RUNNING";
  public List<(string id, string name)> ports { get; } = new() {
    (PORT_ID, PORT_NAME)
  };

  public HttpStatusCode? site_to_site_status { get; set; }
  public HttpStatusCode? create_transaction_status { get; set; }
  public int create_transaction_error_code { get; set; } = 250;
  public bool relative_location { get; set; }

  // Fail the first N flow-files requests with this status
  public int fail_flow_files_times { get; set; }
  public HttpStatusCode fail_flow_files_status { get; set; } =
    HttpStatusCode.ServiceUnavailable;

  public bool corrupt_checksum { get; set; }
  public bool destination_full { get; set; }
  public bool hang_on_flow_files { get; set; }

  public IEnumerable<RecordedRequest> Without(string path_fragment) {
    return requests.Where(r => !r.path.Contains(path_fragment));
  }

  protected override async Task<HttpResponseMessage> SendAsync(
    HttpRequestMessage request, CancellationToken ct) {
    Uri uri = request.RequestUri!;
    string path = uri.AbsolutePath;
    bool has_version = request.Headers.TryGetValues(
                         "x-nifi-site-to-site-protocol-version",
                         out IEnumerable<string>? versions
                       ) &&
                       versions.Single() == "1";
    byte[]? body = request.Content == null
                     ? null
                     : await request.Content.ReadAsByteArrayAsync(ct);
    RecordedRequest recorded = new(
      request.Method,
      uri.PathAndQuery,
      request.Content?.Headers.ContentType?.MediaType,
      has_version,
      body
    );
    requests.Add(recorded);

    if (path == "/nifi-api/site-to-site" && request.Method == HttpMethod.Get) {
      if (site_to_site_status is { } status) {
        return Text(status, "Unable to view Site-to-Site details.");
      }

      return Json(
        HttpStatusCode.OK,
        new {
          controller = new {
            id = "root",
            siteToSiteSecure = true,
            inputPorts = ports.Select(p => new {
                                 p.id,
                                 p.name,
                                 state = port_state
                               })
                              .ToArray()
          }
        }
      );
    }

    // Every data-transfer request requires the protocol version header
    if (path.StartsWith("/nifi-api/data-transfer/", StringComparison.Ordinal) &&
        !has_version) {
      return Json(
        HttpStatusCode.BadRequest,
        new { responseCode = 250, message = "Protocol version was not specified." }
      );
    }

    string transactions_prefix =
      $"/nifi-api/data-transfer/input-ports/{PORT_ID}/transactions";

    if (path == transactions_prefix && request.Method == HttpMethod.Post) {
      if (create_transaction_status is { } status) {
        return Json(
          status,
          new { responseCode = create_transaction_error_code, message = "Port is not running" }
        );
      }

      string id = $"tx-{Interlocked.Increment(ref transaction_counter_)}";
      HttpResponseMessage response = Json(
        HttpStatusCode.Created,
        new {
          flowFileSent = 0,
          responseCode = 1,
          message = "Handshake properties are valid, and port is running. " +
                    "A transaction is created:" + id
        }
      );
      response.Headers.Location = relative_location
                                    ? new Uri($"{transactions_prefix}/{id}",
                                              UriKind.Relative)
                                    : new Uri($"https://nifi.test:8443{transactions_prefix}/{id}");
      response.Headers.Add("x-location-uri-intent", "transaction-url");
      response.Headers.Add("x-nifi-site-to-site-protocol-version", "1");
      response.Headers.Add("x-nifi-site-to-site-server-transaction-ttl", "30");
      return response;
    }

    if (path.StartsWith(transactions_prefix + "/", StringComparison.Ordinal) &&
        path.EndsWith("/flow-files", StringComparison.Ordinal) &&
        request.Method == HttpMethod.Post) {
      if (recorded.content_type != "application/octet-stream") {
        return Text(HttpStatusCode.UnsupportedMediaType, "Unsupported Media Type");
      }

      if (hang_on_flow_files) {
        await Task.Delay(Timeout.Infinite, ct);
      }

      if (fail_flow_files_times > 0) {
        fail_flow_files_times--;
        return Json(
          fail_flow_files_status,
          new { responseCode = 250, message = "Server encountered an exception." }
        );
      }

      flow_file_bodies.Add(body ?? Array.Empty<byte>());
      uint crc = Crc32.HashToUInt32(body ?? Array.Empty<byte>());
      if (corrupt_checksum) {
        crc ^= 0xFFFF;
      }

      return new HttpResponseMessage(HttpStatusCode.Accepted) {
        Content = new StringContent(
          crc.ToString(CultureInfo.InvariantCulture),
          Encoding.UTF8,
          "text/plain"
        )
      };
    }

    if (path.StartsWith(transactions_prefix + "/", StringComparison.Ordinal) &&
        request.Method == HttpMethod.Delete) {
      string? code_text = uri.Query.TrimStart('?')
                             .Split('&')
                             .Select(p => p.Split('='))
                             .Where(p => p[0] == "responseCode")
                             .Select(p => p[1])
                             .FirstOrDefault();
      if (code_text == null) {
        return Json(
          HttpStatusCode.BadRequest,
          new { responseCode = 250, message = "responseCode is required." }
        );
      }

      int code = int.Parse(code_text, CultureInfo.InvariantCulture);
      finish_codes.Add(code);
      int result_code = code switch {
        12 => destination_full ? 14 : 13,
        15 => 15,
        19 => 15,
        _ => 250
      };
      return Json(
        code is 12 or 15 or 19 ? HttpStatusCode.OK : HttpStatusCode.BadRequest,
        new {
          flowFileSent = code == 12 ? CountPackets(flow_file_bodies.Last()) : 0,
          responseCode = result_code,
          message = (string?)null
        }
      );
    }

    return Text(HttpStatusCode.NotFound, "Not Found");
  }

  /// <summary>
  ///   Counts packets in a StandardFlowFileCodec body, like NiFi does.
  /// </summary>
  public static int CountPackets(byte[] body) {
    int offset = 0;
    int count = 0;
    while (offset < body.Length) {
      int attributes = ReadInt32(body, ref offset);
      for (int i = 0; i < attributes * 2; i++) {
        int string_length = ReadInt32(body, ref offset);
        offset += string_length;
      }

      long length = ReadInt64(body, ref offset);
      offset += checked((int)length);
      count++;
    }

    return count;
  }

  private static int ReadInt32(byte[] data, ref int offset) {
    int value = System.Buffers.Binary.BinaryPrimitives.ReadInt32BigEndian(
      data.AsSpan(offset)
    );
    offset += 4;
    return value;
  }

  private static long ReadInt64(byte[] data, ref int offset) {
    long value = System.Buffers.Binary.BinaryPrimitives.ReadInt64BigEndian(
      data.AsSpan(offset)
    );
    offset += 8;
    return value;
  }

  private static HttpResponseMessage Json(HttpStatusCode status, object body) {
    return new HttpResponseMessage(status) {
      Content = new StringContent(
        JsonSerializer.Serialize(body),
        Encoding.UTF8,
        "application/json"
      )
    };
  }

  private static HttpResponseMessage Text(HttpStatusCode status, string body) {
    return new HttpResponseMessage(status) {
      Content = new StringContent(body, Encoding.UTF8, "text/plain")
    };
  }
}

internal sealed record RecordedRequest(
  HttpMethod method,
  string path,
  string? content_type,
  bool has_protocol_version,
  byte[]? body);
