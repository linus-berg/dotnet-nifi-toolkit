using NifiKit.Models;
using NifiKit.SiteToSite;

namespace NifiKit.Tests;

public class SiteToSiteCodecTests {
  [Fact]
  public async Task Encodes_Bytes_Identical_To_NiFi_StandardFlowFileCodec() {
    byte[] body = await EncodeAsync(TestData.PackageA(), TestData.PackageB());

    Assert.Equal(TestData.Fixture("nifi_s2s_two_packets.bin"), body);
  }

  [Fact]
  public async Task Checksum_Matches_Java_Crc32() {
    List<SiteToSitePacket> packets = new() {
      await SiteToSiteCodec.CreatePacketAsync(TestData.PackageA(), default),
      await SiteToSiteCodec.CreatePacketAsync(TestData.PackageB(), default)
    };
    using SiteToSiteContent content = new(packets);

    await content.CopyToAsync(Stream.Null);

    Assert.Equal(TestData.S2S_FIXTURE_CRC32, content.checksum);
  }

  [Fact]
  public async Task Content_Can_Be_Sent_Again_With_The_Same_Bytes() {
    NifiPackage package = new NifiPackage()
                          .AddAttribute("filename", "x")
                          .SetContent(new NonSeekableStream(TestData.Utf8("once")));
    SiteToSitePacket packet =
      await SiteToSiteCodec.CreatePacketAsync(package, default);
    using SiteToSiteContent content = new(new[] { packet });

    byte[] first = await ReadAllAsync(content);
    byte[] second = await ReadAllAsync(content);

    Assert.Equal(first, second);
    Assert.True(content.Headers.ContentLength == first.Length);
  }

  [Fact]
  public async Task Rejects_More_Attributes_Than_NiFi_Accepts() {
    NifiPackage package = new();
    for (int i = 0; i <= 25_000; i++) {
      package.AddAttribute($"a{i}", "v");
    }

    await Assert.ThrowsAsync<ArgumentException>(
      async () => await SiteToSiteCodec.CreatePacketAsync(package, default)
    );
  }

  private static async Task<byte[]> EncodeAsync(params NifiPackage[] packages) {
    List<SiteToSitePacket> packets = new();
    foreach (NifiPackage package in packages) {
      packets.Add(await SiteToSiteCodec.CreatePacketAsync(package, default));
    }

    using SiteToSiteContent content = new(packets);
    return await ReadAllAsync(content);
  }

  private static async Task<byte[]> ReadAllAsync(HttpContent content) {
    using MemoryStream ms = new();
    await content.CopyToAsync(ms);
    return ms.ToArray();
  }
}
