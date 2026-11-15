using System.Text;
using NifiKit.Models;

namespace NifiKit.Tests;

/// <summary>
///   The FlowFiles encoded in the NiFi-generated fixtures (see
///   Fixtures/GenFixtures.java); keep both in sync.
/// </summary>
internal static class TestData {
  public const string S2S_FIXTURE_CRC32 = "2364656160";

  public static string FixturePath(string name) {
    return Path.Combine(AppContext.BaseDirectory, "Fixtures", name);
  }

  public static byte[] Fixture(string name) {
    return File.ReadAllBytes(FixturePath(name));
  }

  public static NifiPackage PackageA() {
    return new NifiPackage()
           .AddAttribute("filename", "a.txt")
           .AddAttribute("unicode", "åäö ✓ 😀")
           .AddAttribute("empty", "")
           .AddAttribute("big", new string('x', 70_000))
           .SetContent("hello world");
  }

  public static NifiPackage PackageB() {
    return new NifiPackage()
           .AddAttribute("filename", "b.bin")
           .AddAttribute("mime.type", "application/octet-stream")
           .SetContent(ContentB());
  }

  public static byte[] ContentB() {
    byte[] bytes = new byte[5_000];
    for (int i = 0; i < bytes.Length; i++) {
      bytes[i] = (byte)(i * 31 + 7);
    }

    return bytes;
  }

  public static Dictionary<string, string> V1Attributes() {
    return new Dictionary<string, string> {
      ["filename"] = "d.txt",
      ["xml<&>\"'"] = "a<b & c>d \"q\" 'a'",
      ["unicode"] = "åäö ✓",
      ["multi"] = "line1\nline2\ttab"
    };
  }

  public static byte[] Utf8(string value) {
    return Encoding.UTF8.GetBytes(value);
  }
}

/// <summary>
///   A stream that cannot seek, like a network stream.
/// </summary>
internal sealed class NonSeekableStream : MemoryStream {
  public NonSeekableStream(byte[] data) : base(data) {
  }

  public override bool CanSeek => false;

  public override long Length => throw new NotSupportedException();

  public override long Position {
    get => throw new NotSupportedException();
    set => throw new NotSupportedException();
  }
}
