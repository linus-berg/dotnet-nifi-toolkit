using System.Formats.Tar;
using System.Xml;
using NifiKit.Models;
using NifiKit.Services;

namespace NifiKit.Tests;

public class FlowFileV1Tests {
  private readonly FlowFileService service_ = new();

  [Fact]
  public async Task Attributes_Xml_Is_Identical_To_NiFi() {
    string nifi_xml = await ReadEntryAsync(
                        TestData.Fixture("nifi_v1.tar"),
                        "flowfile.attributes"
                      );

    string our_xml =
      FlowFileService.SerializeAttributesV1(TestData.V1Attributes());

    Assert.Equal(nifi_xml, our_xml);
  }

  [Fact]
  public async Task Writes_Attributes_And_Content_Entries_In_NiFi_Order() {
    NifiPackage package = new NifiPackage().AddAttributes(TestData.V1Attributes())
                                           .SetContent("v1 content");

    byte[] tar = await service_.CreateFlowFileV1Async(package);

    using TarReader reader = new(new MemoryStream(tar));
    TarEntry first = (await reader.GetNextEntryAsync())!;
    TarEntry second = (await reader.GetNextEntryAsync())!;
    Assert.Equal("flowfile.attributes", first.Name);
    Assert.Equal("flowfile.content", second.Name);
    Assert.Equal("v1 content",
                 await new StreamReader(second.DataStream!).ReadToEndAsync());
    Assert.Null(await reader.GetNextEntryAsync());
  }

  [Fact]
  public async Task Attributes_Round_Trip_Through_An_Xml_Parser() {
    Dictionary<string, string> attributes = new() {
      ["key\nwith\tbreaks"] = "value\r\nwith CRLF",
      ["quotes"] = "\"'<>&"
    };
    NifiPackage package = new NifiPackage().AddAttributes(attributes);

    byte[] tar = await service_.CreateFlowFileV1Async(package);
    string xml = await ReadEntryAsync(tar, "flowfile.attributes");

    Assert.Equal(attributes, ParseJavaPropertiesXml(xml));
  }

  [Fact]
  public void Characters_Xml_Cannot_Represent_Are_Rejected() {
    Dictionary<string, string> attributes = new() {
      ["bad"] = "bell\u0007"
    };

    Assert.Throws<ArgumentException>(
      () => FlowFileService.SerializeAttributesV1(attributes)
    );
  }

  [Fact]
  public async Task NonSeekable_Content_Is_Supported() {
    NifiPackage package = new NifiPackage()
                          .AddAttribute("filename", "x")
                          .SetContent(new NonSeekableStream(TestData.Utf8("abc")));

    byte[] tar = await service_.CreateFlowFileV1Async(package);

    Assert.Equal("abc", await ReadEntryAsync(tar, "flowfile.content"));
  }

  private static async Task<string> ReadEntryAsync(byte[] tar, string name) {
    using TarReader reader = new(new MemoryStream(tar));
    while (await reader.GetNextEntryAsync() is { } entry) {
      if (entry.Name == name) {
        return await new StreamReader(entry.DataStream!).ReadToEndAsync();
      }
    }

    throw new InvalidOperationException($"Entry {name} not found");
  }

  /// <summary>
  ///   Parses the document like java.util.Properties.loadFromXML.
  /// </summary>
  private static Dictionary<string, string> ParseJavaPropertiesXml(string xml) {
    XmlReaderSettings settings = new() { DtdProcessing = DtdProcessing.Ignore };
    using XmlReader reader = XmlReader.Create(new StringReader(xml), settings);
    Dictionary<string, string> result = new();
    while (reader.ReadToFollowing("entry")) {
      string key = reader.GetAttribute("key")!;
      result[key] = reader.ReadElementContentAsString();
    }

    return result;
  }
}
