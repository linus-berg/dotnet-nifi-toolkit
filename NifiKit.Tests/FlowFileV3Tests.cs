using System.IO.Pipelines;
using NifiKit.Models;
using NifiKit.Services;

namespace NifiKit.Tests;

public class FlowFileV3Tests {
  private readonly FlowFileService service_ = new();

  [Fact]
  public async Task Unpacks_FlowFiles_Written_By_NiFi() {
    byte[] fixture = TestData.Fixture("nifi_v3_two_flowfiles.bin");

    List<NifiPackage> packages = await service_
                                       .UnpackFlowFilesV3Async(
                                         new MemoryStream(fixture)
                                       )
                                       .ToListAsync();

    Assert.Equal(2, packages.Count);
    Assert.Equal("a.txt", packages[0].GetAttribute("filename"));
    Assert.Equal("åäö ✓ 😀", packages[0].GetAttribute("unicode"));
    Assert.Equal("", packages[0].GetAttribute("empty"));
    Assert.Equal(70_000, packages[0].GetAttribute("big")!.Length);
    Assert.Equal("hello world", await packages[0].GetContentAsStringAsync());
    Assert.Equal("b.bin", packages[1].GetAttribute("filename"));
    Assert.Equal(TestData.ContentB(), await packages[1].GetContentAsBytesAsync());
  }

  [Fact]
  public async Task Writes_Bytes_Identical_To_NiFi() {
    using MemoryStream output = new();

    await service_.WriteFlowFilesV3Async(
      new[] { TestData.PackageA(), TestData.PackageB() },
      output
    );

    Assert.Equal(TestData.Fixture("nifi_v3_two_flowfiles.bin"),
                 output.ToArray());
  }

  [Fact]
  public async Task Seekable_Content_Is_Written_In_Full_Regardless_Of_Position() {
    MemoryStream content = new();
    content.Write(TestData.Utf8("0123456789")); // position left at the end
    NifiPackage package = new NifiPackage().AddAttribute("filename", "b.txt")
                                           .SetContent(content);

    byte[] flow_file = await service_.CreateFlowFileV3Async(package);
    NifiPackage unpacked = await service_
                                 .UnpackFlowFilesV3Async(
                                   new MemoryStream(flow_file)
                                 )
                                 .SingleAsync();

    Assert.Equal("0123456789", await unpacked.GetContentAsStringAsync());
  }

  [Fact]
  public async Task Writing_The_Same_Package_Twice_Produces_Identical_Output() {
    NifiPackage package = TestData.PackageA();

    byte[] first = await service_.CreateFlowFileV3Async(package);
    byte[] second = await service_.CreateFlowFileV3Async(package);

    Assert.Equal(first, second);
  }

  [Fact]
  public async Task NonSeekable_Content_Is_Buffered_With_The_Correct_Length() {
    NifiPackage package = new NifiPackage()
                          .AddAttribute("filename", "c.txt")
                          .SetContent(
                            new NonSeekableStream(TestData.Utf8("payload-data"))
                          );

    byte[] flow_file = await service_.CreateFlowFileV3Async(package);
    NifiPackage unpacked = await service_
                                 .UnpackFlowFilesV3Async(
                                   new MemoryStream(flow_file)
                                 )
                                 .SingleAsync();

    Assert.Equal("payload-data", await unpacked.GetContentAsStringAsync());
  }

  [Fact]
  public async Task Package_Without_Attributes_Is_Rejected() {
    NifiPackage package = new NifiPackage().SetContent("no attributes");

    ArgumentException ex = await Assert.ThrowsAsync<ArgumentException>(
                             () => service_.CreateFlowFileV3Async(package)
                           );
    Assert.Contains("at least one attribute", ex.Message);
  }

  [Fact]
  public async Task Truncated_Input_Always_Throws_EndOfStreamException() {
    byte[] flow_file =
      await service_.CreateFlowFileV3Async(TestData.PackageB());

    // Cutting the stream anywhere after the first byte must report a
    // truncated stream (never e.g. ArgumentOutOfRangeException)
    for (int length = 1; length < flow_file.Length; length += 7) {
      MemoryStream truncated = new(flow_file, 0, length);
      await Assert.ThrowsAsync<EndOfStreamException>(
        async () => await service_.UnpackFlowFilesV3Async(truncated)
                                  .ToListAsync()
      );
    }
  }

  [Fact]
  public async Task Invalid_Magic_Header_Throws_InvalidDataException() {
    MemoryStream input = new(TestData.Utf8("NotAFlowFile"));

    await Assert.ThrowsAsync<InvalidDataException>(
      async () => await service_.UnpackFlowFilesV3Async(input).ToListAsync()
    );
  }

  [Fact]
  public async Task Limits_Reject_Oversized_Input() {
    byte[] flow_file =
      await service_.CreateFlowFileV3Async(TestData.PackageA());

    FlowFileService strict_attributes = new(
      options: new FlowFileServiceOptions { max_attribute_length = 1000 }
    );
    FlowFileService strict_content = new(
      options: new FlowFileServiceOptions { max_content_length = 5 }
    );
    FlowFileService strict_count = new(
      options: new FlowFileServiceOptions { max_attribute_count = 3 }
    );

    foreach (FlowFileService strict in new[] {
               strict_attributes,
               strict_content,
               strict_count
             }) {
      await Assert.ThrowsAsync<InvalidDataException>(
        async () => await strict
                          .UnpackFlowFilesV3Async(new MemoryStream(flow_file))
                          .ToListAsync()
      );
    }
  }

  [Fact]
  public async Task Cancellation_Throws_OperationCanceledException() {
    byte[] fixture = TestData.Fixture("nifi_v3_two_flowfiles.bin");
    using CancellationTokenSource cts = new();
    cts.Cancel();

    await Assert.ThrowsAnyAsync<OperationCanceledException>(
      async () => await service_
                        .UnpackFlowFilesV3Async(
                          new MemoryStream(fixture),
                          cts.Token
                        )
                        .ToListAsync()
    );
  }

  [Fact]
  public async Task Streams_Are_Left_Open() {
    MemoryStream output = new();
    await service_.WriteFlowFileV3Async(TestData.PackageA(), output);
    output.Position = 0;
    await service_.UnpackFlowFilesV3Async(output).ToListAsync();

    Assert.True(output.CanRead);
  }

  [Fact]
  public async Task Round_Trips_Through_A_Pipe() {
    Pipe pipe = new();
    Task writer = Task.Run(async () => {
      await service_.WriteFlowFileV3Async(TestData.PackageA(), pipe.Writer);
      await service_.WriteFlowFileV3Async(TestData.PackageB(), pipe.Writer);
      await pipe.Writer.CompleteAsync();
    });

    List<NifiPackage> packages = await service_
                                       .UnpackFlowFilesV3Async(pipe.Reader)
                                       .ToListAsync();
    await writer;

    Assert.Equal(2, packages.Count);
    Assert.Equal(TestData.ContentB(), await packages[1].GetContentAsBytesAsync());
  }
}
