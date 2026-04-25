// See https://aka.ms/new-console-template for more information

using System.Text;
using Nifi.App;
using Nifi.Utils.Models;
using Nifi.Utils.Services;

Console.WriteLine("Hello, World!");

string content = "Hello!";
FlowFileService service = new();
Dictionary<string, string> attributes = new() {
  {
    "attribute_1", "attribute-value"
  }
};
byte[] flowfile = await service.CreateFlowFileV3Async(
                    attributes,
                    Encoding.UTF8.GetBytes(content)
                  );

MemoryStream ms = new(flowfile);

List<NifiPackage> packages =
  await service.UnpackFlowFilesV3Async(ms).ToListAsync();

foreach (NifiPackage pkg in packages) {
  using (StreamReader reader = new(pkg.content, Encoding.UTF8)) {
    string result = reader.ReadToEnd();
    Console.WriteLine(result);
  }
}

Guid uuid = Guid.NewGuid();
string dir = Path.Join(SolutionHelper.GetSolutionDirectory(), "nifi-test");

Directory.CreateDirectory(dir);

string test_file = Path.Join(dir, "a-test-file.flowfile");
File.WriteAllBytes(test_file, flowfile);