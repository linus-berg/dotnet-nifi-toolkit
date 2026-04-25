// See https://aka.ms/new-console-template for more information

using System.Text;
using Bogus;
using Nifi.App;
using NifiKit.Models;
using NifiKit.Services;

Console.WriteLine("Hello, World!");
Faker faker = new();
string content = faker.Lorem.Paragraphs(100);
FlowFileService service = new();
using (NifiPackage nifi_tst_pkg = new()) {
  nifi_tst_pkg.AddAttribute("attribute_1", "attribute-value")
              .AddAttribute("xyz", "A GREAT ATTRIBUTE OF GREAT VALUE") 
              .AddAttribute("a-weird-value", "åäö");
  
}

//nifi_pkg.SetContent(Encoding.UTF8.GetBytes(content));
NifiPackage nifi_pkg = new();
  nifi_pkg.AddAttribute("attribute_1", "attribute-value")
          .AddAttribute("xyz", "A GREAT ATTRIBUTE OF GREAT VALUE") 
          .AddAttribute("a-weird-value", "åäö");
  

//nifi_pkg.SetContent(Encoding.UTF8.GetBytes(content));

byte[] flowfile = await service.CreateFlowFileV3Async(nifi_pkg);

MemoryStream ms = new(flowfile);

List<NifiPackage> packages =
  await service.UnpackFlowFilesV3Async(ms).ToListAsync();

foreach (NifiPackage pkg in packages) {
  using StreamReader reader = new(pkg.content, Encoding.UTF8);
  string result = reader.ReadToEnd();
  Console.WriteLine(result);
}

Guid uuid = Guid.NewGuid();
string dir = Path.Join(SolutionHelper.GetSolutionDirectory(), "nifi-test");

Directory.CreateDirectory(dir);

string test_file = Path.Join(dir, "a-test-file.flowfile");
File.WriteAllBytes(test_file, flowfile);