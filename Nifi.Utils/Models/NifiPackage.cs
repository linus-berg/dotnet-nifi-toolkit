namespace Nifi.Utils.Models;

/// <summary>
///   Represents a single NiFi FlowFile (attributes + content).
/// </summary>
public class NifiPackage : IDisposable {
  public IDictionary<string, string> attributes { get; set; } =
    new Dictionary<string, string>();

  public Stream content { get; set; } = Stream.Null;

  public void Dispose() {
    content?.Dispose();
  }
}