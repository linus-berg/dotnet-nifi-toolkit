namespace Nifi.Utils.Models;

/// <summary>
/// Represents a single NiFi FlowFile (attributes + content).
/// </summary>
public class NifiPackage : IDisposable
{
    /// <summary>
    /// FlowFile attributes.
    /// </summary>
    public IDictionary<string, string> attributes { get; set; } = new Dictionary<string, string>();

    /// <summary>
    /// FlowFile content stream.
    /// </summary>
    public Stream content { get; set; } = Stream.Null;

    /// <summary>
    /// Adds a single attribute to the FlowFile.
    /// </summary>
    /// <param name="key">Attribute name.</param>
    /// <param name="value">Attribute value.</param>
    /// <returns>The NifiPackage instance for chaining.</returns>
    public NifiPackage AddAttribute(string key, string value)
    {
        attributes[key] = value;
        return this;
    }

    /// <summary>
    /// Adds multiple attributes to the FlowFile.
    /// </summary>
    /// <param name="new_attributes">Dictionary of attributes to add.</param>
    /// <returns>The NifiPackage instance for chaining.</returns>
    public NifiPackage AddAttributes(IDictionary<string, string> new_attributes)
    {
        foreach (KeyValuePair<string, string> kvp in new_attributes)
        {
            attributes[kvp.Key] = kvp.Value;
        }
        return this;
    }

    /// <summary>
    /// Sets the content of the FlowFile from a byte array.
    /// </summary>
    /// <param name="data">The byte array content.</param>
    /// <returns>The NifiPackage instance for chaining.</returns>
    public NifiPackage SetContent(byte[] data)
    {
        content?.Dispose();
        content = new MemoryStream(data);
        return this;
    }

    /// <summary>
    /// Sets the content of the FlowFile from a stream.
    /// </summary>
    /// <param name="stream">The content stream.</param>
    /// <returns>The NifiPackage instance for chaining.</returns>
    public NifiPackage SetContent(Stream stream)
    {
        content?.Dispose();
        content = stream;
        return this;
    }

    public void Dispose()
    {
        content?.Dispose();
    }
}
