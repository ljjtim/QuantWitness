using System.Text.Json;

namespace QuantWitness.LeanInput;

// 引擎进程只执行一次研究，冻结输入由所有消费者共用同一只读文档。
public static class ManifestCache
{
    private static readonly Dictionary<string, JsonDocument> Documents = new(
        OperatingSystem.IsWindows() ? StringComparer.OrdinalIgnoreCase : StringComparer.Ordinal);

    public static JsonElement Read(string path)
    {
        var absolute = Path.GetFullPath(path);
        lock (Documents)
        {
            if (!Documents.TryGetValue(absolute, out var document))
            {
                using var input = File.OpenRead(absolute);
                document = JsonDocument.Parse(input);
                Documents.Add(absolute, document);
            }
            return document.RootElement;
        }
    }
}
