namespace QuantWitness.LeanInput;

// 行情订阅、历史和执行规则共用冻结的数据读取器。
internal static class MonthlyParquetStore
{
    private static readonly Dictionary<string, FrozenMinuteParquetStore> FrozenStores = new();

    public static FrozenMinuteParquetStore Open(string manifest, string indexDirectory = null)
    {
        lock (FrozenStores)
        {
            var path = Path.GetFullPath(manifest);
            if (!FrozenStores.TryGetValue(path, out var store))
            {
                FrozenStores.Add(path, store = new FrozenMinuteParquetStore(ManifestCache.Read(path), indexDirectory));
            }
            return store;
        }
    }

    internal static void Close(string manifest)
    {
        lock (FrozenStores)
            if (FrozenStores.Remove(Path.GetFullPath(manifest), out var store)) store.Dispose();
    }
}
