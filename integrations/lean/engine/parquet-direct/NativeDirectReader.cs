using System.Text.Json;
using Python.Runtime;
using QuantConnect;
using QuantConnect.Data;
using QuantConnect.Python;

namespace QuantWitness.LeanInput;

internal static class NativeDirectReader
{
    private static readonly object Initialization = new();

    public static IEnumerable<BaseData> Read(string manifestPath, JsonElement binding, Symbol symbol, string code,
        DateTime startUtc, DateTime endUtc, DateTime consumerUtc, string purpose, bool universe)
    {
        lock (Initialization)
            PythonInitializer.Initialize();

        PyObject cursor;
        using (Py.GIL())
        using (var module = Py.Import("research_pipeline.runtime.lean_native_direct"))
        using (var path = new PyString(Path.Combine(Path.GetDirectoryName(manifestPath), "native-bindings.json")))
        using (var bindingId = new PyString(binding.GetProperty("binding_id").GetString()))
        using (var instrument = new PyString(code))
        using (var from = new PyString(NativeDataStore.Stamp(startUtc)))
        using (var to = new PyString(NativeDataStore.Stamp(endUtc)))
        using (var consumer = new PyString(NativeDataStore.Stamp(consumerUtc)))
        using (var use = new PyString(purpose))
        using (var managedSymbol = symbol.ToPython())
        using (var isUniverse = universe.ToPython())
        {
            cursor = module.InvokeMethod("open_cursor", path, bindingId, instrument, from, to, consumer, use,
                managedSymbol, isUniverse);
        }
        try
        {
            while (true)
            {
                List<BaseData> rows;
                using (Py.GIL())
                using (var page = cursor.InvokeMethod("next_page"))
                    rows = (List<BaseData>)page.AsManagedObject(typeof(List<BaseData>));
                if (rows.Count == 0) yield break;
                foreach (var row in rows) yield return row;
            }
        }
        finally
        {
            using (Py.GIL())
            using (cursor)
            using (cursor.InvokeMethod("close")) { }
        }
    }
}
