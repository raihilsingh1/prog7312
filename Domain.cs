using System.Runtime.CompilerServices;
using System.Text.Json.Serialization;

// ---------- Generics: TelemetryPacket<T> (float / int / bool, no boxing) ----------
interface ITelemetryPacket
{
    string Mac { get; }
    string Name { get; }
    string Category { get; }
    string Metric { get; }
    string Unit { get; }
    DateTime Timestamp { get; }
    double Value { get; }
    string ValueType { get; }
}

class TelemetryPacket<T> : ITelemetryPacket where T : struct
{
    T _value;   // stored as T itself, so no boxing/unboxing

    public TelemetryPacket(string mac, string name, string category, string metric, string unit, T value, DateTime timestamp)
    {
        Mac = mac; Name = name; Category = category; Metric = metric; Unit = unit; _value = value; Timestamp = timestamp;
    }

    public string Mac { get; }
    public string Name { get; }
    public string Category { get; }
    public string Metric { get; }
    public string Unit { get; }
    public DateTime Timestamp { get; }
    public T Value => _value;
    public string ValueType => typeof(T).Name;
    double ITelemetryPacket.Value => ToDouble();

    // typeof(T) checks are resolved by the JIT per instantiation; Unsafe.As reads the value without boxing
    public double ToDouble()
    {
        if (typeof(T) == typeof(float)) return Unsafe.As<T, float>(ref _value);
        if (typeof(T) == typeof(int)) return Unsafe.As<T, int>(ref _value);
        if (typeof(T) == typeof(bool)) return Unsafe.As<T, bool>(ref _value) ? 1 : 0;
        throw new NotSupportedException($"Unsupported telemetry type {typeof(T).Name}");
    }
}

static class TelemetryFactory
{
    public static ITelemetryPacket Create(Packet w, DateTime ts)
    {
        var name = string.IsNullOrWhiteSpace(w.Name) ? w.Mac : w.Name;
        var cat = w.Category ?? "";
        var unit = w.Unit ?? "";
        return w.Metric switch
        {
            "power" => new TelemetryPacket<int>(w.Mac, name, cat, w.Metric, unit, (int)Math.Round(w.Value), ts),
            "switch" => new TelemetryPacket<bool>(w.Mac, name, cat, w.Metric, unit, w.Value != 0, ts),
            _ => new TelemetryPacket<float>(w.Mac, name, cat, w.Metric, unit, (float)w.Value, ts)
        };
    }
}

// ---------- Operator overloading ----------
readonly struct PowerMeter : IEquatable<PowerMeter>
{
    public string Name { get; }
    public int Watts { get; }
    public PowerMeter(string name, int watts) { Name = name; Watts = watts; }

    public static PowerMeter operator +(PowerMeter a, PowerMeter b) => new($"{a.Name} + {b.Name}", a.Watts + b.Watts);
    public static PowerMeter operator -(PowerMeter a, PowerMeter b) => new($"{a.Name} - {b.Name}", a.Watts - b.Watts);
    public static bool operator >(PowerMeter a, PowerMeter b) => a.Watts > b.Watts;
    public static bool operator <(PowerMeter a, PowerMeter b) => a.Watts < b.Watts;
    public static bool operator >=(PowerMeter a, PowerMeter b) => a.Watts >= b.Watts;
    public static bool operator <=(PowerMeter a, PowerMeter b) => a.Watts <= b.Watts;
    public static bool operator ==(PowerMeter a, PowerMeter b) => a.Watts == b.Watts;
    public static bool operator !=(PowerMeter a, PowerMeter b) => a.Watts != b.Watts;
    public bool Equals(PowerMeter other) => Watts == other.Watts;
    public override bool Equals(object? obj) => obj is PowerMeter m && Equals(m);
    public override int GetHashCode() => Watts;
}

// ---------- Recursion: deployment tree validation ----------
class DeploymentNode
{
    public string Name { get; set; } = "";
    public string Kind { get; set; } = "";
    public List<DeploymentNode> Children { get; set; } = new();
}

static class Deployment
{
    public static readonly DeploymentNode Root = new()
    {
        Name = "Smart-X Network", Kind = "root",
        Children = new()
        {
            N("Facility A", "facility", N("Zone 1", "zone", N("Sub-Zone A", "subzone"), N("Sub-Zone B", "subzone")),
                                        N("Zone 2", "zone", N("Sub-Zone A", "subzone"))),
            N("Facility B", "facility", N("Zone 1", "zone", N("Sub-Zone A", "subzone")))
        }
    };

    static DeploymentNode N(string name, string kind, params DeploymentNode[] kids) =>
        new() { Name = name, Kind = kind, Children = kids.ToList() };

    // Walks down the tree one level per call: Facility -> Zone -> Sub-Zone
    static bool Validate(DeploymentNode parent, string[] path, int depth, out string error)
    {
        var child = parent.Children.FirstOrDefault(c => c.Name.Equals(path[depth], StringComparison.OrdinalIgnoreCase));
        if (child == null) { error = $"'{path[depth]}' does not exist under '{parent.Name}'."; return false; }
        if (depth == path.Length - 1)
        {
            if (child.Kind != "subzone") { error = $"'{child.Name}' is a {child.Kind}. Sensors must be placed in a Sub-Zone."; return false; }
            error = ""; return true;
        }
        return Validate(child, path, depth + 1, out error);
    }

    // Input is leaf first, like the brief: "Sub-Zone B -> Zone 1 -> Facility A"
    public static bool ValidateLocation(string location, out string canonical, out string error)
    {
        var parts = location.Split(new[] { "->", ">" }, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        canonical = string.Join(" -> ", parts);
        if (parts.Length == 0) { error = "Enter a location such as Sub-Zone B -> Zone 1 -> Facility A."; return false; }
        Array.Reverse(parts);   // validate from the facility downwards
        return Validate(Root, parts, 0, out error);
    }

    public static IEnumerable<string> LeafPaths(DeploymentNode node, string suffix = "")
    {
        foreach (var c in node.Children)
        {
            var here = suffix.Length == 0 ? c.Name : $"{c.Name} -> {suffix}";
            if (c.Kind == "subzone") yield return here;
            else foreach (var p in LeafPaths(c, here)) yield return p;
        }
    }
}

// ---------- Jagged array batches transferred into List<T> ----------
class BatchBuffer
{
    const int BatchSize = 10, BatchCount = 4;
    readonly double[][] _rows = Enumerable.Range(0, BatchCount).Select(_ => new double[BatchSize]).ToArray();
    int _row, _col;
    public List<double> Archive { get; } = new();
    public int Pending => _col;

    public void Add(double v)
    {
        _rows[_row][_col++] = v;
        if (_col < BatchSize) return;
        Archive.AddRange(_rows[_row]);              // full batch moves into the List<T>
        if (Archive.Count > 5000) Archive.RemoveRange(0, 1000);
        _col = 0; _row = (_row + 1) % BatchCount;
    }
}

record Attachment(string FileName, string StoredName, long Size, DateTime UploadedAt);
record RegisterRequest(string Mac, string Name, string Category, string Location);

class Device
{
    public string Mac { get; set; } = "";
    public string Name { get; set; } = "";
    public string Category { get; set; } = "General";
    public string Location { get; set; } = "";
    public bool Registered { get; set; }
    public string Metric { get; set; } = "";
    public double Value { get; set; }
    public string Unit { get; set; } = "";
    public DateTime LastSeen { get; set; }
    public bool Online { get; set; } = true;
    public int Severity { get; set; } = 2;   // 0 critical, 1 warning, 2 ok
    public string State { get; set; } = "off";
    public List<Attachment> Attachments { get; } = new();
    [JsonIgnore] public BatchBuffer Buffer { get; } = new();
    public int Archived => Buffer.Archive.Count;
    public int Pending => Buffer.Pending;
}
