using System.Globalization;
using System.Text.RegularExpressions;
var builder = WebApplication.CreateBuilder(args);
builder.Services.AddSingleton<Hub>();
builder.Services.AddHostedService<Worker>();
var app = builder.Build();
app.UseDefaultFiles();
app.UseStaticFiles();

// Devices (real or simulated) POST telemetry here
app.MapPost("/api/telemetry", (Packet p, Hub h) =>
    string.IsNullOrWhiteSpace(p.Mac) || string.IsNullOrWhiteSpace(p.Metric)
        ? Results.BadRequest("mac and metric are required")
        : Results.Ok(new { status = h.Ingest(p) }));
app.MapGet("/api/snapshot", (string? q, string? category, bool? alertsOnly, Hub h) =>
    h.Snapshot(q, category, alertsOnly ?? false));
app.MapGet("/api/history/{mac}", (string mac, Hub h) => h.History(mac));
app.MapPost("/api/commands", (CommandRequest r, Hub h) =>
    h.Send(r.Mac, r.Action) is { } c ? Results.Ok(c) : Results.NotFound("Unknown device"));
app.MapPost("/api/commands/undo", (Hub h) =>
    h.Undo() is { } c ? Results.Ok(c) : Results.NotFound("Nothing to undo"));
app.MapPost("/api/searches", (SearchRequest r, Hub h) => { h.Search(r.Term); return Results.NoContent(); });
app.MapPost("/api/register", (RegisterRequest r, Hub h) =>
    h.Register(r, out var err) is { } d ? Results.Ok(d) : Results.BadRequest(err));
app.MapGet("/api/deployment", () => new { root = Deployment.Root, paths = Deployment.LeafPaths(Deployment.Root).ToList() });
app.MapGet("/api/aggregate", (string a, string b, Hub h) =>
    h.Aggregate(a, b, out var err) is { } r ? Results.Ok(r) : Results.BadRequest(err));
app.MapPost("/api/devices/{mac}/attachments", async (string mac, IFormFile file, Hub h, IWebHostEnvironment env) =>
{
    var ext = Path.GetExtension(file.FileName).ToLowerInvariant();
    if (!Util.AllowedExt.Contains(ext)) return Results.BadRequest("File type not allowed.");
    if (file.Length == 0 || file.Length > 10 * 1024 * 1024) return Results.BadRequest("File must be between 1 byte and 10 MB.");
    if (!h.Exists(mac)) return Results.NotFound("Unknown device");
    var dir = Path.Combine(env.ContentRootPath, "uploads", Util.Safe(mac));
    Directory.CreateDirectory(dir);
    var stored = Guid.NewGuid().ToString("N") + ext;
    await using (var fs = File.Create(Path.Combine(dir, stored))) await file.CopyToAsync(fs);
    var a = new Attachment(Path.GetFileName(file.FileName), stored, file.Length, DateTime.UtcNow);
    h.AddAttachment(mac, a);
    return Results.Ok(a);
}).DisableAntiforgery();
app.MapGet("/api/devices/{mac}/attachments/{stored}", (string mac, string stored, Hub h, IWebHostEnvironment env) =>
{
    var a = h.FindAttachment(mac, stored);
    if (a == null) return Results.NotFound();
    var path = Path.Combine(env.ContentRootPath, "uploads", Util.Safe(mac), a.StoredName);
    return File.Exists(path) ? Results.File(path, "application/octet-stream", a.FileName) : Results.NotFound();
});
app.MapGet("/api/mesh", (string? from, Hub h) => h.MeshSnapshot(from));
app.MapPost("/api/mesh/link", (LinkRequest r, Hub h) => h.Link(r, out var err) ? Results.Ok() : Results.BadRequest(err));
app.MapGet("/api/mesh/route/{id}", (string id, Hub h) =>
    h.Route(id, out var err) is { } r ? Results.Ok(r) : Results.BadRequest(err));
app.MapGet("/api/history/{mac}/range", (string mac, string? from, string? to, Hub h) =>
    h.Range(mac, Util.ParseUtc(from), Util.ParseUtc(to)) is { } r ? Results.Ok(r) : Results.NotFound("No history for that device."));
app.Run();

record Packet(string Mac, string? Name, string? Category, string Metric, double Value, string? Unit, DateTime? Timestamp);
record CommandRequest(string Mac, string Action);
record SearchRequest(string Term);
static class Util
{
    public static readonly HashSet<string> AllowedExt = new() { ".json", ".cfg", ".ini", ".txt", ".log", ".csv", ".png", ".jpg", ".jpeg", ".webp" };
    public static readonly HashSet<string> Categories = new() { "Environmental", "Power Consumption", "Actuator" };
    public static DateTime? ParseUtc(string? s) =>
        DateTime.TryParse(s, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal, out var d) ? d : null;
    public static string Safe(string s) => new string(s.Select(ch => char.IsLetterOrDigit(ch) ? ch : '-').ToArray());
}
record Alert(DateTime Time, string Message);
record Suggestion(string Kind, string Text, string Mac, string? Action);

class Command
{
    public int Id { get; set; }
    public DateTime Time { get; set; }
    public string Mac { get; set; } = "";
    public string DeviceName { get; set; } = "";
    public string Action { get; set; } = "";
    public string PreviousState { get; set; } = "";
    public string NewState { get; set; } = "";
    public bool Undone { get; set; }
}

class Worker : BackgroundService
{
    readonly Hub _hub;
    public Worker(Hub hub) => _hub = hub;
    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(500));
        while (await timer.WaitForNextTickAsync(ct)) _hub.Tick();
    }
}

class Hub
{
    readonly object _lock = new();
    readonly Queue<ITelemetryPacket> _standard = new();                              // FIFO telemetry
    readonly PriorityQueue<ITelemetryPacket, int> _critical = new();                 // critical alerts jump the line
    readonly Stack<Command> _undo = new();                                 // manual override undo
    readonly List<Command> _commands = new();                              // command history (display)
    readonly Dictionary<string, Device> _devices = new();                  // O(1) lookup by MAC
    readonly SortedDictionary<DateTime, ITelemetryPacket> _history = new();          // logs ordered by timestamp
    readonly HashSet<string> _faults = new();                              // unique active faults
    readonly Dictionary<string, AvlTree<DateTime, Reading>> _trees = new();  // per-device AVL tree by timestamp
    readonly MeshGraph _mesh = new();                                      // mesh network graph
    readonly HashSet<string> _stamps = new();                              // engagement: passport stamps
    readonly List<Alert> _alerts = new();
    readonly List<ITelemetryPacket> _recent = new();
    readonly Dictionary<string, Dictionary<string, int>> _follow = new();  // "after A the user acts on B" counts
    string? _lastAction;
    int _processed, _nextId = 1;

    static int Severity(ITelemetryPacket p) => p.Metric switch
    {
        "moisture" when p.Value < 20 => 0,
        "temperature" when p.Value > 45 => 0,
        "power" when p.Value > 2000 => 0,
        "moisture" when p.Value < 30 => 1,
        "temperature" when p.Value > 35 => 1,
        "power" when p.Value > 1500 => 1,
        _ => 2
    };

    public string Ingest(Packet raw)
    {
        var p = TelemetryFactory.Create(raw, DateTime.UtcNow);
        lock (_lock)
        {
            if (Severity(p) == 0)
            {
                _critical.Enqueue(p, 0);
                DrainCritical();
                return "critical-processed";
            }
            _standard.Enqueue(p);
            return "queued";
        }
    }

    public void Tick()
    {
        lock (_lock)
        {
            DrainCritical();
            for (int i = 0; i < 100 && _standard.Count > 0; i++) Process(_standard.Dequeue());
            var now = DateTime.UtcNow;
            foreach (var d in _devices.Values)
                if (d.Online && (now - d.LastSeen).TotalSeconds > 8)
                {
                    d.Online = false;
                    if (_faults.Add(d.Mac + "|disconnected"))   // set ignores duplicate disconnect alerts
                        AddAlert($"{d.Name} disconnected");
                }
        }
    }

    void DrainCritical() { while (_critical.TryDequeue(out var p, out _)) Process(p); }

    void Process(ITelemetryPacket p)
    {
        if (!_devices.TryGetValue(p.Mac, out var d)) { d = new Device { Mac = p.Mac }; _devices[p.Mac] = d; }
        if (!d.Registered)
        {
            d.Name = p.Name;
            d.Category = string.IsNullOrWhiteSpace(p.Category) ? "General" : p.Category;
        }
        d.Metric = p.Metric; d.Value = Math.Round(p.Value, 1); d.Unit = p.Unit ?? "";
        d.LastSeen = p.Timestamp; d.Online = true; d.Severity = Severity(p);
        _faults.Remove(p.Mac + "|disconnected");
        d.Buffer.Add(p.Value);
        _stamps.Add("firstPacket");

        var key = p.Timestamp;
        while (_history.ContainsKey(key)) key = key.AddTicks(1);
        _history[key] = p;
        if (!_trees.TryGetValue(p.Mac, out var tree)) _trees[p.Mac] = tree = new();
        tree.Insert(key, new Reading(key, p.Metric, p.Value));
        if (_history.Count > 3000) _history.Remove(_history.Keys.First());

        _recent.Insert(0, p);
        if (_recent.Count > 20) _recent.RemoveAt(20);

        var fault = p.Mac + "|critical";
        if (d.Severity == 0) { if (_faults.Add(fault)) { _stamps.Add("critical"); AddAlert($"CRITICAL: {d.Name} reads {d.Value}{d.Unit}"); } }
        else _faults.Remove(fault);
        _processed++;
    }

    void AddAlert(string msg) { _alerts.Insert(0, new Alert(DateTime.UtcNow, msg)); if (_alerts.Count > 30) _alerts.RemoveAt(30); }

    void RecordAction(string name)
    {
        if (_lastAction != null && _lastAction != name)
        {
            if (!_follow.TryGetValue(_lastAction, out var next)) _follow[_lastAction] = next = new();
            next[name] = next.GetValueOrDefault(name) + 1;
        }
        _lastAction = name;
    }

    public Command? Send(string mac, string action)
    {
        lock (_lock)
        {
            if (!_devices.TryGetValue(mac, out var d)) return null;
            var next = action == "toggle" ? (d.State == "on" ? "off" : "on") : action == "on" ? "on" : "off";
            var c = new Command { Id = _nextId++, Time = DateTime.UtcNow, Mac = mac, DeviceName = d.Name, Action = action, PreviousState = d.State, NewState = next };
            d.State = next;
            _undo.Push(c); _commands.Add(c);
            RecordAction(d.Name);
            _stamps.Add("command");
            return c;
        }
    }

    public Command? Undo()
    {
        lock (_lock)
        {
            if (_undo.Count == 0) return null;
            var c = _undo.Pop();
            if (_devices.TryGetValue(c.Mac, out var d)) d.State = c.PreviousState;
            c.Undone = true;
            _stamps.Add("undo");
            return c;
        }
    }

    bool Up(string id) => id == MeshGraph.Gateway || (_devices.TryGetValue(id, out var d) && d.Online);
    string NameOf(string id) => id == MeshGraph.Gateway ? "Gateway" : _devices.TryGetValue(id, out var d) ? d.Name : id;
    string? Resolve(string id)
    {
        if (id.Equals(MeshGraph.Gateway, StringComparison.OrdinalIgnoreCase)) return MeshGraph.Gateway;
        return _devices.Values.FirstOrDefault(d => d.Mac.Equals(id, StringComparison.OrdinalIgnoreCase) || d.Name.Equals(id, StringComparison.OrdinalIgnoreCase))?.Mac;
    }

    public bool Link(LinkRequest r, out string error)
    {
        error = "";
        lock (_lock)
        {
            var a = Resolve(r.A ?? ""); var b = Resolve(r.B ?? "");
            if (a == null || b == null) { error = "Both ends must be the gateway or a known device."; return false; }
            if (a == b) { error = "A node cannot link to itself."; return false; }
            if (r.Cost < 0.1 || r.Cost > 100) { error = "Link cost must be between 0.1 and 100."; return false; }
            _mesh.AddEdge(a, b, r.Cost);
            _stamps.Add("linked");
            return true;
        }
    }

    public object MeshSnapshot(string? from)
    {
        lock (_lock)
        {
            var gw = MeshGraph.Gateway;
            var start = (from != null ? Resolve(from) : null) ?? gw;
            var toGw = _mesh.Bfs(gw, Up);
            var (mst, total) = _mesh.Prim(gw, Up);
            var reach = _mesh.Bfs(start, Up);
            var mstKeys = mst.Where(kv => kv.Value != null).Select(kv => MeshGraph.Key(kv.Key, kv.Value!)).ToHashSet();
            var nodes = _mesh.Nodes.Select(id => new
            {
                id, name = NameOf(id), up = Up(id), connected = toGw.ContainsKey(id),
                hops = toGw.ContainsKey(id) ? MeshGraph.PathToRoot(toGw, id).Count - 1 : -1
            }).ToList();
            var edges = _mesh.Edges().Select(e => new
            {
                a = e.A, b = e.B, w = e.W, mst = mstKeys.Contains(MeshGraph.Key(e.A, e.B)), live = Up(e.A) && Up(e.B)
            }).ToList();
            var down = nodes.Where(n => !n.connected).Select(n => new
            {
                n.id, n.name, reason = n.up ? "cut off from the gateway" : "offline",
                reroutes = _mesh.Neighbours(n.id).Keys.Where(k => k != gw && toGw.ContainsKey(k))
                    .Select(k => new { node = NameOf(k), route = MeshGraph.PathToRoot(toGw, k).Select(NameOf).ToList() }).ToList()
            }).ToList();
            var reachable = reach.Keys.Select(k => new { name = NameOf(k), hops = MeshGraph.PathToRoot(reach, k).Count - 1 }).ToList();
            return new { nodes, edges, mstTotal = total, from = NameOf(start), reachable, down };
        }
    }

    public object? Route(string id, out string error)
    {
        error = "";
        lock (_lock)
        {
            var mac = Resolve(id.Trim());
            if (mac == null || mac == MeshGraph.Gateway) { error = "No sensor with that MAC address or name."; return null; }
            if (!_mesh.Nodes.Contains(mac)) { error = $"{NameOf(mac)} has no mesh links yet."; return null; }
            var toGw = _mesh.Bfs(MeshGraph.Gateway, Up);
            var (mst, total) = _mesh.Prim(MeshGraph.Gateway, Up);
            var hop = toGw.ContainsKey(mac) ? MeshGraph.PathToRoot(toGw, mac) : null;
            var best = mst.ContainsKey(mac) ? MeshGraph.PathToRoot(mst, mac) : null;
            _stamps.Add("routed");
            return new
            {
                name = NameOf(mac), mac, up = Up(mac),
                hopRoute = hop?.Select(NameOf).ToList(), hopCost = hop == null ? 0 : _mesh.PathCost(hop),
                mstRoute = best?.Select(NameOf).ToList(), mstMacs = best, mstCost = best == null ? 0 : _mesh.PathCost(best), mstTotal = total
            };
        }
    }

    public object? Range(string mac, DateTime? from, DateTime? to)
    {
        lock (_lock)
        {
            if (!_trees.TryGetValue(mac, out var tree)) return null;
            var list = tree.Range(from ?? DateTime.MinValue, to ?? DateTime.MaxValue, out var visited);
            _stamps.Add("queried");
            return new { total = tree.Count, height = tree.Height, visited, count = list.Count, readings = list.TakeLast(500) };
        }
    }

    public bool Exists(string mac) { lock (_lock) return _devices.ContainsKey(mac); }

    public Device? Register(RegisterRequest r, out string error)
    {
        error = "";
        var mac = r.Mac?.Trim() ?? "";
        if (!Regex.IsMatch(mac, @"^[A-Za-z0-9:_\-]{3,40}$")) { error = "Enter a MAC address or unique ID (3-40 letters, digits, : _ -)."; return null; }
        if (string.IsNullOrWhiteSpace(r.Name)) { error = "Enter a sensor name."; return null; }
        if (!Util.Categories.Contains(r.Category ?? "")) { error = "Choose Environmental, Power Consumption or Actuator."; return null; }
        if (!Deployment.ValidateLocation(r.Location ?? "", out var location, out var locError)) { error = locError; return null; }
        lock (_lock)
        {
            if (!_devices.TryGetValue(mac, out var d)) { d = new Device { Mac = mac, LastSeen = DateTime.UtcNow }; _devices[mac] = d; }
            d.Name = r.Name.Trim(); d.Category = r.Category!; d.Location = location; d.Registered = true;
            _stamps.Add("registered");
            return d;
        }
    }

    public void AddAttachment(string mac, Attachment a)
    {
        lock (_lock) { if (_devices.TryGetValue(mac, out var d)) { d.Attachments.Add(a); _stamps.Add("attached"); } }
    }

    public Attachment? FindAttachment(string mac, string stored)
    {
        lock (_lock) return _devices.TryGetValue(mac, out var d) ? d.Attachments.FirstOrDefault(a => a.StoredName == stored) : null;
    }

    public object? Aggregate(string a, string b, out string error)
    {
        error = "";
        lock (_lock)
        {
            if (!_devices.TryGetValue(a, out var da) || !_devices.TryGetValue(b, out var db)) { error = "Choose two devices."; return null; }
            if (da.Metric != "power" || db.Metric != "power") { error = "Both devices must report power in watts."; return null; }
            var meter1 = new PowerMeter(da.Name, (int)Math.Round(da.Value));
            var meter2 = new PowerMeter(db.Name, (int)Math.Round(db.Value));
            var meter3 = meter1 + meter2;       // operator +
            var delta = meter1 - meter2;        // operator -
            _stamps.Add("aggregated");
            return new { total = meter3.Watts, delta = delta.Watts, higher = meter1 > meter2 ? meter1.Name : meter2 > meter1 ? meter2.Name : "Equal" };
        }
    }

    public void Search(string term)
    {
        lock (_lock)
        {
            var m = _devices.Values.FirstOrDefault(d => d.Name.Contains(term, StringComparison.OrdinalIgnoreCase));
            if (m != null) RecordAction(m.Name);
        }
    }

    public object[] History(string mac)
    {
        lock (_lock)
            return _history.Values.Where(p => p.Mac == mac).TakeLast(40)
                .Select(p => (object)new { time = p.Timestamp, value = p.Value }).ToArray();
    }

    List<Suggestion> Suggest()
    {
        var list = new List<Suggestion>();
        foreach (var d in _devices.Values.Where(x => !x.Online || x.Severity < 2).OrderBy(x => x.Severity))
        {
            var why = !d.Online ? $"{d.Name} has stopped reporting."
                : $"{d.Name} reads {d.Value}{d.Unit} ({(d.Severity == 0 ? "critical" : "warning")}).";
            list.Add(new Suggestion("Automated insight", why, d.Mac, null));
            if (_follow.TryGetValue(d.Name, out var next) && next.Count > 0)
            {
                var top = next.OrderByDescending(k => k.Value).First();
                var target = _devices.Values.FirstOrDefault(x => x.Name == top.Key);
                if (top.Value >= 2 && target != null)
                    list.Add(new Suggestion("Suggested action", $"After checking {d.Name} you usually act on {top.Key} ({top.Value} times). Turn it on?", target.Mac, "on"));
            }
        }
        return list;
    }

    public object Snapshot(string? q, string? category, bool alertsOnly)
    {
        lock (_lock)
        {
            var all = _devices.Values.OrderBy(d => d.Severity).ThenBy(d => d.Name).ToList();
            var filtered = all.Where(d =>
                (string.IsNullOrWhiteSpace(q) || d.Name.Contains(q, StringComparison.OrdinalIgnoreCase) || d.Location.Contains(q, StringComparison.OrdinalIgnoreCase) || d.Mac.Contains(q, StringComparison.OrdinalIgnoreCase))
                && (string.IsNullOrWhiteSpace(category) || category == "all" || d.Category == category)
                && (!alertsOnly || d.Severity < 2 || !d.Online)).ToList();
            return new
            {
                allDevices = all,
                devices = filtered,
                alerts = _alerts.Take(8),
                faults = _faults.ToList(),
                commands = _commands.AsEnumerable().Reverse().Take(10),
                canUndo = _undo.Count > 0,
                suggestions = Suggest(),
                recent = _recent,
                stamps = _stamps.ToList(),
                stats = new { standard = _standard.Count, critical = _critical.Count, processed = _processed, devices = _devices.Count }
            };
        }
    }
}
