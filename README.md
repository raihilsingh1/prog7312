Smart-X IoT Gateway (PROG7312 POE)

An ASP.NET Core minimal API with a web frontend served from the same project. It covers all three parts: sensor ingestion (Part 1), the real-time command stream (Part 2) and network topology with mesh routing (Part 3).

1. Compile and run

Requirements: the .NET 8 SDK (`dotnet --version` should print 8.x). Visual Studio is not needed.

```
cd SmartX
dotnet run
```

Open the address it prints (for example `http://localhost:5000`). If you have a newer SDK, change `<TargetFramework>` in `SmartX.csproj` (e.g. `net9.0`). If the port is busy: `dotnet run --urls http://localhost:5050`.

Open the app through that address. The page does not work if the HTML file is opened straight from disk.

2. Using the application

1. **Home** shows the three modules.
2. **Sensor Data Ingestion** (Part 1): click *Start simulator*. Five simulated devices register themselves and POST readings to `/api/telemetry`, exactly as hardware would. You can register your own sensor (MAC/ID, name, category, location such as `Sub-Zone B -> Zone 1 -> Facility A`), attach a file to a sensor, and compare two power meters.
3. **Real-Time Command Stream** (Part 2): live device cards, On/Off/Toggle overrides, *Undo last command*, search and filters, and the *Suggested actions and automated insights* panel.
4. **Network Topology** (Part 3):
   - The mesh map shows nodes and links. Thick teal links are the minimum spanning tree. Numbers are link power costs.
   - **Track a node's route**: type a MAC address or name and press *Trace route*.
   - **Connected devices**: choose the Gateway or any node to list everything reachable from it.
   - **Down nodes and detours**: shows offline nodes and how their neighbours now hop.
   - **Historical telemetry log**: pick a sensor and a from/to time, then *Query*.
   - **Add link** connects two nodes with a cost. The simulator creates a demo mesh when started.

To see failure handling: start the simulator, open Network Topology, then on the ingestion page press *Disconnect relay*. After about 8 seconds the relay turns red and the detours panel shows the new hops.

3. Project layout

| File | Contents |
|---|---|
| `Program.cs` | API endpoints and the `Hub` class that owns all in-memory state |
| `Domain.cs` | Part 1: `TelemetryPacket<T>`, `PowerMeter` operators, recursive deployment validation, jagged-array batching |
| `Mesh.cs` | Part 3: `AvlTree`, `MeshGraph` (BFS, Prim's MST) |
| `wwwroot/index.html` | The frontend |

4. Part 3 data structure explanations

4.1 AVL tree: historical telemetry (`Mesh.cs`)

**Role.** Every processed reading is inserted into a per-device AVL tree keyed by timestamp (`Dictionary<string, AvlTree<DateTime, Reading>>` in `Hub`). It answers questions like "all moisture readings for Node A between 14:00 and 16:00".

**Why an AVL tree.** A plain list must be scanned end to end for every range query, which is O(n). A binary search tree is faster but degrades into a linked list if timestamps arrive in order, and telemetry always arrives in time order. An AVL tree rebalances after each insert, so its height stays at most about 1.44 log2(n). Inserts are O(log n) and a range query is O(log n + k), where k is the number of readings returned.

**Balancing.** After inserting, each node's height is updated and its balance factor (left height minus right height) is checked. If it leaves the range -1 to 1, a single or double rotation restores it:

```csharp
int balance = H(n.Left) - H(n.Right);
if (balance > 1)   // left heavy
{
    if (H(n.Left!.Left) < H(n.Left.Right)) n.Left = RotateLeft(n.Left);   // left-right case
    return RotateRight(n);
}
```

**Range query.** The walk skips any subtree that cannot contain results:

```csharp
if (lo.CompareTo(n.Key) < 0) Walk(n.Left, lo, hi, result, ref visited);   // only go left if keys there could be >= lo
if (lo.CompareTo(n.Key) <= 0 && n.Key.CompareTo(hi) <= 0) result.Add(n.Value);
if (n.Key.CompareTo(hi) < 0) Walk(n.Right, lo, hi, result, ref visited);  // only go right if keys there could be <= hi
```

The `visited` counter is returned to the UI, which prints "visited X of N nodes" so the saving over scanning the whole list is visible. The earlier `SortedDictionary` of all history (Part 2) is kept for the live graph.

4.2 Graph: the mesh network (`MeshGraph`)

**Role.** Vertices are the ESP32 sensors plus a `GATEWAY` vertex. Edges are wireless links, each with a cost that stands for power use (lower is better).

**Representation.** An adjacency list: `Dictionary<string, Dictionary<string, double>>`. Links are undirected, so `AddEdge` stores the cost in both directions. An adjacency list suits a sparse mesh: it uses O(V + E) memory, and finding a node's neighbours is a single dictionary lookup, where a matrix would use O(V²).

```csharp
public void AddEdge(string a, string b, double cost)
{
    if (!_adj.ContainsKey(a)) _adj[a] = new();
    if (!_adj.ContainsKey(b)) _adj[b] = new();
    _adj[a][b] = cost; _adj[b][a] = cost;
}
```

A node counts as up only if its device is online (no telemetry for 8 seconds marks it offline). The algorithms take an `up` predicate, so down nodes are simply never traversed.

### 4.3 Breadth-first search: connectivity and detours

**Role.** `Bfs(start, up)` explores outward one hop at a time, recording which node each vertex was reached from.

```csharp
while (queue.Count > 0)
{
    var cur = queue.Dequeue();
    foreach (var nb in Neighbours(cur).Keys)
        if (up(nb) && !parent.ContainsKey(nb)) { parent[nb] = cur; queue.Enqueue(nb); }
}
```

It is used three ways:
- **Connected devices:** every vertex in the result is connected to the start (the gateway or any chosen node), and the hop count is the length of its parent chain.
- **Down or cut-off nodes:** any linked node missing from the gateway's BFS result is either offline or isolated. The UI lists these.
- **Where to hop:** for each down node, the live neighbours' current routes (their parent chains) are shown, giving the detour.

BFS finds the fewest-hop route in O(V + E).

4.4 Minimum spanning tree: Prim's algorithm

**Role.** `Prim` builds the cheapest set of links that connects every reachable node to the gateway. These are the links the mesh would keep active to minimise total power use, and they are drawn as thick teal lines.

**How it works.** Start at the gateway. Repeatedly take the cheapest link that connects a new node to the tree, using a min-heap (`PriorityQueue`) so each choice is O(log E). Total time is O(E log V).

```csharp
heap.Enqueue((root, null), 0);
while (heap.TryDequeue(out var item, out var cost))
{
    if (parent.ContainsKey(item.Node)) continue;     // already in the tree
    parent[item.Node] = item.From; total += cost;
    foreach (var (nb, w) in Neighbours(item.Node))
        if (up(nb) && !parent.ContainsKey(nb)) heap.Enqueue((nb, item.Node), w);
}
```

The result is stored as parent pointers, so a node's power-efficient route is found by following parents to the gateway (`PathToRoot`). With the demo mesh the MST costs 9 in total, against 18 for all links.

**Fewest hops versus MST route.** The route panel shows both. In the demo, Node 2 reaches the gateway directly in one hop (cost 4), while the MST route goes through Node 3 and Node 0 (cost 6). That is expected: an MST minimises the total cost of the whole network, not the cost of every individual route. Dijkstra's algorithm would give the cheapest path per node, and is a sensible extension.

5. Earlier parts (summary)

| Structure | Where it is used |
|---|---|
| `Queue<T>` | FIFO processing of incoming telemetry |
| `PriorityQueue` | Critical alerts (moisture below 20%, power above 2000 W, temperature above 45 °C) are processed immediately |
| `Stack<T>` | Undo history for manual override commands |
| `Dictionary` | Device registry keyed by MAC for O(1) lookup |
| `SortedDictionary` | Timestamp-ordered history for the live graph |
| `HashSet<T>` | Unique active faults, so duplicate disconnect alerts are ignored |
| `TelemetryPacket<T>` | Generic packet for float, int and bool readings without boxing |
| Operator overloading | `PowerMeter` supports `+`, `-` and comparisons |
| Jagged array | `BatchBuffer` collects readings before moving them into a `List<double>` |
| Recursion | `Deployment.Validate` checks a location level by level |

6. API summary

`POST /api/telemetry`, `POST /api/register`, `POST /api/devices/{mac}/attachments`, `GET /api/snapshot`, `POST /api/commands`, `POST /api/commands/undo`, `GET /api/history/{mac}`, `GET /api/history/{mac}/range?from=&to=`, `GET /api/mesh?from=`, `POST /api/mesh/link`, `GET /api/mesh/route/{macOrName}`, `GET /api/aggregate`, `GET /api/deployment`.

7. Limitations

- All data is held in memory and is lost when the app stops. A database would be the next step.
- The AVL tree only grows. There is no pruning of old readings, which a long-running deployment would need.
- The mesh links and costs are entered by hand (or by the simulator). Real nodes would report their neighbours and signal strength.
