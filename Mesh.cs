// ---------- AVL tree: timestamped telemetry with fast range queries ----------
class AvlTree<TKey, TValue> where TKey : IComparable<TKey>
{
    class Node
    {
        public TKey Key; public TValue Value; public Node? Left, Right; public int Height = 1;
        public Node(TKey k, TValue v) { Key = k; Value = v; }
    }
    Node? _root;
    public int Count { get; private set; }
    public int Height => H(_root);

    static int H(Node? n) => n?.Height ?? 0;
    static void Update(Node n) => n.Height = 1 + Math.Max(H(n.Left), H(n.Right));
    static Node RotateRight(Node y) { var x = y.Left!; y.Left = x.Right; x.Right = y; Update(y); Update(x); return x; }
    static Node RotateLeft(Node x) { var y = x.Right!; x.Right = y.Left; y.Left = x; Update(x); Update(y); return y; }

    public void Insert(TKey key, TValue value) => _root = Insert(_root, key, value);

    Node Insert(Node? n, TKey key, TValue value)
    {
        if (n == null) { Count++; return new Node(key, value); }
        int c = key.CompareTo(n.Key);
        if (c < 0) n.Left = Insert(n.Left, key, value);
        else if (c > 0) n.Right = Insert(n.Right, key, value);
        else { n.Value = value; return n; }
        Update(n);
        int balance = H(n.Left) - H(n.Right);
        if (balance > 1)                                   // left heavy
        {
            if (H(n.Left!.Left) < H(n.Left.Right)) n.Left = RotateLeft(n.Left);
            return RotateRight(n);
        }
        if (balance < -1)                                  // right heavy
        {
            if (H(n.Right!.Right) < H(n.Right.Left)) n.Right = RotateRight(n.Right);
            return RotateLeft(n);
        }
        return n;
    }

    // All values with from <= key <= to, in key order. Whole subtrees outside the range are skipped.
    public List<TValue> Range(TKey from, TKey to, out int visited)
    {
        var result = new List<TValue>(); int count = 0;
        Walk(_root, from, to, result, ref count);
        visited = count;
        return result;
    }

    static void Walk(Node? n, TKey lo, TKey hi, List<TValue> result, ref int visited)
    {
        if (n == null) return;
        visited++;
        if (lo.CompareTo(n.Key) < 0) Walk(n.Left, lo, hi, result, ref visited);
        if (lo.CompareTo(n.Key) <= 0 && n.Key.CompareTo(hi) <= 0) result.Add(n.Value);
        if (n.Key.CompareTo(hi) < 0) Walk(n.Right, lo, hi, result, ref visited);
    }
}

record Reading(DateTime Time, string Metric, double Value);
record LinkRequest(string A, string B, double Cost);

// ---------- Weighted undirected graph: the mesh network ----------
class MeshGraph
{
    public const string Gateway = "GATEWAY";
    readonly Dictionary<string, Dictionary<string, double>> _adj = new();   // adjacency list
    public MeshGraph() { _adj[Gateway] = new(); }

    public IEnumerable<string> Nodes => _adj.Keys;
    public IReadOnlyDictionary<string, double> Neighbours(string id) =>
        _adj.TryGetValue(id, out var n) ? n : new Dictionary<string, double>();

    public void AddEdge(string a, string b, double cost)
    {
        if (!_adj.ContainsKey(a)) _adj[a] = new();
        if (!_adj.ContainsKey(b)) _adj[b] = new();
        _adj[a][b] = cost; _adj[b][a] = cost;
    }

    public static string Key(string a, string b) => string.CompareOrdinal(a, b) < 0 ? $"{a}|{b}" : $"{b}|{a}";

    public IEnumerable<(string A, string B, double W)> Edges()
    {
        foreach (var (a, nb) in _adj)
            foreach (var (b, w) in nb)
                if (string.CompareOrdinal(a, b) < 0) yield return (a, b, w);
    }

    // Breadth-first search that only passes through nodes that are up.
    // Returns each reachable node with the node it was reached from (fewest hops).
    public Dictionary<string, string?> Bfs(string start, Func<string, bool> up)
    {
        var parent = new Dictionary<string, string?> { [start] = null };
        var queue = new Queue<string>();
        queue.Enqueue(start);
        while (queue.Count > 0)
        {
            var cur = queue.Dequeue();
            foreach (var nb in Neighbours(cur).Keys)
                if (up(nb) && !parent.ContainsKey(nb)) { parent[nb] = cur; queue.Enqueue(nb); }
        }
        return parent;
    }

    // Prim's algorithm from the gateway using a min-heap (PriorityQueue). Returns the MST as parent pointers.
    public (Dictionary<string, string?> Parent, double Total) Prim(string root, Func<string, bool> up)
    {
        var parent = new Dictionary<string, string?>();
        double total = 0;
        var heap = new PriorityQueue<(string Node, string? From), double>();
        heap.Enqueue((root, null), 0);
        while (heap.TryDequeue(out var item, out var cost))
        {
            if (parent.ContainsKey(item.Node)) continue;
            parent[item.Node] = item.From;
            total += cost;
            foreach (var (nb, w) in Neighbours(item.Node))
                if (up(nb) && !parent.ContainsKey(nb)) heap.Enqueue((nb, item.Node), w);
        }
        return (parent, total);
    }

    public static List<string> PathToRoot(Dictionary<string, string?> parent, string node)
    {
        var path = new List<string>();
        for (string? cur = node; cur != null; cur = parent[cur]) path.Add(cur);
        return path;
    }

    public double PathCost(List<string> path)
    {
        double c = 0;
        for (int i = 0; i + 1 < path.Count; i++) c += _adj[path[i]][path[i + 1]];
        return c;
    }
}
