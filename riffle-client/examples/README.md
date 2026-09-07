# MapReduce Word Count Example

[`map_reduce.rs`](./map_reduce.rs) demonstrates the complete boundary between the three client roles with a word-count job:

1. `Driver` creates an application and shuffle, returning a serializable `ShuffleHandle`.
2. A JSON round trip simulates an execution engine placing the handle in task metadata.
3. Three map tasks independently construct `ShuffleWriter` instances and hash words into two reduce partitions.
4. The simulated scheduler collects accepted attempt IDs from successful `MapOutput` values.
5. Two reduce tasks independently construct `ShuffleReader` instances, filter attempts, and produce the final aggregation.

Riffle only sees opaque bytes. The word-count codec, partition function, and attempt allocation belong to the simulated execution engine.

## Run with no setup

Without arguments, the example starts an in-process mini shuffle server and mock coordinator:

```bash
cargo run -p riffle-client --example map_reduce
```

The final output should be:

```text
Reduce output:
  apple: 3
  banana: 4
  orange: 2
  pear: 2
```

The in-process cluster is only used for demonstrations and tests. Its server dependency is a `dev-dependency` and does not enter the normal `riffle-client` dependency tree.

## Connect to an existing cluster

Pass the coordinator endpoint as the only argument:

```bash
cargo run -p riffle-client --example map_reduce -- http://127.0.0.1:20010
```

The shuffle server hosts and ports returned by the coordinator must be reachable from the machine running the example.
