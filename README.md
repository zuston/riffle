Another implementation of Apache Uniffle shuffle server (Single binary, no extra dependencies and quick)

## Roadmap

- [ ] Support storing data into s3
- [ ] Support single buffer flush
- [ ] Support huge partition limit
- [ ] Quick decommission that will spill data into remote storage like s3
- [ ] Using DirectIO to access file data
- [x] Support customized protocol to interact with netty based uniffle client
- [ ] Support writing multiple replicas by pipeline mode in server side
- [ ] Create the grafana template to show the metrics dashboard by the unified style
- [ ] Introduce the clippy to validate
- [ ] Zero copy for **urpc** and mem + localfile getting/writing
- [ ] Recover when upgrading

## Benchmark report

#### Environment

| type                | description                                                             |
|---------------------|:------------------------------------------------------------------------|
| Software            | Uniffle 0.8.0 / Hadoop 3.2.2 / Spark 3.1.2                              |
| Hardware            | Machine 96 cores, 512G memory, 1T * 4 SATA SSD, network bandwidth 8GB/s |
| Hadoop Yarn Cluster | 1 * ResourceManager + 40 * NodeManager, every machine 1T * 4 SATA SSD   |
| Uniffle Cluster     | 1 * Coordinator + 1 * Shuffle Server, every machine 1T * 4 SATA SSD     |

#### Configuration

__spark's conf__
```yaml
spark.executor.instances 400
spark.executor.cores 1
spark.executor.memory 2g
spark.shuffle.manager org.apache.spark.shuffle.RssShuffleManager
spark.rss.storage.type MEMORY_LOCALFILE
```

__Rust-based shuffle-server conf__
```
store_type = "MEMORY_LOCALFILE"
grpc_port = 21100
coordinator_quorum = ["xxxxx:21000"]
tags = ["riffle2", "datanode", "GRPC", "ss_v5"]

[memory_store]
capacity = "10G"
dashmap_shard_amount = 128

[localfile_store]
data_paths = ["/data1/uniffle/t1", "/data2/uniffle/t1", "/data3/uniffle/t1", "/data4/uniffle/t1"]
healthy_check_min_disks = 0
disk_max_concurrency = 2000

[hybrid_store]
memory_spill_high_watermark = 0.5
memory_spill_low_watermark = 0.2
memory_spill_max_concurrency = 1000

[metrics]
http_port = 19998
push_gateway_endpoint = "http://xxxxx/prometheus/pushgateway"

[runtime_config]
read_thread_num = 40
write_thread_num = 200
grpc_thread_num = 100
http_thread_num = 10
default_thread_num = 20
dispatch_thread_num = 10
```
`GRPC_PARALLELISM=100 WORKER_IP=10.0.0.1 RUST_LOG=info ./uniffle-worker`

#### TeraSort Result

| type/buffer capacity                  | 273G (compressed)  |
|---------------------------------------|:------------------:|
| vanilla spark ESS                     | 4.2min (1.3m/2.9m) |
|                                       |                    |
| riffle(grpc) / 10g                    | 4.0min (1.9m/2.1m) |
| riffle(grpc) / 300g                   | 3.5min (1.4m/2.1m) |
|                                       |                    |
| riffle(urpc) / 10g                    | 3.8min (1.6m/2.2m) |
| riffle(urpc) / 300g                   | 3.2min (1.2m/2.0m) |
|                                       |                    |
| uniffle(grpc)/ 10g                    | 4.0min (1.8m/2.2m) |
| uniffle(grpc)/ 300g                   | 8.6min (2.7m/5.9m) |
|                                       |                    |
| uniffle(netty)(default malloc) 10g    | 5.1min (2.7m/2.4m) |
| uniffle(netty)(jemalloc) 10g          | 4.5min (2.0m/2.5m) |
| uniffle(netty)(default malloc)/ 300g  | 4.0min (1.5m/2.5m) |
| uniffle(netty)(jemalloc)/ 300g        | 6.6min (1.9m/4.7m) |

> tips: the riffle's urpc implements the customized tcp stream's proto, that is named with the NETTY rpc type in java side. 

## Build

`cargo build --release --features hdfs,jemalloc`

Uniffle-x currently treats all compiler warnings as error, with some dead-code warning excluded. When you are developing
and really want to ignore the warnings for now, you can use `cargo --config 'build.rustflags=["-W", "warnings"]' build`
to restore the default behavior. However, before submit your pr, you should fix all the warnings.

## Run

`WORKER_IP={ip} RUST_LOG=info WORKER_CONFIG_PATH=./config.toml ./uniffle-worker`

### HDFS Setup

Benefit from the hdfs-native crate, there is no need to setup the JAVA_HOME and relative dependencies.
If HDFS store is valid, the spark client must specify the conf of `spark.rss.client.remote.storage.useLocalConfAsDefault=true`

```shell
cargo build --features hdfs --release
```

```shell
# configure the kerberos
KRB5_CONFIG=/etc/krb5.conf KRB5CCNAME=/tmp/krb5cc_2002 LOG=info ./uniffle-worker
```

## Profiling

### Heap profiling
1. build with profile support
    ```shell
    cargo build --release --features memory-prof
    ```
2. Start with profile
    ```shell
    curl localhost:20010/debug/heap/profile > heap.pb.gz
    go tool pprof -http="0.0.0.0:8081" heap.pb.gz
    ```
   
### CPU Profiling
1. build with jemalloc feature
    ```shell
    cargo build --release --features jemalloc
    ```
2. Paste following command to get cpu profile flamegraph
    ```shell
    go tool pprof -http="0.0.0.0:8081" http://{remote_ip}:8080/debug/pprof/profile?seconds=30
    ```
   - localhost:8080: riffle server.
   - remote_ip: pprof server address.
   - seconds=30: Profiling lasts for 30 seconds.

   Then open the URL <your-ip>:8081/ui/flamegraph in your browser to view the flamegraph:
