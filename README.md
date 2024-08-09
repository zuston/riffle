Another implementation of Apache Uniffle shuffle server (Single binary, no extra dependencies and quick)

## Roadmap
1. Support storing data into s3
2. Support single buffer flush
3. Support huge partition limit
4. Quick decommission that will spill data into remote storage like s3
5. Using DirectIO to access file data
6. Support netty protocol to interact with netty based uniffle client

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
coordinator_quorum = ["master01-bdxs-t1.qiyi.hadoop:21000"]
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
push_gateway_endpoint = "http://prometheus-api.qiyi.domain/elastic-yarn/pushgateway"

[runtime_config]
read_thread_num = 40
write_thread_num = 200
grpc_thread_num = 100
http_thread_num = 10
default_thread_num = 20
dispatch_thread_num = 10
``` 
`GRPC_PARALLELISM=100 WORKER_IP=10.74.44.129 RUST_LOG=info ./uniffle-worker`

#### TeraSort Result

| type/buffer capacity             | 273G (compressed)  |
|----------------------------------|:------------------:|
| vanilla spark ESS                | 4.2min (1.3m/2.9m) | 
| rust based shuffle server / 10g  | 4.0min (1.9m/2.1m) |
| rust based shuffle server / 300g | 3.5min (1.4m/2.1m) |


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
# configure the kerberos and conf env
HADOOP_CONF_DIR=/etc/hadoop/conf KRB5_CONFIG=/etc/krb5.conf KRB5CCNAME=/tmp/krb5cc_2002 LOG=info ./uniffle-worker
```

## Profiling
   
### Heap profiling
1. build with profile support
    ```shell
    cargo build --release --features memory-prof
    ```
2. Start with profile
    ```shell
    _RJEM_MALLOC_CONF=prof:true,prof_prefix:jeprof.out ./uniffle-worker
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
   