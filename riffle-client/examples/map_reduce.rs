// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

mod support;

use bytes::Bytes;
use futures::TryStreamExt;
use riffle_client::{
    ApplicationId, ApplicationSpec, BlockPayload, Driver, DriverConfig, MapOutput, PartitionId,
    ReadPartitionRequest, RiffleError, ShuffleHandle, ShuffleId, ShuffleReader,
    ShuffleReaderConfig, ShuffleSpec, ShuffleWriter, ShuffleWriterConfig, TaskAttemptId,
};
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::Write;
use std::io;
use std::time::{SystemTime, UNIX_EPOCH};

const REDUCE_PARTITIONS: u32 = 2;
const INPUT_SHARDS: [&str; 3] = [
    "apple orange apple banana",
    "banana apple pear",
    "orange banana banana pear",
];

type DynError = Box<dyn Error + Send + Sync>;

#[tokio::main]
async fn main() -> Result<(), DynError> {
    let mut args = std::env::args().skip(1);
    let endpoint_arg = args.next();
    if let Some(unexpected) = args.next() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("unexpected argument {unexpected}; expected at most one coordinator endpoint"),
        )
        .into());
    }

    let demo_cluster = if endpoint_arg.is_none() {
        println!("Starting an embedded demo cluster");
        Some(support::DemoCluster::start().await?)
    } else {
        None
    };
    let coordinator_endpoint = endpoint_arg.as_deref().unwrap_or_else(|| {
        demo_cluster
            .as_ref()
            .expect("embedded cluster is present")
            .coordinator_endpoint()
    });

    let driver = Driver::connect(DriverConfig::new(vec![coordinator_endpoint.to_string()])).await?;
    let application_id = ApplicationId::new(format!(
        "riffle-map-reduce-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
    ))?;
    let session = driver
        .open_application(ApplicationSpec::new(application_id, "map-reduce-example"))
        .await?;
    let handle = session
        .create_shuffle(ShuffleSpec::new(ShuffleId::new(1)?, REDUCE_PARTITIONS))
        .await?;

    // This round trip represents an engine serializing the handle into task metadata.
    let serialized_handle = serde_json::to_vec(&handle)?;
    let worker_handle: ShuffleHandle = serde_json::from_slice(&serialized_handle)?;
    println!(
        "Driver created a {}-byte ShuffleHandle for {} reduce partitions",
        serialized_handle.len(),
        REDUCE_PARTITIONS
    );

    let job_result = run_word_count(worker_handle).await;
    let cleanup_result = session.close().await;
    let output = job_result?;
    cleanup_result?;

    let expected = BTreeMap::from([
        ("apple".to_string(), 3),
        ("banana".to_string(), 4),
        ("orange".to_string(), 2),
        ("pear".to_string(), 2),
    ]);
    if output != expected {
        return Err(invalid_data(format!(
            "word count mismatch: expected {expected:?}, received {output:?}"
        ))
        .into());
    }

    println!("Reduce output:");
    for (word, count) in output {
        println!("  {word}: {count}");
    }
    Ok(())
}

async fn run_word_count(handle: ShuffleHandle) -> Result<BTreeMap<String, u64>, DynError> {
    let mut map_outputs = Vec::with_capacity(INPUT_SHARDS.len());
    for (map_index, input) in INPUT_SHARDS.iter().enumerate() {
        let task_attempt_id = TaskAttemptId::new(map_index as u64 + 1);
        let output = run_map_task(handle.clone(), task_attempt_id, input).await?;
        println!(
            "Map attempt {} wrote {} blocks and {} bytes",
            output.task_attempt_id, output.blocks_written, output.bytes_written
        );
        map_outputs.push(output);
    }

    // A real scheduler accepts one successful physical attempt for each logical map task.
    let accepted_attempts = map_outputs
        .iter()
        .map(|output| output.task_attempt_id)
        .collect::<Vec<_>>();

    let mut output = BTreeMap::new();
    for partition in 0..REDUCE_PARTITIONS {
        let partial = run_reduce_task(
            handle.clone(),
            PartitionId::new(partition),
            accepted_attempts.clone(),
        )
        .await?;
        for (word, count) in partial {
            let total = output.entry(word).or_insert(0_u64);
            *total = total
                .checked_add(count)
                .ok_or_else(|| invalid_data("reduce count overflowed u64"))?;
        }
    }
    Ok(output)
}

async fn run_map_task(
    handle: ShuffleHandle,
    task_attempt_id: TaskAttemptId,
    input: &str,
) -> Result<MapOutput, RiffleError> {
    let writer = ShuffleWriter::from_handle(handle, ShuffleWriterConfig::default())?;
    let mut attempt = writer.open_attempt(task_attempt_id)?;
    let mut partitioned = BTreeMap::<PartitionId, BTreeMap<String, u64>>::new();

    for (word, count) in local_word_count(input) {
        let partition_id = partition_for(&word, REDUCE_PARTITIONS);
        partitioned
            .entry(partition_id)
            .or_default()
            .insert(word, count);
    }

    for (partition_id, counts) in partitioned {
        let record_count = counts.len() as u64;
        let mut payload = BlockPayload::new(encode_counts(&counts))?;
        payload.record_count = Some(record_count);
        attempt.push(partition_id, vec![payload]).await?;
    }
    attempt.finish().await
}

async fn run_reduce_task(
    handle: ShuffleHandle,
    partition_id: PartitionId,
    accepted_attempts: Vec<TaskAttemptId>,
) -> Result<BTreeMap<String, u64>, DynError> {
    let reader = ShuffleReader::from_handle(handle, ShuffleReaderConfig::default())?;
    let mut blocks = reader
        .read_partition(ReadPartitionRequest::new(partition_id, accepted_attempts))
        .await?;
    let mut output = BTreeMap::new();

    while let Some(block) = blocks.try_next().await? {
        for (word, count) in decode_counts(&block.data)? {
            if partition_for(&word, REDUCE_PARTITIONS) != partition_id {
                return Err(invalid_data(format!(
                    "word {word} was routed to the wrong reduce partition"
                ))
                .into());
            }
            let total = output.entry(word).or_insert(0_u64);
            *total = total
                .checked_add(count)
                .ok_or_else(|| invalid_data("reduce count overflowed u64"))?;
        }
    }
    Ok(output)
}

fn local_word_count(input: &str) -> BTreeMap<String, u64> {
    let mut counts = BTreeMap::new();
    for word in input.split_whitespace() {
        *counts.entry(word.to_string()).or_insert(0) += 1;
    }
    counts
}

fn partition_for(key: &str, partition_count: u32) -> PartitionId {
    const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;

    let hash = key.as_bytes().iter().fold(FNV_OFFSET_BASIS, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(FNV_PRIME)
    });
    PartitionId::new((hash % u64::from(partition_count)) as u32)
}

fn encode_counts(counts: &BTreeMap<String, u64>) -> Bytes {
    let mut encoded = String::new();
    for (word, count) in counts {
        writeln!(&mut encoded, "{word}\t{count}").expect("writing to a String cannot fail");
    }
    Bytes::from(encoded)
}

fn decode_counts(data: &Bytes) -> io::Result<Vec<(String, u64)>> {
    let text = std::str::from_utf8(data)
        .map_err(|error| invalid_data(format!("shuffle block is not UTF-8: {error}")))?;
    text.lines()
        .map(|line| {
            let (word, count) = line
                .split_once('\t')
                .ok_or_else(|| invalid_data(format!("invalid map record {line:?}")))?;
            let count = count.parse::<u64>().map_err(|error| {
                invalid_data(format!("invalid count in map record {line:?}: {error}"))
            })?;
            Ok((word.to_string(), count))
        })
        .collect()
}

fn invalid_data(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn map_codec_round_trips_combined_counts() {
        let counts = BTreeMap::from([("apple".to_string(), 2), ("pear".to_string(), 1)]);
        let decoded = decode_counts(&encode_counts(&counts))
            .unwrap()
            .into_iter()
            .collect::<BTreeMap<_, _>>();

        assert_eq!(decoded, counts);
    }

    #[test]
    fn partitioning_is_deterministic_and_bounded() {
        let first = partition_for("apple", REDUCE_PARTITIONS);

        assert_eq!(first, partition_for("apple", REDUCE_PARTITIONS));
        assert!(first.value() < REDUCE_PARTITIONS);
    }
}
