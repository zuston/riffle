// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use bytes::Bytes;
use criterion::{
    black_box, criterion_group, criterion_main, BatchSize, BenchmarkGroup, BenchmarkId, Criterion,
    Throughput,
};
use riffle_server::constant::INVALID_BLOCK_ID;
use riffle_server::store::mem::buffer::default_buffer::DefaultMemoryBuffer;
use riffle_server::store::mem::buffer::indexed_buffer::IndexedMemoryBuffer;
use riffle_server::store::mem::buffer::{BufferOptions, MemoryBuffer};
use riffle_server::store::Block;
use std::time::Duration;

const BLOCK_COUNTS: &[usize] = &[1_024, 16_384];
const BLOCK_SIZE: usize = 256;
const APPEND_BATCH_BLOCKS: usize = 128;
const READ_BLOCKS: usize = 256;
const READ_BYTES: i64 = (READ_BLOCKS * BLOCK_SIZE) as i64;

fn create_batches(block_count: usize) -> Vec<Vec<Block>> {
    // Share one payload allocation to isolate buffer metadata and indexing costs.
    let payload = Bytes::from(vec![0_u8; BLOCK_SIZE]);
    let mut batches = Vec::with_capacity(block_count.div_ceil(APPEND_BATCH_BLOCKS));

    for batch_start in (0..block_count).step_by(APPEND_BATCH_BLOCKS) {
        let batch_end = (batch_start + APPEND_BATCH_BLOCKS).min(block_count);
        let mut blocks = Vec::with_capacity(batch_end - batch_start);
        for block_id in batch_start..batch_end {
            blocks.push(Block {
                block_id: block_id as i64,
                length: BLOCK_SIZE as i32,
                uncompress_length: BLOCK_SIZE as i32,
                crc: 0,
                data: payload.clone(),
                task_attempt_id: block_id as i64,
            });
        }
        batches.push(blocks);
    }

    batches
}

fn append_batches<B>(buffer: &B, batches: Vec<Vec<Block>>)
where
    B: MemoryBuffer + Send + Sync,
{
    for blocks in batches {
        let size = blocks.len() * BLOCK_SIZE;
        buffer
            .append(blocks, size as u64)
            .expect("benchmark blocks should be appended");
    }
}

fn create_buffer<B>(block_count: usize) -> B
where
    B: MemoryBuffer + Send + Sync,
{
    let buffer = B::new(BufferOptions::default());
    append_batches(&buffer, create_batches(block_count));
    buffer
}

fn assert_get_result<B>(buffer: &B, last_block_id: i64)
where
    B: MemoryBuffer + Send + Sync,
{
    let result = buffer
        .get(last_block_id, READ_BYTES, None)
        .expect("benchmark read should succeed");
    assert_eq!(result.shuffle_data_block_segments.len(), READ_BLOCKS);
    assert_eq!(result.data.len(), READ_BYTES as usize);
}

fn bench_get_case<B>(
    group: &mut BenchmarkGroup<'_, criterion::measurement::WallTime>,
    implementation: &str,
    block_count: usize,
    cursor_name: &str,
    last_block_id: i64,
) where
    B: MemoryBuffer + Send + Sync,
{
    let buffer = create_buffer::<B>(block_count);
    assert_get_result(&buffer, last_block_id);

    let case = format!("blocks={block_count}/state=staging/cursor={cursor_name}");
    group.bench_function(BenchmarkId::new(implementation, case), |b| {
        b.iter(|| {
            let result = buffer
                .get(black_box(last_block_id), black_box(READ_BYTES), None)
                .expect("benchmark read should succeed");
            black_box(result);
        });
    });
}

fn bench_memory_buffer_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_buffer/get");
    group.throughput(Throughput::Bytes(READ_BYTES as u64));

    for &block_count in BLOCK_COUNTS {
        let cursor_cases = [
            ("start", INVALID_BLOCK_ID),
            ("middle", block_count as i64 / 2 - 1),
            ("tail", block_count as i64 - READ_BLOCKS as i64 - 1),
            ("missing", block_count as i64 + 1),
        ];

        for &(cursor_name, last_block_id) in &cursor_cases {
            bench_get_case::<DefaultMemoryBuffer>(
                &mut group,
                "default",
                block_count,
                cursor_name,
                last_block_id,
            );
            bench_get_case::<IndexedMemoryBuffer>(
                &mut group,
                "indexed",
                block_count,
                cursor_name,
                last_block_id,
            );
        }
    }

    group.finish();
}

fn bench_append_case<B>(
    group: &mut BenchmarkGroup<'_, criterion::measurement::WallTime>,
    implementation: &str,
    block_count: usize,
) where
    B: MemoryBuffer + Send + Sync,
{
    group.bench_function(
        BenchmarkId::new(implementation, format!("blocks={block_count}")),
        |b| {
            b.iter_batched(
                || {
                    (
                        B::new(BufferOptions::default()),
                        create_batches(block_count),
                    )
                },
                |(buffer, batches)| {
                    append_batches(&buffer, batches);
                    black_box(
                        buffer
                            .staging_size()
                            .expect("benchmark staging size should be available"),
                    );
                    // Return ownership so Criterion drops the fixture after timing.
                    black_box(buffer)
                },
                BatchSize::LargeInput,
            );
        },
    );
}

fn bench_memory_buffer_append(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_buffer/append");

    for &block_count in BLOCK_COUNTS {
        group.throughput(Throughput::Elements(block_count as u64));
        bench_append_case::<DefaultMemoryBuffer>(&mut group, "default", block_count);
        bench_append_case::<IndexedMemoryBuffer>(&mut group, "indexed", block_count);
    }

    group.finish();
}

fn bench_spill_case<B>(
    group: &mut BenchmarkGroup<'_, criterion::measurement::WallTime>,
    implementation: &str,
    block_count: usize,
) where
    B: MemoryBuffer + Send + Sync,
{
    group.bench_function(
        BenchmarkId::new(implementation, format!("blocks={block_count}")),
        |b| {
            b.iter_batched(
                || create_buffer::<B>(block_count),
                |buffer| {
                    let spill_result = buffer
                        .spill()
                        .expect("benchmark spill should succeed")
                        .expect("benchmark buffer should contain staging data");
                    black_box(spill_result.flight_len());
                    // Return ownership so Criterion drops the fixture after timing.
                    black_box((buffer, spill_result))
                },
                BatchSize::LargeInput,
            );
        },
    );
}

fn bench_memory_buffer_spill(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_buffer/spill");

    for &block_count in BLOCK_COUNTS {
        group.throughput(Throughput::Elements(block_count as u64));
        bench_spill_case::<DefaultMemoryBuffer>(&mut group, "default", block_count);
        bench_spill_case::<IndexedMemoryBuffer>(&mut group, "indexed", block_count);
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3))
        .sample_size(20);
    targets =
        bench_memory_buffer_get,
        bench_memory_buffer_append,
        bench_memory_buffer_spill
}
criterion_main!(benches);
