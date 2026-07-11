use anyhow::Result;
use bytes::{Bytes, BytesMut};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use riffle_server::urpc::frame::write_composed_bytes_for_bench;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::Runtime;

const CASES: &[(usize, usize)] = &[(16, 64 * 1024), (256, 4 * 1024), (1024, 1024)];

fn build_chunks(chunk_count: usize, chunk_size: usize) -> Vec<Bytes> {
    (0..chunk_count)
        .map(|index| Bytes::from(vec![(index % 251) as u8; chunk_size]))
        .collect()
}

async fn create_writer() -> Result<(TcpStream, tokio::task::JoinHandle<()>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let reader = TcpStream::connect(address).await?;
    let (writer, _) = listener.accept().await?;

    let reader_task = tokio::spawn(async move {
        let mut reader = reader;
        let mut buffer = BytesMut::with_capacity(256 * 1024);
        loop {
            match reader.read_buf(&mut buffer).await {
                Ok(0) | Err(_) => break,
                Ok(_) => buffer.clear(),
            }
        }
    });

    Ok((writer, reader_task))
}

async fn write_frozen(stream: &mut TcpStream, chunks: &[Bytes]) -> Result<()> {
    let mut buffer = BytesMut::with_capacity(chunks.iter().map(Bytes::len).sum());
    for chunk in chunks {
        buffer.extend_from_slice(chunk);
    }
    let bytes = buffer.freeze();
    stream.write_all(&bytes).await?;
    Ok(())
}

async fn write_chunked(stream: &mut TcpStream, chunks: &[Bytes]) -> Result<()> {
    for chunk in chunks {
        stream.write_all(chunk).await?;
    }
    Ok(())
}

fn finish_writer(runtime: &Runtime, writer: TcpStream, reader_task: tokio::task::JoinHandle<()>) {
    drop(writer);
    runtime.block_on(async {
        let _ = reader_task.await;
    });
}

fn bench_urpc_composed_write(c: &mut Criterion) {
    let runtime = Runtime::new().expect("benchmark runtime should be created");
    let mut group = c.benchmark_group("urpc_composed_write");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(5));

    for &(chunk_count, chunk_size) in CASES {
        let chunks = build_chunks(chunk_count, chunk_size);
        let case_name = format!("chunks={chunk_count}/chunk_size={chunk_size}");

        let (mut writer, reader_task) = runtime
            .block_on(create_writer())
            .expect("freeze benchmark connection should be created");
        group.bench_function(BenchmarkId::new("freeze", &case_name), |b| {
            b.iter(|| {
                let result = runtime.block_on(write_frozen(&mut writer, black_box(&chunks)));
                black_box(result).expect("freeze write should succeed");
            });
        });
        finish_writer(&runtime, writer, reader_task);

        let (mut writer, reader_task) = runtime
            .block_on(create_writer())
            .expect("chunked benchmark connection should be created");
        group.bench_function(BenchmarkId::new("write_all_per_chunk", &case_name), |b| {
            b.iter(|| {
                let result = runtime.block_on(write_chunked(&mut writer, black_box(&chunks)));
                black_box(result).expect("chunked write should succeed");
            });
        });
        finish_writer(&runtime, writer, reader_task);

        let (mut writer, reader_task) = runtime
            .block_on(create_writer())
            .expect("vectored benchmark connection should be created");
        group.bench_function(BenchmarkId::new("vectored", &case_name), |b| {
            b.iter(|| {
                let result = runtime.block_on(write_composed_bytes_for_bench(
                    &mut writer,
                    black_box(&chunks),
                ));
                black_box(result).expect("vectored write should succeed");
            });
        });
        finish_writer(&runtime, writer, reader_task);
    }

    group.finish();
}

criterion_group!(benches, bench_urpc_composed_write);
criterion_main!(benches);
