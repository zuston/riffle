use anyhow::Result;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use criterion::{criterion_group, criterion_main, Criterion};
use prost::Message;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use riffle_server::urpc::frame::{get_bytes, get_i32};
use std::io::Cursor;

#[derive(Clone, PartialEq, Message)]
pub struct SendShuffleDataRequestNested {
    #[prost(message, repeated, tag = "4")]
    pub shuffle_data: Vec<ShuffleDataNested>,
}

#[derive(Clone, PartialEq, Message)]
pub struct ShuffleDataNested {
    #[prost(message, repeated, tag = "2")]
    pub block: Vec<ShuffleBlockNested>,
}

#[derive(Clone, PartialEq, Message)]
pub struct ShuffleBlockNested {
    #[prost(int32, tag = "1")]
    pub partition_id: i32,
    #[prost(bytes, tag = "5")]
    pub data: Bytes,
}

#[derive(Clone, PartialEq, Message)]
pub struct SendShuffleDataRequestFlat {
    #[prost(message, tag = "4")]
    pub shuffle_data: Option<ShuffleDataFlat>,
}

#[derive(Clone, PartialEq, Message)]
pub struct ShuffleDataFlat {
    #[prost(int32, repeated, tag = "1")]
    pub partition_ids: Vec<i32>,
    #[prost(bytes, tag = "5")]
    pub combined_data: Bytes,
}
fn put_bytes(buf: &mut BytesMut, bytes: &Bytes) {
    if bytes.is_empty() {
        buf.put_i32(0);
    } else {
        buf.put_i32(bytes.len() as i32);
        buf.put_slice(bytes);
    }
}

fn decode_urpc_message(data: &[u8]) -> Result<()> {
    let mut cursor = Cursor::new(data);
    let _partition_id = get_i32(&mut cursor)?;
    let _shuffle_bytes = get_bytes(&mut cursor)?;
    Ok(())
}

fn generate_urpc_message(num_blocks: usize, payload_size: usize) -> Bytes {
    let mut buf = BytesMut::new();
    let mut rng = StdRng::seed_from_u64(42);

    for index in 0..num_blocks {
        let mut data = vec![0u8; payload_size];
        rng.fill(&mut data[..]);
        let data = Bytes::from(data);
        buf.put_i32(index as i32);
        put_bytes(&mut buf, &data);
    }

    buf.freeze()
}

fn generate_nested_message(num_blocks: usize, payload_size: usize) -> SendShuffleDataRequestNested {
    let mut blocks = Vec::with_capacity(num_blocks);
    let mut rng = StdRng::seed_from_u64(42);

    for i in 0..num_blocks {
        let mut data = vec![0u8; payload_size];
        rng.fill(&mut data[..]);
        blocks.push(ShuffleBlockNested {
            partition_id: i as i32,
            data: Bytes::from(data),
        });
    }

    SendShuffleDataRequestNested {
        shuffle_data: vec![ShuffleDataNested { block: blocks }],
    }
}

fn generate_flat_message(num_blocks: usize, payload_size: usize) -> SendShuffleDataRequestFlat {
    let mut partition_ids = Vec::with_capacity(num_blocks);
    let mut combined_data = BytesMut::with_capacity(num_blocks * payload_size);
    let mut rng = StdRng::seed_from_u64(42);

    for i in 0..num_blocks {
        let mut data = vec![0u8; payload_size];
        rng.fill(&mut data[..]);
        partition_ids.push(i as i32);
        combined_data.extend_from_slice(&data);
    }

    SendShuffleDataRequestFlat {
        shuffle_data: Some(ShuffleDataFlat {
            partition_ids,
            combined_data: combined_data.freeze(),
        }),
    }
}

fn bench_protobuf_decode(c: &mut Criterion) {
    let nested = generate_nested_message(2000, 300);
    let flat = generate_flat_message(2000, 300);
    let urpc = generate_urpc_message(2000, 300).to_vec();

    let nested_buf = nested.encode_to_vec();
    let flat_buf = flat.encode_to_vec();

    c.bench_function("grpc_decode_nested", |b| {
        b.iter(|| {
            let _msg = SendShuffleDataRequestNested::decode(&*nested_buf).unwrap();
        })
    });

    c.bench_function("grpc_decode_flat", |b| {
        b.iter(|| {
            let _msg = SendShuffleDataRequestFlat::decode(&*flat_buf).unwrap();
        })
    });

    c.bench_function("urpc_decode", |b| {
        b.iter(|| {
            let _msg = decode_urpc_message(&*urpc);
        })
    });
}

criterion_group!(benches, bench_protobuf_decode);
criterion_main!(benches);
