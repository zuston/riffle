use bytes::{Bytes, BytesMut};
use criterion::{criterion_group, criterion_main, Criterion};
use prost::Message;
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;

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

    let nested_buf = nested.encode_to_vec();
    let flat_buf = flat.encode_to_vec();

    c.bench_function("decode_nested_2000_blocks_1b", |b| {
        b.iter(|| {
            let _msg = SendShuffleDataRequestNested::decode(&*nested_buf).unwrap();
        })
    });

    c.bench_function("decode_flat_2000_blocks_1b", |b| {
        b.iter(|| {
            let _msg = SendShuffleDataRequestFlat::decode(&*flat_buf).unwrap();
        })
    });
}

criterion_group!(benches, bench_protobuf_decode);
criterion_main!(benches);