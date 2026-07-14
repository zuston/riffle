use bytes::{Buf, BufMut, Bytes, BytesMut};
use criterion::{black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};

const CASES: &[usize] = &[128, 1024, 4096];
const SHUFFLE_SERVER_CASES: &[(usize, usize)] = &[(128, 16), (1024, 16), (4096, 16)];

struct V1Reader {
    buf: BytesMut,
}

impl V1Reader {
    fn new(buf: BytesMut) -> Self {
        Self { buf }
    }

    fn consume_bytes(&mut self, len: usize) -> Bytes {
        self.buf.split_to(len).freeze()
    }

    fn consume_i32(&mut self) -> i32 {
        let bytes = self.consume_bytes(4);
        let mut buf = &bytes[..];
        buf.get_i32()
    }

    fn consume_i64(&mut self) -> i64 {
        let bytes = self.consume_bytes(8);
        let mut buf = &bytes[..];
        buf.get_i64()
    }

    fn consume_string(&mut self) -> String {
        let len = self.consume_i32() as usize;
        let bytes = self.consume_bytes(len);
        String::from_utf8(bytes.to_vec()).expect("valid string")
    }
}

struct V2Reader {
    buf: BytesMut,
}

impl V2Reader {
    fn new(buf: BytesMut) -> Self {
        Self { buf }
    }

    fn consume_bytes(&mut self, len: usize) -> Bytes {
        self.buf.split_to(len).freeze()
    }

    fn consume_i32(&mut self) -> i32 {
        let mut bytes = &self.buf[..4];
        let value = bytes.get_i32();
        self.buf.advance(4);
        value
    }

    fn consume_i64(&mut self) -> i64 {
        let mut bytes = &self.buf[..8];
        let value = bytes.get_i64();
        self.buf.advance(8);
        value
    }

    fn consume_string(&mut self) -> String {
        let len = self.consume_i32() as usize;
        let bytes = self.consume_bytes(len);
        String::from_utf8(bytes.to_vec()).expect("valid string")
    }
}

struct ShuffleServerV1Reader {
    buf: BytesMut,
}

impl ShuffleServerV1Reader {
    fn new(buf: BytesMut) -> Self {
        Self { buf }
    }

    fn consume_i32(&mut self) -> i32 {
        let mut bytes = &self.buf[..4];
        let value = bytes.get_i32();
        self.buf.advance(4);
        value
    }

    fn skip_bytes(&mut self, len: usize) {
        self.buf.advance(len);
    }

    fn skip_string(&mut self) {
        let len = self.consume_i32() as usize;
        self.skip_bytes(len);
    }

    fn skip_shuffle_servers(&mut self, count: usize) -> i64 {
        let mut checksum = 0;

        for _ in 0..count {
            self.skip_string();
            self.skip_string();
            checksum ^= self.consume_i32() as i64;
            checksum ^= self.consume_i32() as i64;
        }

        checksum
    }
}

struct ShuffleServerV2Reader {
    buf: BytesMut,
}

impl ShuffleServerV2Reader {
    fn new(buf: BytesMut) -> Self {
        Self { buf }
    }

    fn consume_i32(&mut self) -> i32 {
        let mut bytes = &self.buf[..4];
        let value = bytes.get_i32();
        self.buf.advance(4);
        value
    }

    fn skip_bytes(&mut self, len: usize) {
        self.buf.advance(len);
    }

    fn skip_string(&mut self) {
        let len = self.consume_i32() as usize;
        self.skip_bytes(len);
    }

    fn skip_shuffle_servers(&mut self, count: usize) -> i64 {
        for _ in 0..count {
            self.skip_string();
            self.skip_string();
            self.skip_bytes(8);
        }

        self.buf.len() as i64
    }
}

fn put_string(buf: &mut BytesMut, value: &str) {
    buf.put_i32(value.len() as i32);
    buf.put_slice(value.as_bytes());
}

fn build_send_shuffle_data_payload(block_count: usize) -> Bytes {
    let mut buf = BytesMut::with_capacity(64 + block_count * 56);

    buf.put_i64(42);
    put_string(&mut buf, "app-streaming-parse");
    buf.put_i32(7);
    buf.put_i64(99);
    buf.put_i32(1);
    buf.put_i32(11);
    buf.put_i32(block_count as i32);

    for block_index in 0..block_count {
        buf.put_i32(11);
        buf.put_i64(block_index as i64);
        buf.put_i32(0);
        buf.put_i32(7);
        buf.put_i64(block_index as i64 * 31);
        buf.put_i64(block_index as i64 * 17);
        buf.put_i32(0);
        buf.put_i32(0);
        buf.put_i32(0);
        buf.put_i64(0);
    }

    buf.put_i64(123456);
    buf.freeze()
}

fn build_shuffle_server_payload(server_count: usize, string_len: usize) -> Bytes {
    let mut buf = BytesMut::with_capacity(server_count * (2 * (4 + string_len) + 8));
    let host = "h".repeat(string_len);
    let server_id = "s".repeat(string_len);

    for index in 0..server_count {
        put_string(&mut buf, &host);
        put_string(&mut buf, &server_id);
        buf.put_i32(18000 + index as i32);
        buf.put_i32(19000 + index as i32);
    }

    buf.freeze()
}

fn parse_with_v1_reader(payload: BytesMut) -> i64 {
    let mut reader = V1Reader::new(payload);
    let mut checksum = 0;

    checksum ^= reader.consume_i64();
    checksum ^= reader.consume_string().len() as i64;
    checksum ^= reader.consume_i32() as i64;
    checksum ^= reader.consume_i64();

    let partition_batch_size = reader.consume_i32();
    for _ in 0..partition_batch_size {
        checksum ^= reader.consume_i32() as i64;
        let block_batch_size = reader.consume_i32();

        for _ in 0..block_batch_size {
            checksum ^= reader.consume_i32() as i64;
            checksum ^= reader.consume_i64();
            checksum ^= reader.consume_i32() as i64;
            checksum ^= reader.consume_i32() as i64;
            checksum ^= reader.consume_i64();
            checksum ^= reader.consume_i64();

            let data_len = reader.consume_i32() as usize;
            if data_len > 0 {
                checksum ^= reader.consume_bytes(data_len).len() as i64;
            }

            let shuffle_server_len = reader.consume_i32();
            for _ in 0..shuffle_server_len {
                checksum ^= reader.consume_string().len() as i64;
                checksum ^= reader.consume_string().len() as i64;
                checksum ^= reader.consume_i32() as i64;
                checksum ^= reader.consume_i32() as i64;
            }

            checksum ^= reader.consume_i32() as i64;
            checksum ^= reader.consume_i64();
        }
    }

    checksum ^= reader.consume_i64();
    checksum
}

fn parse_with_v2_reader(payload: BytesMut) -> i64 {
    let mut reader = V2Reader::new(payload);
    let mut checksum = 0;

    checksum ^= reader.consume_i64();
    checksum ^= reader.consume_string().len() as i64;
    checksum ^= reader.consume_i32() as i64;
    checksum ^= reader.consume_i64();

    let partition_batch_size = reader.consume_i32();
    for _ in 0..partition_batch_size {
        checksum ^= reader.consume_i32() as i64;
        let block_batch_size = reader.consume_i32();

        for _ in 0..block_batch_size {
            checksum ^= reader.consume_i32() as i64;
            checksum ^= reader.consume_i64();
            checksum ^= reader.consume_i32() as i64;
            checksum ^= reader.consume_i32() as i64;
            checksum ^= reader.consume_i64();
            checksum ^= reader.consume_i64();

            let data_len = reader.consume_i32() as usize;
            if data_len > 0 {
                checksum ^= reader.consume_bytes(data_len).len() as i64;
            }

            let shuffle_server_len = reader.consume_i32();
            for _ in 0..shuffle_server_len {
                checksum ^= reader.consume_string().len() as i64;
                checksum ^= reader.consume_string().len() as i64;
                checksum ^= reader.consume_i32() as i64;
                checksum ^= reader.consume_i32() as i64;
            }

            checksum ^= reader.consume_i32() as i64;
            checksum ^= reader.consume_i64();
        }
    }

    checksum ^= reader.consume_i64();
    checksum
}

fn skip_shuffle_servers_with_v1_reader(payload: BytesMut, server_count: usize) -> i64 {
    let mut reader = ShuffleServerV1Reader::new(payload);
    reader.skip_shuffle_servers(server_count)
}

fn skip_shuffle_servers_with_v2_reader(payload: BytesMut, server_count: usize) -> i64 {
    let mut reader = ShuffleServerV2Reader::new(payload);
    reader.skip_shuffle_servers(server_count)
}

fn bench_urpc_streaming_scalar_parse(c: &mut Criterion) {
    let mut group = c.benchmark_group("urpc_streaming_scalar_parse");

    for &block_count in CASES {
        let payload = build_send_shuffle_data_payload(block_count);

        group.bench_with_input(
            BenchmarkId::new("v1_split_freeze", block_count),
            &payload,
            |b, payload| {
                b.iter_batched(
                    || BytesMut::from(payload.as_ref()),
                    |payload| black_box(parse_with_v1_reader(payload)),
                    BatchSize::SmallInput,
                )
            },
        );

        group.bench_with_input(
            BenchmarkId::new("v2_direct_prefix", block_count),
            &payload,
            |b, payload| {
                b.iter_batched(
                    || BytesMut::from(payload.as_ref()),
                    |payload| black_box(parse_with_v2_reader(payload)),
                    BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

fn bench_urpc_streaming_shuffle_server_skip(c: &mut Criterion) {
    let mut group = c.benchmark_group("urpc_streaming_shuffle_server_skip");

    for &(server_count, string_len) in SHUFFLE_SERVER_CASES {
        let payload = build_shuffle_server_payload(server_count, string_len);
        let case_name = format!("servers={server_count}/string_len={string_len}");

        group.bench_with_input(
            BenchmarkId::new("v1_field_by_field", &case_name),
            &payload,
            |b, payload| {
                b.iter_batched(
                    || BytesMut::from(payload.as_ref()),
                    |payload| black_box(skip_shuffle_servers_with_v1_reader(payload, server_count)),
                    BatchSize::SmallInput,
                )
            },
        );

        group.bench_with_input(
            BenchmarkId::new("v2_skip_ports", &case_name),
            &payload,
            |b, payload| {
                b.iter_batched(
                    || BytesMut::from(payload.as_ref()),
                    |payload| black_box(skip_shuffle_servers_with_v2_reader(payload, server_count)),
                    BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_urpc_streaming_scalar_parse,
    bench_urpc_streaming_shuffle_server_skip
);
criterion_main!(benches);
