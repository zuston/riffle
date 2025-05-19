use criterion::{black_box, criterion_group, criterion_main, Criterion};
use riffle_server::config_ref::{ByteString, ConfRef, DynamicConfRef};

#[inline]
fn reconf_bytestring(batch: usize) {
    let reconf_ref = DynamicConfRef::new("key", ByteString::new("19M"));
    for _ in 0..batch {
        let _ = reconf_ref.get().as_u64();
    }
}

pub fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("conf_bytestring", |b| {
        b.iter(|| reconf_bytestring(black_box(2000)))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
