use crate::error::WorkerError;
use crate::metric::LOCALFILE_READ_MEMORY_ALLOCATION_LATENCY;
use crate::runtime::RuntimeRef;
use crate::store::local::{FileStat, LocalIO};
use crate::store::BytesWrapper;
use anyhow::anyhow;
use async_trait::async_trait;
use await_tree::InstrumentAwait;
use bytes::Bytes;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;
use std::{fs, io};

#[derive(Clone)]
pub struct SyncLocalIO {
    inner: Arc<Inner>,
}

struct Inner {
    root: String,

    buf_writer_capacity: Option<usize>,
    buf_reader_capacity: Option<usize>,

    read_runtime_ref: RuntimeRef,
    write_runtime_ref: RuntimeRef,
}

impl SyncLocalIO {
    pub fn new(
        read_runtime_ref: &RuntimeRef,
        write_runtime_ref: &RuntimeRef,
        root: &str,
        buf_writer_capacity: Option<usize>,
        buf_reader_capacity: Option<usize>,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                root: root.to_owned(),
                buf_writer_capacity,
                buf_reader_capacity,
                read_runtime_ref: read_runtime_ref.clone(),
                write_runtime_ref: write_runtime_ref.clone(),
            }),
        }
    }

    fn with_root(&self, path: &str) -> String {
        format!("{}/{}", &self.inner.root, path)
    }
}

#[async_trait]
impl LocalIO for SyncLocalIO {
    async fn create_dir(&self, dir: &str) -> anyhow::Result<(), WorkerError> {
        let dir = self.with_root(dir);
        let r = self
            .inner
            .write_runtime_ref
            .spawn_blocking(move || fs::create_dir_all(dir))
            .instrument_await("wait the spawned block future")
            .await??;
        Ok(())
    }

    async fn append(&self, path: &str, data: BytesWrapper) -> anyhow::Result<(), WorkerError> {
        let path = self.with_root(path);
        let buffer_capacity = self.inner.buf_writer_capacity.clone();

        let r = self
            .inner
            .write_runtime_ref
            .spawn_blocking(move || {
                let path = Path::new(&path);
                let mut file = OpenOptions::new().append(true).create(true).open(path)?;
                let mut buf_writer = match buffer_capacity {
                    Some(capacity) => BufWriter::with_capacity(capacity, file),
                    _ => BufWriter::new(file),
                };

                match data {
                    BytesWrapper::Direct(bytes) => buf_writer.write_all(&bytes)?,
                    BytesWrapper::Composed(composed) => {
                        buf_writer.write_all(&composed.freeze())?;
                    }
                }
                buf_writer.flush()?;
                Ok::<(), io::Error>(())
            })
            .instrument_await("wait the spawned block future")
            .await
            .map_err(|e| anyhow!(e))??;

        Ok(())
    }

    async fn read(
        &self,
        path: &str,
        offset: i64,
        length: Option<i64>,
    ) -> anyhow::Result<Bytes, WorkerError> {
        let path = self.with_root(path);
        let buf = self.inner.buf_reader_capacity.clone();

        let r = self
            .inner
            .read_runtime_ref
            .spawn_blocking(move || {
                let path = Path::new(&path);
                if length.is_none() {
                    let data = fs::read(path)?;
                    return Ok(Bytes::from(data));
                }

                let len = length.unwrap() as usize;
                let mut file = File::open(path)?;

                let start = Instant::now();
                let mut buffer = vec![0; len];
                LOCALFILE_READ_MEMORY_ALLOCATION_LATENCY.record(start.elapsed().as_nanos() as u64);

                let bytes_read = match buf {
                    Some(capacity) => {
                        let mut reader = BufReader::with_capacity(capacity, file);
                        reader.seek(SeekFrom::Start(offset as u64))?;
                        reader.read(&mut buffer)?
                    }
                    _ => {
                        file.seek(SeekFrom::Start(offset as u64))?;
                        file.read(&mut buffer)?
                    }
                };

                if bytes_read != len {
                    return Err(anyhow!(format!(
                        "Not expected bytes reading. expected: {}, actual: {}",
                        len, bytes_read
                    )));
                }

                Ok(Bytes::from(buffer))
            })
            .instrument_await("wait the spawned block future")
            .await??;

        Ok(r)
    }

    async fn delete(&self, path: &str) -> anyhow::Result<(), WorkerError> {
        let path = self.with_root(path);

        let r = self
            .inner
            .write_runtime_ref
            .spawn_blocking(move || {
                let path = Path::new(&path);
                if path.is_dir() {
                    fs::remove_dir_all(path)
                } else if path.is_file() {
                    fs::remove_file(path)
                } else {
                    Ok(())
                }
            })
            .await??;

        Ok(())
    }

    async fn write(&self, path: &str, data: Bytes) -> anyhow::Result<(), WorkerError> {
        let path = self.with_root(path);
        let r = self
            .inner
            .write_runtime_ref
            .spawn_blocking(move || fs::write(path, data))
            .await??;
        Ok(())
    }

    async fn file_stat(&self, path: &str) -> anyhow::Result<FileStat, WorkerError> {
        let path = self.with_root(path);
        let r = self
            .inner
            .read_runtime_ref
            .spawn_blocking(move || fs::metadata(&path))
            .await??;
        Ok(FileStat {
            content_length: r.len(),
        })
    }
}

#[cfg(test)]
mod test {
    use crate::runtime::manager::create_runtime;
    use crate::store::local::sync_io::SyncLocalIO;
    use crate::store::local::LocalIO;
    use bytes::Bytes;
    use std::thread::{sleep, Thread};
    use std::time::Duration;

    #[test]
    fn test_io() -> anyhow::Result<()> {
        let base_runtime_ref = create_runtime(2, "base");

        let read_rumtime_ref = create_runtime(1, "read");
        let write_rumtime_ref = create_runtime(1, "write");

        let temp_dir = tempdir::TempDir::new("test_sync_io").unwrap();
        let temp_path = temp_dir.path().to_str().unwrap().to_string();
        println!("created the temp file path: {}", &temp_path);

        let data_file_name = "1.data";
        let io_handler = SyncLocalIO::new(
            &read_rumtime_ref,
            &write_rumtime_ref,
            &temp_path,
            None,
            None,
        );

        // append
        base_runtime_ref
            .block_on(io_handler.append(data_file_name, Bytes::from(vec![0; 1000]).into()))?;
        base_runtime_ref
            .block_on(io_handler.append(data_file_name, Bytes::from(vec![0; 1000]).into()))?;
        base_runtime_ref
            .block_on(io_handler.append(data_file_name, Bytes::from(vec![0; 1000]).into()))?;

        // stat
        let stat = base_runtime_ref.block_on(io_handler.file_stat(data_file_name))?;
        assert_eq!(1000 * 3, stat.content_length);

        // read all
        let data = base_runtime_ref.block_on(io_handler.read(data_file_name, 0, None))?;
        assert_eq!(vec![0; 3000], *data);

        // seek read
        let data = base_runtime_ref.block_on(io_handler.read(data_file_name, 10, Some(20)))?;
        assert_eq!(vec![0; 20], *data);

        // delete
        base_runtime_ref.block_on(io_handler.delete(data_file_name))?;
        match base_runtime_ref.block_on(io_handler.file_stat(data_file_name)) {
            Err(_) => {}
            Ok(_) => panic!(),
        };

        Ok(())
    }

    #[test]
    #[ignore]
    fn test_blocking_operations() -> anyhow::Result<()> {
        let base_runtime_ref = create_runtime(2, "base");
        let runtime_ref = create_runtime(2, "blocking");

        for _ in 0..2 {
            // runtime_ref.spawn(async {
            //     sleep(Duration::from_millis(1000000));
            // });

            runtime_ref.spawn_blocking(move || {
                sleep(Duration::from_millis(1000));
            });
        }

        let mut handles = vec![];
        for _ in 0..10 {
            let h = runtime_ref.spawn(async { 2 });
            handles.push(h);
        }

        let mut sum = 0;
        for handle in handles {
            sum += base_runtime_ref.block_on(handle)?;
        }

        assert_eq!(20, sum);

        Ok(())
    }
}
