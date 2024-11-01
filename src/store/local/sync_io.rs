use crate::store::local::{FileStat, LocalIO};
use anyhow::anyhow;
use async_trait::async_trait;
use bytes::Bytes;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;

#[derive(Clone)]
pub struct SyncLocalIO {
    inner: Arc<Inner>,
}

struct Inner {
    root: String,

    buf_writer_capacity: Option<usize>,
    buf_reader_capacity: Option<usize>,
}

impl SyncLocalIO {
    pub fn new(
        root: &str,
        buf_writer_capacity: Option<usize>,
        buf_reader_capacity: Option<usize>,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                root: root.to_owned(),
                buf_writer_capacity,
                buf_reader_capacity,
            }),
        }
    }

    fn with_root(&self, path: &str) -> String {
        format!("{}/{}", &self.inner.root, path)
    }
}

#[async_trait]
impl LocalIO for SyncLocalIO {
    async fn create_dir(&self, dir: &str) -> anyhow::Result<()> {
        fs::create_dir_all(self.with_root(dir))?;
        Ok(())
    }

    async fn append(&self, path: &str, data: Bytes) -> anyhow::Result<()> {
        let path = &self.with_root(path);
        let path = Path::new(path);
        let mut file = OpenOptions::new().append(true).create(true).open(path)?;

        match self.inner.buf_writer_capacity {
            Some(capacity) => {
                let mut buf_writer = BufWriter::with_capacity(capacity, file);
                buf_writer.write_all(&data)?;
                buf_writer.flush()?
            }
            _ => {
                file.write_all(&data)?;
                file.flush()?
            }
        }

        Ok(())
    }

    async fn read(&self, path: &str, offset: i64, length: Option<i64>) -> anyhow::Result<Bytes> {
        let path = &self.with_root(path);
        let path = Path::new(path);
        if length.is_none() {
            let data = fs::read(path)?;
            return Ok(Bytes::from(data));
        }

        let len = length.unwrap() as usize;
        let mut file = File::open(path)?;
        let mut buffer = vec![0; len];

        let bytes_read = match self.inner.buf_reader_capacity {
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
    }

    async fn delete(&self, path: &str) -> anyhow::Result<()> {
        let path = &self.with_root(path);
        let path = Path::new(path);
        if path.is_dir() {
            fs::remove_dir_all(path)?;
        } else if path.is_file() {
            fs::remove_file(path)?;
        }
        Ok(())
    }

    async fn write(&self, path: &str, data: Bytes) -> anyhow::Result<()> {
        let path = self.with_root(path);
        fs::write(path, data)?;
        Ok(())
    }

    async fn file_stat(&self, path: &str) -> anyhow::Result<FileStat> {
        let path = self.with_root(path);
        let meta = fs::metadata(&path)?;
        Ok(FileStat {
            content_length: meta.len(),
        })
    }
}

#[cfg(test)]
mod test {
    use crate::store::local::sync_io::SyncLocalIO;
    use crate::store::local::LocalIO;
    use bytes::Bytes;

    #[tokio::test]
    async fn test_io() -> anyhow::Result<()> {
        let temp_dir = tempdir::TempDir::new("test_sync_io").unwrap();
        let temp_path = temp_dir.path().to_str().unwrap().to_string();
        println!("created the temp file path: {}", &temp_path);

        let data_file_name = "1.data";
        let io_handler = SyncLocalIO::new(&temp_path, None, None);

        // append
        io_handler
            .append(data_file_name, Bytes::from(vec![0; 1000]))
            .await?;
        io_handler
            .append(data_file_name, Bytes::from(vec![0; 1000]))
            .await?;
        io_handler
            .append(data_file_name, Bytes::from(vec![0; 1000]))
            .await?;

        // stat
        let stat = io_handler.file_stat(data_file_name).await?;
        assert_eq!(1000 * 3, stat.content_length);

        // read all
        let data = io_handler.read(data_file_name, 0, None).await?;
        assert_eq!(vec![0; 3000], *data);

        // seek read
        let data = io_handler.read(data_file_name, 10, Some(20)).await?;
        assert_eq!(vec![0; 20], *data);

        // delete
        io_handler.delete(data_file_name).await?;
        match io_handler.file_stat(data_file_name).await {
            Err(_) => {}
            Ok(_) => panic!(),
        };

        Ok(())
    }
}
