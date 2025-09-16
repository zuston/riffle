use crate::actions::Action;
use bytes::Bytes;
use log::info;
use riffle_server::store::hadoop::get_hdfs_client;
use riffle_server::store::DataBytes;
use riffle_server::util;
use std::path::Path;
use url::Url;

pub struct HdfsAppendAction {
    root: String,
    file_name: String,
    total_size: u64,
    batch_size: u64,
}

impl HdfsAppendAction {
    pub fn new(absolute_path: &str, total_size: &str, batch_size: &str) -> Self {
        let total_size = util::to_bytes(total_size);
        let batch_size = util::to_bytes(batch_size);

        let path = Path::new(absolute_path);
        let file_name = path.file_name().unwrap().to_str().unwrap().to_string();
        let root = path.parent().unwrap().to_str().unwrap().to_string();

        info!(
            "root: {}. file_name: {}. total_size: {}. batch_size: {}",
            &root, &file_name, total_size, batch_size
        );
        HdfsAppendAction {
            root,
            file_name,
            total_size,
            batch_size,
        }
    }
}

#[async_trait::async_trait]
impl Action for HdfsAppendAction {
    async fn act(&self) -> anyhow::Result<()> {
        let client = get_hdfs_client(&self.root, Default::default())?;

        info!("Creating file: {}", &self.file_name);
        client.touch(&self.file_name).await?;

        let loop_cnt = self.total_size / self.batch_size;
        info!("Will append {} loop cnt", loop_cnt);

        let test_data = vec![0u8; self.batch_size as usize];
        let test_data = Bytes::from(test_data);

        for idx in 0..loop_cnt {
            info!("Appending with index:{}", idx);
            client
                .append(&self.file_name, test_data.clone().into())
                .await?;
        }

        info!("Finished.");
        Ok(())
    }
}
