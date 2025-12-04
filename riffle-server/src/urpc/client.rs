use crate::urpc::command::GetLocalDataRequestCommand;
use crate::urpc::frame::MessageType;
use anyhow::Result;
use bytes::{BufMut, Bytes, BytesMut};
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// This is the simple urpc client, only for tests
pub struct UrpcClient {
    stream: TcpStream,
}

impl UrpcClient {
    pub async fn connect(host: &str, port: usize) -> Result<Self> {
        let addr = format!("{}:{}", host, port);
        let stream = TcpStream::connect(addr).await?;
        Ok(UrpcClient { stream })
    }

    async fn send(&mut self, data: &[u8]) -> Result<()> {
        self.stream.write_all(data).await?;
        self.stream.flush().await?;
        Ok(())
    }

    async fn recv(&mut self, buf: &mut [u8]) -> Result<usize> {
        let n = self.stream.read(buf).await?;
        Ok(n)
    }

    fn put_string(buf: &mut BytesMut, data: &str) -> Result<()> {
        buf.put_i32(data.len() as i32);
        buf.put_slice(data.as_bytes());
        Ok(())
    }

    async fn get_local_shuffle_data_response(
        &mut self,
        request_id: i64,
        _data_req_len: usize,
    ) -> Result<Bytes> {
        use crate::urpc::frame::{get_i32, get_i64, get_string, get_u8};
        use std::io::Cursor;

        // HEADER: 4 (content_len) + 1 (message_type) + 4 (body_len)
        let mut header_buf = [0u8; 9];
        self.stream.read_exact(&mut header_buf).await?;

        let mut cursor = Cursor::new(&header_buf[..]);
        let content_len = get_i32(&mut cursor)?; // content/body length (i32)
        let msg_type = get_u8(&mut cursor)?; // message type (u8)
        let body_len = get_i32(&mut cursor)?; // payload length (i32)

        use crate::urpc::frame::MessageType;
        if msg_type != MessageType::GetLocalDataResponse as u8 {
            return Err(anyhow::anyhow!("unexpected message type: {}", msg_type));
        }

        // Read the response body (content_len bytes)
        let mut body_buf = vec![0u8; content_len as usize];
        self.stream.read_exact(&mut body_buf).await?;
        let mut body_cursor = Cursor::new(&body_buf[..]);
        let resp_request_id = get_i64(&mut body_cursor)?;
        let status_code = get_i32(&mut body_cursor)?;
        let ret_msg = get_string(&mut body_cursor)?;

        if resp_request_id != request_id {
            return Err(anyhow::anyhow!(
                "request id mismatch: expected {}, got {}",
                request_id,
                resp_request_id
            ));
        }
        if status_code != 0 {
            return Err(anyhow::anyhow!(
                "server returned error {}: {}",
                status_code,
                ret_msg
            ));
        }

        // Read the payload data (body_len bytes)
        let mut data_buf = vec![0u8; body_len as usize];
        self.stream.read_exact(&mut data_buf).await?;
        Ok(Bytes::from(data_buf))
    }

    pub async fn get_local_shuffle_data(
        &mut self,
        req: GetLocalDataRequestCommand,
    ) -> Result<Bytes> {
        // compose the request
        // for a simulation client, there is no need to introduce the shared buffer
        let mut body_buffer = BytesMut::new();

        body_buffer.put_i64(req.request_id);
        Self::put_string(&mut body_buffer, &req.app_id)?;
        body_buffer.put_i32(req.shuffle_id);
        body_buffer.put_i32(req.partition_id);
        body_buffer.put_i32(req.partition_num_per_range);
        body_buffer.put_i32(req.partition_num);
        body_buffer.put_i64(req.offset);
        body_buffer.put_i32(req.length);
        body_buffer.put_i64(req.timestamp);

        let body_len = body_buffer.len();

        let mut header_buf = BytesMut::new();
        // msg len should be 0
        header_buf.put_i32(0);
        header_buf.put_u8(MessageType::GetLocalData as u8);
        header_buf.put_i32(body_len as i32);

        self.stream.write_all(&header_buf).await?;
        self.stream.write_all(&body_buffer).await?;
        self.stream.flush().await?;

        let data = self
            .get_local_shuffle_data_response(req.request_id, req.length as usize)
            .await?;
        Ok(data)
    }
}

#[cfg(test)]
mod tests {
    use crate::app_manager::application_identifier::ApplicationId;
    use crate::app_manager::AppManager;
    use crate::config::Config;
    use crate::config_reconfigure::ReconfigurableConfManager;
    use crate::runtime::manager::RuntimeManager;
    use crate::server_state_manager::ServerStateManager;
    use crate::storage::StorageService;
    use crate::urpc::client::UrpcClient;
    use crate::urpc::command::GetLocalDataRequestCommand;
    use crate::util;
    use anyhow::Result;
    use std::thread;
    use std::time::Duration;
    use tokio::sync::broadcast::Sender;

    fn setup_urpc_server(port: u16) -> Result<Sender<()>> {
        let mut config = Config::create_simple_config();
        config.urpc_port = Some(port);

        let reconf_manager = ReconfigurableConfManager::new(&config, None)?;
        let runtime_manager = RuntimeManager::from(config.clone().runtime_config.clone());
        let storage = StorageService::init(&runtime_manager, &config, &reconf_manager);
        let app_manager_ref = AppManager::get_ref(
            runtime_manager.clone(),
            config.clone(),
            &storage,
            &reconf_manager,
        );

        let shutdown_sender = crate::rpc::DefaultRpcService {}.start_urpc(
            &config,
            runtime_manager.clone(),
            app_manager_ref.clone(),
            &ServerStateManager::new(&app_manager_ref, &config),
        )?;

        Ok(shutdown_sender)
    }

    #[test]
    fn test_client() -> Result<()> {
        let port = util::find_available_port().unwrap();
        let _ = setup_urpc_server(port)?;

        // force sleep 1s to wait urpc start
        thread::sleep(Duration::from_secs(1));

        let rt = tokio::runtime::Runtime::new()?;
        let f = rt.block_on(async move {
            let mut client = UrpcClient::connect("0.0.0.0", port as usize).await.unwrap();
            let command = GetLocalDataRequestCommand {
                request_id: 0,
                app_id: ApplicationId::mock().to_string(),
                shuffle_id: 0,
                partition_id: 0,
                partition_num_per_range: 0,
                partition_num: 0,
                offset: 0,
                length: 0,
                timestamp: 0,
            };
            if let Err(e) = client.get_local_shuffle_data(command).await {
                // should throw error due to the invalid app
                println!("error: {}", e);
            } else {
                panic!("should panic");
            }
        });

        Ok(())
    }
}
