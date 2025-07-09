use crate::app_manager::SHUFFLE_SERVER_ID;
use crate::config::LogConfig;
use logforth::append::rolling_file::{RollingFileBuilder, Rotation};
use logforth::append::{rolling_file, RollingFile};
use logforth::DropGuard;

const LOG_FILE_NAME: &str = "riffle-server";

pub struct LogService;
impl LogService {
    pub fn init(log: &LogConfig) -> DropGuard {
        let (rolling_writer, _guard) = RollingFileBuilder::new("logs")
            .rotation(Rotation::Daily)
            .filename_prefix(LOG_FILE_NAME)
            .max_file_size(512 * 1024 * 1024)
            .max_log_files(10)
            .build()
            .unwrap();

        logforth::builder()
            .dispatch(|d| d.filter(log::LevelFilter::Info).append(rolling_writer))
            .apply();

        _guard
    }
}
