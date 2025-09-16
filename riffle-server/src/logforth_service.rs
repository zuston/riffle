use crate::config::{LogConfig, LogLevel, RotationConfig};
use crate::util;
use logforth::append::rolling_file::{RollingFileBuilder, Rotation};
use logforth::append::{rolling_file, RollingFile};
use logforth::DropGuard;

const LOG_FILE_NAME_PREFIX: &str = "riffle-server";

pub struct LogService;
impl LogService {
    pub fn init(log: &LogConfig) -> DropGuard {
        let rotation = match log.rotation {
            RotationConfig::Hourly => Rotation::Hourly,
            RotationConfig::Daily => Rotation::Daily,
            RotationConfig::Never => Rotation::Never,
        };
        let max_file_size = util::to_bytes(&log.max_file_size);
        let (rolling_writer, _guard) = RollingFileBuilder::new(&log.path)
            .rotation(rotation)
            .filename_prefix(LOG_FILE_NAME_PREFIX)
            .max_file_size(max_file_size as usize)
            .max_log_files(log.max_log_files)
            .build()
            .unwrap();

        let log_level = match log.log_level {
            LogLevel::DEBUG => log::LevelFilter::Debug,
            LogLevel::INFO => log::LevelFilter::Info,
            LogLevel::WARN => log::LevelFilter::Warn,
        };
        logforth::builder()
            .dispatch(|d| d.filter(log_level).append(rolling_writer))
            .apply();

        _guard
    }
}
