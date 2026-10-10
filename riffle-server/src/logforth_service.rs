use crate::config::{LogConfig, LogLevel, RotationConfig};
use crate::util;
use logforth::append::asynchronous::AsyncBuilder;
use logforth::append::file::FileBuilder;
use logforth::append::{Async, Stdout};
use logforth::bridge::log::LogBridge;
use logforth::core::Logger;
use logforth::layout::TextLayout;
use logforth::record::{Level, LevelFilter};
use logforth::Append;
use std::num::NonZeroUsize;

const LOG_FILE_NAME_PREFIX: &str = "riffle-server";
// Bound the backlog when request logging outpaces disk writes.
const LOG_BUFFERED_LINES_LIMIT: usize = 16_384;

pub struct LogGuard;

impl Drop for LogGuard {
    fn drop(&mut self) {
        log::logger().flush();
    }
}

pub struct LogService;
impl LogService {
    pub fn init(log: Option<&LogConfig>) -> LogGuard {
        let logger = match log {
            Some(log) => file_logger(log),
            None => logforth::core::builder()
                .dispatch(|d| {
                    d.filter(LevelFilter::MoreSevereEqual(Level::Info))
                        .append(Stdout::default().with_layout(TextLayout::default()))
                })
                .build(),
        };

        log::set_boxed_logger(Box::new(LogBridge::new(logger)))
            .expect("Failed to initialize the global logger");
        log::set_max_level(log::LevelFilter::Trace);
        LogGuard
    }
}

fn file_logger(log: &LogConfig) -> Logger {
    let max_file_size = NonZeroUsize::new(util::to_bytes(&log.max_file_size) as usize)
        .expect("log.max_file_size must be positive");
    let max_log_files =
        NonZeroUsize::new(log.max_log_files).expect("log.max_log_files must be positive");
    let file = FileBuilder::new(&log.path, LOG_FILE_NAME_PREFIX)
        .filename_suffix("log")
        .layout(TextLayout::default().no_color())
        .rollover_size(max_file_size)
        .max_log_files(max_log_files);
    let file = match log.rotation {
        RotationConfig::Hourly => file.rollover_hourly(),
        RotationConfig::Daily => file.rollover_daily(),
        RotationConfig::Never => file,
    };
    let appender = async_appender(file.build().expect("Failed to initialize the log file"));
    let level = match log.log_level {
        LogLevel::DEBUG => Level::Debug,
        LogLevel::INFO => Level::Info,
        LogLevel::WARN => Level::Warn,
    };

    logforth::core::builder()
        .dispatch(|d| {
            d.filter(LevelFilter::MoreSevereEqual(level))
                .append(appender)
        })
        .build()
}

fn async_appender(appender: impl Append) -> Async {
    AsyncBuilder::new("riffle-log")
        .buffered_lines_limit(Some(LOG_BUFFERED_LINES_LIMIT))
        .overflow_drop_incoming()
        .append(appender)
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use log::Log;
    use logforth::record::{Record, RecordBuilder};
    use logforth::{Diagnostic, Error};
    use std::sync::{mpsc, Arc, Mutex};
    use std::time::Duration;

    #[derive(Debug)]
    struct BlockingAppender {
        started: mpsc::Sender<()>,
        release: Mutex<mpsc::Receiver<()>>,
        recorded: mpsc::Sender<String>,
    }

    impl Append for BlockingAppender {
        fn append(&self, record: &Record, _: &[Box<dyn Diagnostic>]) -> Result<(), Error> {
            let message = record.payload().to_string();
            if message == "blocked" {
                self.started.send(()).unwrap();
                self.release.lock().unwrap().recv().unwrap();
            }
            self.recorded.send(message).unwrap();
            Ok(())
        }

        fn flush(&self) -> Result<(), Error> {
            Ok(())
        }
    }

    #[test]
    fn full_queue_drops_new_logs_without_blocking_and_recovers() {
        let timeout = Duration::from_secs(3);
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let (recorded_tx, recorded_rx) = mpsc::channel();
        let appender = Arc::new(async_appender(BlockingAppender {
            started: started_tx,
            release: Mutex::new(release_rx),
            recorded: recorded_tx,
        }));
        let blocked = RecordBuilder::default()
            .payload(format_args!("blocked"))
            .build();
        appender.append(&blocked, &[]).unwrap();
        started_rx.recv_timeout(timeout).unwrap();

        let queued = RecordBuilder::default()
            .payload(format_args!("queued"))
            .build();
        for _ in 0..LOG_BUFFERED_LINES_LIMIT {
            appender.append(&queued, &[]).unwrap();
        }

        let (done_tx, done_rx) = mpsc::channel();
        let producer = {
            let appender = appender.clone();
            std::thread::spawn(move || {
                let overflow = RecordBuilder::default()
                    .payload(format_args!("overflow"))
                    .build();
                done_tx.send(appender.append(&overflow, &[])).unwrap();
            })
        };
        let result = done_rx.recv_timeout(timeout);
        release_tx.send(()).unwrap();
        producer.join().unwrap();
        result
            .expect("Logging must not block when the queue is full")
            .unwrap();

        assert_eq!(recorded_rx.recv_timeout(timeout).unwrap(), "blocked");
        for _ in 0..LOG_BUFFERED_LINES_LIMIT {
            assert_eq!(recorded_rx.recv_timeout(timeout).unwrap(), "queued");
        }

        let recovered = RecordBuilder::default()
            .payload(format_args!("recovered"))
            .build();
        appender.append(&recovered, &[]).unwrap();
        assert_eq!(recorded_rx.recv_timeout(timeout).unwrap(), "recovered");
        assert!(recorded_rx.try_recv().is_err());
    }

    #[test]
    fn file_logging_preserves_info_metadata_and_filters_debug() {
        let directory = tempfile::tempdir().unwrap();
        let config = LogConfig {
            path: directory.path().to_str().unwrap().to_owned(),
            rotation: RotationConfig::Never,
            ..LogConfig::default()
        };
        let logger = LogBridge::new(file_logger(&config));
        logger.log(
            &log::Record::builder()
                .level(log::Level::Info)
                .target("log_test")
                .file(Some("logger.rs"))
                .line(Some(42))
                .args(format_args!("normal info"))
                .build(),
        );
        logger.log(
            &log::Record::builder()
                .level(log::Level::Debug)
                .args(format_args!("filtered debug"))
                .build(),
        );
        logger.flush();

        let contents = std::fs::read_to_string(directory.path().join("riffle-server.log")).unwrap();
        assert!(contents.contains("INFO log_test: logger.rs:42 normal info"));
        assert!(!contents.contains("filtered debug"));
        assert!(!contents.contains('\u{1b}'));
    }

    #[test]
    fn file_logging_preserves_size_rotation_and_retention() {
        let directory = tempfile::tempdir().unwrap();
        let config = LogConfig {
            path: directory.path().to_str().unwrap().to_owned(),
            rotation: RotationConfig::Never,
            max_file_size: "1K".to_owned(),
            max_log_files: 3,
            ..LogConfig::default()
        };
        let logger = LogBridge::new(file_logger(&config));
        let payload = "x".repeat(1_500);
        for index in 0..8 {
            logger.log(
                &log::Record::builder()
                    .level(log::Level::Info)
                    .args(format_args!("record {index}: {payload}"))
                    .build(),
            );
        }
        logger.flush();

        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 3);
        let current = std::fs::read_to_string(directory.path().join("riffle-server.log")).unwrap();
        assert!(current.contains("record 7:"));
        assert!(!current.contains("record 0:"));
    }
}
