pub const CPU_ARCH: &str = std::env::consts::ARCH;

#[allow(non_camel_case_types)]
pub enum StatusCode {
    SUCCESS = 0,
    DOUBLE_REGISTER = 1,
    NO_BUFFER = 2,
    INVALID_STORAGE = 3,
    NO_REGISTER = 4,
    NO_PARTITION = 5,
    INTERNAL_ERROR = 6,
    TIMEOUT = 7,
    ACCESS_DENIED = 8,
    INVALID_REQUEST = 9,
    NO_BUFFER_FOR_HUGE_PARTITION = 10,
    // to indicate shuffle-writing not retry!
    // todo: we should introduce the dedicated status code to indicate this
    EXCEED_HUGE_PARTITION_HARD_LIMIT = 12,
}

impl Into<i32> for StatusCode {
    fn into(self) -> i32 {
        self as i32
    }
}

pub const ALL_LABEL: &str = "ALL";

pub const INVALID_BLOCK_ID: i64 = -1;
