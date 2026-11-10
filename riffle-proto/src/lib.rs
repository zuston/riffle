pub mod rss {
    pub mod common {
        tonic::include_proto!("rss.common");
    }
}

pub use rss::common as uniffle;
