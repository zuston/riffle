use crate::metric::{
    GAUGE_URPC_REQUEST_QUEUE_SIZE, TOTAL_URPC_REQUEST, URPC_REQUEST_PROCESSING_LATENCY,
};
use crate::urpc::frame::Frame;
use prometheus::HistogramTimer;

pub struct RequestMetricTracker {
    rpc_path: String,
    process_timer: HistogramTimer,
}

impl RequestMetricTracker {
    pub fn new(frame: &Frame) -> Self {
        let timer = URPC_REQUEST_PROCESSING_LATENCY
            .with_label_values(&[&format!("{}", &frame)])
            .start_timer();
        RequestMetricTracker {
            rpc_path: frame.to_string(),
            process_timer: timer,
        }
    }

    pub fn start(&self) {
        TOTAL_URPC_REQUEST.with_label_values(&[&"ALL"]).inc();
        TOTAL_URPC_REQUEST
            .with_label_values(&[self.rpc_path.as_str()])
            .inc();
        GAUGE_URPC_REQUEST_QUEUE_SIZE.inc();
    }
}

impl Drop for RequestMetricTracker {
    fn drop(&mut self) {
        GAUGE_URPC_REQUEST_QUEUE_SIZE.dec();
    }
}
