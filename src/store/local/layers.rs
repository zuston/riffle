use crate::store::local::LocalIO;
use std::sync::Arc;

pub type Handler = Arc<Box<dyn LocalIO>>;

pub struct OperatorBuilder {
    handler: Handler,
}

impl OperatorBuilder {
    pub fn new(handler: Handler) -> Self {
        OperatorBuilder { handler }
    }

    pub fn layer(self, layer: impl Layer) -> OperatorBuilder {
        OperatorBuilder {
            handler: layer.wrap(self.handler),
        }
    }

    pub fn build(self) -> Handler {
        self.handler
    }
}

pub trait Layer {
    fn wrap(&self, handler: Handler) -> Handler;
}
