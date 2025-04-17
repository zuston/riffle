use crate::store::local::LocalIO;

pub type Handler = Box<dyn LocalIO>;

pub struct OperatorBuilder {
    handler: Handler,
}

impl OperatorBuilder {
    fn new(handler: Handler) -> Self {
        OperatorBuilder { handler }
    }

    fn layer<L: Layer>(self, layer: L) -> OperatorBuilder {
        OperatorBuilder {
            handler: layer.wrap(self.handler),
        }
    }

    fn build(self) -> Handler {
        self.handler
    }
}

pub trait Layer {
    fn wrap(&self, handler: Handler) -> Handler;
}
