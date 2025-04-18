use crate::http::Handler;
use crate::server_state_manager::{ServerState, SERVER_STATE_MANAGER_REF};
use anyhow::Result;
use clap::builder::Str;
use poem::{handler, Request, RouteMethod};
use serde::Deserialize;

#[derive(Default)]
pub struct AdminHandler;

impl Handler for AdminHandler {
    fn get_route_method(&self) -> RouteMethod {
        RouteMethod::new().get(adminHandler)
    }

    /// request with /admin?operation=DECOMMISSION
    fn get_route_path(&self) -> String {
        "/admin".to_string()
    }
}

#[derive(Deserialize)]
enum Operation {
    DECOMMISSION,
    CANCEL_DECOMMISSION,
}

#[derive(Deserialize)]
struct OperationParam {
    operation: Operation,
}

#[handler]
fn adminHandler(req: &Request) -> poem::Result<String> {
    let params = req.params::<OperationParam>()?;
    let decom_manager_ref = SERVER_STATE_MANAGER_REF.get().unwrap();

    match params.operation {
        Operation::DECOMMISSION => {
            decom_manager_ref.as_state(ServerState::DECOMMISSIONING);
        }
        Operation::CANCEL_DECOMMISSION => {
            decom_manager_ref.as_state(ServerState::CANCEL_DECOMMISSION);
        }
    }

    Ok("Done".to_string())
}
