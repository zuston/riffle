use crate::http::Handler;
use crate::server_state_manager::{ServerState, TransitionReason, SERVER_STATE_MANAGER_REF};
use crate::service_tags_manager::SERVICE_TAGS_MANAGER_REF;
use anyhow::{anyhow, Result};
use clap::builder::Str;
use poem::http::StatusCode;
use poem::{handler, Request, RouteMethod};
use serde::Deserialize;
use std::str::FromStr;
use strum_macros::{Display, EnumString};

#[derive(Default)]
pub struct AdminHandler;

impl Handler for AdminHandler {
    fn get_route_method(&self) -> RouteMethod {
        RouteMethod::new().get(admin_handler)
    }

    /// request with the following urls
    /// 1. /admin?kill
    /// 2. /admin?kill=force
    /// 3. /admin?get_state
    /// 4. /admin?update_state=DECOMMISSION
    /// 5. /admin?update_tags=a1,a2
    /// 6. /admin?add_tag=a1
    /// 7. /admin?delete_tag=a1
    fn get_route_path(&self) -> String {
        "/admin".to_string()
    }
}

#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, Deserialize, EnumString, Display)]
enum Operation {
    KILL,
    GET_STATE,
    UPDATE_STATE,
    UPDATE_TAGS,
    ADD_TAG,
    DELETE_TAG,
}

#[handler]
async fn admin_handler(req: &Request) -> poem::Result<String> {
    let query = req.uri().query().unwrap_or("");
    let server_state_manager_ref = SERVER_STATE_MANAGER_REF.get();
    if server_state_manager_ref.is_none() {
        return Ok("Uninitialized server_state_manager. Ignore!".to_string());
    }
    let server_state_manager_ref = server_state_manager_ref.unwrap();

    let mut found_operation = false;
    for pair in query.split('&') {
        if pair.is_empty() {
            continue;
        }
        let mut parts = pair.splitn(2, '=');
        let key = parts.next().unwrap();
        let value_opt = parts.next();

        let operation = Operation::from_str(&key.to_ascii_uppercase())
            .map_err(|_| anyhow!("Invalid operation"))?;
        match operation {
            Operation::KILL => {
                let force = value_opt.is_some() && value_opt.unwrap() == "force";
                server_state_manager_ref.shutdown(force).await?;
                found_operation = true;
            }
            Operation::GET_STATE => {
                found_operation = true;
                return Ok(server_state_manager_ref.get_state().to_string());
            }
            Operation::UPDATE_STATE => {
                if let Some(raw_state) = value_opt {
                    if let Some(state) = ServerState::from_str(&raw_state.to_ascii_uppercase()).ok()
                    {
                        server_state_manager_ref.as_state(state, TransitionReason::ADMIN_HTTP_API);
                        found_operation = true;
                    } else {
                        return Err(poem::Error::from_status(StatusCode::BAD_REQUEST));
                    }
                } else {
                    return Err(poem::Error::from_status(StatusCode::BAD_REQUEST));
                }
            }
            Operation::UPDATE_TAGS | Operation::ADD_TAG | Operation::DELETE_TAG => {
                let service_tags_manager_ref = SERVICE_TAGS_MANAGER_REF.get();
                if service_tags_manager_ref.is_none() {
                    return Ok("Uninitialized service_tags_manager. Ignore!".to_string());
                }
                let service_tags_manager_ref = service_tags_manager_ref.unwrap();

                match operation {
                    Operation::UPDATE_TAGS => {
                        if let Some(raw_tags) = value_opt {
                            let tags = if raw_tags.is_empty() {
                                vec![]
                            } else {
                                raw_tags.split(',').map(|tag| tag.to_string()).collect()
                            };
                            service_tags_manager_ref.update_tags(tags);
                            found_operation = true;
                        } else {
                            return Err(poem::Error::from_status(StatusCode::BAD_REQUEST));
                        }
                    }
                    Operation::ADD_TAG => {
                        if let Some(tag) = value_opt {
                            if tag.is_empty() {
                                return Err(poem::Error::from_status(StatusCode::BAD_REQUEST));
                            }
                            service_tags_manager_ref.add_tag(tag.to_string());
                            found_operation = true;
                        } else {
                            return Err(poem::Error::from_status(StatusCode::BAD_REQUEST));
                        }
                    }
                    Operation::DELETE_TAG => {
                        if let Some(tag) = value_opt {
                            if tag.is_empty() {
                                return Err(poem::Error::from_status(StatusCode::BAD_REQUEST));
                            }
                            if !service_tags_manager_ref.delete_tag(tag.to_string()) {
                                return Err(poem::Error::from_status(StatusCode::BAD_REQUEST));
                            }
                            found_operation = true;
                        } else {
                            return Err(poem::Error::from_status(StatusCode::BAD_REQUEST));
                        }
                    }
                    _ => unreachable!(),
                }
            }
        }
    }

    if !found_operation {
        return Ok("Invalid admin operation".to_string());
    }

    Ok("OK".to_string())
}

#[cfg(test)]
mod tests {
    use crate::app_manager::test::mock_config;
    use crate::app_manager::AppManager;
    use crate::config_reconfigure::ReconfigurableConfManager;
    use crate::http::admin::AdminHandler;
    use crate::http::Handler;
    use crate::runtime::manager::RuntimeManager;
    use crate::server_state_manager::{ServerStateManager, SERVER_STATE_MANAGER_REF};
    use crate::service_tags_manager::{ServiceTagsManager, SERVICE_TAGS_MANAGER_REF};
    use crate::storage::StorageService;
    use poem::http::StatusCode;
    use poem::test::TestClient;
    use poem::Route;

    #[tokio::test]
    async fn test_router() {
        let config = mock_config();
        let runtime_manager: RuntimeManager = Default::default();
        let reconf_manager = ReconfigurableConfManager::new(&config, None).unwrap();
        let storage = StorageService::init(&runtime_manager, &config, &reconf_manager);
        let app_manager_ref = AppManager::get_ref(
            Default::default(),
            config.clone(),
            &storage,
            &reconf_manager,
        )
        .clone();
        let server_state_manager = ServerStateManager::new(&app_manager_ref, &config);
        let _ = SERVER_STATE_MANAGER_REF.set(server_state_manager.clone());
        let service_tags_manager = ServiceTagsManager::new(&config);
        let _ = SERVICE_TAGS_MANAGER_REF.set(service_tags_manager);

        let handler = AdminHandler::default();
        let app = Route::new().at(handler.get_route_path(), handler.get_route_method());
        let cli = TestClient::new(app);

        // case1
        let resp = cli.get("/admin?get_state").send().await;
        resp.assert_status_is_ok();

        // case2
        let resp = cli.get("/admin?update_state=unhealthy").send().await;
        resp.assert_status_is_ok();

        // case3: illegal update_state request
        let resp = cli.get("/admin?update_state=unknown").send().await;
        resp.assert_status(StatusCode::BAD_REQUEST);

        // case4: illegal operation
        let resp = cli.get("/admin?unknown").send().await;
        resp.assert_status(StatusCode::INTERNAL_SERVER_ERROR);

        // case5: update tags
        let resp = cli.get("/admin?update_tags=a1,a2").send().await;
        resp.assert_status_is_ok();

        // case6: add tag
        let resp = cli.get("/admin?add_tag=a3").send().await;
        resp.assert_status_is_ok();

        // case7: delete tag
        let resp = cli.get("/admin?delete_tag=a1").send().await;
        resp.assert_status_is_ok();

        // case8: cannot delete builtin tag
        let resp = cli.get("/admin?delete_tag=GRPC").send().await;
        resp.assert_status(StatusCode::BAD_REQUEST);
    }
}
