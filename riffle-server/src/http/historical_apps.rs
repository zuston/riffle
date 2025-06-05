use crate::app_manager::APP_MANAGER_REF;
use crate::historical_apps::HistoricalAppInfo;
use crate::http::Handler;
use log::info;
use poem::web::Json;
use poem::{handler, RouteMethod};
use tokio::time::Instant;

#[derive(Default)]
pub struct HistoricalAppsHandler;

impl Handler for HistoricalAppsHandler {
    fn get_route_method(&self) -> RouteMethod {
        RouteMethod::new().get(json)
    }

    fn get_route_path(&self) -> String {
        "/apps/history".to_string()
    }
}

#[handler]
async fn json() -> Json<Vec<HistoricalAppInfo>> {
    let timer = Instant::now();
    let manager_ref = APP_MANAGER_REF.get().unwrap();
    let mut apps = vec![];
    if let Some(historical_manager) = manager_ref.get_historical_app_manager() {
        apps = historical_manager.load().await.unwrap();
    }
    let num = apps.len();
    let apps = Json(apps);
    info!(
        "Gotten {} historical apps from http that costs {} ms",
        num,
        timer.elapsed().as_millis()
    );
    apps
}
