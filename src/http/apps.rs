use crate::app::{self, App, APP_MANAGER_REF};
use crate::http::{Format, Handler};
use crate::util;
use chrono::{Local, TimeZone, Utc};
use clap::builder::Str;
use hyper::{Body, StatusCode};
use poem::web::{Html, Json};
use poem::{handler, Request, Response, RouteMethod};
use serde::{Deserialize, Serialize};
use std::num::NonZeroI32;
use std::sync::Arc;
use tonic::IntoRequest;

fn milliseconds_to_minutes(milliseconds: u128) -> f64 {
    (milliseconds / 1000 / 60) as f64
}

fn bytes_to_gb(bytes: u64) -> f64 {
    (bytes / 1024 / 1024 / 1024) as f64
}

fn table() -> String {
    let mut html_content = r#"
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Riffle Dashboard</title>
        <style>
            body {
                font-family: Arial, sans-serif;
                background-color: #f4f4f4;
                margin: 0;
                padding: 20px;
            }
            table {
                border-collapse: collapse;
                width: 100%;
                margin-top: 20px;
            }
            th, td {
                padding: 8px;
                text-align: left;
                border-bottom: 1px solid #ddd;
            }
            th {
                background-color: #4CAF50;
                color: white;
            }
            tr:hover {
                background-color: #f5f5f5;
            }
        </style>
    </head>
    <body>
        <table border="1">
            <tr>
                <th>app id</th>
                <th>registry date</th>
                <th>duration (minutes)</th>
                <th>resident data (gb)</th>
                <th>partition number/huge partition</th>
                <th>reported block id number</th>
            </tr>
    "#
    .to_string();

    let manager_ref = APP_MANAGER_REF.get().unwrap();
    let apps = &manager_ref.apps;

    for entry in apps.iter() {
        let app_info = AppInfo::from(entry.value());

        let readable_date = Local
            .timestamp((&app_info.registry_timestamp / 1000) as i64, 0)
            .format("%Y-%m-%d %H:%M:%S")
            .to_string();

        html_content.push_str(&format!(
            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}/{}</td><td>{}</td></tr>",
            &app_info.app_id,
            readable_date,
            app_info.duration_minutes,
            bytes_to_gb(app_info.resident_bytes),
            &app_info.partition_number,
            &app_info.huge_partition_number,
            &app_info.reported_block_id_number
        ));
    }

    html_content.push_str(
        r#"
        </table>
    </body>
    </html>
    "#,
    );

    html_content.to_string()
}

fn json() -> anyhow::Result<String> {
    let manager_ref = APP_MANAGER_REF.get().unwrap();
    let apps = &manager_ref.apps;
    let data = apps
        .iter()
        .map(|entry| AppInfo::from(entry.value()))
        .collect::<Vec<AppInfo>>();
    let data = serde_json::to_string(&data)?;
    Ok(data)
}

#[derive(Serialize)]
struct AppInfo {
    app_id: String,
    registry_timestamp: u128,
    duration_minutes: f64,
    resident_bytes: u64,
    partition_number: usize,
    huge_partition_number: u64,
    reported_block_id_number: u64,
}

impl From<&Arc<App>> for AppInfo {
    fn from(app: &Arc<App>) -> Self {
        let timestamp = app.registry_timestamp;
        let resident_bytes = app.total_resident_data_size();
        let duration_min = milliseconds_to_minutes(util::now_timestamp_as_millis() - timestamp);
        let app_id = app.app_id.to_string();

        Self {
            app_id,
            registry_timestamp: timestamp,
            duration_minutes: duration_min,
            resident_bytes,
            partition_number: app.partition_number(),
            huge_partition_number: app.huge_partition_number(),
            reported_block_id_number: app.reported_block_id_number(),
        }
    }
}

#[derive(Default)]
pub struct AppsHandler {}
impl Handler for AppsHandler {
    fn get_route_method(&self) -> RouteMethod {
        RouteMethod::new().get(request_handler)
    }

    fn get_route_path(&self) -> String {
        "/apps".to_string()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
enum FormatType {
    Json,
    Html,
}

#[derive(Deserialize, Serialize)]
#[serde(default)]
struct AppsRequest {
    format: FormatType,
}

impl Default for AppsRequest {
    fn default() -> Self {
        Self {
            format: FormatType::Html,
        }
    }
}

#[handler]
async fn request_handler(req: &Request) -> poem::Result<Response> {
    let app_request = req.params::<AppsRequest>()?;
    let format = app_request.format;

    let response = match format {
        FormatType::Json => {
            let data = json()?;
            Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "application/json")
                .body(Body::from(data))
        }
        FormatType::Html => {
            let data = table();
            Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "text/html")
                .body(Body::from(data))
        }
    };

    Ok(response)
}
