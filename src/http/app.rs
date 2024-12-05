use crate::app::APP_MANAGER_REF;
use crate::http::Handler;
use crate::util;
use chrono::{TimeZone, Utc};
use poem::web::Html;
use poem::{handler, RouteMethod};

#[derive(Default)]
pub struct Application {}

#[handler]
fn table() -> Html<String> {
    let mut html_content = r#"
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Simple Table</title>
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
            </tr>
    "#
    .to_string();

    let manager_ref = APP_MANAGER_REF.get().unwrap();
    let apps = &manager_ref.apps;

    for entry in apps.iter() {
        let app_id = entry.key();
        let app = entry.value();
        let timestamp = app.registry_timestamp;
        let resident_bytes = app.total_resident_data_size();
        let duration = util::now_timestamp_as_sec() * 1000 - timestamp;
        let duration_min = milliseconds_to_minutes(duration);

        let date = Utc
            .timestamp(timestamp as i64, 0)
            .format("%Y-%m-%d %H:%M:%S")
            .to_string();

        html_content.push_str(&format!(
            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
            app_id,
            date,
            duration_min,
            bytes_to_gb(resident_bytes)
        ));
    }

    html_content.push_str(
        r#"
        </table>
    </body>
    </html>
    "#,
    );

    Html(html_content.to_string())
}

fn milliseconds_to_minutes(milliseconds: u64) -> f64 {
    milliseconds as f64 / 60000.0
}

fn bytes_to_gb(bytes: u64) -> f64 {
    (bytes / 1024 / 1024 / 1024) as f64
}

impl Handler for Application {
    fn get_route_method(&self) -> RouteMethod {
        RouteMethod::new().get(table)
    }

    fn get_route_path(&self) -> String {
        "/apps".to_string()
    }
}
