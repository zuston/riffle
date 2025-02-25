use crate::app::APP_MANAGER_REF;
use crate::http::Handler;
use crate::util;
use chrono::{Local, TimeZone, Utc};
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

        // refer: https://www.w3schools.com/howto/howto_js_sort_table.asp
        <script>
            function sortTable(n) {
              var table, rows, switching, i, x, y, shouldSwitch, dir, switchcount = 0;
              table = document.getElementById("myTable2");
              switching = true;
              // Set the sorting direction to ascending:
              dir = "asc";
              /* Make a loop that will continue until
              no switching has been done: */
              while (switching) {
                // Start by saying: no switching is done:
                switching = false;
                rows = table.rows;
                /* Loop through all table rows (except the
                first, which contains table headers): */
                for (i = 1; i < (rows.length - 1); i++) {
                  // Start by saying there should be no switching:
                  shouldSwitch = false;
                  /* Get the two elements you want to compare,
                  one from current row and one from the next: */
                  x = rows[i].getElementsByTagName("TD")[n];
                  y = rows[i + 1].getElementsByTagName("TD")[n];
                  /* Check if the two rows should switch place,
                  based on the direction, asc or desc: */
                  if (dir == "asc") {
                    if (x.innerHTML.toLowerCase() > y.innerHTML.toLowerCase()) {
                      // If so, mark as a switch and break the loop:
                      shouldSwitch = true;
                      break;
                    }
                  } else if (dir == "desc") {
                    if (x.innerHTML.toLowerCase() < y.innerHTML.toLowerCase()) {
                      // If so, mark as a switch and break the loop:
                      shouldSwitch = true;
                      break;
                    }
                  }
                }
                if (shouldSwitch) {
                  /* If a switch has been marked, make the switch
                  and mark that a switch has been done: */
                  rows[i].parentNode.insertBefore(rows[i + 1], rows[i]);
                  switching = true;
                  // Each time a switch is done, increase this count by 1:
                  switchcount ++;
                } else {
                  /* If no switching has been done AND the direction is "asc",
                  set the direction to "desc" and run the while loop again. */
                  if (switchcount == 0 && dir == "asc") {
                    dir = "desc";
                    switching = true;
                  }
                }
              }
            }
        </script>
    </head>
    <body>
        <table border="1">
            <tr>
                <th onclick="sortTable(0)">app id</th>
                <th onclick="sortTable(1)">registry date</th>
                <th onclick="sortTable(2)">duration (minutes)</th>
                <th onclick="sortTable(3)">resident data (gb)</th>
                <th onclick="sortTable(4)">partition number/huge partition</th>
                <th onclick="sortTable(5)">reported block id number</th>
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

        let duration = util::now_timestamp_as_millis() - timestamp as u128;
        let duration_min = milliseconds_to_minutes(duration);

        let date = Local
            .timestamp((timestamp / 1000) as i64, 0)
            .format("%Y-%m-%d %H:%M:%S")
            .to_string();

        html_content.push_str(&format!(
            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}/{}</td><td>{}</td></tr>",
            app_id,
            date,
            duration_min,
            bytes_to_gb(resident_bytes),
            app.partition_number(),
            app.huge_partition_number(),
            app.reported_block_id_number(),
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

fn milliseconds_to_minutes(milliseconds: u128) -> f64 {
    (milliseconds / 1000 / 60) as f64
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
