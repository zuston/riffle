use once_cell::sync::Lazy;
use riffle_server::client_configs::ClientConfigOption;

pub static COORDINATOR_QUORUM: Lazy<ClientConfigOption<Vec<String>>> =
    Lazy::new(|| ClientConfigOption::list_key("riffle.client.coordinator.quorum"));

pub static HEARTBEAT_INTERVAL_SECONDS: Lazy<ClientConfigOption<u64>> = Lazy::new(|| {
    ClientConfigOption::key("riffle.client.heartbeat.intervalSeconds").default_value(10)
});

pub static ASSIGNMENT_TAGS: Lazy<ClientConfigOption<Vec<String>>> = Lazy::new(|| {
    ClientConfigOption::list_key("riffle.client.assignment.tags").default_value(Vec::new())
});

pub static ASSIGNMENT_SERVER_NUMBER: Lazy<ClientConfigOption<i32>> =
    Lazy::new(|| ClientConfigOption::key("riffle.client.assignment.serverNumber"));
