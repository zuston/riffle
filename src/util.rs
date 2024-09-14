// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use bytes::Bytes;
use crc32fast::Hasher;

use crate::config::Config;
use std::net::IpAddr;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

const WORKER_IP: &str = "WORKER_IP";

pub fn get_local_ip() -> Result<IpAddr, std::io::Error> {
    let ip = std::env::var(WORKER_IP);
    if ip.is_ok() {
        Ok(ip.unwrap().parse().unwrap())
    } else {
        let socket = std::net::UdpSocket::bind("0.0.0.0:0")?;
        socket.connect("8.8.8.8:80")?;
        let local_addr = socket.local_addr()?;
        Ok(local_addr.ip())
    }
}

pub fn generate_worker_uid(config: &Config) -> String {
    let ip = get_local_ip().unwrap().to_string();
    let grpc_port = config.grpc_port;
    let urpc_port = config.urpc_port;
    if urpc_port.is_none() {
        return format!("{}-{}", &ip, grpc_port);
    }
    return format!("{}-{}-{}", &ip, grpc_port, urpc_port.unwrap());
}

pub fn gen_worker_uid(grpc_port: i32) -> String {
    let ip = get_local_ip().unwrap().to_string();
    format!("{}-{}", ip.clone(), grpc_port)
}

const LENGTH_PER_CRC: usize = 4 * 1024;
pub fn get_crc(bytes: &Bytes) -> i64 {
    let mut crc32 = Hasher::new();
    let offset = 0;
    let length = bytes.len();
    let crc_buffer = &bytes[offset..(offset + length)];

    for i in (0..length).step_by(LENGTH_PER_CRC) {
        let len = std::cmp::min(LENGTH_PER_CRC, length - i);
        let crc_slice = &crc_buffer[i..(i + len)];

        crc32.update(crc_slice);
    }

    crc32.finalize() as i64
}

pub fn now_timestamp_as_millis() -> u128 {
    let current_time = SystemTime::now();
    let timestamp = current_time.duration_since(UNIX_EPOCH).unwrap().as_millis();
    timestamp
}

pub fn now_timestamp_as_sec() -> u64 {
    let current_time = SystemTime::now();
    let timestamp = current_time.duration_since(UNIX_EPOCH).unwrap().as_secs();
    timestamp
}

#[cfg(test)]
mod test {
    use crate::util::{get_crc, now_timestamp_as_sec};
    use bytes::Bytes;

    #[test]
    fn time_test() {
        println!("{}", now_timestamp_as_sec());
    }

    #[test]
    fn crc_test() {
        let data = Bytes::from("hello world! hello china!");
        let crc_value = get_crc(&data);
        // This value is the same with java's implementation
        assert_eq!(3871485936, crc_value);
    }
}
