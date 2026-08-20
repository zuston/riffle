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

use std::process::Command;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Inject build time
    let output = Command::new("date")
        .output()
        .expect("Failed to execute `date` command");
    let build_datetime = String::from_utf8(output.stdout).expect("Failed to parse date output");
    println!("cargo:rustc-env=BUILD_DATE={}", build_datetime.trim());

    // Inject commit id into the compiler envs
    let raw_commit_id_output = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .expect("Failed to execute git command");
    let git_hash = String::from_utf8_lossy(&raw_commit_id_output.stdout)
        .trim()
        .to_string();
    println!("cargo:rustc-env=GIT_COMMIT_HASH={git_hash}");

    Ok(())
}
