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

use crate::app_manager::request_context::PurgeDataContext;
use crate::urpc::command::ReadSegment;

#[derive(Clone, Debug)]
pub struct ReadOptions {
    pub io_mode: IoMode,
    pub read_range: ReadRange,
    // for the read ahead layer
    pub ahead_options: Option<AheadOptions>,
    // align with the spark's task attempt id
    pub task_id: i64,
}

impl ReadOptions {
    pub fn get_ahead(&self) -> AheadOptions {
        let ahead = self.ahead_options.as_ref().unwrap();
        ahead.clone()
    }
}

#[derive(Clone, Debug)]
pub struct AheadOptions {
    pub sequential: bool,
    pub read_batch_number: Option<usize>,
    pub read_batch_size: Option<usize>,

    pub next_read_segments: Vec<ReadSegment>,
}

#[allow(non_camel_case_types)]
#[derive(Clone, Debug)]
pub enum ReadRange {
    ALL,
    // offset, length
    RANGE(u64, u64),
}

impl ReadRange {
    pub fn get_range(&self) -> (u64, u64) {
        if let ReadRange::RANGE(offset, length) = self {
            (*offset, *length)
        } else {
            panic!("Expected RANGE, but got ALL");
        }
    }
}

#[derive(Clone, Debug)]
#[allow(non_camel_case_types)]
pub enum IoMode {
    SENDFILE,
    DIRECT_IO,
    BUFFER_IO,
}

impl Default for ReadOptions {
    fn default() -> Self {
        Self {
            io_mode: IoMode::BUFFER_IO,
            read_range: ReadRange::ALL,
            ahead_options: None,
            task_id: 0,
        }
    }
}

impl ReadOptions {
    pub fn with_sequential(mut self) -> Self {
        self.ahead_options = Some(AheadOptions {
            sequential: true,
            read_batch_number: None,
            read_batch_size: None,
            next_read_segments: vec![],
        });
        self
    }

    pub fn with_ahead_options(mut self, options: AheadOptions) -> Self {
        self.ahead_options = Some(options);
        self
    }

    pub fn with_read_all(mut self) -> Self {
        self.io_mode = IoMode::BUFFER_IO;
        self.read_range = ReadRange::ALL;
        self
    }

    pub fn with_read_range(mut self, range: ReadRange) -> Self {
        self.read_range = range;
        self
    }

    pub fn with_buffer_io(mut self) -> Self {
        self.io_mode = IoMode::BUFFER_IO;
        self
    }

    pub fn with_direct_io(mut self) -> Self {
        self.io_mode = IoMode::DIRECT_IO;
        self
    }

    pub fn with_sendfile(mut self) -> Self {
        self.io_mode = IoMode::SENDFILE;
        self
    }

    pub fn with_task_id(mut self, task_id: i64) -> Self {
        self.task_id = task_id;
        self
    }
}

#[cfg(test)]
mod test {
    use crate::store::local::read_options::{AheadOptions, IoMode, ReadOptions, ReadRange};

    fn create_read_options() -> ReadOptions {
        let opts = ReadOptions {
            io_mode: IoMode::DIRECT_IO,
            read_range: ReadRange::RANGE(10, 20),
            ahead_options: Some(AheadOptions {
                sequential: true,
                read_batch_number: None,
                read_batch_size: None,
                next_read_segments: vec![],
            }),
            task_id: 0,
        };
        opts
    }

    #[test]
    fn test_read_options() {
        use super::{IoMode, ReadOptions, ReadRange};

        // Default ReadOptions
        let default_opts = ReadOptions::default();
        assert!(matches!(default_opts.io_mode, IoMode::BUFFER_IO));
        assert!(matches!(default_opts.read_range, ReadRange::ALL));
        assert_eq!(default_opts.ahead_options.is_some(), false);

        // Custom ReadOptions
        let opts = create_read_options();
        assert!(matches!(opts.io_mode, IoMode::DIRECT_IO));
        assert!(matches!(opts.read_range, ReadRange::RANGE(10, 20)));
        assert_eq!(opts.ahead_options.is_some(), true);

        // with_read_all
        let opts2 = ReadOptions::default().with_read_all();
        assert!(matches!(opts2.io_mode, IoMode::BUFFER_IO));
        assert!(matches!(opts2.read_range, ReadRange::ALL));
        assert_eq!(opts2.ahead_options.is_some(), false);

        // with_read_range
        let opts3 = ReadOptions::default().with_read_range(ReadRange::RANGE(100, 200));
        assert!(matches!(opts3.read_range, ReadRange::RANGE(100, 200)));
        assert_eq!(opts3.ahead_options.is_some(), false);

        // with_buffer_io
        let opts4 = ReadOptions::default().with_buffer_io();
        assert!(matches!(opts4.io_mode, IoMode::BUFFER_IO));
        assert_eq!(opts4.ahead_options.is_some(), false);

        // with_direct_io
        let opts5 = ReadOptions::default().with_direct_io();
        assert!(matches!(opts5.io_mode, IoMode::DIRECT_IO));
        assert_eq!(opts5.ahead_options.is_some(), false);

        // with_sendfile
        let opts6 = ReadOptions::default().with_sendfile();
        assert!(matches!(opts6.io_mode, IoMode::SENDFILE));
        assert_eq!(opts6.ahead_options.is_some(), false);
    }
}
