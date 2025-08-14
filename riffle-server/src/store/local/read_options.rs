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

#[derive(Clone, Debug)]
pub struct ReadOptions {
    pub io_mode: IoMode,
    pub read_range: ReadRange,
    pub sequential: bool,
}

#[allow(non_camel_case_types)]
#[derive(Clone, Debug)]
pub enum ReadRange {
    ALL,
    // offset, length
    RANGE(u64, u64),
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
            sequential: false,
        }
    }
}

impl ReadOptions {
    pub fn new(io_mode: IoMode, range: ReadRange, is_sequential: bool) -> Self {
        Self {
            io_mode,
            read_range: range,
            sequential: is_sequential,
        }
    }

    pub fn with_sequential(self) -> Self {
        Self {
            io_mode: self.io_mode,
            read_range: self.read_range,
            sequential: true,
        }
    }

    pub fn with_read_all(self) -> Self {
        Self {
            io_mode: IoMode::BUFFER_IO,
            read_range: ReadRange::ALL,
            sequential: false,
        }
    }

    pub fn with_read_range(self, range: ReadRange) -> Self {
        Self {
            io_mode: self.io_mode,
            read_range: range,
            sequential: self.sequential,
        }
    }

    pub fn with_buffer_io(self) -> Self {
        Self {
            io_mode: IoMode::BUFFER_IO,
            read_range: self.read_range,
            sequential: self.sequential,
        }
    }

    pub fn with_direct_io(self) -> Self {
        Self {
            io_mode: IoMode::DIRECT_IO,
            read_range: self.read_range,
            sequential: self.sequential,
        }
    }

    pub fn with_sendfile(self) -> Self {
        Self {
            io_mode: IoMode::SENDFILE,
            read_range: self.read_range,
            sequential: self.sequential,
        }
    }
}

#[cfg(test)]
mod test {

    #[test]
    fn test_read_options() {
        use super::{IoMode, ReadOptions, ReadRange};

        // Default ReadOptions
        let default_opts = ReadOptions::default();
        assert!(matches!(default_opts.io_mode, IoMode::BUFFER_IO));
        assert!(matches!(default_opts.read_range, ReadRange::ALL));
        assert_eq!(default_opts.sequential, false);

        // Custom ReadOptions
        let opts = ReadOptions::new(IoMode::DIRECT_IO, ReadRange::RANGE(10, 20), true);
        assert!(matches!(opts.io_mode, IoMode::DIRECT_IO));
        assert!(matches!(opts.read_range, ReadRange::RANGE(10, 20)));
        assert_eq!(opts.sequential, true);

        // with_read_all
        let opts2 = opts.clone().with_read_all();
        assert!(matches!(opts2.io_mode, IoMode::BUFFER_IO));
        assert!(matches!(opts2.read_range, ReadRange::ALL));
        assert_eq!(opts2.sequential, false);

        // with_read_range
        let opts3 = opts2.clone().with_read_range(ReadRange::RANGE(100, 200));
        assert!(matches!(opts3.read_range, ReadRange::RANGE(100, 200)));
        assert!(matches!(opts3.io_mode, IoMode::BUFFER_IO));
        assert_eq!(opts3.sequential, false);

        // with_buffer_io
        let opts4 = opts3.clone().with_buffer_io();
        assert!(matches!(opts4.io_mode, IoMode::BUFFER_IO));
        assert!(matches!(opts4.read_range, ReadRange::RANGE(100, 200)));
        assert_eq!(opts4.sequential, false);

        // with_direct_io
        let opts5 = opts4.clone().with_direct_io();
        assert!(matches!(opts5.io_mode, IoMode::DIRECT_IO));
        assert!(matches!(opts5.read_range, ReadRange::RANGE(100, 200)));
        assert_eq!(opts5.sequential, false);

        // with_sendfile
        let opts6 = opts5.clone().with_sendfile();
        assert!(matches!(opts6.io_mode, IoMode::SENDFILE));
        assert!(matches!(opts6.read_range, ReadRange::RANGE(100, 200)));
        assert_eq!(opts6.sequential, false);
    }
}
