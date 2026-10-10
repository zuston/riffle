// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::RiffleError;
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
}

impl RetryPolicy {
    pub fn validate(&self) -> Result<(), RiffleError> {
        if self.max_attempts == 0 {
            return Err(RiffleError::InvalidArgument(
                "retry max_attempts must be positive".to_string(),
            ));
        }
        if self.initial_backoff.is_zero() || self.max_backoff.is_zero() {
            return Err(RiffleError::InvalidArgument(
                "retry backoffs must be positive".to_string(),
            ));
        }
        if self.initial_backoff > self.max_backoff {
            return Err(RiffleError::InvalidArgument(
                "retry initial_backoff must not exceed max_backoff".to_string(),
            ));
        }
        Ok(())
    }

    pub(crate) fn delay_for(&self, failed_attempt: u32) -> Duration {
        let exponent = failed_attempt.saturating_sub(1).min(31);
        self.initial_backoff
            .saturating_mul(1_u32 << exponent)
            .min(self.max_backoff)
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 4,
            initial_backoff: Duration::from_millis(50),
            max_backoff: Duration::from_secs(2),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exponential_backoff_is_bounded() {
        let policy = RetryPolicy {
            max_attempts: 8,
            initial_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_millis(35),
        };

        assert_eq!(policy.delay_for(1), Duration::from_millis(10));
        assert_eq!(policy.delay_for(2), Duration::from_millis(20));
        assert_eq!(policy.delay_for(3), Duration::from_millis(35));
        assert_eq!(policy.delay_for(8), Duration::from_millis(35));
    }
}
