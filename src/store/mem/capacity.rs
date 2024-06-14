pub struct CapacitySnapshot {
    capacity: i64,
    allocated: i64,
    used: i64,
}

impl From<(i64, i64, i64)> for CapacitySnapshot {
    fn from(value: (i64, i64, i64)) -> Self {
        CapacitySnapshot {
            capacity: value.0,
            allocated: value.1,
            used: value.2,
        }
    }
}

impl CapacitySnapshot {
    pub fn get_capacity(&self) -> i64 {
        self.capacity
    }
    pub fn get_allocated(&self) -> i64 {
        self.allocated
    }
    pub fn get_used(&self) -> i64 {
        self.used
    }
    fn get_used_percent(&self) -> f32 {
        (self.allocated + self.used) as f32 / self.capacity as f32
    }
}
