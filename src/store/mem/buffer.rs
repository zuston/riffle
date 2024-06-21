use crate::store::{ExecutionTime, PartitionedDataBlock};
use anyhow::Result;
use croaring::Treemap;
use hashlink::LinkedHashMap;
use spin::RwLock;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Instant;

pub struct MemoryBuffer {
    buffer: RwLock<BufferInternal>,
}

#[derive(Debug, Clone)]
pub struct BufferInternal {
    total_size: i64,
    staging_size: i64,
    flight_size: i64,

    staging_block_num: i64,
    flight_block_num: i64,

    blocks: LinkedHashMap<i64, Arc<PartitionedDataBlock>>,
    // the boundary blockId to distinguish the staging and flush blocks
    boundary_block_id: i64,
}

impl MemoryBuffer {
    pub fn new() -> MemoryBuffer {
        MemoryBuffer {
            buffer: RwLock::new(BufferInternal {
                total_size: 0,
                staging_size: 0,
                flight_size: 0,
                staging_block_num: 0,
                flight_block_num: 0,
                blocks: Default::default(),
                boundary_block_id: -1,
            }),
        }
    }

    pub fn get_total_size(&self) -> Result<i64> {
        return Ok(self.buffer.read().total_size);
    }

    pub fn get_flight_size(&self) -> Result<i64> {
        return Ok(self.buffer.read().flight_size);
    }

    pub fn get_staging_size(&self) -> Result<i64> {
        return Ok(self.buffer.read().staging_size);
    }

    pub fn read(
        &self,
        last_block_id: i64,
        max_len: i64,
        serialized_expected_task_ids_bitmap: Option<Treemap>,
    ) -> Result<(i64, Vec<Arc<PartitionedDataBlock>>)> {
        let buffer = &self.buffer.read();
        let blocks_map = &buffer.blocks;

        let mut started_tag = false;
        let mut read_size = 0i64;
        let mut read_blocks = vec![];
        let mut last_block_id = last_block_id;
        // if the last_block_id is not found, that means the data has been flushed into persistent store
        // and so it could read from the head of the memory directly.
        if !blocks_map.contains_key(&last_block_id) {
            last_block_id = -1;
        }
        for (block_id, block) in blocks_map {
            if started_tag || last_block_id == -1 {
                if read_size > max_len {
                    break;
                }
                if let Some(ref filter) = serialized_expected_task_ids_bitmap {
                    if !filter.contains(block.task_attempt_id as u64) {
                        continue;
                    }
                }
                read_size += block.length as i64;
                read_blocks.push((*block).clone());
            }

            if !started_tag && *block_id == last_block_id {
                started_tag = true;
                continue;
            }
        }
        Ok((read_size, read_blocks))
    }

    pub fn clear_flight(&self, block_ids: Vec<i64>) -> Result<i64> {
        let buffer = &mut self.buffer.write();
        let blocks_map = &mut buffer.blocks;

        let mut block_cnt = 0i64;
        let mut removed = 0;
        for block_id in block_ids {
            match blocks_map.remove(&block_id) {
                Some(block) => {
                    block_cnt += 1;
                    removed += block.length;
                },
                _ => {
                    // ignore
                }
            }
        }
        buffer.flight_size -= removed as i64;
        buffer.total_size -= removed as i64;
        buffer.flight_block_num -= block_cnt;
        Ok(removed as i64)
    }

    pub fn create_flight(&self) -> Result<(ExecutionTime, i64, Vec<Arc<PartitionedDataBlock>>)> {
        let timer = Instant::now();
        let buffer = &mut self.buffer.write();
        let lock_time = timer.elapsed().as_millis();

        let timer = Instant::now();
        let blocks_map = &buffer.blocks;
        if blocks_map.is_empty() {
            return Ok((ExecutionTime::BUFFER_CREATE_FLIGHT(0, 0, 0), 0, vec![]));
        }

        let mut flight_len = 0;
        let mut flight_blocks_ref = vec![];
        let last_boundary_block_id = buffer.boundary_block_id;
        let mut started_tag = false;
        let mut block_cnt = 0i64;
        let mut search_time = 0;
        let mut pick_time = 0;
        for (k, v) in blocks_map.iter() {
            if started_tag || last_boundary_block_id == -1 {
                let pick_timer = Instant::now();
                flight_len += v.length;
                flight_blocks_ref.push((*v).clone());
                block_cnt += 1;
                pick_time = pick_timer.elapsed().as_millis();
            }

            let search_timer = Instant::now();
            if !started_tag && *k == last_boundary_block_id {
                started_tag = true;
                continue;
            }
            search_time += search_timer.elapsed().as_millis();
        }

        buffer.boundary_block_id = *blocks_map.front().unwrap().0;
        buffer.flight_size += flight_len as i64;
        buffer.staging_size -= flight_len as i64;
        buffer.staging_block_num -= block_cnt;
        buffer.flight_block_num += block_cnt;

        Ok((
            ExecutionTime::BUFFER_CREATE_FLIGHT(lock_time, pick_time, timer.elapsed().as_millis()),
            flight_len as i64,
            flight_blocks_ref,
        ))
    }

    pub fn add(&self, blocks: Vec<PartitionedDataBlock>) -> Result<i64> {
        let mut buffer = self.buffer.write();
        let mut blocks_map = &mut buffer.blocks;

        let mut block_cnt = 0i64;
        let mut add_size = 0;
        for block in blocks {
            let id = block.block_id;
            let len = block.length;
            add_size += len as i64;
            blocks_map.insert(id, Arc::new(block));
            block_cnt += 1;
        }

        buffer.staging_size += add_size;
        buffer.total_size += add_size;
        buffer.staging_block_num += block_cnt;

        Ok(add_size)
    }
}

#[cfg(test)]
mod test {
    use crate::store::mem::buffer::MemoryBuffer;
    use crate::store::PartitionedDataBlock;
    use hashlink::LinkedHashMap;
    use std::sync::RwLock;

    fn create_blocks(start_block_idx: i32, cnt: i32, len: i32) -> Vec<PartitionedDataBlock> {
        let mut blocks = vec![];
        for idx in 0..cnt {
            blocks.push(PartitionedDataBlock {
                block_id: (start_block_idx + idx) as i64,
                length: len,
                uncompress_length: 0,
                crc: 0,
                data: Default::default(),
                task_attempt_id: idx as i64,
            });
        }
        return blocks;
    }

    #[test]
    fn test_read() {
        let mut buffer = MemoryBuffer::new();
        let added = buffer.add(create_blocks(0, 10, 10));
        assert_eq!(10 * 10, added.unwrap());

        // case1: nothing in flight.
        let (_, blocks) = buffer.read(-1, 19, None).unwrap();
        assert_eq!(2, blocks.len());
        let last_block_id = blocks.get(blocks.len() - 1).unwrap().block_id;
        let (_, blocks) = buffer.read(last_block_id, 69, None).unwrap();
        assert_eq!(7, blocks.len());
        let last_block_id = blocks.get(blocks.len() - 1).unwrap().block_id;
        let (_, blocks) = buffer.read(last_block_id, 70, None).unwrap();
        assert_eq!(1, blocks.len());

        // case2: partial in flight, partial in staging
        let _ = buffer.create_flight();
        let _ = buffer.add(create_blocks(10, 10, 10));
        let (_, blocks) = buffer.read(-1, 99, None).unwrap();
        assert_eq!(10, blocks.len());
        for block in &blocks {
            assert!(block.block_id < 10);
        }
        let last_block_id = blocks.get(blocks.len() - 1).unwrap().block_id;
        let (_, blocks) = buffer.read(last_block_id, 1000, None).unwrap();
        assert_eq!(10, blocks.len());
        for block in &blocks {
            assert!(block.block_id >= 10);
        }

        // case3: something has been flushed into persistent store
        let mut remove_block_ids: Vec<i64> = Vec::new();
        for i in 0..10 {
            remove_block_ids.push(i);
        }
        let _ = buffer.clear_flight(remove_block_ids);
        let last_block_id = 5i64;
        let (_, blocks) = buffer.read(last_block_id, 1000, None).unwrap();
        assert_eq!(10, blocks.len());
    }

    #[test]
    fn test_create_flight() {
        let mut buffer = MemoryBuffer::new();
        let added = buffer.add(vec![PartitionedDataBlock {
            block_id: 1,
            length: 10,
            uncompress_length: 0,
            crc: 0,
            data: Default::default(),
            task_attempt_id: 0,
        }]);
        assert_eq!(10, added.unwrap());

        // case1: firstly making flight.
        let (_, flight_size, blocks) = buffer.create_flight().unwrap();
        assert_eq!(10, flight_size);
        assert_eq!(1, blocks[0].block_id);

        // cas2: send to append data and then to make flight
        let added = buffer.add(vec![
            PartitionedDataBlock {
                block_id: 2,
                length: 20,
                uncompress_length: 0,
                crc: 0,
                data: Default::default(),
                task_attempt_id: 0,
            },
            PartitionedDataBlock {
                block_id: 3,
                length: 30,
                uncompress_length: 0,
                crc: 0,
                data: Default::default(),
                task_attempt_id: 0,
            },
        ]);
        assert_eq!(50, added.unwrap());
        let (_, flight_size, blocks) = buffer.create_flight().unwrap();
        assert_eq!(50, flight_size);
        assert_eq!(2, blocks.len());
        assert_eq!(2, blocks[0].block_id);
        assert_eq!(3, blocks[1].block_id);
    }

    #[test]
    fn test_linked_hashmap() {
        let mut map = LinkedHashMap::new();
        map.insert(1, 1);
        map.insert(2, 2);
        map.insert(3, 3);

        assert_eq!(1, *map.front().unwrap().0);
        assert_eq!(3, *map.back().unwrap().0);
    }

    #[test]
    fn lifetime_with_lock_test() {
        struct MyStruct {
            vec: Vec<i64>,
        }

        struct LockedStruct {
            lock: RwLock<i64>,
            inner: MyStruct
        }

        impl LockedStruct {
            fn get_ref(&self) -> &Vec<i64> {
                let _guarder = self.lock.read().unwrap();
                &self.inner.vec
            }
        }

        let data = LockedStruct {
            lock: Default::default(),
            inner: MyStruct { vec: vec![1, 2, 3] },
        };
        assert_eq!(3, data.get_ref().len());
    }

    #[test]
    fn single_lifetime_test() {
        struct MyStruct {
            vec: Vec<i32>,
        }

        impl MyStruct {
            fn get_element<'a>(&'a self, index: usize) -> Option<&'a i32> {
                self.vec.get(index)
            }
        }

        let my_struct = MyStruct {
            vec: vec![1, 2, 3, 4, 5],
        };

        let locked = RwLock::new(my_struct);
        let guarder = locked.read().unwrap();
        if let Some(val) = guarder.get_element(2) {
            println!("The value is: {}", val);
        } else {
            println!("No value found at that index");
        }
    }
}
