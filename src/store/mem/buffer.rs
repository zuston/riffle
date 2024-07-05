use std::collections::{HashMap, LinkedList};
use std::hash::Hash;
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

    // segment locks
    buffers: Vec<std::sync::RwLock<BufferInternal>>,
    partitions: u32,
}

#[derive(Debug)]
pub struct BufferInternal {
    total_size: i64,
    staging_size: i64,
    flight_size: i64,

    staging_block_num: i64,
    flight_block_num: i64,

    blocks: LinkedHashMap<i64, Arc<PartitionedDataBlock>>,
    // the boundary blockId to distinguish the staging and flush blocks
    boundary_block_id: i64,

    staging: Wrapper<LinkedList<Vec<PartitionedDataBlock>>>,
    flight: HashMap<u64, Arc<LinkedList<Vec<PartitionedDataBlock>>>>,
    flight_inc: u64,
}

impl BufferInternal {
    fn new() -> Self {
        let mut staging_wrapper = Wrapper::new();
        staging_wrapper.wrap(LinkedList::new());
        BufferInternal {
            total_size: 0,
            staging_size: 0,
            flight_size: 0,
            staging_block_num: 0,
            flight_block_num: 0,
            blocks: Default::default(),
            boundary_block_id: 0,
            staging: staging_wrapper,
            flight: Default::default(),
            flight_inc: 0,
        }
    }
}

impl MemoryBuffer {
    pub fn new() -> MemoryBuffer {
        let partition_num = 1;
        let mut buffers = vec![];
        for _ in 0..partition_num {
            buffers.push(
                std::sync::RwLock::new(BufferInternal::new())
            );
        }
        MemoryBuffer {
            buffer: RwLock::new(BufferInternal::new()),
            buffers,
            partitions: partition_num,
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

    pub fn clear_flight_v2(&self, flight_id: u64) {
        let buffer = &mut self.buffer.write();
        let flight = &mut buffer.flight;
        flight.remove(&flight_id);
    }

    pub fn create_flight_v2(&self) -> Result<(ExecutionTime, i64, Arc<LinkedList<Vec<PartitionedDataBlock>>>, u64)> {
        let buffer = &mut self.buffer.write();
        let timer = Instant::now();
        let list = buffer.staging.unwrap();
        buffer.staging.wrap(LinkedList::new());

        let size = buffer.staging_size;
        buffer.staging_size = 0;
        buffer.flight_size += size;
        let flight_id = buffer.flight_inc;
        buffer.flight_inc += 1;

        let arc_staging = Arc::new(list);
        let flight = &mut buffer.flight;
        flight.insert(flight_id, arc_staging.clone());

        Ok(
            (ExecutionTime::BUFFER_CREATE_FLIGHT(0, 0, timer.elapsed().as_millis()), size, arc_staging.clone(), flight_id)
        )
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
            let pick_timer = Instant::now();
            if started_tag || last_boundary_block_id == -1 {
                flight_len += v.length;
                flight_blocks_ref.push((*v).clone());
                block_cnt += 1;
            }
            pick_time += pick_timer.elapsed().as_millis();

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

    fn hash(&self, task_attempt_id: i64) -> usize {
        (task_attempt_id % self.partitions as i64) as usize
    }

    pub fn add(&self, blocks: Vec<PartitionedDataBlock>) -> Result<i64> {
        let hash_idx = self.hash(blocks.get(0).unwrap().task_attempt_id);
        let buffer= self.buffers.get(hash_idx).unwrap();
        let mut buffer = buffer.write().unwrap();

        let staging = &mut buffer.staging;

        let mut block_cnt = 0i64;
        let mut add_size = 0;
        for block in &blocks {
            let id = block.block_id;
            let len = block.length;
            add_size += len as i64;
            block_cnt += 1;
        }
        staging.get_mut().push_front(blocks);

        buffer.staging_size += add_size;
        buffer.total_size += add_size;
        buffer.staging_block_num += block_cnt;

        Ok(add_size)
    }
}

#[derive(Debug)]
struct Wrapper<T> {
    obj: Vec<T>
}

impl<T> Wrapper<T> {
    fn new() -> Wrapper<T> {
        Wrapper {
            obj: vec![],
        }
    }

    fn get_mut(&mut self) -> &mut T {
        self.obj.get_mut(0).unwrap()
    }

    fn unwrap(&mut self) -> T {
        self.obj.remove(0)
    }

    fn wrap(&mut self, t: T) {
        self.obj.push(t)
    }
}

#[derive(Debug)]
pub enum FLIGHT_ID {
    ID(u64),
}

#[cfg(test)]
mod test {
    use std::collections::LinkedList;
    use crate::store::mem::buffer::{MemoryBuffer, Wrapper};
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

    #[test]
    fn test_linkedlist() {
        let mut list: Vec<LinkedList<i32>> = vec![LinkedList::new()];
        let mut a= list.get_mut(0).unwrap();
        a.push_front(1);

        let data = list.remove(0);
        list.push(LinkedList::new());
    }

    #[test]
    fn test_wrap() {
        let mut wrapper = Wrapper::new();
        let mut list: LinkedList<i64> = LinkedList::new();
        list.push_front(1i64);
        wrapper.wrap(list);

        let list = wrapper.get_mut();
        list.push_front(2);
        assert_eq!(2, list.len());

        let list = wrapper.unwrap();
        assert_eq!(2, list.len());

        let mut list = LinkedList::new();
        list.push_front(10);
        wrapper.wrap(list);
    }

    #[test]
    fn test_cache_line() {

    }

    #[test]
    fn test_hash() {
        let id = 10;
        assert_eq!(1, id % 9);
        assert_eq!(2, id % 8);
        assert_eq!(10, id % 20);
    }
}
