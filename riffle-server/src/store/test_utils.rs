use crate::store::Block;

/// Creates test blocks with specified parameters  
pub fn create_blocks(start_block_idx: i32, cnt: i32, block_len: i32) -> Vec<Block> {
    let mut blocks = vec![];
    for idx in 0..cnt {
        blocks.push(Block {
            block_id: (start_block_idx + idx) as i64,
            length: block_len,
            uncompress_length: 0,
            crc: 0,
            data: Default::default(),
            task_attempt_id: idx as i64,
        });
    }
    blocks
}

/// create test block with specified parameters
fn create_block(block_len: i32, block_id: i64) -> Block {
    Block {
        block_id,
        length: block_len,
        uncompress_length: 0,
        crc: 0,
        data: Default::default(),
        task_attempt_id: 0,
    }
}
