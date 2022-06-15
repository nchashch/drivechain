use bitcoin::hash_types::BlockHash;
use bitcoin::hashes::Hash;

#[derive(Debug)]
pub struct PrevBlockHashes {
    pub prev_main_block_hash: BlockHash,
    pub prev_side_block_hash: BlockHash,
}

impl PrevBlockHashes {
    pub fn serialize(&self) -> Vec<u8> {
        let main_hash = self.prev_main_block_hash.to_vec();
        let side_hash = self.prev_side_block_hash.to_vec();
        [main_hash, side_hash].concat()
    }

    pub fn deserialize(bytes: &[u8]) -> Option<PrevBlockHashes> {
        if bytes.len() != 64 {
            return None;
        }
        let prev_main_block_hash = BlockHash::from_slice(&bytes[0..32]).ok()?;
        let prev_side_block_hash = BlockHash::from_slice(&bytes[32..64]).ok()?;
        Some(PrevBlockHashes {
            prev_main_block_hash,
            prev_side_block_hash,
        })
    }
}
