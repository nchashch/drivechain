use bitcoin::hash_types::BlockHash;
use bitcoin::hashes::Hash;

#[derive(Debug)]
pub struct CoinbaseData {
    pub prev_main_block_hash: BlockHash,
    pub prev_side_block_hash: BlockHash,
}

impl CoinbaseData {
    pub fn serialize(&self) -> Vec<u8> {
        let main_hash = self.prev_main_block_hash.to_vec();
        let side_hash = self.prev_side_block_hash.to_vec();
        [main_hash, side_hash].concat()
    }

    pub fn deserialize(bytes: &[u8]) -> Option<CoinbaseData> {
        if bytes.len() != 64 {
            return None;
        }
        let prev_main_block_hash = BlockHash::from_slice(&bytes[0..32]).unwrap();
        let prev_side_block_hash = BlockHash::from_slice(&bytes[32..64]).unwrap();
        Some(CoinbaseData {
            prev_main_block_hash,
            prev_side_block_hash,
        })
    }
}