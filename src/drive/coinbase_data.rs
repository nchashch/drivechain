use bitcoin::hash_types::BlockHash;
use bitcoin::hashes::Hash;
use serde::de::Error;

#[derive(Debug)]
pub struct CoinbaseData {
    pub prev_main_block_hash: BlockHash,
    pub deposits: Option<(usize, usize)>,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct SerdeCoinbaseData {
    prev_main_block_hash: [u8; 32],
    deposits: Option<(u32, u32)>,
}

impl serde::Serialize for CoinbaseData {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let serialize_coinbase_data = SerdeCoinbaseData {
            prev_main_block_hash: *self.prev_main_block_hash.as_inner(),
            deposits: self.deposits.map(|(fst, snd)| (fst as u32, snd as u32)),
        };
        serialize_coinbase_data.serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for CoinbaseData {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        match SerdeCoinbaseData::deserialize(deserializer) {
            Ok(scd) => {
                let coinbase_data = CoinbaseData {
                    prev_main_block_hash: BlockHash::from_inner(scd.prev_main_block_hash),
                    deposits: scd.deposits.map(|(fst, snd)| (fst as usize, snd as usize)),
                };
                Ok(coinbase_data)
            }
            Err(err) => Err(D::Error::custom(err)),
        }
    }
}
