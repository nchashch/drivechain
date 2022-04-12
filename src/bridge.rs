use crate::drive;
use bitcoin::hash_types::{BlockHash, TxMerkleNode};
use bitcoin::hashes::hex::ToHex;
use std::str::FromStr;

#[cxx::bridge]
mod ffi {
    #[derive(Debug)]
    struct Block {
        data: String,
        time: i64,
        main_block_hash: String,
    }
    #[derive(Debug)]
    struct Output {
        destination: String,
        amount: u64,
    }
    extern "Rust" {
        type Drivechain;
        fn new_drivechain(
            db_path: &str,
            this_sidechain: usize,
            key_hash: &str,
            rpcuser: &str,
            rpcpassword: &str,
        ) -> Box<Drivechain>;

        fn get_coinbase_data(&self) -> Vec<u8>;
        fn attempt_bmm(&mut self, critical_hash: &str, block_data: &str, amount: u64)
            -> Vec<Block>;
        fn verify_header_bmm(&self, main_block_hash: &str, critical_hash: &str) -> bool;
        fn verify_block_bmm(
            &self,
            main_block_hash: &str,
            critical_hash: &str,
            coinbase_data: &str,
        ) -> bool;
    }
}

pub struct Drivechain(drive::Drivechain);

impl Drop for Drivechain {
    fn drop(&mut self) {
        println!("drivechain dropped");
    }
}

fn new_drivechain(
    db_path: &str,
    this_sidechain: usize,
    key_hash: &str,
    rpcuser: &str,
    rpcpassword: &str,
) -> Box<Drivechain> {
    let drivechain = drive::Drivechain::new(
        db_path,
        this_sidechain,
        key_hash.into(),
        rpcuser.into(),
        rpcpassword.into(),
    );
    Box::new(Drivechain(drivechain))
}

impl Drivechain {
    fn get_coinbase_data(&self) -> Vec<u8> {
        let coinbase_data = self.0.get_coinbase_data();
        let coinbase_data = bincode::serialize(&coinbase_data).unwrap();
        dbg!(coinbase_data.len());
        dbg!(hex::encode(&coinbase_data));
        coinbase_data
    }

    fn attempt_bmm(
        &mut self,
        critical_hash: &str,
        block_data: &str,
        amount: u64,
    ) -> Vec<ffi::Block> {
        dbg!("attempt bmm");
        let critical_hash = TxMerkleNode::from_str(critical_hash).unwrap();
        let block_data = hex::decode(block_data).unwrap();
        let amount = bitcoin::Amount::from_sat(amount);
        let block = self.0.attempt_bmm(&critical_hash, &block_data, amount);
        block
            .map(|block| ffi::Block {
                data: hex::encode(block.data),
                time: block.time,
                main_block_hash: block.main_block_hash.to_hex(),
            })
            .into_iter()
            .collect()
    }

    fn verify_header_bmm(&self, main_block_hash: &str, critical_hash: &str) -> bool {
        let main_block_hash = BlockHash::from_str(main_block_hash).unwrap();
        let critical_hash = TxMerkleNode::from_str(critical_hash).unwrap();
        self.0
            .client
            .verify_bmm(&main_block_hash, &critical_hash)
            .is_ok()
    }

    fn verify_block_bmm(
        &self,
        main_block_hash: &str,
        critical_hash: &str,
        coinbase_data: &str,
    ) -> bool {
        if !self.verify_header_bmm(main_block_hash, critical_hash) {
            return false;
        }
        let main_block_hash = BlockHash::from_str(main_block_hash).unwrap();
        let critical_hash = TxMerkleNode::from_str(critical_hash).unwrap();
        let coinbase_data = hex::decode(coinbase_data).unwrap();
        let coinbase_data =
            bincode::deserialize::<drive::CoinbaseData>(coinbase_data.as_slice()).unwrap();

        if let Some(prev_main_block_hash) = self.0.client.get_prev_block_hash(&main_block_hash) {
            if prev_main_block_hash != coinbase_data.prev_main_block_hash {
                return false;
            }
        } else {
            return false;
        }

        dbg!(&main_block_hash, &critical_hash, &coinbase_data);
        true
    }
}
