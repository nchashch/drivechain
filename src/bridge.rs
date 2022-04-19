use crate::drive;
use bitcoin::hash_types::{BlockHash, TxMerkleNode};
use bitcoin::hashes::hex::ToHex;
use bitcoin::util::amount::Amount;
use std::collections::HashMap;
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
        address: String,
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

        fn get_coinbase_data(&self, prev_side_block_hash: &str) -> Vec<u8>;
        fn confirm_bmm(&mut self) -> Vec<Block>;
        fn attempt_bmm(&mut self, critical_hash: &str, block_data: &str, amount: u64);

        fn connect_deposit_outputs(&mut self, outputs: Vec<Output>, just_check: bool) -> bool;
        fn verify_header_bmm(&self, main_block_hash: &str, critical_hash: &str) -> bool;
        fn verify_block_bmm(
            &self,
            main_block_hash: &str,
            critical_hash: &str,
            coinbase_data: &str,
        ) -> bool;
        fn get_deposit_outputs(&self) -> Vec<Output>;
        fn format_deposit_address(&self, address: &str) -> String;
    }
}

pub struct Drivechain(drive::Drivechain);

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
    fn get_coinbase_data(&self, prev_side_block_hash: &str) -> Vec<u8> {
        let prev_side_block_hash = BlockHash::from_str(prev_side_block_hash).unwrap();
        let coinbase_data = self.0.get_coinbase_data(prev_side_block_hash);
        let coinbase_data = coinbase_data.serialize();
        coinbase_data
    }

    fn confirm_bmm(&mut self) -> Vec<ffi::Block> {
        let block = self.0.confirm_bmm();
        block
            .map(|block| ffi::Block {
                data: hex::encode(block.data),
                time: block.time,
                main_block_hash: block.main_block_hash.to_hex(),
            })
            .into_iter()
            .collect()
    }

    fn attempt_bmm(&mut self, critical_hash: &str, block_data: &str, amount: u64) {
        let critical_hash = TxMerkleNode::from_str(critical_hash).unwrap();
        let block_data = hex::decode(block_data).unwrap();
        let amount = bitcoin::Amount::from_sat(amount);
        self.0.attempt_bmm(&critical_hash, &block_data, amount);
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
        let coinbase_data = drive::CoinbaseData::deserialize(&coinbase_data).unwrap();

        if let Some(prev_main_block_hash) = self.0.client.get_prev_block_hash(&main_block_hash) {
            if prev_main_block_hash != coinbase_data.prev_main_block_hash {
                return false;
            }
        } else {
            return false;
        }
        true
    }

    fn get_deposit_outputs(&self) -> Vec<ffi::Output> {
        self.0.update_deposits();
        self.0
            .db
            .get_deposit_outputs()
            .iter()
            .map(|output| ffi::Output {
                address: output.address.clone(),
                amount: output.amount,
            })
            .collect()
    }

    fn connect_deposit_outputs(&mut self, outputs: Vec<ffi::Output>, just_check: bool) -> bool {
        let outputs = outputs.iter().map(|output| drive::deposit::Output {
            address: output.address.clone(),
            amount: output.amount,
        });
        self.0.db.connect_side_outputs(outputs, just_check)
    }

    fn format_deposit_address(&self, address: &str) -> String {
        self.0.format_deposit_address(address)
    }
}

fn aggregate_outputs<'a>(
    outputs_vec: impl Iterator<Item = &'a ffi::Output>,
) -> HashMap<String, Amount> {
    let mut outputs = HashMap::<String, Amount>::new();
    for output in outputs_vec {
        let amount = outputs
            .entry(output.address.clone())
            .or_insert(Amount::ZERO);
        *amount += Amount::from_sat(output.amount);
    }
    outputs
}