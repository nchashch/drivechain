use crate::drive;
use bitcoin::util::uint::{Uint256};
use bitcoin::hashes::hex::ToHex;

#[cxx::bridge]
mod ffi {
    struct Block {
        data: String,
        time: i64,
        main_block_hash: String,
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

        fn attempt_bmm(
            &mut self,
            critical_hash: &str,
            block_data: &str,
            amount: u64,
        ) -> Vec<Block>;
        fn test();
    }
}

fn test() {
    println!("Hello, world!");
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
    println!("new drivechain");
    let drivechain = drive::Drivechain::new(db_path, this_sidechain, key_hash.into(), rpcuser.into(), rpcpassword.into());
    Box::new(Drivechain(drivechain))
}

impl Drivechain {
    fn attempt_bmm(
        &mut self,
        critical_hash: &str,
        block_data: &str,
        amount: u64,
    ) -> Vec<ffi::Block> {
        dbg!("attempt bmm");
        let critical_hash = hex::decode(critical_hash).unwrap();
        let critical_hash = Uint256::from_be_slice(critical_hash.as_slice()).unwrap();
        let block_data = hex::decode(block_data).unwrap();
        let amount = bitcoin::Amount::from_sat(amount);
        let block = self.0.attempt_bmm(&critical_hash, &block_data, amount);
        dbg!(&block);
        block.map(|block| ffi::Block{
            data: hex::encode(block.data),
            time: block.time,
            main_block_hash: block.main_block_hash.to_hex(),
        }).into_iter().collect()
    }
}
