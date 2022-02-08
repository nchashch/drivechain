use crate::lib::client::{DrivechainClient};
use bitcoin::util::uint::Uint256;
use std::collections::{HashMap};
use bitcoin::blockdata::transaction::{OutPoint};
use bitcoin::hash_types::{Txid, BlockHash};
use bitcoin::util::amount::{Amount};
use std;
use bitcoin::hashes::{sha256, Hash};
use crate::lib::db;
use crate::lib::deposit::{Deposit};

#[derive(Debug)]
pub struct Block {
    pub data: Vec<u8>,
    pub time: i64,
    pub main_block_hash: BlockHash,
}

pub struct Drivechain {
    pub client: DrivechainClient,
    bmm: BMM,
    pub db: db::DB,
}

#[derive(Debug)]
struct BMM {
    requests: Vec<BMMRequest>,
    prev_main_block_hash: BlockHash,
}

#[derive(Debug)]
struct BMMRequest {
    txid: Txid,
    critical_hash: Uint256,
    side_block_data: Vec<u8>,
}


impl Drivechain {
    pub fn new<P: AsRef<std::path::Path>>(path: P, this_sidechain: usize, key_hash: String, rpcuser: String, rpcpassword: String) -> Result<Drivechain, db::Error> {
        let client = DrivechainClient{
            this_sidechain: this_sidechain,
            key_hash: key_hash,
            host: "127.0.0.1".into(),
            port: 18443,
            rpcuser: rpcuser,
            rpcpassword: rpcpassword,
        };

        let drive = Drivechain{
            client: client,
            bmm: BMM{
                requests: Vec::new(),
                prev_main_block_hash: BlockHash::default(),
            },
            db: db::DB::new(path)?,
        };
        Ok(drive)
    }

    // Attempts to blind merge mine a block.
    pub fn attempt_bmm(&mut self, critical_hash: &Uint256, block_data: &Vec<u8>, amount: Amount) -> Option<Block> {
        let mainchain_tip_hash = self.client.get_mainchain_tip().unwrap();
        // Check if any bmm request was accepted.
        if let Some(block) = self.confirm_bmm(&mainchain_tip_hash) {
            // Return block data if it was.
            return Some(block);
        }
        // If there are no accepted bmm requests create another one
        let txid = self.client.send_bmm_request(critical_hash, &mainchain_tip_hash, 0, amount);
        let bmm_request = BMMRequest{
            txid: txid,
            critical_hash: *critical_hash,
            side_block_data: block_data.to_vec(),
        };
        // and add request data to the requests vec.
        self.bmm.requests.push(bmm_request);
        None
    }

    // Check if any bmm request was accepted.
    fn confirm_bmm(&mut self, mainchain_tip_hash: &BlockHash) -> Option<Block> {
        if self.bmm.prev_main_block_hash == *mainchain_tip_hash {
            // If no blocks were mined on mainchain no bmm requests could have
            // possibly been accepted.
            return None;
        }
        // Mainchain tip has changed so all requests for previous tip are now
        // invalid hence we update our prev_main_block_hash
        self.bmm.prev_main_block_hash = *mainchain_tip_hash;
        // and delete all requests with drain method.
        for request in self.bmm.requests.drain(..) {
            // We check if our request was included in a block so the mainchain
            // miner was paid the bribe.
            if let Some(main_block_hash) = self.client.get_tx_block_hash(&request.txid) {
                // And we check that critical_hash was actually included in
                // coinbase.
                if let Ok(verified) = self.client.verify_bmm(&main_block_hash, &request.critical_hash) {
                    // If we find a bmm request that was accepted we return the
                    // corresponding block data.
                    let block = Block {
                        data: request.side_block_data,
                        time: verified.time,
                        main_block_hash: main_block_hash,
                    };
                    return Some(block);
                }
            }
        }
        None
    }

    pub fn format_deposit_address(&self, str_dest: &String) -> String {
        let deposit_address: String = format!("s{}_{}_", self.client.this_sidechain, str_dest);
        let hash = sha256::Hash::hash(deposit_address.as_bytes()).to_string();
        let hash: String = hash[..6].into();
        format!("{}{}", deposit_address, hash)
    }

    pub fn get_deposits(&self) -> Result<Vec<Deposit>, db::Error> {
        let last_deposit = self.db.get_last_deposit()?;
        dbg!(last_deposit);
        let deposits = self.client.get_deposits(last_deposit);
        if deposits.len() == 0 {
            return Ok(vec![]);
        }
        let mut outpoint_to_deposit = HashMap::<OutPoint, Deposit>::new();
        let mut spent_by = HashMap::<OutPoint, OutPoint>::new();
        for deposit in &deposits {
            outpoint_to_deposit.insert(deposit.outpoint(), deposit.clone());
        }
        let mut first_outpoint: Option<OutPoint> = None;
        for deposit in &deposits {
            let input_txids = deposit.tx.input
                                        .iter()
                                        .filter(|input| {
                                            outpoint_to_deposit.contains_key(&input.previous_output)
                                        })
                                        .map(|input| {
                                            input.previous_output
                                        })
                                        .collect::<Vec<OutPoint>>();
            if input_txids.len() > 1 {
                panic!("Invalid deposit transaction - input spends more than one previous CTIP");
            } else if input_txids.len() == 0 {
                first_outpoint = Some(deposit.outpoint());
                continue;
            }
            let input_txid = input_txids[0];
            spent_by.insert(input_txid, deposit.outpoint());

            if let Some(dep) = outpoint_to_deposit.get_mut(&deposit.outpoint()) {
                dep.prev_txid = Some(input_txid.txid);
            }
        }
        let mut sorted_deposits = Vec::<Deposit>::new();
        // Get first deposit.
        if let Some(first_outpoint) = first_outpoint {
            sorted_deposits.push(outpoint_to_deposit[&first_outpoint].clone());
        }
        let first_deposit = &sorted_deposits[0];
        let mut deposit_outpoint = first_deposit.outpoint();
        while let Some(next) = spent_by.get(&deposit_outpoint) {
            if let Some(dep) = outpoint_to_deposit.get(next) {
                sorted_deposits.push(dep.clone());
                deposit_outpoint = dep.outpoint();
            }
        }
        if let Some(last_deposit) = sorted_deposits.last() {
            dbg!(last_deposit.tx.txid());
            self.db.last_deposit.set("last_deposit", last_deposit.tx.txid().as_inner())?;
        }
        self.db.update(sorted_deposits.as_slice())?;
        Ok(sorted_deposits)
    }
}

#[derive(Debug, Clone)]
pub struct SidechainOutput {
    amount: Amount,
    deposit: Deposit,
}
