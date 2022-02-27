mod client;
mod db;
mod deposit;
pub mod withdrawal;
use bitcoin::blockdata::{
    opcodes, script,
    transaction::{Transaction, TxIn, TxOut},
};
use bitcoin::hash_types::{BlockHash, Txid};
use bitcoin::hashes::{sha256, Hash};
use bitcoin::util::address::Address;
use bitcoin::util::amount::Amount;
use bitcoin::util::uint::Uint256;
use client::DrivechainClient;
pub use deposit::{Deposit, DepositOutput};
use std;
use std::collections::HashMap;
use std::str::FromStr;
pub use withdrawal::WithdrawalOutput;

#[derive(Debug)]
pub struct Block {
    pub data: Vec<u8>,
    pub time: i64,
    pub main_block_hash: BlockHash,
}

pub struct Drivechain {
    pub client: DrivechainClient,
    bmm_cache: BMMCache,
    pub db: db::DB,
}

// The destination string for the change of a WT^
const SIDECHAIN_WTPRIME_RETURN_DEST: &[u8] = b"D";
const MIN_WT_COUNT: usize = 10;
const MAX_WT_COUNT: usize = 100;
const WT_MIN_BLOCK_COUNT: usize = 10;

impl Drivechain {
    pub fn new<P: AsRef<std::path::Path>>(
        db_path: P,
        this_sidechain: usize,
        key_hash: String,
        rpcuser: String,
        rpcpassword: String,
    ) -> Drivechain {
        const LOCALHOST: &str = "127.0.0.1";
        const MAINCHAIN_PORT: usize = 18443;

        let client = DrivechainClient {
            this_sidechain: this_sidechain,
            key_hash: key_hash,
            host: LOCALHOST.into(),
            port: MAINCHAIN_PORT,
            rpcuser: rpcuser,
            rpcpassword: rpcpassword,
        };

        Drivechain {
            client: client,
            bmm_cache: BMMCache::new(),
            db: db::DB::new(db_path),
        }
    }

    // Attempts to blind merge mine a block.
    pub fn attempt_bmm(
        &mut self,
        critical_hash: &Uint256,
        block_data: &Vec<u8>,
        amount: Amount,
    ) -> Option<Block> {
        let mainchain_tip_hash = self
            .client
            .get_mainchain_tip()
            .expect("failed to get mainchain tip");
        // Check if any bmm request was accepted.
        if let Some(block) = self.confirm_bmm(&mainchain_tip_hash) {
            // Return block data if it was.
            return Some(block);
        }
        // If there are no accepted bmm requests create another one
        let txid = self
            .client
            .send_bmm_request(critical_hash, &mainchain_tip_hash, 0, amount);
        let bmm_request = BMMRequest {
            txid: txid,
            critical_hash: *critical_hash,
            side_block_data: block_data.to_vec(),
        };
        // and add request data to the requests vec.
        self.bmm_cache.requests.push(bmm_request);
        None
    }

    // Check if any bmm request was accepted.
    fn confirm_bmm(&mut self, mainchain_tip_hash: &BlockHash) -> Option<Block> {
        if self.bmm_cache.prev_main_block_hash == *mainchain_tip_hash {
            // If no blocks were mined on mainchain no bmm requests could have
            // possibly been accepted.
            return None;
        }
        // Mainchain tip has changed so all requests for previous tip are now
        // invalid hence we update our prev_main_block_hash
        self.bmm_cache.prev_main_block_hash = *mainchain_tip_hash;
        // and delete all requests with drain method.
        for request in self.bmm_cache.requests.drain(..) {
            // We check if our request was included in a mainchain block.
            if let Some(main_block_hash) = self.client.get_tx_block_hash(&request.txid) {
                // And we check that critical_hash was actually included in
                // coinbase.
                if let Ok(verified) = self
                    .client
                    .verify_bmm(&main_block_hash, &request.critical_hash)
                {
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

    pub fn update_deposits(&self) {
        let mut last_deposit = self
            .db
            .get_last_deposit()
            .map(|(_, last_deposit)| last_deposit);
        while !last_deposit.clone().map_or(true, |last_deposit| {
            self.client.verify_deposit(&last_deposit)
        }) {
            self.db.remove_last_deposit();
            last_deposit = self
                .db
                .get_last_deposit()
                .map(|(_, last_deposit)| last_deposit);
        }
        let last_output = last_deposit.map(|deposit| (deposit.tx.txid(), deposit.nburnindex));
        let deposits = self.client.get_deposits(last_output);
        self.db.update_deposits(deposits.as_slice());
    }

    pub fn validate_deposits(&self, start_index: usize) -> bool {
        self.db
            .deposits_since(start_index)
            .iter()
            .all(|(_, deposit)| self.client.verify_deposit(&deposit))
    }

    pub fn deposit_output(&self, index: usize) -> Option<DepositOutput> {
        let deposit = self.db.get_deposit(index);
        let prev_amount = match index {
            0 => Some(Amount::from_sat(0)),
            _ => self.db.get_deposit(index - 1).map(|d| d.amount()),
        };
        match (deposit, prev_amount) {
            (Some(deposit), Some(prev_amount)) => Some(DepositOutput {
                index: index,
                address: deposit.strdest.clone(),
                amount: deposit.amount() - prev_amount,
            }),
            _ => None,
        }
    }

    pub fn deposit_outputs(&self, last_index: usize) -> Vec<DepositOutput> {
        let mut deposit_outputs = vec![];
        for item in self.db.deposits_since(last_index) {
            let (index, deposit) = item.clone();
            let prev_deposit = match index {
                0 => None,
                _ => self.db.get_deposit(index - 1),
            };
            let prev_amount = prev_deposit
                .as_ref()
                .map_or(Amount::from_sat(0), |dep| dep.amount());
            if deposit.amount() < prev_amount {
                continue;
            }
            let deposit_output = DepositOutput {
                index: index,
                address: deposit.strdest.clone(),
                amount: deposit.amount() - prev_amount,
            };
            deposit_outputs.push(deposit_output);
        }
        deposit_outputs
    }

    pub fn create_wt_bundle(&self) -> Option<Transaction> {
        // Add SIDECHAIN_WTPRIME_RETURN_DEST OP_RETURN output
        let script = script::Builder::new()
            .push_opcode(opcodes::all::OP_RETURN)
            .push_slice(SIDECHAIN_WTPRIME_RETURN_DEST)
            .into_script();
        let txout0 = TxOut {
            value: 0,
            script_pubkey: script,
        };
        let withdrawals = self.db.get_unspent_withdrawals();
        if withdrawals.len() == 0 {
            return None;
        }
        if withdrawals.len() < MIN_WT_COUNT {
            return None;
        }
        let sum_mainchain_fees: u64 = withdrawals
            .iter()
            .map(|(_, wt)| wt.iter().map(|out| out.mainchain_fee))
            .flatten()
            .sum();
        // Add an output for mainchain fee encoding (updated later)
        let script = script::Builder::new()
            .push_opcode(opcodes::all::OP_RETURN)
            .push_slice(sum_mainchain_fees.to_le_bytes().as_slice())
            .into_script();
        let txout1 = TxOut {
            value: 0,
            script_pubkey: script,
        };
        let mut dest_to_outputs: HashMap<String, (u64, Vec<[u8; 32]>)> = HashMap::new();
        for (wtid, outs) in withdrawals.iter() {
            for out in outs {
                let (value, wtids) = dest_to_outputs
                    .entry(out.dest.clone())
                    .or_insert((0, vec![]));
                *value += out.amount;
                wtids.push(wtid.clone());
            }
        }
        let mut dest_to_outputs = dest_to_outputs
            .into_iter()
            .collect::<Vec<(String, (u64, Vec<[u8; 32]>))>>();
        // We sort all wtid vectors and all outputs lexicographicaliy to make
        // sure is the same for the same set of withdrawals.
        dest_to_outputs
            .iter_mut()
            .for_each(|(_, (_, wtids))| wtids.sort());
        dest_to_outputs.sort_by(|(a, _), (b, _)| a.partial_cmp(b).unwrap());
        let outputs = dest_to_outputs
            .iter()
            .map(|(dest, (value, wtids))| {
                [
                    vec![TxOut {
                        value: *value,
                        script_pubkey: Address::from_str(dest.as_str()).unwrap().script_pubkey(),
                    }],
                    wtids
                        .iter()
                        .map(|wtid| TxOut {
                            value: 0,
                            script_pubkey: script::Builder::new()
                                .push_opcode(opcodes::all::OP_RETURN)
                                .push_slice(wtid)
                                .into_script(),
                        })
                        .collect(),
                ]
                .concat()
            })
            .flatten()
            .collect();
        let mut txin = TxIn::default();
        // OP_FALSE == OP_0
        txin.script_sig = script::Builder::new()
            .push_opcode(opcodes::OP_FALSE)
            .into_script();
        Some(Transaction {
            version: 2,
            lock_time: 0,
            input: vec![txin],
            output: [vec![txout0, txout1], outputs].concat(),
        })
    }

    pub fn add_withdrawal(
        &mut self,
        wtid: [u8; 32],
        withdrawal: &Vec<WithdrawalOutput>,
    ) -> Result<(), withdrawal::Error> {
        for out in withdrawal {
            Address::from_str(out.dest.as_str())?;
        }
        // Address::from_str(withdrawal.refund_dest.as_str())?;
        self.db.add_withdrawal(wtid, withdrawal)?;
        Ok(())
    }

    pub fn connect_block(&mut self, block: &BlockData) {

    }
}

pub struct BlockData {
    block_number: usize,
    withdrawals: HashMap<[u8;32], Vec<WithdrawalOutput>>,
}

#[derive(Debug)]
pub struct BMMCache {
    requests: Vec<BMMRequest>,
    prev_main_block_hash: BlockHash,
}

impl BMMCache {
    fn new() -> BMMCache {
        BMMCache {
            requests: Vec::new(),
            prev_main_block_hash: BlockHash::default(),
        }
    }
}

#[derive(Debug)]
struct BMMRequest {
    txid: Txid,
    critical_hash: Uint256,
    side_block_data: Vec<u8>,
}
